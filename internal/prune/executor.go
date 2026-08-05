package prune

import (
	"context"
	"errors"
	"fmt"
	"path"
	"sync"
	"time"

	"github.com/labring-sigs/kbbackup-prune/internal/domain"
	"github.com/labring-sigs/kbbackup-prune/internal/ports"
)

type ExecuteOptions struct {
	DryRun        bool
	Concurrency   int
	PurgeVersions bool
}

type Executor struct {
	Kube  ports.Kubernetes
	Store ports.ObjectStore
}

func (e Executor) Run(
	ctx context.Context,
	plan domain.Plan,
	opts ExecuteOptions,
) (domain.Execution, error) {
	if e.Kube == nil || e.Store == nil {
		return domain.Execution{}, errors.New("kubernetes and object store clients are required")
	}

	if opts.Concurrency < 1 {
		opts.Concurrency = 1
	}

	if err := validateExecutionPlan(plan, opts); err != nil {
		return domain.Execution{}, err
	}

	result := domain.Execution{DryRun: opts.DryRun}

	var candidates []domain.Candidate
	for _, candidate := range plan.Candidates {
		if candidate.State == domain.StateOrphan {
			candidates = append(candidates, candidate)
		}
	}

	if opts.DryRun {
		for _, candidate := range candidates {
			result.Results = append(result.Results, domain.DeleteResult{
				Prefix:         candidate.Prefix,
				ObjectsDeleted: candidate.ObjectCount,
				BytesDeleted:   candidate.Bytes,
			})
		}

		return result, nil
	}

	if err := e.revalidateInventory(ctx, plan); err != nil {
		return result, err
	}

	if plan.VersioningSource != domain.BucketVersioningSourceOverride {
		currentVersioning, err := e.Store.Versioning(ctx)
		if err != nil {
			return result, fmt.Errorf("refresh bucket versioning: %w", err)
		}

		if currentVersioning != plan.Versioning {
			return result, fmt.Errorf(
				"bucket versioning changed after planning from %q to %q",
				plan.Versioning,
				currentVersioning,
			)
		}
	}

	strayCandidates := make([]domain.Candidate, 0)

	otherCandidates := make([]domain.Candidate, 0, len(candidates))
	for _, candidate := range candidates {
		if normalizedCandidateKind(candidate.Kind) == domain.CandidateRepositoryStray {
			strayCandidates = append(strayCandidates, candidate)
		} else {
			otherCandidates = append(otherCandidates, candidate)
		}
	}

	var failed int

	validator := candidateSafetyValidator{executor: e, plan: plan}
	for _, group := range [][]domain.Candidate{strayCandidates, otherCandidates} {
		for _, deleteResult := range e.runCandidateGroup(ctx, group, opts, &validator) {
			if deleteResult.Error != "" {
				failed++
			}

			result.Results = append(result.Results, deleteResult)
		}
	}

	if err := ctx.Err(); err != nil {
		return result, err
	}

	if failed > 0 {
		return result, fmt.Errorf("%d cleanup candidates failed deletion", failed)
	}

	return result, nil
}

func (e Executor) runCandidateGroup(
	ctx context.Context,
	candidates []domain.Candidate,
	opts ExecuteOptions,
	validator *candidateSafetyValidator,
) []domain.DeleteResult {
	if len(candidates) == 0 {
		return nil
	}

	jobs := make(chan domain.Candidate)
	results := make(chan domain.DeleteResult, len(candidates))

	var workers sync.WaitGroup
	for range opts.Concurrency {
		workers.Go(func() {
			for candidate := range jobs {
				results <- e.deleteCandidate(
					ctx,
					candidate,
					opts.PurgeVersions,
					validator,
				)
			}
		})
	}

	go func() {
		defer close(jobs)

		for _, candidate := range candidates {
			select {
			case jobs <- candidate:
			case <-ctx.Done():
				return
			}
		}
	}()

	workers.Wait()
	close(results)

	collected := make([]domain.DeleteResult, 0, len(candidates))
	for result := range results {
		collected = append(collected, result)
	}

	return collected
}

func validateExecutionPlan(plan domain.Plan, opts ExecuteOptions) error {
	if opts.DryRun {
		return nil
	}

	if !validBucketVersioningState(plan.Versioning) {
		return fmt.Errorf("plan has unknown bucket versioning state %q", plan.Versioning)
	}

	switch plan.VersioningSource {
	case "", domain.BucketVersioningSourceDetected, domain.BucketVersioningSourceOverride:
	default:
		return fmt.Errorf("plan has unknown bucket versioning source %q", plan.VersioningSource)
	}

	if len(plan.BlockingReasons) > 0 {
		return fmt.Errorf("execution blocked: %v", plan.BlockingReasons)
	}

	if plan.Versioning != domain.BucketVersioningDisabled && !opts.PurgeVersions {
		return fmt.Errorf(
			"bucket versioning is %s; use --purge-versions to remove backup versions",
			plan.Versioning,
		)
	}

	var (
		snapshotObjects int
		snapshotBytes   int64
	)

	for _, candidate := range plan.Candidates {
		if candidate.State != domain.StateOrphan {
			continue
		}

		kind := normalizedCandidateKind(candidate.Kind)
		switch kind {
		case domain.CandidateBackup:
			if candidate.Backup.Namespace == "" || candidate.Backup.Name == "" {
				return fmt.Errorf(
					"backup candidate %q has an incomplete Kubernetes identity",
					candidate.Prefix,
				)
			}
		case domain.CandidateOrphanClusterRoot,
			domain.CandidateOrphanRepositoryRoot,
			domain.CandidateOrphanVolumeRoot:
		case domain.CandidateRepositoryStray:
			if !plan.DeleteRepositoryStray {
				return errors.New(
					"plan contains repository-stray deletion without explicit authorization",
				)
			}
		case domain.CandidateProtectedUserVolume:
			return errors.New("plan marks a protected user volume as deletion-eligible")
		default:
			return fmt.Errorf("plan has unknown candidate type %q", candidate.Kind)
		}

		if candidate.DeferredScan {
			return fmt.Errorf(
				"object snapshot for %q is still deferred",
				candidate.Prefix,
			)
		}

		if len(candidate.Objects) != candidate.ObjectCount {
			return fmt.Errorf(
				"object snapshot for %s contains %d objects; plan records %d",
				candidate.Backup,
				len(candidate.Objects),
				candidate.ObjectCount,
			)
		}

		if candidate.FullScopeSnapshot && len(candidate.ScopeObjects) == 0 {
			return fmt.Errorf("full scope snapshot for %s is empty", candidate.Prefix)
		}

		snapshotObjects += len(candidate.Objects)
		for _, object := range candidate.Objects {
			snapshotBytes += object.Size
		}
	}

	if snapshotObjects != plan.DeleteObjects || snapshotBytes != plan.DeleteBytes {
		return fmt.Errorf(
			"object snapshot totals are %d objects and %d bytes; plan records %d objects and %d bytes",
			snapshotObjects,
			snapshotBytes,
			plan.DeleteObjects,
			plan.DeleteBytes,
		)
	}

	return nil
}

func (e Executor) revalidateInventory(ctx context.Context, plan domain.Plan) error {
	inventory, err := e.refreshInventory(ctx, plan)
	if err != nil {
		return err
	}

	dependencies := liveDependencyKeys(inventory.Backups)
	for _, candidate := range plan.Candidates {
		if candidate.State != domain.StateOrphan {
			continue
		}

		if err := validateCandidateInventory(candidate, inventory, dependencies); err != nil {
			return err
		}
	}

	return nil
}

func (e Executor) refreshInventory(
	ctx context.Context,
	plan domain.Plan,
) (domain.Inventory, error) {
	inventory, settings, err := e.Kube.Inventory(ctx, plan.Repository, plan.Namespace, false)
	if err != nil {
		return domain.Inventory{}, fmt.Errorf("refresh Kubernetes inventory: %w", err)
	}

	if len(inventory.BlockingReasons) > 0 {
		return domain.Inventory{}, fmt.Errorf(
			"refreshed Kubernetes inventory has blockers: %v",
			inventory.BlockingReasons,
		)
	}

	if plan.RepositoryUID != "" && inventory.Repo.UID != plan.RepositoryUID {
		return domain.Inventory{}, errors.New("BackupRepo was recreated after planning")
	}

	if plan.RepositoryGeneration != 0 &&
		inventory.Repo.Generation != plan.RepositoryGeneration {
		return domain.Inventory{}, errors.New("BackupRepo specification changed after planning")
	}

	if settings.Bucket != "" && settings.Bucket != plan.Bucket {
		return domain.Inventory{}, fmt.Errorf(
			"BackupRepo bucket changed after planning from %q to %q",
			plan.Bucket,
			settings.Bucket,
		)
	}

	for namespace, plannedPrefix := range plan.ObjectPrefixes {
		currentPrefix := cleanKey(inventory.Repo.ObjectPrefixes[namespace])
		if currentPrefix != cleanKey(plannedPrefix) {
			return domain.Inventory{}, fmt.Errorf(
				"BackupRepo object prefix for namespace %q changed after planning from %q to %q",
				namespace,
				plannedPrefix,
				currentPrefix,
			)
		}
	}

	plannedPrefixes := plan.Prefixes
	if len(plannedPrefixes) == 0 {
		plannedPrefixes = []string{plan.Prefix}
	}

	if !plan.VolumeDiscovery && len(plan.ObjectPrefixes) == 0 {
		repoPrefix := cleanKey(inventory.Repo.PathPrefix)
		for _, plannedPrefix := range plannedPrefixes {
			if repoPrefix != "" && !containsPrefix(repoPrefix, plannedPrefix) {
				return domain.Inventory{}, fmt.Errorf(
					"planned prefix %q is outside refreshed BackupRepo prefix %q",
					plannedPrefix,
					repoPrefix,
				)
			}
		}
	} else if !plan.VolumeDiscovery {
		for _, plannedPrefix := range plannedPrefixes {
			insideRoot := false
			for _, root := range plan.ObjectPrefixes {
				if containsPrefix(root, plannedPrefix) {
					insideRoot = true
					break
				}
			}

			if !insideRoot {
				return domain.Inventory{}, fmt.Errorf(
					"planned prefix %q is outside the planned BackupRepo PVC object prefixes",
					plannedPrefix,
				)
			}
		}
	}

	return inventory, nil
}

type candidateSafetyValidator struct {
	executor Executor
	plan     domain.Plan

	mutex          sync.Mutex
	refreshStarted time.Time
	inventory      domain.Inventory
	inventoryValid bool
}

func (v *candidateSafetyValidator) validate(
	ctx context.Context,
	candidate domain.Candidate,
	after time.Time,
) error {
	if normalizedCandidateKind(candidate.Kind) == domain.CandidateBackup {
		exists, err := v.executor.Kube.BackupExists(ctx, candidate.Backup)
		if err != nil {
			return fmt.Errorf("get Backup CR: %w", err)
		}

		if exists {
			return errors.New("backup CR appeared after the final object snapshot")
		}

		return nil
	}

	v.mutex.Lock()
	defer v.mutex.Unlock()

	if !v.inventoryValid || v.refreshStarted.Before(after) {
		v.refreshStarted = time.Now()
		v.inventoryValid = false

		inventory, err := v.executor.refreshInventory(ctx, v.plan)
		if err != nil {
			v.inventory = domain.Inventory{}
			return err
		}

		v.inventory = inventory
		v.inventoryValid = true
	}

	return validateCandidateInventory(
		candidate,
		v.inventory,
		liveDependencyKeys(v.inventory.Backups),
	)
}

func validateCandidateInventory(
	candidate domain.Candidate,
	inventory domain.Inventory,
	dependencies map[domain.BackupKey]struct{},
) error {
	switch normalizedCandidateKind(candidate.Kind) {
	case domain.CandidateOrphanVolumeRoot:
		if owner, exists := inventory.VolumeRoots[candidate.Prefix]; exists {
			return fmt.Errorf(
				"plan is stale: orphan volume root %q is now owned by %s",
				candidate.Prefix,
				owner.Resource,
			)
		}

		return nil
	case domain.CandidateOrphanRepositoryRoot:
		root := cleanKey(candidate.Prefix)
		if !isCanonicalPVCRoot(root) {
			return fmt.Errorf(
				"plan has invalid orphan repository root %q",
				candidate.Prefix,
			)
		}

		owner, exists := inventory.VolumeRoots[root]
		if !exists || owner.Kind != domain.VolumeRootRepository {
			return fmt.Errorf(
				"plan is stale: orphan repository root %q is no longer a historical BackupRepo PV",
				candidate.Prefix,
			)
		}

		if isCurrentRepositoryRoot(inventory, root) {
			return fmt.Errorf(
				"plan is stale: orphan repository root %q is now a current BackupRepo PVC root",
				candidate.Prefix,
			)
		}

		if reason := repositoryRootReference(
			inventory,
			root,
			repositoryTopologyFromObjects(root, candidate.Objects),
		); reason != "" {
			return fmt.Errorf(
				"plan is stale: orphan repository root %q is now referenced: %s",
				candidate.Prefix,
				reason,
			)
		}

		return nil
	case domain.CandidateOrphanClusterRoot:
		clusterPrefix := cleanKey(candidate.Prefix)
		namespace := path.Base(path.Dir(clusterPrefix))
		root := repositoryRootForObject(inventory, namespace, clusterPrefix)
		expectedPrefix, valid := objectPathClusterPrefix(root, namespace, clusterPrefix)

		clusterUID, validUID := clusterDirectoryUID(clusterPrefix)
		if !valid || !validUID || expectedPrefix != clusterPrefix {
			return fmt.Errorf(
				"plan has invalid orphan cluster root %q",
				candidate.Prefix,
			)
		}

		if reason := clusterBackupReference(
			inventory,
			namespace,
			clusterPrefix,
			clusterUID,
		); reason != "" {
			return fmt.Errorf(
				"plan is stale: orphan cluster root %q is now referenced: %s",
				candidate.Prefix,
				reason,
			)
		}

		protection := matchingProtection(clusterPrefix, inventory.Protections)
		if protection != nil {
			return fmt.Errorf(
				"plan is stale: orphan cluster root %q is now protected by %s",
				candidate.Prefix,
				protection.Resource,
			)
		}

		return nil
	case domain.CandidateRepositoryStray:
		refreshed := candidate
		protectRepositoryStray(&refreshed, inventory)

		if refreshed.State != domain.StateOrphan {
			return fmt.Errorf(
				"plan is stale: repository stray %q is now protected: %s",
				candidate.Prefix,
				refreshed.Reason,
			)
		}

		return nil
	case domain.CandidateBackup:
	default:
		return fmt.Errorf("plan has unknown deletion candidate type %q", candidate.Kind)
	}

	if applyInventoryProtection(&candidate, inventory) {
		return fmt.Errorf(
			"plan is stale: backup %s is now %s: %s",
			candidate.Backup,
			candidate.State,
			candidate.Reason,
		)
	}

	if _, protected := dependencies[candidate.Backup]; protected {
		return fmt.Errorf(
			"plan is stale: backup %s is now referenced by a live backup",
			candidate.Backup,
		)
	}

	return nil
}

func (e Executor) deleteCandidate(
	ctx context.Context,
	candidate domain.Candidate,
	purgeVersions bool,
	validator *candidateSafetyValidator,
) domain.DeleteResult {
	switch normalizedCandidateKind(candidate.Kind) {
	case domain.CandidateOrphanClusterRoot,
		domain.CandidateOrphanRepositoryRoot,
		domain.CandidateOrphanVolumeRoot:
		return e.deletePrefixSnapshot(ctx, candidate, purgeVersions, validator)
	case domain.CandidateRepositoryStray:
		return e.deleteRepositoryStray(ctx, candidate, purgeVersions, validator)
	case domain.CandidateBackup:
		return e.deleteBackupCandidate(ctx, candidate, purgeVersions, validator)
	default:
		return domain.DeleteResult{
			Prefix: candidate.Prefix,
			Error:  fmt.Sprintf("unsupported candidate type %q", candidate.Kind),
		}
	}
}

func (e Executor) deleteBackupCandidate(
	ctx context.Context,
	candidate domain.Candidate,
	purgeVersions bool,
	validator *candidateSafetyValidator,
) domain.DeleteResult {
	result := domain.DeleteResult{Prefix: candidate.Prefix}

	exists, err := e.Kube.BackupExists(ctx, candidate.Backup)
	if err != nil {
		result.Error = fmt.Sprintf("recheck Backup CR: %v", err)
		return result
	}

	if exists {
		result.Error = "Backup CR appeared after planning"
		return result
	}

	if candidate.ManifestKey == "" {
		return e.deletePrefixSnapshot(ctx, candidate, purgeVersions, validator)
	}

	marker, err := e.Store.Stat(ctx, candidate.ManifestKey)
	if err != nil {
		result.Error = fmt.Sprintf("recheck manifest: %v", err)
		return result
	}

	if marker.ETag != candidate.ManifestETag || !marker.LastModified.Equal(candidate.LastModified) {
		result.Error = "manifest changed after planning"
		return result
	}

	currentObjects, err := e.Store.List(ctx, candidate.Prefix, purgeVersions)
	if err != nil {
		result.Error = fmt.Sprintf("recheck backup objects: %v", err)

		return result
	}

	currentObjects = objectsInsidePrefix(candidate.Prefix, currentObjects)
	if !sameObjectSnapshot(candidate.Objects, currentObjects) {
		result.Error = "backup objects changed after planning"

		return result
	}

	dataObjects := make([]domain.Object, 0, len(candidate.Objects))

	historicalManifests := make([]domain.Object, 0, 1)

	currentManifests := make([]domain.Object, 0, 1)
	for _, object := range candidate.Objects {
		if object.Key == candidate.ManifestKey {
			if object.VersionID == marker.VersionID {
				currentManifests = append(currentManifests, object)
			} else {
				historicalManifests = append(historicalManifests, object)
			}

			continue
		}

		dataObjects = append(dataObjects, object)
	}

	if len(currentManifests) != 1 {
		result.Error = "planned objects do not contain exactly one current backup manifest"

		return result
	}

	if err := validator.validate(ctx, candidate, time.Now()); err != nil {
		result.Error = fmt.Sprintf("final Kubernetes recheck: %v", err)
		return result
	}

	if len(dataObjects) > 0 {
		if err := e.Store.Delete(ctx, dataObjects); err != nil {
			result.Error = err.Error()

			return result
		}
	}

	if len(historicalManifests) > 0 {
		if err := e.Store.Delete(ctx, historicalManifests); err != nil {
			result.Error = err.Error()

			return result
		}
	}

	if err := e.Store.Delete(ctx, currentManifests); err != nil {
		result.Error = err.Error()

		return result
	}

	result.ObjectsDeleted = candidate.ObjectCount
	result.BytesDeleted = candidate.Bytes

	return result
}

func (e Executor) deletePrefixSnapshot(
	ctx context.Context,
	candidate domain.Candidate,
	purgeVersions bool,
	validator *candidateSafetyValidator,
) domain.DeleteResult {
	result := domain.DeleteResult{Prefix: candidate.Prefix}

	current, err := e.Store.List(ctx, candidate.Prefix, purgeVersions)
	if err != nil {
		result.Error = fmt.Sprintf("recheck prefix objects: %v", err)
		return result
	}

	current = objectsInsidePrefix(candidate.Prefix, current)
	if !sameObjectSnapshot(candidate.Objects, current) {
		result.Error = "prefix objects changed after planning"
		return result
	}

	if err := validator.validate(ctx, candidate, time.Now()); err != nil {
		result.Error = fmt.Sprintf("final Kubernetes recheck: %v", err)
		return result
	}

	if len(current) > 0 {
		if err := e.Store.Delete(ctx, current); err != nil {
			result.Error = err.Error()
			return result
		}
	}

	result.ObjectsDeleted = candidate.ObjectCount
	result.BytesDeleted = candidate.Bytes

	return result
}

func (e Executor) deleteRepositoryStray(
	ctx context.Context,
	candidate domain.Candidate,
	purgeVersions bool,
	validator *candidateSafetyValidator,
) domain.DeleteResult {
	result := domain.DeleteResult{Prefix: candidate.Prefix}

	scope := candidate.ScopePrefix
	if scope == "" {
		scope = candidate.Prefix
	}

	current, err := e.Store.List(ctx, scope, purgeVersions)
	if err != nil {
		result.Error = fmt.Sprintf("recheck repository stray objects: %v", err)
		return result
	}

	current = objectsInsidePrefix(scope, current)
	if candidate.FullScopeSnapshot && !sameObjectSnapshot(candidate.ScopeObjects, current) {
		result.Error = "repository scope changed after planning"
		return result
	}

	current = selectObjectsByPlannedKeys(candidate.Objects, current)
	if !sameObjectSnapshot(candidate.Objects, current) {
		result.Error = "repository stray objects changed after planning"
		return result
	}

	if err := validator.validate(ctx, candidate, time.Now()); err != nil {
		result.Error = fmt.Sprintf("final Kubernetes recheck: %v", err)
		return result
	}

	if len(current) > 0 {
		if err := e.Store.Delete(ctx, current); err != nil {
			result.Error = err.Error()
			return result
		}
	}

	result.ObjectsDeleted = candidate.ObjectCount
	result.BytesDeleted = candidate.Bytes

	return result
}

func normalizedCandidateKind(kind domain.CandidateKind) domain.CandidateKind {
	if kind == "" {
		return domain.CandidateBackup
	}

	return kind
}

type objectIdentity struct {
	key       string
	versionID string
}

func objectsInsidePrefix(prefix string, objects []domain.Object) []domain.Object {
	result := make([]domain.Object, 0, len(objects))
	for _, object := range objects {
		if containsPrefix(prefix, object.Key) {
			result = append(result, object)
		}
	}

	return result
}

func sameObjectSnapshot(planned, current []domain.Object) bool {
	if len(planned) != len(current) {
		return false
	}

	plannedByID := make(map[objectIdentity]domain.Object, len(planned))
	for _, object := range planned {
		identity := objectIdentity{key: object.Key, versionID: object.VersionID}
		if _, exists := plannedByID[identity]; exists {
			return false
		}

		plannedByID[identity] = object
	}

	for _, object := range current {
		identity := objectIdentity{key: object.Key, versionID: object.VersionID}

		plannedObject, exists := plannedByID[identity]
		if !exists || plannedObject.Size != object.Size || plannedObject.ETag != object.ETag ||
			plannedObject.DeleteMarker != object.DeleteMarker ||
			!plannedObject.LastModified.Equal(object.LastModified) {
			return false
		}

		delete(plannedByID, identity)
	}

	return len(plannedByID) == 0
}
