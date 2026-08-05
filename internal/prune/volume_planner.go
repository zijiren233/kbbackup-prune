package prune

import (
	"context"
	"fmt"
	"maps"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/labring-sigs/kbbackup-prune/internal/domain"
	"k8s.io/apimachinery/pkg/util/validation"
)

//nolint:containedctx // The builder is scoped to one synchronous Planner.Build call.
type volumePlanBuilder struct {
	planner   Planner
	ctx       context.Context
	inventory domain.Inventory
	opts      PlanOptions
	cutoff    time.Time
	versions  bool
	requested string
	plan      *domain.Plan
}

type repositoryRootLayout struct {
	root            string
	level           domain.ObjectLevel
	namespaceLevels map[string]domain.ObjectLevel
}

type repositoryRootTopology map[string]map[string]string

func (p Planner) buildVolumePlan(
	ctx context.Context,
	inventory domain.Inventory,
	opts PlanOptions,
	now time.Time,
	cutoff time.Time,
) (domain.Plan, error) {
	versioning, versioningSource, err := resolveBucketVersioning(
		ctx,
		p.Store,
		opts.BucketVersioning,
	)
	if err != nil {
		return domain.Plan{}, fmt.Errorf("get bucket versioning: %w", err)
	}

	plan := domain.Plan{
		GeneratedAt:           now,
		Repository:            opts.Repository,
		RepositoryUID:         inventory.Repo.UID,
		RepositoryGeneration:  inventory.Repo.Generation,
		Bucket:                opts.Bucket,
		Namespace:             opts.Namespace,
		Prefix:                cleanKey(opts.Prefix),
		ObjectPrefixes:        cloneStringMap(inventory.Repo.ObjectPrefixes),
		Versioning:            versioning,
		VersioningSource:      versioningSource,
		VolumeDiscovery:       true,
		VolumeRootCounts:      &domain.VolumeRootCounts{},
		DeleteRepositoryStray: opts.DeleteRepositoryStray,
		StateCounts:           make(map[domain.CandidateState]int),
		BlockingReasons:       append([]string(nil), inventory.BlockingReasons...),
	}

	unsupportedRoots := make([]string, 0)
	for prefix, root := range inventory.VolumeRoots {
		if root.Kind == domain.VolumeRootRepository && !isCanonicalPVCRoot(prefix) {
			unsupportedRoots = append(unsupportedRoots, prefix)
		}
	}

	sort.Strings(unsupportedRoots)

	for _, prefix := range unsupportedRoots {
		plan.BlockingReasons = append(
			plan.BlockingReasons,
			fmt.Sprintf("BackupRepo volume root %q is not a canonical pvc-UUID prefix", prefix),
		)
	}

	builder := volumePlanBuilder{
		planner: p, ctx: ctx, inventory: inventory, opts: opts, cutoff: cutoff,
		versions:  opts.PurgeVersions && versioning != domain.BucketVersioningDisabled,
		requested: cleanKey(opts.Prefix), plan: &plan,
	}

	if err := builder.discover(); err != nil {
		return domain.Plan{}, err
	}

	sort.Slice(plan.Candidates, func(i, j int) bool {
		if plan.Candidates[i].Prefix == plan.Candidates[j].Prefix {
			return plan.Candidates[i].Kind < plan.Candidates[j].Kind
		}
		return plan.Candidates[i].Prefix < plan.Candidates[j].Prefix
	})
	p.protectDependencies(&plan, inventory)

	if opts.CaptureObjects && len(plan.BlockingReasons) == 0 {
		if err := builder.captureEligibleObjects(); err != nil {
			return domain.Plan{}, err
		}

		p.protectDependencies(&plan, inventory)
	}

	for i := range plan.Candidates {
		candidate := &plan.Candidates[i]
		if !opts.CaptureObjects || candidate.State != domain.StateOrphan {
			candidate.Objects = nil
			candidate.ScopeObjects = nil
		}
	}

	refreshOrphanVolumeRoots(&plan)
	summarizePlan(&plan)

	return plan, nil
}

func supportsVolumeDiscovery(roots map[string]domain.VolumeRoot) bool {
	for prefix, root := range roots {
		if root.Kind == domain.VolumeRootRepository && isCanonicalPVCRoot(prefix) {
			return true
		}
	}

	return false
}

func (b *volumePlanBuilder) discover() error {
	level, err := b.planner.Store.ListLevel(b.ctx, "", "/", b.opts.PurgeVersions)
	if err != nil {
		return fmt.Errorf("discover bucket volume roots: %w", err)
	}

	matchedRequested := b.requested == ""
	for _, discoveredPrefix := range level.Prefixes {
		root := strings.TrimSuffix(discoveredPrefix, "/")
		if !isCanonicalPVCRoot(root) || !overlaps(root, b.requested) {
			continue
		}

		matchedRequested = true

		owner, owned := b.inventory.VolumeRoots[root]
		if owned && owner.Kind == domain.VolumeRootRepository &&
			b.opts.Namespace != "" && owner.Namespace != "" &&
			owner.Namespace != b.opts.Namespace {
			continue
		}

		b.plan.Prefixes = append(b.plan.Prefixes, root)
		b.countVolumeRoot(owner, owned)

		switch {
		case !owned:
			if err := b.addOrphanVolumeRoot(root); err != nil {
				return err
			}
		case owner.Kind == domain.VolumeRootUser:
			b.plan.Candidates = append(b.plan.Candidates, domain.Candidate{
				Kind:   domain.CandidateProtectedUserVolume,
				Prefix: root,
				State:  domain.StateProtected,
				Reason: "volume root belongs to " + owner.Resource + "; internal listing skipped",
				Protection: &domain.Protection{
					Prefix: root, Kind: "volume-root", Resource: owner.Resource,
				},
			})
		case owner.Kind == domain.VolumeRootRepository:
			layout, err := b.loadRepositoryRootLayout(root)
			if err != nil {
				return err
			}

			if !isCurrentRepositoryRoot(b.inventory, root) &&
				repositoryRootReference(
					b.inventory,
					root,
					repositoryTopologyFromLayout(layout),
				) == "" {
				b.addShallowScannedObjects(layout)
				b.addOrphanRepositoryRoot(root)

				continue
			}

			if err := b.scanRepositoryRoot(layout); err != nil {
				return err
			}
		default:
			b.plan.Candidates = append(b.plan.Candidates, domain.Candidate{
				Kind: domain.CandidateProtectedUserVolume, Prefix: root,
				State: domain.StateProtected, Reason: "volume root has an unknown owner type",
			})
		}
	}

	sort.Strings(b.plan.Prefixes)

	if b.plan.Prefix == "" && len(b.plan.Prefixes) == 1 {
		b.plan.Prefix = b.plan.Prefixes[0]
	}

	if b.requested != "" && !matchedRequested {
		return fmt.Errorf(
			"scan prefix %q does not overlap a discovered pvc-UUID volume root",
			b.requested,
		)
	}

	return nil
}

func (b *volumePlanBuilder) countVolumeRoot(owner domain.VolumeRoot, owned bool) {
	counts := b.plan.VolumeRootCounts
	counts.Total++

	switch {
	case !owned:
		counts.Unowned++
	case owner.Kind == domain.VolumeRootRepository:
		counts.Repository++
	case owner.Kind == domain.VolumeRootUser:
		counts.ProtectedUser++
	default:
		counts.Other++
	}
}

func (b *volumePlanBuilder) addOrphanVolumeRoot(root string) error {
	candidate := domain.Candidate{
		Kind: domain.CandidateOrphanVolumeRoot, Prefix: root, ScopePrefix: root,
		State:  domain.StateOrphan,
		Reason: "no current PVC or PV owns this volume root",
	}
	if b.opts.Namespace != "" {
		candidate.State = domain.StateProtected
		candidate.Reason = "namespace filtering cannot safely authorize deletion of an unowned volume root"
		candidate.DeletionConfigurable = true
		b.plan.Candidates = append(b.plan.Candidates, candidate)

		return nil
	}

	if b.requested != "" && !containsPrefix(b.requested, root) {
		candidate.State = domain.StateProtected
		candidate.Reason = "the requested prefix covers only part of this unowned volume root"
		candidate.DeletionConfigurable = true
		b.plan.Candidates = append(b.plan.Candidates, candidate)

		return nil
	}

	objects, err := b.listCandidateObjects(root)
	if err != nil {
		return fmt.Errorf("list orphan volume root %q: %w", root, err)
	}

	b.addScannedObjects(objects.current)
	setCandidateObjects(&candidate, objects.candidate, b.cutoff)

	if candidate.ObjectCount == 0 {
		candidate.State = domain.StateProtected
		candidate.Reason = "volume root contains no current objects"
	}

	b.plan.Candidates = append(b.plan.Candidates, candidate)

	return nil
}

func (b *volumePlanBuilder) addOrphanRepositoryRoot(root string) {
	candidate := domain.Candidate{
		Kind:         domain.CandidateOrphanRepositoryRoot,
		Prefix:       root,
		ScopePrefix:  root,
		DeferredScan: true,
		State:        domain.StateOrphan,
		Reason:       "historical BackupRepo PVC root has no current backup or storage reference",
	}

	if b.requested != "" && !containsPrefix(b.requested, root) {
		candidate.State = domain.StateProtected
		candidate.Reason = "the requested prefix covers only part of this orphan repository root"
		candidate.DeletionConfigurable = true
	}

	b.plan.Candidates = append(b.plan.Candidates, candidate)
}

func (b *volumePlanBuilder) loadRepositoryRootLayout(
	root string,
) (repositoryRootLayout, error) {
	rootPrefix := root + "/"

	level, err := b.planner.Store.ListLevel(
		b.ctx,
		rootPrefix,
		"/",
		b.opts.PurgeVersions,
	)
	if err != nil {
		return repositoryRootLayout{}, fmt.Errorf(
			"discover repository namespaces under %q: %w",
			root,
			err,
		)
	}

	layout := repositoryRootLayout{
		root: root, level: level,
		namespaceLevels: make(map[string]domain.ObjectLevel),
	}
	for _, namespacePrefix := range level.Prefixes {
		namespace := strings.TrimSuffix(strings.TrimPrefix(namespacePrefix, rootPrefix), "/")
		if len(validation.IsDNS1123Label(namespace)) > 0 {
			continue
		}

		namespaceLevel, listErr := b.planner.Store.ListLevel(
			b.ctx,
			namespacePrefix,
			"/",
			b.opts.PurgeVersions,
		)
		if listErr != nil {
			return repositoryRootLayout{}, fmt.Errorf(
				"discover repository clusters under %q: %w",
				namespacePrefix,
				listErr,
			)
		}

		layout.namespaceLevels[namespacePrefix] = namespaceLevel
	}

	return layout, nil
}

func (b *volumePlanBuilder) addShallowScannedObjects(layout repositoryRootLayout) {
	b.addScannedObjects(layout.level.Objects)

	for _, level := range layout.namespaceLevels {
		b.addScannedObjects(level.Objects)
	}
}

func (b *volumePlanBuilder) scanRepositoryRoot(layout repositoryRootLayout) error {
	root := layout.root
	rootPrefix := root + "/"
	level := layout.level

	b.addScannedObjects(level.Objects)
	b.addStrayObjects(root, rootPrefix, level.Objects, "loose object at repository volume root")

	for _, namespacePrefix := range level.Prefixes {
		if !overlaps(namespacePrefix, b.requested) {
			continue
		}

		namespace := strings.TrimSuffix(strings.TrimPrefix(namespacePrefix, rootPrefix), "/")
		if b.opts.Namespace != "" && namespace != b.opts.Namespace {
			continue
		}

		if len(validation.IsDNS1123Label(namespace)) > 0 {
			if err := b.scanUnknownStrayPrefix(
				namespacePrefix,
				"invalid Kubernetes namespace directory",
			); err != nil {
				return err
			}

			continue
		}

		if err := b.scanRepositoryNamespace(
			namespacePrefix,
			layout.namespaceLevels[namespacePrefix],
		); err != nil {
			return err
		}
	}

	return nil
}

func (b *volumePlanBuilder) scanRepositoryNamespace(
	namespacePrefix string,
	level domain.ObjectLevel,
) error {
	b.addScannedObjects(level.Objects)
	b.addStrayObjects(
		strings.TrimSuffix(namespacePrefix, "/"),
		namespacePrefix,
		level.Objects,
		"loose object at repository namespace level",
	)

	for _, clusterPrefix := range level.Prefixes {
		if !overlaps(clusterPrefix, b.requested) {
			continue
		}

		clusterUID, ok := clusterDirectoryUID(clusterPrefix)
		if !ok {
			if err := b.scanUnknownStrayPrefix(
				clusterPrefix,
				"directory does not end with a canonical cluster UUID",
			); err != nil {
				return err
			}

			continue
		}

		namespace := path.Base(strings.TrimSuffix(namespacePrefix, "/"))
		if clusterBackupReference(
			b.inventory,
			namespace,
			strings.TrimSuffix(clusterPrefix, "/"),
			clusterUID,
		) == "" {
			b.addOrphanClusterRoot(clusterPrefix)

			continue
		}

		if err := b.scanBackupCluster(clusterPrefix); err != nil {
			return err
		}
	}

	return nil
}

func (b *volumePlanBuilder) addOrphanClusterRoot(prefix string) {
	prefix = strings.TrimSuffix(prefix, "/")
	candidate := domain.Candidate{
		Kind:         domain.CandidateOrphanClusterRoot,
		Prefix:       prefix,
		ScopePrefix:  prefix,
		DeferredScan: true,
		State:        domain.StateOrphan,
		Reason:       "no current Backup CR references this repository cluster",
	}

	if b.requested != "" && !containsPrefix(b.requested, prefix) {
		candidate.State = domain.StateProtected
		candidate.Reason = "the requested prefix covers only part of this orphan cluster root"
		candidate.DeletionConfigurable = true
	}

	if candidate.State == domain.StateOrphan {
		if protection := matchingProtection(prefix, b.inventory.Protections); protection != nil {
			candidate.State = domain.StateProtected
			candidate.Reason = "cluster root overlaps storage used by " + protection.Resource
			candidate.Protection = protection
		}
	}

	b.plan.Candidates = append(b.plan.Candidates, candidate)
}

func (b *volumePlanBuilder) scanBackupCluster(clusterPrefix string) error {
	current, err := b.planner.Store.List(b.ctx, clusterPrefix, false)
	if err != nil {
		return fmt.Errorf("list repository cluster %q: %w", clusterPrefix, err)
	}

	current = objectsInsidePrefix(clusterPrefix, current)
	b.addScannedObjects(current)

	backupPrefixes := make(map[string]struct{})

	markers := make(map[string]domain.Object)
	for _, object := range current {
		backupPrefix, ok := backupDirectoryPrefix(clusterPrefix, object.Key)
		if !ok {
			continue
		}

		backupPrefixes[backupPrefix] = struct{}{}
		if object.Key == joins(backupPrefix, b.opts.ManifestName) {
			markers[backupPrefix] = object
		}
	}

	orderedPrefixes := make([]string, 0, len(backupPrefixes))
	for backupPrefix := range backupPrefixes {
		orderedPrefixes = append(orderedPrefixes, backupPrefix)
	}

	sort.Strings(orderedPrefixes)

	candidates := make([]domain.Candidate, 0, len(orderedPrefixes))
	for _, backupPrefix := range orderedPrefixes {
		if !overlaps(backupPrefix, b.requested) {
			continue
		}

		candidate := b.directoryCandidate(backupPrefix, markers)

		if candidate.State == domain.StateOrphan && b.requested != "" &&
			!containsPrefix(b.requested, candidate.Prefix) {
			candidate.State = domain.StateProtected
			candidate.Reason = "the requested prefix covers only part of this backup"
			candidate.DeletionConfigurable = true
		}

		candidates = append(candidates, candidate)
	}

	localPlan := domain.Plan{Candidates: candidates}
	matcher := newCandidateMatcher(localPlan.Candidates)
	b.planner.protectNestedCandidates(&localPlan, matcher)
	candidates = localPlan.Candidates

	objects := current
	if b.versions {
		objects, err = b.planner.Store.List(b.ctx, clusterPrefix, true)
		if err != nil {
			return fmt.Errorf("list repository cluster versions %q: %w", clusterPrefix, err)
		}

		objects = objectsInsidePrefix(clusterPrefix, objects)
	}

	stray := make([]domain.Object, 0)
	for _, object := range objects {
		matched := matcher.match(object.Key)
		if len(matched) == 0 {
			stray = append(stray, object)
			continue
		}

		for _, index := range matched {
			candidate := &candidates[index]
			candidate.Objects = append(candidate.Objects, object)
			candidate.ObjectCount++

			candidate.Bytes += object.Size
			if candidate.ManifestKey == "" && object.LastModified.After(candidate.LastModified) {
				candidate.LastModified = object.LastModified
				candidate.CreatedAt = object.LastModified
			}

			protectYoungObject(candidate, object, b.cutoff)
		}
	}

	b.plan.Candidates = append(b.plan.Candidates, candidates...)

	strayIndex := b.addStrayObjects(
		strings.TrimSuffix(clusterPrefix, "/"),
		clusterPrefix,
		stray,
		"cluster backup data does not belong to a canonical component/backup directory",
	)
	if strayIndex >= 0 {
		b.plan.Candidates[strayIndex].FullScopeSnapshot = true

		b.plan.Candidates[strayIndex].ScopeObjects = append(
			[]domain.Object(nil),
			objects...,
		)
	}

	return nil
}

func (b *volumePlanBuilder) directoryCandidate(
	backupPrefix string,
	markers map[string]domain.Object,
) domain.Candidate {
	if marker, ok := markers[backupPrefix]; ok {
		candidate := b.planner.readCandidate(
			b.ctx,
			marker,
			b.opts,
			b.inventory,
			b.cutoff,
		)
		candidate.Kind = domain.CandidateBackup

		return candidate
	}

	namespace := path.Base(path.Dir(path.Dir(path.Dir(backupPrefix))))
	candidate := domain.Candidate{
		Kind:   domain.CandidateBackup,
		Backup: domain.BackupKey{Namespace: namespace, Name: path.Base(backupPrefix)},
		Prefix: backupPrefix,
		State:  domain.StateOrphan,
		Reason: "Backup CR is absent and repository layout identifies a backup directory",
	}

	applyInventoryProtection(&candidate, b.inventory)

	return candidate
}

func backupDirectoryPrefix(clusterPrefix, objectKey string) (string, bool) {
	clusterPrefix = cleanKey(clusterPrefix)

	objectKey = cleanKey(objectKey)
	if objectKey == clusterPrefix || !containsPrefix(clusterPrefix, objectKey) {
		return "", false
	}

	relative := strings.TrimPrefix(objectKey, clusterPrefix+"/")

	segments := splitKey(relative)
	if len(segments) < 3 || len(validation.IsDNS1123Label(segments[0])) > 0 ||
		len(validation.IsDNS1123Subdomain(segments[1])) > 0 {
		return "", false
	}

	return path.Join(clusterPrefix, segments[0], segments[1]), true
}

func (b *volumePlanBuilder) scanUnknownStrayPrefix(prefix, reason string) error {
	prefix = strings.TrimSuffix(prefix, "/")
	if !b.opts.DeleteRepositoryStray {
		b.plan.Candidates = append(b.plan.Candidates, domain.Candidate{
			Kind:                 domain.CandidateRepositoryStray,
			Prefix:               prefix,
			ScopePrefix:          prefix,
			State:                domain.StateProtected,
			Reason:               reason + "; recursive listing skipped; enable --delete-repository-stray to delete it",
			DeletionConfigurable: true,
		})

		return nil
	}

	scanPrefix := prefix
	if b.requested != "" && containsPrefix(prefix, b.requested) {
		scanPrefix = b.requested
	}

	objects, err := b.listCandidateObjects(scanPrefix)
	if err != nil {
		return fmt.Errorf("list repository stray prefix %q: %w", scanPrefix, err)
	}

	b.addScannedObjects(objects.current)
	b.addStrayObjects(scanPrefix, scanPrefix, objects.candidate, reason)

	return nil
}

func (b *volumePlanBuilder) addStrayObjects(
	prefix, scope string,
	objects []domain.Object,
	reason string,
) int {
	objects = filterDirectoryMarkers(objects)

	objects = filterRequestedObjects(objects, b.requested)
	if len(objects) == 0 {
		return -1
	}

	candidate := domain.Candidate{
		Kind: domain.CandidateRepositoryStray, Prefix: cleanKey(prefix),
		ScopePrefix: cleanKey(scope), State: domain.StateOrphan,
		Reason:  reason + "; explicit repository-stray deletion is enabled",
		Objects: append([]domain.Object(nil), objects...),
	}

	protectRepositoryStray(&candidate, b.inventory)
	setCandidateObjects(&candidate, objects, b.cutoff)

	if !b.opts.DeleteRepositoryStray && candidate.State != domain.StateProtected {
		candidate.State = domain.StateProtected
		candidate.Reason = reason + "; enable --delete-repository-stray to delete it"
		candidate.DeletionConfigurable = true
	}

	b.plan.Candidates = append(b.plan.Candidates, candidate)

	return len(b.plan.Candidates) - 1
}

func filterDirectoryMarkers(objects []domain.Object) []domain.Object {
	result := make([]domain.Object, 0, len(objects))
	for _, object := range objects {
		if object.Size == 0 && strings.HasSuffix(object.Key, "/") {
			continue
		}

		result = append(result, object)
	}

	return result
}

type listedCandidateObjects struct {
	current   []domain.Object
	candidate []domain.Object
}

func (b *volumePlanBuilder) listCandidateObjects(prefix string) (listedCandidateObjects, error) {
	current, err := b.planner.Store.List(b.ctx, prefix, false)
	if err != nil {
		return listedCandidateObjects{}, err
	}

	current = objectsInsidePrefix(prefix, current)

	result := listedCandidateObjects{current: current, candidate: current}
	if !b.versions {
		return result, nil
	}

	result.candidate, err = b.planner.Store.List(b.ctx, prefix, true)
	if err != nil {
		return listedCandidateObjects{}, err
	}

	result.candidate = objectsInsidePrefix(prefix, result.candidate)

	return result, nil
}

func (b *volumePlanBuilder) captureEligibleObjects() error {
	for i := range b.plan.Candidates {
		candidate := &b.plan.Candidates[i]
		if candidate.State != domain.StateOrphan {
			continue
		}

		planned := append([]domain.Object(nil), candidate.Objects...)

		scope := candidate.Prefix
		if candidate.ScopePrefix != "" {
			scope = candidate.ScopePrefix
		}

		objects, err := b.planner.Store.List(b.ctx, scope, b.versions)
		if err != nil {
			return fmt.Errorf("capture object snapshot under %q: %w", scope, err)
		}

		objects = objectsInsidePrefix(scope, objects)

		candidate.DeferredScan = false
		if len(candidate.ScopeObjects) > 0 {
			candidate.ScopeObjects = append(candidate.ScopeObjects[:0], objects...)
		}

		if candidate.Kind == domain.CandidateRepositoryStray {
			objects = selectObjectsByPlannedKeys(planned, objects)
		}

		candidate.State = domain.StateOrphan
		setCandidateObjects(candidate, objects, b.cutoff)

		if (candidate.Kind == domain.CandidateOrphanClusterRoot ||
			candidate.Kind == domain.CandidateOrphanRepositoryRoot) &&
			candidate.State == domain.StateOrphan {
			current := objects
			if b.versions {
				current, err = b.planner.Store.List(b.ctx, scope, false)
				if err != nil {
					return fmt.Errorf(
						"list current objects under orphan cluster %q: %w",
						scope,
						err,
					)
				}

				current = objectsInsidePrefix(scope, current)
			}

			b.protectOrphanPrefixManifests(candidate, current)
		}

		if candidate.Kind == domain.CandidateRepositoryStray {
			protectRepositoryStray(candidate, b.inventory)
		}
	}

	return nil
}

func (b *volumePlanBuilder) protectOrphanPrefixManifests(
	candidate *domain.Candidate,
	objects []domain.Object,
) {
	for _, object := range objects {
		if path.Base(object.Key) != b.opts.ManifestName {
			continue
		}

		backup := b.planner.readCandidate(
			b.ctx,
			object,
			b.opts,
			b.inventory,
			b.cutoff,
		)
		if backup.State == domain.StateOrphan {
			continue
		}

		candidate.State = backup.State
		candidate.Reason = fmt.Sprintf(
			"cluster root contains protected manifest %q: %s",
			object.Key,
			backup.Reason,
		)

		return
	}
}

func (b *volumePlanBuilder) addScannedObjects(objects []domain.Object) {
	for _, object := range objects {
		b.plan.ScannedObjects++
		b.plan.ScannedBytes += object.Size
	}
}

func setCandidateObjects(
	candidate *domain.Candidate,
	objects []domain.Object,
	cutoff time.Time,
) {
	manifestModified := candidate.LastModified
	candidate.Objects = append(candidate.Objects[:0], objects...)
	candidate.ObjectCount = len(objects)
	candidate.Bytes = 0

	candidate.LastModified = time.Time{}
	for _, object := range objects {
		candidate.Bytes += object.Size
		if object.LastModified.After(candidate.LastModified) {
			candidate.LastModified = object.LastModified
		}
	}

	switch {
	case candidate.Kind != domain.CandidateBackup:
		candidate.CreatedAt = candidate.LastModified
	case candidate.ManifestKey != "":
		candidate.LastModified = manifestModified
	default:
		candidate.CreatedAt = candidate.LastModified
	}

	if candidate.State == domain.StateOrphan {
		for _, object := range objects {
			protectYoungObject(candidate, object, cutoff)
		}
	}
}

func protectRepositoryStray(candidate *domain.Candidate, inventory domain.Inventory) {
	if candidate.State != domain.StateOrphan {
		return
	}

	for _, object := range candidate.Objects {
		if reason := uncertainClusterReferenceForObject(inventory, object.Key); reason != "" {
			candidate.State = domain.StateProtected
			candidate.Reason = "stray set overlaps a cluster with an uncertain reference: " + reason

			return
		}

		for _, backup := range inventory.Backups {
			if backup.Path != "" && containsPrefix(backup.Path, object.Key) {
				candidate.State = domain.StateProtected
				candidate.Reason = "stray set overlaps a live Backup CR path"
				return
			}

			if backup.KopiaRepoPath != "" && containsPrefix(backup.KopiaRepoPath, object.Key) {
				candidate.State = domain.StateProtected
				candidate.Reason = "stray set overlaps a live Kopia repository"
				return
			}
		}

		if protection := matchingProtection(object.Key, inventory.Protections); protection != nil {
			candidate.State = domain.StateProtected
			candidate.Reason = "stray set overlaps storage used by " + protection.Resource
			candidate.Protection = protection
			return
		}
	}
}

func filterRequestedObjects(objects []domain.Object, requested string) []domain.Object {
	if requested == "" {
		return objects
	}

	result := make([]domain.Object, 0, len(objects))
	for _, object := range objects {
		if containsPrefix(requested, object.Key) {
			result = append(result, object)
		}
	}

	return result
}

func selectObjectsByPlannedKeys(planned, current []domain.Object) []domain.Object {
	keys := make(map[string]struct{}, len(planned))
	for _, object := range planned {
		keys[object.Key] = struct{}{}
	}

	result := make([]domain.Object, 0, len(planned))
	for _, object := range current {
		if _, ok := keys[object.Key]; ok {
			result = append(result, object)
		}
	}

	return result
}

func refreshOrphanVolumeRoots(plan *domain.Plan) {
	plan.OrphanVolumeRoots = nil
	for _, candidate := range plan.Candidates {
		if candidate.Kind == domain.CandidateOrphanVolumeRoot &&
			candidate.State == domain.StateOrphan {
			plan.OrphanVolumeRoots = append(plan.OrphanVolumeRoots, candidate.Prefix)
		}
	}

	sort.Strings(plan.OrphanVolumeRoots)
}

func isCanonicalPVCRoot(value string) bool {
	if strings.Contains(value, "/") || !strings.HasPrefix(value, "pvc-") {
		return false
	}

	parsed, err := uuid.Parse(strings.TrimPrefix(value, "pvc-"))

	return err == nil && value == "pvc-"+parsed.String()
}

func clusterDirectoryUID(prefix string) (string, bool) {
	name := path.Base(strings.TrimSuffix(prefix, "/"))
	if len(name) <= 37 || name[len(name)-37] != '-' {
		return "", false
	}

	value := name[len(name)-36:]

	parsed, err := uuid.Parse(value)
	if err != nil || parsed.String() != value || name[:len(name)-37] == "" {
		return "", false
	}

	return value, true
}

func isCurrentRepositoryRoot(inventory domain.Inventory, root string) bool {
	root = cleanKey(root)
	if owner, found := inventory.VolumeRoots[root]; found && owner.Current {
		return true
	}

	for _, current := range inventory.Repo.ObjectPrefixes {
		if cleanKey(current) == root {
			return true
		}
	}

	return false
}

func repositoryTopologyFromLayout(layout repositoryRootLayout) repositoryRootTopology {
	topology := make(repositoryRootTopology)
	for namespacePrefix, level := range layout.namespaceLevels {
		namespace := path.Base(strings.TrimSuffix(namespacePrefix, "/"))

		topology[namespace] = make(map[string]string)
		for _, clusterPrefix := range level.Prefixes {
			clusterPrefix = strings.TrimSuffix(clusterPrefix, "/")

			clusterUID, ok := clusterDirectoryUID(clusterPrefix)
			if !ok {
				continue
			}

			topology[namespace][clusterPrefix] = clusterUID
		}
	}

	return topology
}

func repositoryTopologyFromObjects(
	root string,
	objects []domain.Object,
) repositoryRootTopology {
	root = cleanKey(root)

	topology := make(repositoryRootTopology)
	for _, object := range objects {
		key := cleanKey(object.Key)
		if !containsPrefix(root, key) || key == root {
			continue
		}

		relative := strings.TrimPrefix(key, root+"/")

		segments := splitKey(relative)
		if len(segments) == 0 || len(validation.IsDNS1123Label(segments[0])) > 0 {
			continue
		}

		if topology[segments[0]] == nil {
			topology[segments[0]] = make(map[string]string)
		}

		if len(segments) < 2 {
			continue
		}

		clusterPrefix := path.Join(root, segments[0], segments[1])

		clusterUID, ok := clusterDirectoryUID(clusterPrefix)
		if !ok {
			continue
		}

		topology[segments[0]][clusterPrefix] = clusterUID
	}

	return topology
}

func repositoryRootReference(
	inventory domain.Inventory,
	root string,
	topology repositoryRootTopology,
) string {
	root = cleanKey(root)
	if protection := matchingProtection(root, inventory.Protections); protection != nil {
		return "storage is used by " + protection.Resource
	}

	for mapKey, backup := range inventory.Backups {
		key := backup.Key
		if key.Namespace == "" {
			key.Namespace = mapKey.Namespace
		}

		if key.Name == "" {
			key.Name = mapKey.Name
		}

		for _, objectPrefix := range backupObjectPrefixes(backup) {
			objectPrefix = cleanKey(objectPrefix)

			segments := splitKey(objectPrefix)
			if len(segments) > 0 && isCanonicalPVCRoot(segments[0]) &&
				overlaps(root, objectPrefix) {
				return "Backup CR " + key.String() + " has an object path in this repository root"
			}
		}
	}

	for namespace, clusters := range topology {
		if reason := repositoryNamespaceReference(
			inventory,
			root,
			namespace,
			clusters,
		); reason != "" {
			return reason
		}
	}

	return ""
}

func repositoryNamespaceReference(
	inventory domain.Inventory,
	root string,
	namespace string,
	clusters map[string]string,
) string {
	for mapKey, backup := range inventory.Backups {
		key := backup.Key
		if key.Namespace == "" {
			key.Namespace = mapKey.Namespace
		}

		if key.Name == "" {
			key.Name = mapKey.Name
		}

		if key.Namespace != namespace {
			continue
		}

		located := false
		if isCanonicalUUID(backup.ClusterUID) {
			located = true

			for _, clusterUID := range clusters {
				if clusterUID == backup.ClusterUID {
					return "Backup CR " + key.String() + " has the same cluster UID"
				}
			}
		}

		for _, objectPrefix := range backupObjectPrefixes(backup) {
			clusterPrefix, ok := repositoryObjectPathClusterPrefix(
				root,
				namespace,
				objectPrefix,
			)
			if !ok {
				continue
			}

			located = true

			if _, found := clusters[clusterPrefix]; found {
				return "Backup CR " + key.String() + " has an overlapping object path"
			}
		}

		if !located {
			return "Backup CR " + key.String() + " has no usable cluster locator"
		}
	}

	for key := range inventory.ProtectedBackups {
		if key.Namespace == namespace {
			return "protected Backup " + key.String() + " may be stored in this repository root"
		}
	}

	for key := range liveDependencyKeys(inventory.Backups) {
		if key.Namespace == namespace {
			return "live backup dependency " + key.String() + " may be stored in this repository root"
		}
	}

	return ""
}

func repositoryObjectPathClusterPrefix(
	root string,
	namespace string,
	objectPrefix string,
) (string, bool) {
	objectPrefix = cleanKey(objectPrefix)
	if objectPrefix == "" {
		return "", false
	}

	segments := splitKey(objectPrefix)
	if len(segments) == 0 || !isCanonicalPVCRoot(segments[0]) {
		objectPrefix = path.Join(root, objectPrefix)
	}

	return objectPathClusterPrefix(root, namespace, objectPrefix)
}

func backupObjectPrefixes(backup domain.Backup) []string {
	values := []string{
		backup.Path,
		backup.KopiaRepoPath,
		backup.RawPath,
		backup.RawKopiaRepoPath,
	}
	result := make([]string, 0, len(values))

	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = cleanKey(value)
		if value == "" {
			continue
		}

		if _, found := seen[value]; found {
			continue
		}

		seen[value] = struct{}{}
		result = append(result, value)
	}

	return result
}

func clusterBackupReference(
	inventory domain.Inventory,
	namespace string,
	clusterPrefix string,
	clusterUID string,
) string {
	root := repositoryRootForObject(inventory, namespace, clusterPrefix)
	if root == "" {
		root = cleanKey(inventory.Repo.ObjectPrefixes[namespace])
	}

	return repositoryNamespaceReference(
		inventory,
		root,
		namespace,
		map[string]string{cleanKey(clusterPrefix): clusterUID},
	)
}

func uncertainClusterReferenceForObject(inventory domain.Inventory, objectKey string) string {
	for _, root := range repositoryRoots(inventory) {
		clusterPrefix, ok := objectPathClusterPrefix(root.Prefix, root.Namespace, objectKey)
		if !ok {
			continue
		}

		clusterUID, _ := clusterDirectoryUID(clusterPrefix)

		return uncertainClusterBackupReference(
			inventory,
			root.Namespace,
			clusterPrefix,
			clusterUID,
		)
	}

	return ""
}

func repositoryRootForObject(
	inventory domain.Inventory,
	namespace string,
	objectKey string,
) string {
	for _, root := range repositoryRoots(inventory) {
		if root.Namespace == namespace && containsPrefix(root.Prefix, objectKey) {
			return root.Prefix
		}
	}

	return ""
}

func repositoryRoots(inventory domain.Inventory) []domain.VolumeRoot {
	rootsByPrefix := make(map[string]domain.VolumeRoot)
	for namespace, prefix := range inventory.Repo.ObjectPrefixes {
		prefix = cleanKey(prefix)
		if prefix == "" {
			continue
		}

		rootsByPrefix[prefix] = domain.VolumeRoot{
			Prefix: prefix, Kind: domain.VolumeRootRepository, Namespace: namespace,
		}
	}

	for prefix, root := range inventory.VolumeRoots {
		if root.Kind != domain.VolumeRootRepository {
			continue
		}

		prefix = cleanKey(prefix)
		root.Prefix = prefix
		rootsByPrefix[prefix] = root
	}

	result := make([]domain.VolumeRoot, 0, len(rootsByPrefix))
	for _, root := range rootsByPrefix {
		result = append(result, root)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Prefix < result[j].Prefix
	})

	return result
}

func uncertainClusterBackupReference(
	inventory domain.Inventory,
	namespace string,
	clusterPrefix string,
	clusterUID string,
) string {
	root := repositoryRootForObject(inventory, namespace, clusterPrefix)
	if root == "" {
		root = cleanKey(inventory.Repo.ObjectPrefixes[namespace])
	}

	for mapKey, backup := range inventory.Backups {
		key := backup.Key
		if key.Namespace == "" {
			key.Namespace = mapKey.Namespace
		}

		if key.Name == "" {
			key.Name = mapKey.Name
		}

		if key.Namespace != namespace {
			continue
		}

		pathMatches, pathLocated := backupPathsReferenceCluster(
			root,
			namespace,
			clusterPrefix,
			backup.Path,
			backup.RawPath,
		)
		kopiaMatches, kopiaLocated := backupPathsReferenceCluster(
			root,
			namespace,
			clusterPrefix,
			backup.KopiaRepoPath,
			backup.RawKopiaRepoPath,
		)

		referencesTarget := backup.ClusterUID == clusterUID ||
			pathMatches || kopiaMatches
		if referencesTarget && !pathMatches {
			return "Backup CR " + key.String() + " has no matching status.path"
		}

		located := isCanonicalUUID(backup.ClusterUID) || pathLocated || kopiaLocated
		if !located {
			return "Backup CR " + key.String() + " has no usable cluster locator"
		}
	}

	for key := range inventory.ProtectedBackups {
		if key.Namespace == namespace {
			return "protected Backup " + key.String() + " has no current object path"
		}
	}

	for key := range liveDependencyKeys(inventory.Backups) {
		if key.Namespace == namespace {
			return "live backup dependency " + key.String() + " has no current object path"
		}
	}

	return ""
}

func backupPathsReferenceCluster(
	root string,
	namespace string,
	clusterPrefix string,
	paths ...string,
) (bool, bool) {
	located := false
	for _, objectPrefix := range paths {
		candidate, ok := repositoryObjectPathClusterPrefix(root, namespace, objectPrefix)
		if !ok {
			continue
		}

		located = true

		if candidate == clusterPrefix {
			return true, true
		}
	}

	return false, located
}

func objectPathClusterPrefix(root, namespace, objectPrefix string) (string, bool) {
	root = cleanKey(root)

	objectPrefix = cleanKey(objectPrefix)
	if root == "" || !containsPrefix(root, objectPrefix) {
		return "", false
	}

	relative := strings.TrimPrefix(objectPrefix, root)
	relative = strings.TrimPrefix(relative, "/")

	segments := splitKey(relative)
	if len(segments) < 2 || segments[0] != namespace {
		return "", false
	}

	clusterPrefix := path.Join(root, segments[0], segments[1])
	_, valid := clusterDirectoryUID(clusterPrefix)

	return clusterPrefix, valid
}

func isCanonicalUUID(value string) bool {
	parsed, err := uuid.Parse(value)

	return err == nil && value == parsed.String()
}

func cloneStringMap(source map[string]string) map[string]string {
	if len(source) == 0 {
		return nil
	}

	result := make(map[string]string, len(source))
	maps.Copy(result, source)

	return result
}
