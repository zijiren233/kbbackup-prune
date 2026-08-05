package prune

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/labring-sigs/kbbackup-prune/internal/domain"
	"github.com/labring-sigs/kbbackup-prune/internal/ports"
)

const maxManifestBytes = 4 << 20

type PlanOptions struct {
	Repository            string
	Bucket                string
	Namespace             string
	Prefix                string
	ManifestName          string
	MinAge                time.Duration
	IncludeRetained       bool
	PurgeVersions         bool
	CaptureObjects        bool
	BucketVersioning      string
	DeleteRepositoryStray bool
}

type Planner struct {
	Store ports.ObjectStore
	Now   func() time.Time
}

func (p Planner) Build(
	ctx context.Context,
	inventory domain.Inventory,
	opts PlanOptions,
) (domain.Plan, error) {
	if p.Store == nil {
		return domain.Plan{}, errors.New("object store is required")
	}

	if opts.MinAge < 0 {
		return domain.Plan{}, errors.New("minimum age must be zero or greater")
	}

	if p.Now == nil {
		p.Now = time.Now
	}

	now := p.Now().UTC()
	cutoff := now.Add(-opts.MinAge)

	if opts.ManifestName == "" {
		opts.ManifestName = domain.DefaultManifest
	}

	if path.Base(opts.ManifestName) != opts.ManifestName {
		return domain.Plan{}, fmt.Errorf("manifest name must be a file name: %q", opts.ManifestName)
	}

	if supportsVolumeDiscovery(inventory.VolumeRoots) {
		return p.buildVolumePlan(ctx, inventory, opts, now, cutoff)
	}

	prefixes, objectPrefixes, err := resolveScanPrefixes(
		inventory.Repo,
		opts.Namespace,
		opts.Prefix,
	)
	if err != nil {
		return domain.Plan{}, err
	}

	versioning, versioningSource, err := resolveBucketVersioning(
		ctx,
		p.Store,
		opts.BucketVersioning,
	)
	if err != nil {
		return domain.Plan{}, fmt.Errorf("get bucket versioning: %w", err)
	}

	plan := domain.Plan{
		GeneratedAt:          now,
		Repository:           opts.Repository,
		RepositoryUID:        inventory.Repo.UID,
		RepositoryGeneration: inventory.Repo.Generation,
		Bucket:               opts.Bucket,
		Namespace:            opts.Namespace,
		Prefixes:             prefixes,
		ObjectPrefixes:       objectPrefixes,
		Versioning:           versioning,
		VersioningSource:     versioningSource,
		StateCounts:          make(map[domain.CandidateState]int),
		BlockingReasons:      append([]string(nil), inventory.BlockingReasons...),
	}
	if len(prefixes) == 1 {
		plan.Prefix = prefixes[0]
	}

	var markers []domain.Object
	for _, prefix := range prefixes {
		if err := p.Store.Walk(ctx, prefix, false, func(object domain.Object) error {
			if prefix != "" && !containsPrefix(prefix, object.Key) {
				return nil
			}

			plan.ScannedObjects++

			plan.ScannedBytes += object.Size
			if path.Base(object.Key) == opts.ManifestName {
				markers = append(markers, object)
			}

			return nil
		}); err != nil {
			return domain.Plan{}, fmt.Errorf("list objects under %q: %w", prefix, err)
		}
	}

	for _, marker := range markers {
		candidate := p.readCandidate(ctx, marker, opts, inventory, cutoff)
		plan.Candidates = append(plan.Candidates, candidate)
	}

	sort.Slice(plan.Candidates, func(i, j int) bool {
		return plan.Candidates[i].Prefix < plan.Candidates[j].Prefix
	})

	matcher := newCandidateMatcher(plan.Candidates)
	p.protectNestedCandidates(&plan, matcher)

	versions := opts.PurgeVersions && versioning != domain.BucketVersioningDisabled
	if err := p.scanCandidateObjects(ctx, &plan, matcher, prefixes, versions, cutoff); err != nil {
		return domain.Plan{}, err
	}

	p.protectDependencies(&plan, inventory)
	summarizePlan(&plan)

	if opts.CaptureObjects && len(plan.BlockingReasons) == 0 {
		if err := p.captureCandidateObjects(
			ctx,
			&plan,
			matcher,
			prefixes,
			versions,
			cutoff,
		); err != nil {
			return domain.Plan{}, err
		}

		p.protectDependencies(&plan, inventory)

		for i := range plan.Candidates {
			if plan.Candidates[i].State != domain.StateOrphan {
				plan.Candidates[i].Objects = nil
			}
		}

		summarizePlan(&plan)
	}

	return plan, nil
}

func resolveScanPrefixes(
	repo domain.Repository,
	namespace string,
	requested string,
) ([]string, map[string]string, error) {
	requested = cleanKey(requested)
	selectedRoots := make(map[string]string)

	if len(repo.ObjectPrefixes) > 0 {
		if namespace != "" {
			root := cleanKey(repo.ObjectPrefixes[namespace])
			if root == "" {
				return nil, nil, fmt.Errorf(
					"BackupRepo has no safely mapped object prefix for namespace %q",
					namespace,
				)
			}

			selectedRoots[namespace] = root
		} else {
			for key, value := range repo.ObjectPrefixes {
				root := cleanKey(value)
				if root == "" {
					return nil, nil, fmt.Errorf(
						"BackupRepo has an empty object prefix for namespace %q",
						key,
					)
				}

				selectedRoots[key] = root
			}
		}
	} else if repo.BackupPVCName != "" {
		return nil, nil, errors.New("BackupRepo PVC object prefixes are unavailable")
	}

	if len(selectedRoots) == 0 {
		base := cleanKey(repo.PathPrefix)
		if namespace != "" {
			base = cleanKey(path.Join(base, namespace))
		}

		if requested != "" {
			if base != "" && !containsPrefix(base, requested) {
				return nil, nil, fmt.Errorf(
					"scan prefix %q is outside BackupRepo pathPrefix %q",
					requested,
					base,
				)
			}

			return []string{requested}, nil, nil
		}

		return []string{base}, nil, nil
	}

	if requested != "" {
		for key, root := range selectedRoots {
			if containsPrefix(root, requested) {
				return []string{requested}, map[string]string{key: root}, nil
			}
		}

		return nil, nil, fmt.Errorf(
			"scan prefix %q is outside the selected BackupRepo PVC object prefixes",
			requested,
		)
	}

	prefixes := make([]string, 0, len(selectedRoots))
	for _, root := range selectedRoots {
		prefixes = append(prefixes, root)
	}

	sort.Strings(prefixes)

	return prefixes, selectedRoots, nil
}

func ValidateBucketVersioningMode(mode string) error {
	mode = strings.ToLower(strings.TrimSpace(mode))
	if mode == "" {
		mode = domain.BucketVersioningModeAuto
	}

	switch mode {
	case domain.BucketVersioningModeAuto,
		domain.BucketVersioningModeDisabled,
		domain.BucketVersioningModeEnabled,
		domain.BucketVersioningModeSuspended:
		return nil
	default:
		return fmt.Errorf(
			"invalid --bucket-versioning %q; use auto, disabled, enabled, or suspended",
			mode,
		)
	}
}

func resolveBucketVersioning(
	ctx context.Context,
	store ports.ObjectStore,
	mode string,
) (string, string, error) {
	mode = strings.ToLower(strings.TrimSpace(mode))
	if mode == "" {
		mode = domain.BucketVersioningModeAuto
	}

	if err := ValidateBucketVersioningMode(mode); err != nil {
		return "", "", err
	}

	switch mode {
	case domain.BucketVersioningModeDisabled:
		return domain.BucketVersioningDisabled, domain.BucketVersioningSourceOverride, nil
	case domain.BucketVersioningModeEnabled:
		return domain.BucketVersioningEnabled, domain.BucketVersioningSourceOverride, nil
	case domain.BucketVersioningModeSuspended:
		return domain.BucketVersioningSuspended, domain.BucketVersioningSourceOverride, nil
	}

	versioning, err := store.Versioning(ctx)
	if err != nil {
		return "", "", err
	}

	if !validBucketVersioningState(versioning) {
		return "", "", fmt.Errorf("object store returned unknown versioning state %q", versioning)
	}

	return versioning, domain.BucketVersioningSourceDetected, nil
}

func validBucketVersioningState(versioning string) bool {
	switch versioning {
	case domain.BucketVersioningDisabled,
		domain.BucketVersioningEnabled,
		domain.BucketVersioningSuspended:
		return true
	default:
		return false
	}
}

func (p Planner) readCandidate(
	ctx context.Context,
	marker domain.Object,
	opts PlanOptions,
	inventory domain.Inventory,
	cutoff time.Time,
) domain.Candidate {
	prefix := strings.TrimSuffix(marker.Key, "/"+opts.ManifestName)

	candidate := domain.Candidate{
		Kind:         domain.CandidateBackup,
		Prefix:       cleanKey(prefix),
		ManifestKey:  marker.Key,
		ManifestETag: marker.ETag,
		LastModified: marker.LastModified,
		State:        domain.StateInvalidManifest,
	}
	if strings.Trim(prefix, "/") != candidate.Prefix {
		candidate.Reason = "manifest object key is not canonical"

		return candidate
	}

	body, err := p.Store.Open(ctx, marker.Key, maxManifestBytes)
	if err != nil {
		candidate.Reason = fmt.Sprintf("read manifest: %v", err)
		return candidate
	}
	defer body.Close()

	decoder := json.NewDecoder(io.LimitReader(body, maxManifestBytes+1))

	var manifest domain.BackupManifest
	if err := decoder.Decode(&manifest); err != nil {
		candidate.Reason = fmt.Sprintf("decode manifest: %v", err)
		return candidate
	}

	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		candidate.Reason = "manifest contains trailing JSON data"

		return candidate
	}

	candidate.Backup = manifest.Key()
	candidate.UID = manifest.Metadata.UID
	candidate.CreatedAt = manifest.Metadata.CreationTimestamp
	candidate.DeletionPolicy = manifest.Spec.DeletionPolicy

	candidate.ParentBackup = manifest.Status.ParentBackup
	if candidate.ParentBackup == "" {
		candidate.ParentBackup = manifest.Spec.ParentBackupName
	}

	candidate.BaseBackup = manifest.Status.BaseBackup
	candidate.Manifest = &manifest
	objectPrefixes := inventory.Repo.ObjectPrefixes

	if root := repositoryRootForObject(
		inventory,
		manifest.Metadata.Namespace,
		candidate.Prefix,
	); root != "" {
		objectPrefixes = map[string]string{manifest.Metadata.Namespace: root}
	}

	if reason := validateManifest(
		manifest,
		candidate.Prefix,
		opts.Repository,
		objectPrefixes,
	); reason != "" {
		candidate.Reason = reason
		return candidate
	}

	if applyInventoryProtection(&candidate, inventory) {
		return candidate
	}

	if manifest.Spec.DeletionPolicy == domain.DeletionPolicyRetain && !opts.IncludeRetained {
		candidate.State = domain.StateRetained
		candidate.Reason = "embedded Backup has deletionPolicy Retain"
		return candidate
	}

	if marker.LastModified.After(cutoff) || manifest.Metadata.CreationTimestamp.IsZero() ||
		manifest.Metadata.CreationTimestamp.After(cutoff) {
		candidate.State = domain.StateTooYoung
		candidate.Reason = "manifest or backup creation time is inside the minimum-age window"
		return candidate
	}

	candidate.State = domain.StateOrphan
	candidate.Reason = "Backup CR is absent and all safety checks passed"

	return candidate
}

func applyInventoryProtection(candidate *domain.Candidate, inventory domain.Inventory) bool {
	if live, ok := inventory.Backups[candidate.Backup]; ok {
		candidate.State = domain.StateLive

		candidate.Reason = "Backup CR exists"
		if live.UID != "" && live.UID != candidate.UID {
			candidate.Reason = "Backup CR name was recreated; path remains protected"
		}

		return true
	}

	if reason, protected := inventory.ProtectedBackups[candidate.Backup]; protected {
		candidate.State = domain.StateProtected
		candidate.Reason = reason
		return true
	}

	for _, live := range inventory.Backups {
		if live.Path != "" && overlaps(candidate.Prefix, live.Path) {
			candidate.State = domain.StateLive
			candidate.Reason = "path overlaps a live Backup CR"
			return true
		}

		if live.KopiaRepoPath != "" && overlaps(candidate.Prefix, live.KopiaRepoPath) {
			candidate.State = domain.StateProtected
			candidate.Reason = "path overlaps a live Kopia repository"
			return true
		}
	}

	if protection := matchingProtection(
		candidate.Prefix,
		inventory.Protections,
	); protection != nil {
		candidate.State = domain.StateProtected
		candidate.Reason = "path overlaps storage used by " + protection.Resource
		candidate.Protection = protection
		return true
	}

	return false
}

func validateManifest(
	manifest domain.BackupManifest,
	prefix string,
	repo string,
	prefixMaps ...map[string]string,
) string {
	var objectPrefixes map[string]string
	if len(prefixMaps) > 0 {
		objectPrefixes = prefixMaps[0]
	}

	if manifest.APIVersion != domain.BackupAPIVersion || manifest.Kind != domain.BackupKind {
		return "manifest is not a KubeBlocks Backup"
	}

	if manifest.Spec.DeletionPolicy != domain.DeletionPolicyDelete &&
		manifest.Spec.DeletionPolicy != domain.DeletionPolicyRetain {
		return "manifest deletionPolicy is unsupported"
	}

	if manifest.Metadata.Namespace == "" || manifest.Metadata.Name == "" ||
		manifest.Metadata.UID == "" {
		return "manifest identity is incomplete"
	}

	if manifest.Metadata.CreationTimestamp.IsZero() {
		return "manifest creationTimestamp is missing"
	}

	if path.Base(prefix) != manifest.Metadata.Name {
		return "manifest name does not match its directory"
	}

	expectedPath := cleanKey(manifest.Status.Path)
	if len(objectPrefixes) > 0 {
		root := cleanKey(objectPrefixes[manifest.Metadata.Namespace])
		if root == "" {
			return "manifest namespace has no BackupRepo PVC object prefix"
		}

		if !containsPrefix(root, expectedPath) {
			expectedPath = cleanKey(path.Join(root, expectedPath))
		}
	}

	if expectedPath != prefix {
		return "manifest status.path does not match its directory"
	}

	if clusterUID := manifest.Metadata.Labels[domain.ClusterUIDLabel]; clusterUID != "" &&
		len(objectPrefixes) > 0 {
		root := cleanKey(objectPrefixes[manifest.Metadata.Namespace])
		relative := strings.TrimPrefix(prefix, root+"/")

		segments := splitKey(relative)
		if len(segments) < 2 {
			return "manifest path does not contain a cluster directory"
		}

		directoryUID, valid := clusterDirectoryUID(segments[1])
		if !valid || directoryUID != clusterUID {
			return "manifest cluster-uid label does not match its directory"
		}
	}

	if manifest.Repo() != repo {
		return "manifest BackupRepo does not match the selected repository"
	}

	return ""
}

func (p Planner) protectNestedCandidates(plan *domain.Plan, matcher *candidateNode) {
	matcher.protectNested(plan)
}

type candidateNode struct {
	children   map[string]*candidateNode
	candidates []int
}

func newCandidateMatcher(candidates []domain.Candidate) *candidateNode {
	root := &candidateNode{}
	for index, candidate := range candidates {
		node := root
		for _, segment := range splitKey(candidate.Prefix) {
			if node.children == nil {
				node.children = make(map[string]*candidateNode)
			}

			if node.children[segment] == nil {
				node.children[segment] = &candidateNode{}
			}

			node = node.children[segment]
		}

		node.candidates = append(node.candidates, index)
	}

	return root
}

func (n *candidateNode) match(key string) []int {
	matched := append([]int(nil), n.candidates...)

	node := n
	for _, segment := range splitKey(key) {
		node = node.children[segment]
		if node == nil {
			break
		}

		matched = append(matched, node.candidates...)
	}

	return matched
}

func (n *candidateNode) protectNested(plan *domain.Plan) bool {
	hasDescendant := false
	for _, child := range n.children {
		if child.protectNested(plan) {
			hasDescendant = true
		}
	}

	if hasDescendant {
		for _, index := range n.candidates {
			candidate := &plan.Candidates[index]
			if candidate.State == domain.StateOrphan {
				candidate.State = domain.StateProtected
				candidate.Reason = "directory contains another backup manifest"
			}
		}
	}

	return hasDescendant || len(n.candidates) > 0
}

func splitKey(key string) []string {
	key = cleanKey(key)
	if key == "" {
		return nil
	}

	return strings.Split(key, "/")
}

func (p Planner) scanCandidateObjects(
	ctx context.Context,
	plan *domain.Plan,
	matcher *candidateNode,
	prefixes []string,
	versions bool,
	cutoff time.Time,
) error {
	for _, prefix := range prefixes {
		err := p.Store.Walk(ctx, prefix, versions, func(object domain.Object) error {
			if prefix != "" && !containsPrefix(prefix, object.Key) {
				return nil
			}

			matched := matcher.match(object.Key)
			if len(matched) == 0 {
				plan.UnclassifiedObjects++
				plan.UnclassifiedBytes += object.Size
				return nil
			}

			for _, index := range matched {
				candidate := &plan.Candidates[index]
				candidate.ObjectCount++
				candidate.Bytes += object.Size
				protectYoungObject(candidate, object, cutoff)
			}

			return nil
		})
		if err != nil {
			kind := "objects"
			if versions {
				kind = "object versions"
			}

			return fmt.Errorf("list %s under %q: %w", kind, prefix, err)
		}
	}

	return nil
}

func protectYoungObject(candidate *domain.Candidate, object domain.Object, cutoff time.Time) {
	if candidate.State != domain.StateOrphan {
		return
	}

	if object.LastModified.IsZero() {
		candidate.State = domain.StateTooYoung
		candidate.Reason = "object modification time is unavailable"
		return
	}

	if object.LastModified.After(cutoff) {
		candidate.State = domain.StateTooYoung
		candidate.Reason = "backup contains an object inside the minimum-age window"
	}
}

func (p Planner) captureCandidateObjects(
	ctx context.Context,
	plan *domain.Plan,
	matcher *candidateNode,
	prefixes []string,
	versions bool,
	cutoff time.Time,
) error {
	captured := make([]bool, len(plan.Candidates))
	for i := range plan.Candidates {
		candidate := &plan.Candidates[i]
		if candidate.State != domain.StateOrphan {
			continue
		}

		captured[i] = true
		candidate.Objects = nil
		candidate.ObjectCount = 0
		candidate.Bytes = 0
	}

	for _, prefix := range prefixes {
		err := p.Store.Walk(ctx, prefix, versions, func(object domain.Object) error {
			if prefix != "" && !containsPrefix(prefix, object.Key) {
				return nil
			}

			for _, index := range matcher.match(object.Key) {
				if !captured[index] {
					continue
				}

				candidate := &plan.Candidates[index]
				candidate.Objects = append(candidate.Objects, object)
				candidate.ObjectCount++
				candidate.Bytes += object.Size
				protectYoungObject(candidate, object, cutoff)
			}

			return nil
		})
		if err != nil {
			return fmt.Errorf("capture object snapshot under %q: %w", prefix, err)
		}
	}

	return nil
}

func summarizePlan(plan *domain.Plan) {
	plan.StateCounts = make(map[domain.CandidateState]int)
	plan.DeleteObjects = 0
	plan.DeleteBytes = 0

	for i := range plan.Candidates {
		candidate := &plan.Candidates[i]

		plan.StateCounts[candidate.State]++

		if candidate.State == domain.StateOrphan {
			plan.DeleteObjects += candidate.ObjectCount
			plan.DeleteBytes += candidate.Bytes
		}
	}
}

func (p Planner) protectDependencies(plan *domain.Plan, inventory domain.Inventory) {
	protected := liveDependencyKeys(inventory.Backups)

	for {
		for i := range plan.Candidates {
			candidate := &plan.Candidates[i]
			if candidate.State == domain.StateOrphan {
				continue
			}

			addCandidateDependencies(protected, *candidate)
		}

		changed := false
		for i := range plan.Candidates {
			candidate := &plan.Candidates[i]
			if candidate.State != domain.StateOrphan {
				continue
			}

			if _, ok := protected[candidate.Backup]; ok {
				candidate.State = domain.StateDependency
				candidate.Reason = "referenced by a backup that will remain"
				changed = true
			}
		}

		if !changed {
			return
		}
	}
}

func addCandidateDependencies(protected map[domain.BackupKey]struct{}, candidate domain.Candidate) {
	if candidate.ParentBackup != "" {
		protected[domain.BackupKey{
			Namespace: candidate.Backup.Namespace,
			Name:      candidate.ParentBackup,
		}] = struct{}{}
	}

	if candidate.BaseBackup != "" {
		protected[domain.BackupKey{
			Namespace: candidate.Backup.Namespace,
			Name:      candidate.BaseBackup,
		}] = struct{}{}
	}
}

func liveDependencyKeys(backups map[domain.BackupKey]domain.Backup) map[domain.BackupKey]struct{} {
	protected := make(map[domain.BackupKey]struct{})
	for _, backup := range backups {
		if backup.ParentBackupName != "" {
			protected[domain.BackupKey{
				Namespace: backup.Key.Namespace,
				Name:      backup.ParentBackupName,
			}] = struct{}{}
		}

		if backup.BaseBackupName != "" {
			protected[domain.BackupKey{
				Namespace: backup.Key.Namespace,
				Name:      backup.BaseBackupName,
			}] = struct{}{}
		}
	}

	return protected
}
