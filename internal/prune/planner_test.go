package prune //nolint:testpackage // White-box tests exercise manifest safety helpers and failure injection.

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/labring-sigs/kbbackup-prune/internal/domain"
	"github.com/stretchr/testify/require"
)

type memoryStore struct {
	current         []domain.Object
	versions        []domain.Object
	walks           [][]domain.Object
	walkCalls       int
	bodies          map[string]string
	openErrors      map[string]error
	closeErrors     map[string]error
	versioning      string
	versioningCalls int
	listErr         error
	statErr         error
	deleteErr       error
	deleted         []domain.Object
	deleteCalls     [][]domain.Object
	deleteErrAt     int
	listMutex       sync.Mutex
	listCalls       []string
}

type closeErrorReader struct {
	io.Reader
	err error
}

func (reader closeErrorReader) Close() error {
	return reader.err
}

func (s *memoryStore) ListLevel(
	ctx context.Context,
	prefix string,
	delimiter string,
	versions bool,
) (domain.ObjectLevel, error) {
	objects, err := s.List(ctx, prefix, versions)
	if err != nil {
		return domain.ObjectLevel{}, err
	}

	level := domain.ObjectLevel{}

	prefixes := make(map[string]struct{})
	for _, object := range objects {
		remainder := strings.TrimPrefix(object.Key, prefix)
		if index := strings.Index(remainder, delimiter); index >= 0 {
			prefixes[prefix+remainder[:index+len(delimiter)]] = struct{}{}
			continue
		}

		level.Objects = append(level.Objects, object)
	}

	for value := range prefixes {
		level.Prefixes = append(level.Prefixes, value)
	}

	slices.Sort(level.Prefixes)

	return level, nil
}

func (s *memoryStore) List(
	_ context.Context,
	prefix string,
	versions bool,
) ([]domain.Object, error) {
	s.listMutex.Lock()
	s.listCalls = append(s.listCalls, prefix)
	s.listMutex.Unlock()

	if s.listErr != nil {
		return nil, s.listErr
	}

	objects := s.current
	if versions {
		objects = s.versions
	}

	var result []domain.Object
	for _, object := range objects {
		if prefix == "" || strings.HasPrefix(object.Key, prefix) {
			result = append(result, object)
		}
	}

	return result, nil
}

func (s *memoryStore) Walk(
	ctx context.Context,
	prefix string,
	versions bool,
	visit func(domain.Object) error,
) error {
	if len(s.walks) > 0 {
		index := min(s.walkCalls, len(s.walks)-1)

		s.walkCalls++

		for _, object := range s.walks[index] {
			if prefix != "" && !strings.HasPrefix(object.Key, prefix) {
				continue
			}

			if err := visit(object); err != nil {
				return err
			}
		}

		return nil
	}

	objects, err := s.List(ctx, prefix, versions)
	if err != nil {
		return err
	}

	for _, object := range objects {
		if err := visit(object); err != nil {
			return err
		}
	}

	return nil
}

func (s *memoryStore) Open(_ context.Context, key string, _ int64) (io.ReadCloser, error) {
	if err := s.openErrors[key]; err != nil {
		return nil, err
	}

	if err := s.closeErrors[key]; err != nil {
		return closeErrorReader{Reader: strings.NewReader(s.bodies[key]), err: err}, nil
	}

	return io.NopCloser(strings.NewReader(s.bodies[key])), nil
}

func (s *memoryStore) Stat(_ context.Context, key string) (domain.Object, error) {
	if s.statErr != nil {
		return domain.Object{}, s.statErr
	}

	for _, object := range s.current {
		if object.Key == key {
			return object, nil
		}
	}

	return domain.Object{}, errors.New("not found")
}

func (s *memoryStore) Delete(
	_ context.Context,
	objects []domain.Object,
) (domain.DeleteReport, error) {
	s.deleteCalls = append(s.deleteCalls, append([]domain.Object(nil), objects...))
	if s.deleteErr != nil && (s.deleteErrAt == 0 || len(s.deleteCalls) == s.deleteErrAt) {
		return domain.DeleteReport{}, s.deleteErr
	}

	s.deleted = append(s.deleted, objects...)

	return domain.DeleteReport{Deleted: append([]domain.Object(nil), objects...)}, nil
}

func (s *memoryStore) Versioning(_ context.Context) (string, error) {
	s.versioningCalls++

	if s.versioning == "error" {
		return "", errors.New("versioning failed")
	}

	if s.versioning == "" {
		return "Disabled", nil
	}

	return s.versioning, nil
}

func backupManifest(
	t *testing.T,
	repo, name, prefix string,
	created time.Time,
	policy string,
) string {
	t.Helper()

	return backupManifestForNamespace(t, repo, "ns", name, prefix, created, policy)
}

func backupManifestForNamespace(
	t *testing.T,
	repo, namespace, name, prefix string,
	created time.Time,
	policy string,
) string {
	t.Helper()

	manifest := domain.BackupManifest{
		APIVersion: "dataprotection.kubeblocks.io/v1alpha1",
		Kind:       "Backup",
	}
	manifest.Metadata.Name = name
	manifest.Metadata.Namespace = namespace
	manifest.Metadata.UID = "uid-" + name
	manifest.Metadata.CreationTimestamp = created
	manifest.Metadata.Labels = map[string]string{domain.BackupRepoLabel: repo}
	manifest.Spec.DeletionPolicy = policy
	manifest.Status.BackupRepoName = repo
	manifest.Status.Path = "/" + prefix
	body, err := json.Marshal(manifest)
	require.NoError(t, err)

	return string(body)
}

func TestPlannerUsesRepositoryPVCObjectPrefixes(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.August, 5, 12, 0, 0, 0, time.UTC)
	old := now.Add(-30 * 24 * time.Hour)
	prefix := "pvc-root/ns/cluster/component/orphan"
	marker := prefix + "/" + domain.DefaultManifest
	store := &memoryStore{
		current: []domain.Object{
			markerObject(marker, old),
			{Key: prefix + "/data", Size: 900, LastModified: old},
		},
		bodies: map[string]string{
			marker: backupManifest(
				t,
				"repo",
				"orphan",
				"ns/cluster/component/orphan",
				old,
				"Delete",
			),
		},
	}
	inventory := domain.Inventory{Repo: domain.Repository{
		Name:           "repo",
		BackupPVCName:  "pvc-repo",
		ObjectPrefixes: map[string]string{"ns": "pvc-root"},
	}}

	plan, err := (Planner{Store: store, Now: func() time.Time { return now }}).Build(
		context.Background(),
		inventory,
		PlanOptions{
			Repository:       "repo",
			Bucket:           "bucket",
			Namespace:        "ns",
			MinAge:           7 * 24 * time.Hour,
			BucketVersioning: domain.BucketVersioningModeDisabled,
		},
	)
	require.NoError(t, err)
	require.Equal(t, []string{"pvc-root"}, plan.Prefixes)
	require.Equal(t, "pvc-root", plan.Prefix)
	require.Equal(t, map[string]string{"ns": "pvc-root"}, plan.ObjectPrefixes)
	require.Len(t, plan.Candidates, 1)
	require.Equal(t, domain.StateOrphan, plan.Candidates[0].State)
	require.Equal(t, prefix, plan.Candidates[0].Prefix)
	require.Zero(t, store.versioningCalls)
}

func TestResolveScanPrefixesForRepositoryPVCs(t *testing.T) {
	t.Parallel()

	repo := domain.Repository{
		BackupPVCName: "pvc-repo",
		ObjectPrefixes: map[string]string{
			"ns-a": "pvc-a",
			"ns-b": "pvc-b",
		},
	}

	prefixes, roots, err := resolveScanPrefixes(repo, "", "")
	require.NoError(t, err)
	require.Equal(t, []string{"pvc-a", "pvc-b"}, prefixes)
	require.Equal(t, repo.ObjectPrefixes, roots)

	prefixes, roots, err = resolveScanPrefixes(repo, "ns-b", "pvc-b/ns-b/backup")
	require.NoError(t, err)
	require.Equal(t, []string{"pvc-b/ns-b/backup"}, prefixes)
	require.Equal(t, map[string]string{"ns-b": "pvc-b"}, roots)

	_, _, err = resolveScanPrefixes(repo, "ns-a", "pvc-b/ns-b/backup")
	require.ErrorContains(t, err, "outside the selected")

	_, _, err = resolveScanPrefixes(repo, "missing", "")
	require.ErrorContains(t, err, "no safely mapped object prefix")

	_, _, err = resolveScanPrefixes(
		domain.Repository{BackupPVCName: "pvc-repo"},
		"ns",
		"",
	)
	require.ErrorContains(t, err, "object prefixes are unavailable")
}

func TestPlannerScansMultipleRepositoryPVCObjectPrefixes(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.August, 5, 12, 0, 0, 0, time.UTC)
	old := now.Add(-30 * 24 * time.Hour)
	store := &memoryStore{bodies: make(map[string]string)}

	for _, item := range []struct {
		namespace string
		root      string
	}{
		{namespace: "ns-a", root: "tenant-volume-a"},
		{namespace: "ns-b", root: "unrelated-volume-b"},
	} {
		name := "orphan-" + item.namespace
		relativePrefix := item.namespace + "/cluster/component/" + name
		prefix := item.root + "/" + relativePrefix
		marker := prefix + "/" + domain.DefaultManifest
		store.current = append(
			store.current,
			markerObject(marker, old),
			domain.Object{Key: prefix + "/data", Size: 900, LastModified: old},
		)
		store.bodies[marker] = backupManifestForNamespace(
			t,
			"repo",
			item.namespace,
			name,
			relativePrefix,
			old,
			"Delete",
		)
	}

	plan, err := (Planner{Store: store, Now: func() time.Time { return now }}).Build(
		context.Background(),
		domain.Inventory{Repo: domain.Repository{
			Name:          "repo",
			BackupPVCName: "pvc-repo",
			ObjectPrefixes: map[string]string{
				"ns-a": "tenant-volume-a",
				"ns-b": "unrelated-volume-b",
			},
		}},
		PlanOptions{
			Repository:       "repo",
			Bucket:           "bucket",
			MinAge:           7 * 24 * time.Hour,
			BucketVersioning: domain.BucketVersioningModeDisabled,
		},
	)
	require.NoError(t, err)
	require.Empty(t, plan.Prefix)
	require.Equal(
		t,
		[]string{"tenant-volume-a", "unrelated-volume-b"},
		plan.Prefixes,
	)
	require.Len(t, plan.Candidates, 2)
	require.Equal(t, 4, plan.ScannedObjects)
	require.Zero(t, plan.UnclassifiedObjects)

	for _, candidate := range plan.Candidates {
		require.Equal(t, domain.StateOrphan, candidate.State)
		require.Equal(t, 2, candidate.ObjectCount)
	}
}

func TestPlannerUsesBucketVersioningOverride(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mode string
		want string
	}{
		{mode: domain.BucketVersioningModeDisabled, want: domain.BucketVersioningDisabled},
		{mode: domain.BucketVersioningModeEnabled, want: domain.BucketVersioningEnabled},
		{mode: domain.BucketVersioningModeSuspended, want: domain.BucketVersioningSuspended},
	}
	for _, test := range tests {
		t.Run(test.mode, func(t *testing.T) {
			t.Parallel()

			store := &memoryStore{versioning: "error"}
			plan, err := (Planner{Store: store}).Build(
				context.Background(),
				domain.Inventory{Repo: domain.Repository{PathPrefix: "root"}},
				PlanOptions{
					Repository:       "repo",
					Bucket:           "bucket",
					BucketVersioning: test.mode,
				},
			)
			require.NoError(t, err)
			require.Equal(t, test.want, plan.Versioning)
			require.Equal(t, domain.BucketVersioningSourceOverride, plan.VersioningSource)
			require.Zero(t, store.versioningCalls)
		})
	}
}

func manifestWithSpecParent(t *testing.T, body, parent string) string {
	t.Helper()

	var manifest domain.BackupManifest
	require.NoError(t, json.Unmarshal([]byte(body), &manifest))
	manifest.Spec.ParentBackupName = parent
	encoded, err := json.Marshal(manifest)
	require.NoError(t, err)

	return string(encoded)
}

func markerObject(key string, modified time.Time) domain.Object {
	return domain.Object{Key: key, Size: 100, LastModified: modified, ETag: "etag-" + key}
}

func TestReadCandidateRejectsManifestCloseError(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.August, 12, 12, 0, 0, 0, time.UTC)
	created := now.Add(-30 * 24 * time.Hour)
	prefix := "root/ns/cluster/component/orphan"
	marker := markerObject(prefix+"/"+domain.DefaultManifest, created)
	closeErr := errors.New("checksum validation failed")
	store := &memoryStore{
		bodies: map[string]string{
			marker.Key: backupManifest(t, "repo", "orphan", prefix, created, "Delete"),
		},
		closeErrors: map[string]error{marker.Key: closeErr},
	}

	candidate := (Planner{Store: store}).readCandidate(
		context.Background(),
		marker,
		PlanOptions{ManifestName: domain.DefaultManifest, Repository: "repo"},
		domain.Inventory{Repo: domain.Repository{Name: "repo", PathPrefix: "root"}},
		now.Add(-7*24*time.Hour),
	)
	require.Equal(t, domain.StateInvalidManifest, candidate.State)
	require.Equal(t, "close manifest: checksum validation failed", candidate.Reason)
	require.Nil(t, candidate.Manifest)
}

func TestPlannerBuildSafetyStates(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.August, 5, 12, 0, 0, 0, time.UTC)
	old := now.Add(-30 * 24 * time.Hour)
	young := now.Add(-time.Hour)
	repo := "backup-io"

	type item struct {
		name    string
		policy  string
		created time.Time
	}

	items := []item{
		{name: "orphan", policy: "Delete", created: old},
		{name: "live", policy: "Delete", created: old},
		{name: "retained", policy: "Retain", created: old},
		{name: "young", policy: "Delete", created: young},
		{name: "pvc", policy: "Delete", created: old},
		{name: "base", policy: "Delete", created: old},
		{name: "restore-source", policy: "Delete", created: old},
	}

	store := &memoryStore{bodies: make(map[string]string)}
	for _, entry := range items {
		prefix := "root/ns/cluster/component/" + entry.name
		marker := prefix + "/" + domain.DefaultManifest

		modified := old
		if entry.name == "young" {
			modified = young
		}

		store.current = append(
			store.current,
			markerObject(marker, modified),
			domain.Object{Key: prefix + "/data", Size: 900, LastModified: modified},
		)
		store.bodies[marker] = backupManifest(
			t,
			repo,
			entry.name,
			prefix,
			entry.created,
			entry.policy,
		)
	}

	invalidKey := "root/ns/cluster/component/bad/" + domain.DefaultManifest
	store.current = append(
		store.current,
		markerObject(invalidKey, old),
		domain.Object{Key: "root/unclassified", Size: 17},
	)
	store.bodies[invalidKey] = "{bad json"
	inventory := domain.Inventory{
		Repo: domain.Repository{Name: repo, PathPrefix: "root"},
		ProtectedBackups: map[domain.BackupKey]string{
			{Namespace: "ns", Name: "restore-source"}: "referenced by active Restore restore-ns/active (Running)",
		},
		Backups: map[domain.BackupKey]domain.Backup{
			{Namespace: "ns", Name: "live"}: {
				Key: domain.BackupKey{Namespace: "ns", Name: "live"}, UID: "uid-live", Repo: repo,
				Path: "root/ns/cluster/component/live",
			},
			{Namespace: "ns", Name: "child"}: {
				Key: domain.BackupKey{Namespace: "ns", Name: "child"}, ParentBackupName: "base",
			},
		},
		Protections: []domain.Protection{{
			Prefix: "root/ns/cluster/component/pvc", Kind: "pvc", Resource: "PVC ns/user-data",
		}},
	}
	planner := Planner{Store: store, Now: func() time.Time { return now }}
	plan, err := planner.Build(context.Background(), inventory, PlanOptions{
		Repository: repo, Bucket: "bucket", MinAge: 7 * 24 * time.Hour,
		CaptureObjects: true,
	})
	require.NoError(t, err)
	require.Equal(t, domain.BucketVersioningSourceDetected, plan.VersioningSource)
	require.Equal(t, 1, store.versioningCalls)

	states := make(map[string]domain.CandidateState)

	objectSnapshots := make(map[string]int)
	for _, candidate := range plan.Candidates {
		states[candidate.Backup.Name] = candidate.State
		objectSnapshots[candidate.Backup.Name] = len(candidate.Objects)
	}

	require.Equal(t, domain.StateOrphan, states["orphan"])
	require.Equal(t, domain.StateLive, states["live"])
	require.Equal(t, domain.StateRetained, states["retained"])
	require.Equal(t, domain.StateTooYoung, states["young"])
	require.Equal(t, domain.StateProtected, states["pvc"])
	require.Equal(t, domain.StateDependency, states["base"])
	require.Equal(t, domain.StateProtected, states["restore-source"])
	require.Equal(t, 1, plan.StateCounts[domain.StateInvalidManifest])
	require.Equal(t, 2, plan.DeleteObjects)
	require.EqualValues(t, 1000, plan.DeleteBytes)
	require.Equal(t, 1, plan.UnclassifiedObjects)
	require.EqualValues(t, 17, plan.UnclassifiedBytes)
	require.Equal(t, 2, objectSnapshots["orphan"])
	require.Zero(t, objectSnapshots["live"])
}

func TestPlannerIncludesRetainedAndVersions(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.August, 5, 12, 0, 0, 0, time.UTC)
	prefix := "root/ns/cluster/component/retained"
	marker := prefix + "/" + domain.DefaultManifest
	store := &memoryStore{
		versioning: "Enabled",
		bodies: map[string]string{
			marker: backupManifest(
				t,
				"repo",
				"retained",
				prefix,
				now.Add(-30*24*time.Hour),
				"Retain",
			),
		},
		current: []domain.Object{markerObject(marker, now.Add(-30*24*time.Hour))},
		versions: []domain.Object{
			{Key: marker, VersionID: "v1", Size: 100, LastModified: now.Add(-30 * 24 * time.Hour)},
			{Key: marker, VersionID: "v0", Size: 90, LastModified: now.Add(-31 * 24 * time.Hour)},
			{
				Key: prefix + "/data", VersionID: "v1", Size: 500,
				LastModified: now.Add(-30 * 24 * time.Hour),
			},
			{
				Key: prefix + "/data", VersionID: "delete", DeleteMarker: true,
				LastModified: now.Add(-29 * 24 * time.Hour),
			},
		},
	}
	plan, err := (Planner{Store: store, Now: func() time.Time { return now }}).Build(
		context.Background(),
		domain.Inventory{
			Repo:    domain.Repository{Name: "repo", PathPrefix: "root"},
			Backups: map[domain.BackupKey]domain.Backup{},
		},
		PlanOptions{
			Repository:      "repo",
			Bucket:          "bucket",
			MinAge:          24 * time.Hour,
			IncludeRetained: true,
			PurgeVersions:   true,
		},
	)
	require.NoError(t, err)
	require.Equal(t, domain.StateOrphan, plan.Candidates[0].State)
	require.Equal(t, 4, plan.DeleteObjects)
	require.EqualValues(t, 690, plan.DeleteBytes)
}

func TestPlannerUsesCapturedSnapshot(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.August, 5, 12, 0, 0, 0, time.UTC)
	old := now.Add(-30 * 24 * time.Hour)
	prefix := "root/ns/backup"
	marker := markerObject(prefix+"/"+domain.DefaultManifest, old)
	initial := []domain.Object{
		marker,
		{Key: prefix + "/data", Size: 500, LastModified: old, ETag: "data-etag"},
	}
	captured := append(append([]domain.Object(nil), initial...), domain.Object{
		Key: prefix + "/added", Size: 700, LastModified: old, ETag: "added-etag",
	})
	store := &memoryStore{
		walks: [][]domain.Object{initial, initial, captured},
		bodies: map[string]string{
			marker.Key: backupManifest(
				t, "repo", "backup", prefix, old, domain.DeletionPolicyDelete,
			),
		},
	}

	plan, err := (Planner{Store: store, Now: func() time.Time { return now }}).Build(
		context.Background(),
		domain.Inventory{Repo: domain.Repository{PathPrefix: "root"}},
		PlanOptions{
			Repository: "repo", Bucket: "bucket", MinAge: 24 * time.Hour,
			CaptureObjects: true,
		},
	)
	require.NoError(t, err)
	require.Equal(t, 3, plan.DeleteObjects)
	require.EqualValues(t, 1300, plan.DeleteBytes)
	require.Len(t, plan.Candidates[0].Objects, 3)
	require.Equal(t, plan.DeleteObjects, plan.Candidates[0].ObjectCount)
}

func TestPlannerRechecksObjectAgeDuringCapture(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.August, 5, 12, 0, 0, 0, time.UTC)
	old := now.Add(-30 * 24 * time.Hour)
	prefix := "root/ns/backup"
	marker := markerObject(prefix+"/"+domain.DefaultManifest, old)
	initial := []domain.Object{marker}
	captured := append(append([]domain.Object(nil), initial...), domain.Object{
		Key: prefix + "/added", Size: 700, LastModified: now, ETag: "added-etag",
	})
	store := &memoryStore{
		walks: [][]domain.Object{initial, initial, captured},
		bodies: map[string]string{
			marker.Key: backupManifest(
				t, "repo", "backup", prefix, old, domain.DeletionPolicyDelete,
			),
		},
	}

	plan, err := (Planner{Store: store, Now: func() time.Time { return now }}).Build(
		context.Background(),
		domain.Inventory{Repo: domain.Repository{PathPrefix: "root"}},
		PlanOptions{
			Repository: "repo", Bucket: "bucket", MinAge: 24 * time.Hour,
			CaptureObjects: true,
		},
	)
	require.NoError(t, err)
	require.Equal(t, domain.StateTooYoung, plan.Candidates[0].State)
	require.Contains(t, plan.Candidates[0].Reason, "minimum-age window")
	require.Empty(t, plan.Candidates[0].Objects)
	require.Zero(t, plan.DeleteObjects)
	require.Equal(t, 1, plan.StateCounts[domain.StateTooYoung])
}

func TestPlannerProtectsYoungAndUndatedObjects(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.August, 5, 12, 0, 0, 0, time.UTC)
	old := now.Add(-30 * 24 * time.Hour)

	tests := []struct {
		name       string
		modified   time.Time
		wantReason string
	}{
		{name: "young", modified: now.Add(-time.Hour), wantReason: "minimum-age window"},
		{name: "undated", wantReason: "modification time is unavailable"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			prefix := "root/ns/" + test.name
			marker := prefix + "/" + domain.DefaultManifest
			store := &memoryStore{
				current: []domain.Object{
					markerObject(marker, old),
					{Key: prefix + "/data", Size: 500, LastModified: test.modified},
				},
				bodies: map[string]string{
					marker: backupManifest(t, "repo", test.name, prefix, old, "Delete"),
				},
			}
			plan, err := (Planner{Store: store, Now: func() time.Time { return now }}).Build(
				context.Background(),
				domain.Inventory{
					Repo:    domain.Repository{PathPrefix: "root"},
					Backups: map[domain.BackupKey]domain.Backup{},
				},
				PlanOptions{Repository: "repo", Bucket: "bucket", MinAge: 24 * time.Hour},
			)
			require.NoError(t, err)
			require.Equal(t, domain.StateTooYoung, plan.Candidates[0].State)
			require.Contains(t, plan.Candidates[0].Reason, test.wantReason)
			require.Zero(t, plan.DeleteObjects)
		})
	}
}

func TestPlannerProtectsNestedManifest(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.August, 5, 12, 0, 0, 0, time.UTC)
	old := now.Add(-30 * 24 * time.Hour)
	parent := "root/ns/parent"
	sibling := "root/ns/parent-other"
	child := parent + "/child"
	parentMarker := parent + "/" + domain.DefaultManifest
	siblingMarker := sibling + "/" + domain.DefaultManifest
	childMarker := child + "/" + domain.DefaultManifest
	store := &memoryStore{
		current: []domain.Object{
			markerObject(parentMarker, old),
			markerObject(siblingMarker, old),
			markerObject(childMarker, old),
		},
		bodies: map[string]string{
			parentMarker:  backupManifest(t, "repo", "parent", parent, old, "Delete"),
			siblingMarker: backupManifest(t, "repo", "parent-other", sibling, old, "Delete"),
			childMarker:   backupManifest(t, "repo", "child", child, old, "Delete"),
		},
	}
	plan, err := (Planner{Store: store, Now: func() time.Time { return now }}).Build(
		context.Background(),
		domain.Inventory{
			Repo:    domain.Repository{PathPrefix: "root"},
			Backups: map[domain.BackupKey]domain.Backup{},
		},
		PlanOptions{Repository: "repo", Bucket: "bucket", MinAge: 24 * time.Hour},
	)
	require.NoError(t, err)

	states := make(map[string]domain.CandidateState)
	for _, candidate := range plan.Candidates {
		states[candidate.Backup.Name] = candidate.State
	}

	require.Equal(t, domain.StateProtected, states["parent"])
	require.Equal(t, domain.StateOrphan, states["parent-other"])
	require.Equal(t, domain.StateOrphan, states["child"])
}

func TestPlannerHonorsS3PrefixPathBoundary(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.August, 5, 12, 0, 0, 0, time.UTC)
	old := now.Add(-30 * 24 * time.Hour)
	inside := "root/ns/backup"
	outside := "root-other/ns/backup"
	insideMarker := inside + "/" + domain.DefaultManifest
	outsideMarker := outside + "/" + domain.DefaultManifest
	store := &memoryStore{
		current: []domain.Object{
			markerObject(insideMarker, old),
			{Key: inside + "/data", Size: 500, LastModified: old},
			markerObject(outsideMarker, old),
			{Key: outside + "/data", Size: 700, LastModified: old},
		},
		bodies: map[string]string{
			insideMarker: backupManifest(
				t, "repo", "backup", inside, old, domain.DeletionPolicyDelete,
			),
			outsideMarker: backupManifest(
				t, "repo", "backup", outside, old, domain.DeletionPolicyDelete,
			),
		},
	}

	plan, err := (Planner{Store: store, Now: func() time.Time { return now }}).Build(
		context.Background(),
		domain.Inventory{Repo: domain.Repository{PathPrefix: "root"}},
		PlanOptions{Repository: "repo", Bucket: "bucket", MinAge: 24 * time.Hour},
	)
	require.NoError(t, err)
	require.Len(t, plan.Candidates, 1)
	require.Equal(t, inside, plan.Candidates[0].Prefix)
	require.Equal(t, 2, plan.ScannedObjects)
	require.Equal(t, 2, plan.DeleteObjects)
}

func TestPlannerProtectsParentDeclaredInManifestSpec(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.August, 5, 12, 0, 0, 0, time.UTC)
	old := now.Add(-30 * 24 * time.Hour)
	basePrefix := "root/ns/base"
	childPrefix := "root/ns/child"
	baseMarker := basePrefix + "/" + domain.DefaultManifest
	childMarker := childPrefix + "/" + domain.DefaultManifest
	store := &memoryStore{
		current: []domain.Object{markerObject(baseMarker, old), markerObject(childMarker, old)},
		bodies: map[string]string{
			baseMarker: backupManifest(
				t, "repo", "base", basePrefix, old, domain.DeletionPolicyDelete,
			),
			childMarker: manifestWithSpecParent(
				t,
				backupManifest(
					t, "repo", "child", childPrefix, old, domain.DeletionPolicyRetain,
				),
				"base",
			),
		},
	}
	plan, err := (Planner{Store: store, Now: func() time.Time { return now }}).Build(
		context.Background(),
		domain.Inventory{
			Repo:    domain.Repository{PathPrefix: "root"},
			Backups: map[domain.BackupKey]domain.Backup{},
		},
		PlanOptions{Repository: "repo", Bucket: "bucket", MinAge: 24 * time.Hour},
	)
	require.NoError(t, err)
	require.Equal(t, domain.StateDependency, plan.Candidates[0].State)
	require.Equal(t, domain.StateRetained, plan.Candidates[1].State)
}

func TestPlannerErrors(t *testing.T) {
	t.Parallel()

	baseInventory := domain.Inventory{
		Repo:    domain.Repository{PathPrefix: "root"},
		Backups: map[domain.BackupKey]domain.Backup{},
	}

	tests := []struct {
		name    string
		store   *memoryStore
		opts    PlanOptions
		wantErr string
	}{
		{name: "store required", opts: PlanOptions{}, wantErr: "object store is required"},
		{
			name:    "bad manifest name",
			store:   &memoryStore{},
			opts:    PlanOptions{ManifestName: "dir/file"},
			wantErr: "must be a file name",
		},
		{
			name:    "outside prefix",
			store:   &memoryStore{},
			opts:    PlanOptions{Prefix: "other"},
			wantErr: "outside BackupRepo",
		},
		{
			name:    "versioning",
			store:   &memoryStore{versioning: "error"},
			opts:    PlanOptions{},
			wantErr: "versioning failed",
		},
		{
			name:    "unknown versioning mode",
			store:   &memoryStore{},
			opts:    PlanOptions{BucketVersioning: "unknown"},
			wantErr: "invalid --bucket-versioning",
		},
		{
			name:    "unknown detected versioning state",
			store:   &memoryStore{versioning: "Unknown"},
			opts:    PlanOptions{},
			wantErr: "unknown versioning state",
		},
		{
			name:    "list",
			store:   &memoryStore{listErr: errors.New("list failed")},
			opts:    PlanOptions{},
			wantErr: "list failed",
		},
		{
			name:    "negative minimum age",
			store:   &memoryStore{},
			opts:    PlanOptions{MinAge: -time.Second},
			wantErr: "minimum age must be zero or greater",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			planner := Planner{}
			if test.store != nil {
				planner.Store = test.store
			}

			_, err := planner.Build(context.Background(), baseInventory, test.opts)
			require.ErrorContains(t, err, test.wantErr)
		})
	}
}

func TestManifestValidation(t *testing.T) {
	t.Parallel()

	prefix := "root/ns/backup"
	valid := domain.BackupManifest{
		APIVersion: "dataprotection.kubeblocks.io/v1alpha1",
		Kind:       "Backup",
	}
	valid.Metadata.Namespace = "ns"
	valid.Metadata.Name = "backup"
	valid.Metadata.UID = "uid"
	valid.Metadata.CreationTimestamp = time.Now().UTC()
	valid.Spec.DeletionPolicy = domain.DeletionPolicyDelete
	valid.Status.Path = "/" + prefix
	valid.Status.BackupRepoName = "repo"
	require.Empty(t, validateManifest(valid, prefix, "repo"))

	tests := []struct {
		name   string
		mutate func(*domain.BackupManifest)
		want   string
		prefix string
	}{
		{
			name:   "kind",
			mutate: func(m *domain.BackupManifest) { m.Kind = "Secret" },
			want:   "not a KubeBlocks Backup",
		},
		{
			name:   "api version",
			mutate: func(m *domain.BackupManifest) { m.APIVersion = "dataprotection.kubeblocks.io/v2" },
			want:   "not a KubeBlocks Backup",
		},
		{
			name:   "deletion policy",
			mutate: func(m *domain.BackupManifest) { m.Spec.DeletionPolicy = "Archive" },
			want:   "deletionPolicy is unsupported",
		},
		{
			name:   "identity",
			mutate: func(m *domain.BackupManifest) { m.Metadata.UID = "" },
			want:   "identity is incomplete",
		},
		{
			name:   "timestamp",
			mutate: func(m *domain.BackupManifest) { m.Metadata.CreationTimestamp = time.Time{} },
			want:   "creationTimestamp is missing",
		},
		{name: "name", prefix: "root/ns/other", want: "name does not match"},
		{
			name:   "path",
			mutate: func(m *domain.BackupManifest) { m.Status.Path = "/other" },
			want:   "status.path does not match",
		},
		{
			name:   "repo",
			mutate: func(m *domain.BackupManifest) { m.Status.BackupRepoName = "other" },
			want:   "BackupRepo does not match",
		},
	}
	for _, test := range tests {
		manifest := valid
		if test.mutate != nil {
			test.mutate(&manifest)
		}

		candidatePrefix := prefix
		if test.prefix != "" {
			candidatePrefix = test.prefix
		}

		require.Contains(
			t,
			validateManifest(manifest, candidatePrefix, "repo"),
			test.want,
			test.name,
		)
	}

	clusterPrefix := "pvc-11111111-1111-4111-8111-111111111111/ns/test-db-aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa/component/backup"
	clusterManifest := valid
	clusterManifest.Metadata.Namespace = "ns"
	clusterManifest.Metadata.Labels = map[string]string{
		domain.ClusterUIDLabel: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
	}
	clusterManifest.Status.Path = "/ns/test-db-aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa/component/backup"
	require.Empty(t, validateManifest(clusterManifest, clusterPrefix, "repo", map[string]string{
		"ns": "pvc-11111111-1111-4111-8111-111111111111",
	}))
	clusterManifest.Metadata.Labels[domain.ClusterUIDLabel] = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
	require.Contains(t, validateManifest(clusterManifest, clusterPrefix, "repo", map[string]string{
		"ns": "pvc-11111111-1111-4111-8111-111111111111",
	}), "cluster-uid")
}

func TestPlannerRejectsNonCanonicalAndTrailingManifests(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.August, 5, 12, 0, 0, 0, time.UTC)
	old := now.Add(-30 * 24 * time.Hour)
	canonicalPrefix := "root/ns/trailing"
	canonicalMarker := canonicalPrefix + "/" + domain.DefaultManifest
	nonCanonicalMarker := "root/ns/../bad/" + domain.DefaultManifest
	store := &memoryStore{
		current: []domain.Object{
			markerObject(canonicalMarker, old),
			markerObject(nonCanonicalMarker, old),
		},
		bodies: map[string]string{
			canonicalMarker: backupManifest(
				t,
				"repo",
				"trailing",
				canonicalPrefix,
				old,
				"Delete",
			) + `{}`,
		},
	}
	plan, err := (Planner{Store: store, Now: func() time.Time { return now }}).Build(
		context.Background(),
		domain.Inventory{
			Repo:    domain.Repository{PathPrefix: "root"},
			Backups: map[domain.BackupKey]domain.Backup{},
		},
		PlanOptions{Repository: "repo", Bucket: "bucket", MinAge: 24 * time.Hour},
	)
	require.NoError(t, err)
	require.Len(t, plan.Candidates, 2)
	reasons := []string{plan.Candidates[0].Reason, plan.Candidates[1].Reason}
	require.Contains(t, reasons, "manifest object key is not canonical")
	require.Contains(t, reasons, "manifest contains trailing JSON data")
}

func TestPathHelpers(t *testing.T) {
	t.Parallel()
	require.Equal(t, "a/b", cleanKey("//a/./b/"))
	require.Equal(t, "a/b", joins("a", "b"))
	require.True(t, containsPrefix("a/b", "a/b/c"))
	require.False(t, containsPrefix("a/b", "a/bc"))
	require.True(t, overlaps("a/b", "a/b/c"))

	protection := matchingProtection("a/b/c", []domain.Protection{{Prefix: "a/b", Resource: "pvc"}})
	require.Equal(t, "pvc", protection.Resource)
}
