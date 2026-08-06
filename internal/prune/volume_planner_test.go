package prune //nolint:testpackage // White-box tests exercise volume-root topology decisions.

import (
	"context"
	"testing"
	"time"

	"github.com/labring-sigs/kbbackup-prune/internal/domain"
	"github.com/stretchr/testify/require"
)

const (
	testRepositoryRoot  = "pvc-11111111-1111-4111-8111-111111111111"
	testUserRoot        = "pvc-22222222-2222-4222-8222-222222222222"
	testOrphanRoot      = "pvc-33333333-3333-4333-8333-333333333333"
	testHistoricalRoot  = "pvc-44444444-4444-4444-8444-444444444444"
	testClusterUID      = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	testOtherClusterUID = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
)

func volumeInventory() domain.Inventory {
	clusterPrefix := testRepositoryRoot + "/dataflow-system/test-db-" + testClusterUID
	liveKey := domain.BackupKey{Namespace: "dataflow-system", Name: "live"}

	return domain.Inventory{
		Repo: domain.Repository{
			Name: "repo", BackupPVCName: "pvc-repository",
			ObjectPrefixes: map[string]string{"dataflow-system": testRepositoryRoot},
		},
		Backups: map[domain.BackupKey]domain.Backup{
			liveKey: {
				Key: liveKey, ClusterUID: testClusterUID,
				Repo: "repo", Path: clusterPrefix + "/postgresql/live",
			},
		},
		ProtectedBackups: map[domain.BackupKey]string{},
		VolumeRoots: map[string]domain.VolumeRoot{
			testRepositoryRoot: {
				Prefix:    testRepositoryRoot,
				Kind:      domain.VolumeRootRepository,
				Namespace: "dataflow-system",
				Resource:  "BackupRepo PVC dataflow-system/pvc-repository",
			},
			testUserRoot: {
				Prefix: testUserRoot, Kind: domain.VolumeRootUser,
				Namespace: "ns-user", Resource: "PVC ns-user/user-data",
			},
		},
	}
}

func volumeStore(t *testing.T, now time.Time) *memoryStore {
	t.Helper()

	old := now.Add(-30 * 24 * time.Hour)
	cluster := testRepositoryRoot + "/dataflow-system/test-db-" + testClusterUID
	backupPrefix := cluster + "/postgresql/orphan"
	manifestKey := backupPrefix + "/" + domain.DefaultManifest

	return &memoryStore{
		current: []domain.Object{
			{
				Key:          testRepositoryRoot + "/dataflow-system/loose.txt",
				Size:         1,
				LastModified: old,
				ETag:         "loose",
			},
			{Key: cluster + "/unexpected.bin", Size: 2, LastModified: old, ETag: "unexpected"},
			markerObject(manifestKey, old),
			{Key: backupPrefix + "/data", Size: 3, LastModified: old, ETag: "data"},
			{
				Key:          testRepositoryRoot + "/dataflow-system/manual/data",
				Size:         4,
				LastModified: old,
				ETag:         "manual",
			},
			{Key: testUserRoot + "/ns-user/cluster/data", Size: 5, LastModified: old, ETag: "user"},
			{
				Key:          testOrphanRoot + "/ns-user/cluster/data",
				Size:         6,
				LastModified: old,
				ETag:         "orphan",
			},
			{Key: "unrelated/top-level", Size: 7, LastModified: old, ETag: "unrelated"},
		},
		bodies: map[string]string{
			manifestKey: backupManifestForNamespace(
				t, "repo", "dataflow-system", "orphan",
				"dataflow-system/test-db-"+testClusterUID+"/postgresql/orphan", old, "Delete",
			),
		},
	}
}

func TestVolumePlannerDiscoversOrphanRootsAndProtectsStraysByDefault(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.August, 5, 12, 0, 0, 0, time.UTC)

	store := volumeStore(t, now)
	plan, err := (Planner{Store: store, Now: func() time.Time { return now }}).Build(
		context.Background(), volumeInventory(), PlanOptions{
			Repository: "repo", Bucket: "bucket", MinAge: 7 * 24 * time.Hour,
			BucketVersioning: domain.BucketVersioningModeDisabled,
		},
	)
	require.NoError(t, err)
	require.True(t, plan.VolumeDiscovery)
	require.Equal(t, &domain.VolumeRootCounts{
		Total: 3, Repository: 1, ProtectedUser: 1, Unowned: 1,
	}, plan.VolumeRootCounts)
	require.Contains(t, plan.OrphanVolumeRoots, testOrphanRoot)
	require.Equal(t, 3, plan.DeleteObjects)

	byKind := make(map[domain.CandidateKind][]domain.Candidate)
	for _, candidate := range plan.Candidates {
		byKind[candidate.Kind] = append(byKind[candidate.Kind], candidate)
	}

	require.Len(t, byKind[domain.CandidateProtectedUserVolume], 1)
	require.Equal(t, domain.StateProtected, byKind[domain.CandidateProtectedUserVolume][0].State)
	require.Len(t, byKind[domain.CandidateOrphanVolumeRoot], 1)
	require.Equal(t, domain.StateOrphan, byKind[domain.CandidateOrphanVolumeRoot][0].State)
	require.Len(t, byKind[domain.CandidateBackup], 1)
	require.Equal(t, domain.StateOrphan, byKind[domain.CandidateBackup][0].State)
	require.Empty(t, byKind[domain.CandidateOrphanClusterRoot])
	require.Len(t, byKind[domain.CandidateRepositoryStray], 3)

	for _, candidate := range byKind[domain.CandidateRepositoryStray] {
		require.Equal(t, domain.StateProtected, candidate.State)
	}

	store.listMutex.Lock()
	deferredCalls := append([]string(nil), store.listCalls...)
	store.listMutex.Unlock()
	require.Contains(t, deferredCalls, clusterPrefix()+"/")

	for _, prefix := range deferredCalls {
		require.NotEqual(t, testUserRoot, prefix)
	}
}

func TestVolumePlannerScansHistoricalRepositoryRoot(t *testing.T) {
	t.Parallel()

	const historicalRoot = "pvc-44444444-4444-4444-8444-444444444444"

	now := time.Date(2026, time.August, 5, 12, 0, 0, 0, time.UTC)
	old := now.Add(-30 * 24 * time.Hour)
	cluster := historicalRoot + "/dataflow-system/test-db-" + testClusterUID
	backupPrefix := cluster + "/postgresql/historical"
	manifestKey := backupPrefix + "/" + domain.DefaultManifest

	inventory := volumeInventory()
	inventory.VolumeRoots[historicalRoot] = domain.VolumeRoot{
		Prefix: historicalRoot, Kind: domain.VolumeRootRepository,
		Namespace: "dataflow-system", Resource: "BackupRepo PV " + historicalRoot,
	}
	store := &memoryStore{
		current: []domain.Object{
			markerObject(manifestKey, old),
			{Key: backupPrefix + "/data", Size: 10, ETag: "data", LastModified: old},
		},
		bodies: map[string]string{
			manifestKey: backupManifestForNamespace(
				t,
				"repo",
				"dataflow-system",
				"historical",
				"dataflow-system/test-db-"+testClusterUID+"/postgresql/historical",
				old,
				"Delete",
			),
		},
	}

	plan, err := (Planner{Store: store, Now: func() time.Time { return now }}).Build(
		context.Background(),
		inventory,
		PlanOptions{
			Repository: "repo", Bucket: "bucket", MinAge: 7 * 24 * time.Hour,
			BucketVersioning: domain.BucketVersioningModeDisabled,
		},
	)
	require.NoError(t, err)
	require.Equal(t, &domain.VolumeRootCounts{Total: 1, Repository: 1}, plan.VolumeRootCounts)

	candidate := candidateByKind(t, plan, domain.CandidateBackup)
	require.Equal(t, domain.StateOrphan, candidate.State)
	require.Equal(t, backupPrefix, candidate.Prefix)
}

func TestVolumePlannerCollapsesUnreferencedHistoricalRepositoryRoot(t *testing.T) {
	t.Parallel()

	const historicalRoot = "pvc-44444444-4444-4444-8444-444444444444"

	now := time.Date(2026, time.August, 5, 12, 0, 0, 0, time.UTC)
	old := now.Add(-30 * 24 * time.Hour)
	cluster := historicalClusterPrefix()
	store := &memoryStore{current: []domain.Object{
		{Key: historicalRoot + "/loose.txt", Size: 1, ETag: "root", LastModified: old},
		{
			Key:  historicalRoot + "/dataflow-system/loose.txt",
			Size: 2, ETag: "namespace", LastModified: old,
		},
		{Key: cluster + "/postgresql/old/data", Size: 3, ETag: "backup", LastModified: old},
		{
			Key:  historicalRoot + "/invalid_namespace/arbitrary/data",
			Size: 4, ETag: "invalid", LastModified: old,
		},
	}}

	plan, err := (Planner{Store: store, Now: func() time.Time { return now }}).Build(
		context.Background(), historicalRepositoryInventory(), PlanOptions{
			Repository: "repo", Bucket: "bucket", MinAge: 7 * 24 * time.Hour,
			BucketVersioning: domain.BucketVersioningModeDisabled,
		},
	)
	require.NoError(t, err)
	require.Equal(t, &domain.VolumeRootCounts{Total: 1, Repository: 1}, plan.VolumeRootCounts)
	require.Len(t, plan.Candidates, 1)

	candidate := candidateByKind(t, plan, domain.CandidateOrphanRepositoryRoot)
	require.Equal(t, historicalRoot, candidate.Prefix)
	require.Equal(t, domain.StateOrphan, candidate.State)
	require.True(t, candidate.DeferredScan)
	require.Zero(t, candidate.ObjectCount)
	require.Equal(t, 2, plan.ScannedObjects)
	require.NotContains(t, candidateKinds(plan), domain.CandidateOrphanClusterRoot)
	require.NotContains(t, candidateKinds(plan), domain.CandidateRepositoryStray)
	require.NotContains(t, candidateKinds(plan), domain.CandidateBackup)

	store.listMutex.Lock()
	listCalls := append([]string(nil), store.listCalls...)
	store.listMutex.Unlock()
	require.Contains(t, listCalls, historicalRoot+"/")
	require.Contains(t, listCalls, historicalRoot+"/dataflow-system/")
	require.NotContains(t, listCalls, cluster)
	require.NotContains(t, listCalls, cluster+"/")
}

func TestVolumePlannerKeepsReferencedHistoricalRootGranular(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.August, 5, 12, 0, 0, 0, time.UTC)
	old := now.Add(-30 * 24 * time.Hour)
	cluster := historicalClusterPrefix()
	liveKey := domain.BackupKey{Namespace: "dataflow-system", Name: "live"}
	inventory := historicalRepositoryInventory()
	inventory.Backups[liveKey] = domain.Backup{
		Key: liveKey, Repo: "repo",
		Path: "dataflow-system/test-db-" + testClusterUID + "/postgresql/live",
	}
	store := &memoryStore{current: []domain.Object{
		{Key: cluster + "/postgresql/live/data", Size: 1, ETag: "live", LastModified: old},
		{Key: cluster + "/postgresql/orphan/data", Size: 2, ETag: "orphan", LastModified: old},
	}}

	plan, err := (Planner{Store: store, Now: func() time.Time { return now }}).Build(
		context.Background(), inventory, PlanOptions{
			Repository: "repo", Bucket: "bucket", MinAge: 7 * 24 * time.Hour,
			BucketVersioning: domain.BucketVersioningModeDisabled,
		},
	)
	require.NoError(t, err)
	require.NotContains(t, candidateKinds(plan), domain.CandidateOrphanRepositoryRoot)
	require.Equal(
		t,
		domain.StateLive,
		candidateByPrefix(t, plan, cluster+"/postgresql/live").State,
	)
	require.Equal(
		t,
		domain.StateOrphan,
		candidateByPrefix(t, plan, cluster+"/postgresql/orphan").State,
	)
}

func TestVolumePlannerCollapsesHistoricalRootWithUnrelatedNamespaceBackup(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.August, 5, 12, 0, 0, 0, time.UTC)
	old := now.Add(-30 * 24 * time.Hour)
	inventory := historicalRepositoryInventory()
	key := domain.BackupKey{Namespace: "dataflow-system", Name: "other"}
	inventory.Backups[key] = domain.Backup{
		Key: key, Repo: "repo", ClusterUID: testOtherClusterUID,
		Path: clusterPrefixForUID(testOtherClusterUID) + "/postgresql/other",
	}
	store := &memoryStore{current: []domain.Object{{
		Key:  historicalClusterPrefix() + "/postgresql/old/data",
		Size: 1, ETag: "old", LastModified: old,
	}}}

	plan, err := (Planner{Store: store, Now: func() time.Time { return now }}).Build(
		context.Background(), inventory, PlanOptions{
			Repository: "repo", Bucket: "bucket",
			BucketVersioning: domain.BucketVersioningModeDisabled,
		},
	)
	require.NoError(t, err)
	require.Equal(
		t,
		domain.StateOrphan,
		candidateByKind(t, plan, domain.CandidateOrphanRepositoryRoot).State,
	)
}

func TestVolumePlannerHistoricalRootReferenceSafety(t *testing.T) {
	t.Parallel()

	const historicalRoot = "pvc-44444444-4444-4444-8444-444444444444"

	now := time.Date(2026, time.August, 5, 12, 0, 0, 0, time.UTC)
	old := now.Add(-30 * 24 * time.Hour)
	cluster := historicalClusterPrefix()
	key := domain.BackupKey{Namespace: "dataflow-system", Name: "live"}

	tests := []struct {
		name   string
		mutate func(*domain.Inventory)
	}{
		{
			name: "cluster UID",
			mutate: func(inventory *domain.Inventory) {
				inventory.Backups[key] = domain.Backup{Key: key, ClusterUID: testClusterUID}
			},
		},
		{
			name: "absolute path",
			mutate: func(inventory *domain.Inventory) {
				inventory.Backups[key] = domain.Backup{Key: key, Path: cluster + "/postgresql/live"}
			},
		},
		{
			name: "relative Kopia path",
			mutate: func(inventory *domain.Inventory) {
				inventory.Backups[key] = domain.Backup{
					Key: key,
					KopiaRepoPath: "dataflow-system/test-db-" + testClusterUID +
						"/postgresql/_kopia",
				}
			},
		},
		{
			name: "qualified current path retains historical relative path",
			mutate: func(inventory *domain.Inventory) {
				inventory.Backups[key] = domain.Backup{
					Key:        key,
					ClusterUID: testOtherClusterUID,
					Path: testRepositoryRoot + "/dataflow-system/test-db-" +
						testClusterUID + "/postgresql/live",
					RawPath: "dataflow-system/test-db-" + testClusterUID +
						"/postgresql/live",
				}
			},
		},
		{
			name: "ambiguous backup",
			mutate: func(inventory *domain.Inventory) {
				inventory.Backups[key] = domain.Backup{Key: key}
			},
		},
		{
			name: "active restore",
			mutate: func(inventory *domain.Inventory) {
				inventory.ProtectedBackups[key] = "active Restore restore/live"
			},
		},
		{
			name: "live dependency",
			mutate: func(inventory *domain.Inventory) {
				inventory.Backups[key] = domain.Backup{
					Key: key, ClusterUID: testOtherClusterUID, ParentBackupName: "base",
				}
			},
		},
		{
			name: "storage protection",
			mutate: func(inventory *domain.Inventory) {
				inventory.Protections = []domain.Protection{{
					Prefix: cluster, Kind: "pvc", Resource: "PVC dataflow-system/user",
				}}
			},
		},
		{
			name: "current pre-check root",
			mutate: func(inventory *domain.Inventory) {
				root := inventory.VolumeRoots[historicalRoot]
				root.Current = true
				inventory.VolumeRoots[historicalRoot] = root
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			inventory := historicalRepositoryInventory()
			test.mutate(&inventory)

			store := &memoryStore{current: []domain.Object{{
				Key: cluster + "/postgresql/old/data", Size: 1,
				ETag: "old", LastModified: old,
			}}}
			plan, err := (Planner{Store: store, Now: func() time.Time { return now }}).Build(
				context.Background(), inventory, PlanOptions{
					Repository: "repo", Bucket: "bucket",
					BucketVersioning: domain.BucketVersioningModeDisabled,
				},
			)
			require.NoError(t, err)
			require.NotContains(t, candidateKinds(plan), domain.CandidateOrphanRepositoryRoot)
		})
	}
}

func TestVolumePlannerProtectsPartialOrphanRepositoryRoot(t *testing.T) {
	t.Parallel()

	cluster := historicalClusterPrefix()
	store := &memoryStore{current: []domain.Object{{
		Key: cluster + "/postgresql/old/data", LastModified: time.Now().UTC(),
	}}}
	plan, err := (Planner{Store: store}).Build(
		context.Background(), historicalRepositoryInventory(), PlanOptions{
			Repository: "repo", Bucket: "bucket", Prefix: cluster + "/postgresql",
			BucketVersioning: domain.BucketVersioningModeDisabled,
		},
	)
	require.NoError(t, err)
	require.Len(t, plan.Candidates, 1)
	candidate := candidateByKind(t, plan, domain.CandidateOrphanRepositoryRoot)
	require.Equal(t, domain.StateProtected, candidate.State)
	require.True(t, candidate.DeletionConfigurable)
	require.True(t, candidate.DeferredScan)
}

func TestOrphanRepositoryRootSuppressesStraysWhenStrayDeletionEnabled(t *testing.T) {
	t.Parallel()

	const historicalRoot = "pvc-44444444-4444-4444-8444-444444444444"

	old := time.Now().UTC().Add(-30 * 24 * time.Hour)
	store := &memoryStore{current: []domain.Object{
		{Key: historicalRoot + "/loose", Size: 1, ETag: "root", LastModified: old},
		{
			Key:  historicalRoot + "/invalid_namespace/arbitrary/data",
			Size: 2, ETag: "invalid", LastModified: old,
		},
	}}
	plan, err := (Planner{Store: store}).Build(
		context.Background(), historicalRepositoryInventory(), PlanOptions{
			Repository: "repo", Bucket: "bucket", DeleteRepositoryStray: true,
			BucketVersioning: domain.BucketVersioningModeDisabled,
		},
	)
	require.NoError(t, err)
	require.Len(t, plan.Candidates, 1)
	require.Equal(
		t,
		domain.CandidateOrphanRepositoryRoot,
		plan.Candidates[0].Kind,
	)
	require.NotContains(t, candidateKinds(plan), domain.CandidateRepositoryStray)
}

func TestVolumePlannerCapturesOrphanRepositoryRootPolicies(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.August, 5, 12, 0, 0, 0, time.UTC)
	old := now.Add(-30 * 24 * time.Hour)
	young := now.Add(-time.Hour)
	cluster := historicalClusterPrefix()
	backupPrefix := cluster + "/postgresql/old"
	manifestKey := backupPrefix + "/" + domain.DefaultManifest

	tests := []struct {
		name         string
		objects      []domain.Object
		manifestBody string
		wantState    domain.CandidateState
	}{
		{
			name: "old objects", objects: []domain.Object{{
				Key: backupPrefix + "/data", Size: 1, ETag: "old", LastModified: old,
			}}, wantState: domain.StateOrphan,
		},
		{
			name: "young object", objects: []domain.Object{{
				Key: backupPrefix + "/data", Size: 1, ETag: "young", LastModified: young,
			}}, wantState: domain.StateTooYoung,
		},
		{
			name: "retained manifest",
			objects: []domain.Object{
				markerObject(manifestKey, old),
				{Key: backupPrefix + "/data", Size: 1, ETag: "old", LastModified: old},
			},
			manifestBody: backupManifestForNamespace(
				t, "repo", "dataflow-system", "old",
				"dataflow-system/test-db-"+testClusterUID+"/postgresql/old",
				old, "Retain",
			),
			wantState: domain.StateRetained,
		},
		{
			name: "invalid manifest",
			objects: []domain.Object{
				markerObject(manifestKey, old),
				{Key: backupPrefix + "/data", Size: 1, ETag: "old", LastModified: old},
			},
			manifestBody: "{]",
			wantState:    domain.StateInvalidManifest,
		},
		{
			name: "deletable manifest",
			objects: []domain.Object{
				markerObject(manifestKey, old),
				{Key: backupPrefix + "/data", Size: 1, ETag: "old", LastModified: old},
			},
			manifestBody: backupManifestForNamespace(
				t, "repo", "dataflow-system", "old",
				"dataflow-system/test-db-"+testClusterUID+"/postgresql/old",
				old, "Delete",
			),
			wantState: domain.StateOrphan,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			store := &memoryStore{
				current: test.objects,
				bodies:  map[string]string{manifestKey: test.manifestBody},
			}
			plan, err := (Planner{Store: store, Now: func() time.Time { return now }}).Build(
				context.Background(), historicalRepositoryInventory(), PlanOptions{
					Repository: "repo", Bucket: "bucket", MinAge: 7 * 24 * time.Hour,
					CaptureObjects:   true,
					BucketVersioning: domain.BucketVersioningModeDisabled,
				},
			)
			require.NoError(t, err)
			require.Len(t, plan.Candidates, 1)
			candidate := candidateByKind(t, plan, domain.CandidateOrphanRepositoryRoot)
			require.Equal(t, test.wantState, candidate.State)
			require.False(t, candidate.DeferredScan)
			require.Equal(t, len(test.objects), candidate.ObjectCount)
		})
	}
}

func TestVolumePlannerDeletesRepositoryStraysOnlyWhenEnabled(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.August, 5, 12, 0, 0, 0, time.UTC)
	store := volumeStore(t, now)
	plan, err := (Planner{Store: store, Now: func() time.Time { return now }}).Build(
		context.Background(), volumeInventory(), PlanOptions{
			Repository: "repo", Bucket: "bucket", MinAge: 7 * 24 * time.Hour,
			CaptureObjects: true, DeleteRepositoryStray: true,
			BucketVersioning: domain.BucketVersioningModeDisabled,
		},
	)
	require.NoError(t, err)

	foundClusterScope := false

	for _, candidate := range plan.Candidates {
		switch candidate.Kind {
		case domain.CandidateRepositoryStray, domain.CandidateOrphanVolumeRoot:
			require.Equal(t, domain.StateOrphan, candidate.State, candidate.Prefix)
			require.Len(t, candidate.Objects, candidate.ObjectCount, candidate.Prefix)
		}

		if candidate.Kind == domain.CandidateRepositoryStray && len(candidate.ScopeObjects) > 0 {
			foundClusterScope = true

			require.Greater(t, len(candidate.ScopeObjects), len(candidate.Objects))
		}
	}

	require.True(t, foundClusterScope)
	require.Greater(t, plan.DeleteObjects, 4)
	require.True(t, plan.DeleteRepositoryStray)
}

func TestVolumePlannerDiscoversManifestlessBackupDirectories(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.August, 5, 12, 0, 0, 0, time.UTC)
	old := now.Add(-30 * 24 * time.Hour)
	cluster := clusterPrefix()
	orphanPrefix := cluster + "/postgresql/historical"
	livePrefix := cluster + "/postgresql/live"

	store := volumeStore(t, now)
	store.current = append(store.current,
		domain.Object{Key: cluster + "/postgresql/", LastModified: old},
		domain.Object{Key: orphanPrefix + "/", LastModified: old},
		domain.Object{
			Key: orphanPrefix + "/historical.tar.zst", Size: 100,
			ETag: "historical", LastModified: old,
		},
		domain.Object{Key: livePrefix + "/", LastModified: old},
		domain.Object{
			Key: livePrefix + "/live.tar.zst", Size: 200,
			ETag: "live", LastModified: old,
		},
	)

	plan, err := (Planner{Store: store, Now: func() time.Time { return now }}).Build(
		context.Background(), volumeInventory(), PlanOptions{
			Repository: "repo", Bucket: "bucket", MinAge: 7 * 24 * time.Hour,
			BucketVersioning: domain.BucketVersioningModeDisabled,
		},
	)
	require.NoError(t, err)

	orphan := candidateByPrefix(t, plan, orphanPrefix)
	require.Equal(t, domain.CandidateBackup, orphan.Kind)
	require.Equal(t, domain.StateOrphan, orphan.State)
	require.Equal(t, domain.BackupKey{
		Namespace: "dataflow-system",
		Name:      "historical",
	}, orphan.Backup)
	require.Empty(t, orphan.ManifestKey)
	require.Equal(t, 2, orphan.ObjectCount)
	require.Equal(t, int64(100), orphan.Bytes)
	require.Equal(t, old, orphan.CreatedAt)

	live := candidateByPrefix(t, plan, livePrefix)
	require.Equal(t, domain.StateLive, live.State)
	require.Equal(t, "Backup CR exists", live.Reason)
	require.Empty(t, live.ManifestKey)

	clusterStray := candidateByPrefixAndKind(
		t,
		plan,
		cluster,
		domain.CandidateRepositoryStray,
	)
	require.Equal(t, 1, clusterStray.ObjectCount)
	require.Equal(t, int64(2), clusterStray.Bytes)
}

func TestVolumePlannerOmitsBackupDirectoriesOutsideRequestedPrefix(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.August, 5, 12, 0, 0, 0, time.UTC)
	old := now.Add(-30 * 24 * time.Hour)
	cluster := clusterPrefix()
	selected := cluster + "/postgresql/selected"
	outside := cluster + "/postgresql/outside"
	store := volumeStore(t, now)
	store.current = append(store.current,
		domain.Object{Key: selected + "/data", LastModified: old},
		domain.Object{Key: outside + "/data", LastModified: old},
	)

	plan, err := (Planner{Store: store, Now: func() time.Time { return now }}).Build(
		context.Background(), volumeInventory(), PlanOptions{
			Repository: "repo", Bucket: "bucket", Prefix: selected,
			BucketVersioning: domain.BucketVersioningModeDisabled,
		},
	)
	require.NoError(t, err)
	require.Equal(t, selected, candidateByPrefix(t, plan, selected).Prefix)

	for _, candidate := range plan.Candidates {
		require.NotEqual(t, outside, candidate.Prefix)
	}
}

func TestVolumePlannerProtectsManifestlessBackupSafetyConditions(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.August, 5, 12, 0, 0, 0, time.UTC)
	old := now.Add(-30 * 24 * time.Hour)
	prefix := clusterPrefix() + "/postgresql/historical"
	key := domain.BackupKey{Namespace: "dataflow-system", Name: "historical"}

	tests := []struct {
		name      string
		inventory func() domain.Inventory
		modified  time.Time
		prefix    string
		wantState domain.CandidateState
	}{
		{
			name:      "minimum age",
			inventory: volumeInventory,
			modified:  now,
			wantState: domain.StateTooYoung,
		},
		{
			name: "active restore",
			inventory: func() domain.Inventory {
				inventory := volumeInventory()
				inventory.ProtectedBackups[key] = "active Restore restore/historical"

				return inventory
			},
			modified:  old,
			wantState: domain.StateProtected,
		},
		{
			name: "live incremental dependency",
			inventory: func() domain.Inventory {
				inventory := volumeInventory()
				liveKey := domain.BackupKey{Namespace: "dataflow-system", Name: "live"}
				live := inventory.Backups[liveKey]
				live.ParentBackupName = key.Name
				inventory.Backups[liveKey] = live

				return inventory
			},
			modified:  old,
			wantState: domain.StateDependency,
		},
		{
			name:      "partial requested prefix",
			inventory: volumeInventory,
			modified:  old,
			prefix:    prefix + "/historical.tar.zst",
			wantState: domain.StateProtected,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			store := volumeStore(t, now)
			store.current = append(store.current, domain.Object{
				Key: prefix + "/historical.tar.zst", Size: 100,
				ETag: "historical", LastModified: test.modified,
			})

			plan, err := (Planner{Store: store, Now: func() time.Time { return now }}).Build(
				context.Background(), test.inventory(), PlanOptions{
					Repository: "repo", Bucket: "bucket", Prefix: test.prefix,
					MinAge:           7 * 24 * time.Hour,
					BucketVersioning: domain.BucketVersioningModeDisabled,
				},
			)
			require.NoError(t, err)
			require.Equal(t, test.wantState, candidateByPrefix(t, plan, prefix).State)
		})
	}
}

func TestVolumePlannerCapturesManifestlessBackupVersions(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.August, 5, 12, 0, 0, 0, time.UTC)
	old := now.Add(-30 * 24 * time.Hour)
	prefix := clusterPrefix() + "/postgresql/historical"
	store := volumeStore(t, now)
	store.current = append(store.current, domain.Object{
		Key: prefix + "/historical.tar.zst", Size: 100,
		ETag: "current", VersionID: "current", LastModified: old,
	})
	store.versions = append(append([]domain.Object(nil), store.current...), domain.Object{
		Key: prefix + "/historical.tar.zst", Size: 90,
		ETag: "previous", VersionID: "previous", LastModified: old.Add(-24 * time.Hour),
	})

	plan, err := (Planner{Store: store, Now: func() time.Time { return now }}).Build(
		context.Background(), volumeInventory(), PlanOptions{
			Repository: "repo", Bucket: "bucket", MinAge: 7 * 24 * time.Hour,
			CaptureObjects: true, PurgeVersions: true,
			BucketVersioning: domain.BucketVersioningModeEnabled,
		},
	)
	require.NoError(t, err)

	candidate := candidateByPrefix(t, plan, prefix)
	require.Equal(t, domain.StateOrphan, candidate.State)
	require.Len(t, candidate.Objects, 2)
	require.Equal(t, 2, candidate.ObjectCount)
	require.Equal(t, int64(190), candidate.Bytes)
}

func TestBackupDirectoryPrefix(t *testing.T) {
	t.Parallel()

	cluster := clusterPrefix()
	tests := []struct {
		name   string
		key    string
		prefix string
		ok     bool
	}{
		{
			name: "backup object", key: cluster + "/postgresql/backup/data.tar.zst",
			prefix: cluster + "/postgresql/backup", ok: true,
		},
		{name: "cluster root", key: cluster + "/", ok: false},
		{name: "component marker", key: cluster + "/postgresql/", ok: false},
		{name: "backup marker", key: cluster + "/postgresql/backup/", ok: false},
		{name: "invalid component", key: cluster + "/_meta/backup/data", ok: false},
		{name: "invalid backup name", key: cluster + "/postgresql/_kopia/data", ok: false},
		{
			name: "sibling cluster",
			key: testRepositoryRoot + "/dataflow-system/other-" + testOtherClusterUID +
				"/postgresql/backup/data",
			ok: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			prefix, ok := backupDirectoryPrefix(cluster, test.key)
			require.Equal(t, test.ok, ok)
			require.Equal(t, test.prefix, prefix)
		})
	}
}

func TestVolumePlannerNamespaceProtectsUnownedRoot(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.August, 5, 12, 0, 0, 0, time.UTC)
	store := volumeStore(t, now)
	plan, err := (Planner{Store: store, Now: func() time.Time { return now }}).Build(
		context.Background(), volumeInventory(), PlanOptions{
			Repository: "repo", Bucket: "bucket", Namespace: "dataflow-system",
			MinAge: 7 * 24 * time.Hour, BucketVersioning: domain.BucketVersioningModeDisabled,
		},
	)
	require.NoError(t, err)

	for _, candidate := range plan.Candidates {
		if candidate.Kind == domain.CandidateOrphanVolumeRoot {
			require.Equal(t, domain.StateProtected, candidate.State)
			require.Contains(t, candidate.Reason, "namespace filtering")
		}
	}
}

func TestVolumePlannerDiscoversVersionOnlyVolumeRoot(t *testing.T) {
	t.Parallel()

	const root = "pvc-55555555-5555-4555-8555-555555555555"

	now := time.Date(2026, time.August, 5, 12, 0, 0, 0, time.UTC)
	old := now.Add(-30 * 24 * time.Hour)
	store := &memoryStore{versions: []domain.Object{
		{Key: root + "/ns/cluster/data", VersionID: "v1", Size: 10, LastModified: old},
		{
			Key: root + "/ns/cluster/data", VersionID: "delete-v2", DeleteMarker: true,
			LastModified: old.Add(time.Hour),
		},
	}}
	plan, err := (Planner{Store: store, Now: func() time.Time { return now }}).Build(
		context.Background(), volumeInventory(), PlanOptions{
			Repository: "repo", Bucket: "bucket", MinAge: 7 * 24 * time.Hour,
			CaptureObjects: true, PurgeVersions: true,
			BucketVersioning: domain.BucketVersioningModeEnabled,
		},
	)
	require.NoError(t, err)
	candidate := candidateByPrefix(t, plan, root)
	require.Equal(t, domain.CandidateOrphanVolumeRoot, candidate.Kind)
	require.Equal(t, domain.StateOrphan, candidate.State)
	require.Equal(t, 2, candidate.ObjectCount)
	require.EqualValues(t, 10, candidate.Bytes)
}

func TestVolumePlannerRejectsPrefixOutsideDiscoveredRoots(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.August, 5, 12, 0, 0, 0, time.UTC)
	_, err := (Planner{Store: volumeStore(t, now), Now: func() time.Time { return now }}).Build(
		context.Background(), volumeInventory(), PlanOptions{
			Repository: "repo", Bucket: "bucket", Prefix: "other/root",
			BucketVersioning: domain.BucketVersioningModeDisabled,
		},
	)
	require.ErrorContains(t, err, "does not overlap")
}

func TestVolumePlannerProtectsYoungAndPartiallySelectedOrphanRoots(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.August, 5, 12, 0, 0, 0, time.UTC)

	store := volumeStore(t, now)
	for i := range store.current {
		if containsPrefix(testOrphanRoot, store.current[i].Key) {
			store.current[i].LastModified = now
		}
	}

	plan, err := (Planner{Store: store, Now: func() time.Time { return now }}).Build(
		context.Background(), volumeInventory(), PlanOptions{
			Repository: "repo", Bucket: "bucket", MinAge: 7 * 24 * time.Hour,
			BucketVersioning: domain.BucketVersioningModeDisabled,
		},
	)
	require.NoError(t, err)
	require.Equal(
		t,
		domain.StateTooYoung,
		candidateByKind(t, plan, domain.CandidateOrphanVolumeRoot).State,
	)

	plan, err = (Planner{Store: volumeStore(t, now), Now: func() time.Time { return now }}).Build(
		context.Background(), volumeInventory(), PlanOptions{
			Repository: "repo", Bucket: "bucket", Prefix: testOrphanRoot + "/ns-user",
			MinAge: 7 * 24 * time.Hour, BucketVersioning: domain.BucketVersioningModeDisabled,
		},
	)
	require.NoError(t, err)
	partial := candidateByKind(t, plan, domain.CandidateOrphanVolumeRoot)
	require.Equal(t, domain.StateProtected, partial.State)
	require.Contains(t, partial.Reason, "only part")
}

func TestVolumePlannerRecordsUnsupportedMixedRepositoryRoot(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.August, 5, 12, 0, 0, 0, time.UTC)
	inventory := volumeInventory()
	inventory.VolumeRoots["custom-repository-root"] = domain.VolumeRoot{
		Prefix: "custom-repository-root", Kind: domain.VolumeRootRepository,
		Resource: "BackupRepo PVC ns/custom",
	}
	plan, err := (Planner{Store: volumeStore(t, now), Now: func() time.Time { return now }}).Build(
		context.Background(), inventory, PlanOptions{
			Repository: "repo", Bucket: "bucket",
			BucketVersioning: domain.BucketVersioningModeDisabled,
		},
	)
	require.NoError(t, err)
	require.Contains(
		t,
		plan.BlockingReasons,
		`BackupRepo volume root "custom-repository-root" is not a canonical pvc-UUID prefix`,
	)
}

func TestRepositoryStrayProtectionAndRequestedFiltering(t *testing.T) {
	t.Parallel()

	object := domain.Object{Key: "root/ns/cluster/loose"}

	tests := []struct {
		name      string
		inventory domain.Inventory
		reason    string
	}{
		{
			name: "live path",
			inventory: domain.Inventory{Backups: map[domain.BackupKey]domain.Backup{
				{Namespace: "ns", Name: "live"}: {Path: "root/ns/cluster"},
			}},
			reason: "live Backup CR path",
		},
		{
			name: "kopia path",
			inventory: domain.Inventory{Backups: map[domain.BackupKey]domain.Backup{
				{Namespace: "ns", Name: "live"}: {KopiaRepoPath: "root/ns/cluster"},
			}},
			reason: "Kopia",
		},
		{
			name: "PVC protection",
			inventory: domain.Inventory{Protections: []domain.Protection{{
				Prefix: "root/ns/cluster", Resource: "PVC ns/data",
			}}},
			reason: "PVC ns/data",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			candidate := domain.Candidate{
				Kind: domain.CandidateRepositoryStray, State: domain.StateOrphan,
				Objects: []domain.Object{object},
			}
			protectRepositoryStray(&candidate, test.inventory)
			require.Equal(t, domain.StateProtected, candidate.State)
			require.Contains(t, candidate.Reason, test.reason)
		})
	}

	filtered := filterRequestedObjects(
		[]domain.Object{{Key: "root/ns/a"}, {Key: "root/other/b"}},
		"root/ns",
	)
	require.Equal(t, []domain.Object{{Key: "root/ns/a"}}, filtered)
	require.Len(t, filterRequestedObjects(filtered, ""), 1)
}

func TestRepositoryStrayProtectsUncertainClusterReferences(t *testing.T) {
	t.Parallel()

	key := domain.BackupKey{Namespace: "dataflow-system", Name: "live"}
	tests := []struct {
		name      string
		inventory domain.Inventory
		wantState domain.CandidateState
	}{
		{
			name: "matching UID without path",
			inventory: inventoryWithBackup(key, domain.Backup{
				ClusterUID: testClusterUID,
			}),
			wantState: domain.StateProtected,
		},
		{
			name:      "ambiguous Backup",
			inventory: inventoryWithBackup(key, domain.Backup{}),
			wantState: domain.StateProtected,
		},
		{
			name: "Kopia path without primary path",
			inventory: inventoryWithBackup(key, domain.Backup{
				KopiaRepoPath: clusterPrefix() + "/postgresql/_kopia",
			}),
			wantState: domain.StateProtected,
		},
		{
			name: "active Restore",
			inventory: func() domain.Inventory {
				inventory := inventoryWithoutClusterBackups()
				inventory.ProtectedBackups[key] = "active Restore"

				return inventory
			}(),
			wantState: domain.StateProtected,
		},
		{
			name: "live dependency",
			inventory: inventoryWithBackup(key, domain.Backup{
				ClusterUID:       testOtherClusterUID,
				ParentBackupName: "base",
			}),
			wantState: domain.StateProtected,
		},
		{
			name: "fully located Backup",
			inventory: inventoryWithBackup(key, domain.Backup{
				ClusterUID: testClusterUID,
				Path:       clusterPrefix() + "/postgresql/live",
			}),
			wantState: domain.StateOrphan,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			candidate := domain.Candidate{
				Kind: domain.CandidateRepositoryStray, State: domain.StateOrphan,
				Objects: []domain.Object{{Key: clusterPrefix() + "/unexpected"}},
			}
			protectRepositoryStray(&candidate, test.inventory)
			require.Equal(t, test.wantState, candidate.State, candidate.Reason)
		})
	}
}

func TestVolumePlannerIgnoresRepositoryDirectoryMarkers(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.August, 5, 12, 0, 0, 0, time.UTC)
	old := now.Add(-30 * 24 * time.Hour)
	store := &memoryStore{current: []domain.Object{
		{Key: testRepositoryRoot + "/", LastModified: old},
		{Key: testRepositoryRoot + "/dataflow-system/", LastModified: old},
	}}
	plan, err := (Planner{Store: store, Now: func() time.Time { return now }}).Build(
		context.Background(), volumeInventory(), PlanOptions{
			Repository: "repo", Bucket: "bucket",
			BucketVersioning: domain.BucketVersioningModeDisabled,
		},
	)
	require.NoError(t, err)
	require.Empty(t, plan.Candidates)
	require.Equal(t, 2, plan.ScannedObjects)
}

func TestVolumePlannerExcludesDirectoryMarkersFromStrayCandidates(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.August, 5, 12, 0, 0, 0, time.UTC)
	old := now.Add(-30 * 24 * time.Hour)
	store := volumeStore(t, now)
	store.current = append(store.current,
		domain.Object{Key: testRepositoryRoot + "/", LastModified: old},
		domain.Object{Key: testRepositoryRoot + "/loose-root", Size: 1, LastModified: old},
		domain.Object{Key: testRepositoryRoot + "/dataflow-system/", LastModified: old},
		domain.Object{Key: clusterPrefix() + "/", LastModified: old},
	)
	plan, err := (Planner{Store: store, Now: func() time.Time { return now }}).Build(
		context.Background(), volumeInventory(), PlanOptions{
			Repository: "repo", Bucket: "bucket", MinAge: 7 * 24 * time.Hour,
			BucketVersioning: domain.BucketVersioningModeDisabled,
		},
	)
	require.NoError(t, err)

	counts := make(map[string]int)
	for _, candidate := range plan.Candidates {
		if candidate.Kind == domain.CandidateRepositoryStray {
			counts[candidate.Prefix] = candidate.ObjectCount
		}
	}

	require.Equal(t, 1, counts[testRepositoryRoot])
	require.Equal(t, 1, counts[testRepositoryRoot+"/dataflow-system"])
	require.Equal(t, 1, counts[clusterPrefix()])
}

func TestFilterDirectoryMarkers(t *testing.T) {
	t.Parallel()

	objects := []domain.Object{
		{Key: "root/", Size: 0},
		{Key: "root/empty", Size: 0},
		{Key: "root/non-empty/", Size: 1},
	}
	require.Equal(t, objects[1:], filterDirectoryMarkers(objects))
}

func TestSupportsVolumeDiscoveryRequiresCanonicalRepositoryRoot(t *testing.T) {
	t.Parallel()

	require.False(t, supportsVolumeDiscovery(map[string]domain.VolumeRoot{
		testUserRoot: {Prefix: testUserRoot, Kind: domain.VolumeRootUser},
	}))
	require.False(t, supportsVolumeDiscovery(map[string]domain.VolumeRoot{
		"custom-root": {Prefix: "custom-root", Kind: domain.VolumeRootRepository},
	}))
	require.True(t, supportsVolumeDiscovery(volumeInventory().VolumeRoots))
}

func TestVolumePlannerDiscoversUnreferencedClusterBackups(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.August, 5, 12, 0, 0, 0, time.UTC)
	store := volumeStore(t, now)
	plan, err := (Planner{Store: store, Now: func() time.Time { return now }}).Build(
		context.Background(), inventoryWithoutClusterBackups(), PlanOptions{
			Repository: "repo", Bucket: "bucket", MinAge: 7 * 24 * time.Hour,
			BucketVersioning: domain.BucketVersioningModeDisabled,
		},
	)
	require.NoError(t, err)

	candidate := candidateByPrefix(t, plan, clusterPrefix()+"/postgresql/orphan")
	require.Equal(t, domain.CandidateBackup, candidate.Kind)
	require.Equal(t, domain.StateOrphan, candidate.State)
	require.False(t, candidate.DeferredScan)
	require.Equal(t, 2, candidate.ObjectCount)
	require.Empty(t, candidate.Objects)
	require.NotContains(t, candidateKinds(plan), domain.CandidateOrphanClusterRoot)
	require.Contains(t, store.listCalls, clusterPrefix())
}

func TestVolumePlannerCapturesUnreferencedClusterForDeletion(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.August, 5, 12, 0, 0, 0, time.UTC)
	store := volumeStore(t, now)
	plan, err := (Planner{Store: store, Now: func() time.Time { return now }}).Build(
		context.Background(), inventoryWithoutClusterBackups(), PlanOptions{
			Repository: "repo", Bucket: "bucket", MinAge: 7 * 24 * time.Hour,
			CaptureObjects: true, BucketVersioning: domain.BucketVersioningModeDisabled,
		},
	)
	require.NoError(t, err)

	candidate := candidateByPrefix(t, plan, clusterPrefix()+"/postgresql/orphan")
	require.Equal(t, domain.CandidateBackup, candidate.Kind)
	require.Equal(t, domain.StateOrphan, candidate.State)
	require.False(t, candidate.DeferredScan)
	require.Len(t, candidate.Objects, 2)
	require.Equal(t, 2, candidate.ObjectCount)
	require.Contains(t, plan.StateCounts, domain.StateOrphan)
}

func TestVolumePlannerSplitsOldAndYoungBackupsWithinOrphanCluster(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.August, 5, 12, 0, 0, 0, time.UTC)
	cluster := clusterPrefix()
	oldPrefix := cluster + "/postgresql/orphan"
	youngPrefix := cluster + "/postgresql/young"
	youngManifest := youngPrefix + "/" + domain.DefaultManifest
	store := volumeStore(t, now)
	store.current = append(store.current,
		domain.Object{
			Key:          youngManifest,
			Size:         100,
			ETag:         "young-manifest",
			LastModified: now.Add(-time.Hour),
		},
		domain.Object{
			Key:          youngPrefix + "/data",
			Size:         200,
			ETag:         "young-data",
			LastModified: now.Add(-time.Hour),
		},
	)
	store.bodies[youngManifest] = backupManifestForNamespace(
		t,
		"repo",
		"dataflow-system",
		"young",
		"dataflow-system/test-db-"+testClusterUID+"/postgresql/young",
		now.Add(-time.Hour), domain.DeletionPolicyDelete,
	)

	plan, err := (Planner{Store: store, Now: func() time.Time { return now }}).Build(
		context.Background(), inventoryWithoutClusterBackups(), PlanOptions{
			Repository: "repo", Bucket: "bucket", MinAge: 7 * 24 * time.Hour,
			CaptureObjects: true, BucketVersioning: domain.BucketVersioningModeDisabled,
		},
	)
	require.NoError(t, err)

	oldCandidate := candidateByPrefix(t, plan, oldPrefix)
	require.Equal(t, domain.CandidateBackup, oldCandidate.Kind)
	require.Equal(t, domain.StateOrphan, oldCandidate.State)
	require.Equal(t, 2, oldCandidate.ObjectCount)

	youngCandidate := candidateByPrefix(t, plan, youngPrefix)
	require.Equal(t, domain.CandidateBackup, youngCandidate.Kind)
	require.Equal(t, domain.StateTooYoung, youngCandidate.State)
	require.Equal(t, 2, youngCandidate.ObjectCount)
	stray := candidateByPrefixAndKind(t, plan, cluster, domain.CandidateRepositoryStray)
	require.Equal(t, domain.StateProtected, stray.State)
	require.Contains(t, stray.Reason, "enable --delete-repository-stray")

	require.NotContains(t, candidateKinds(plan), domain.CandidateOrphanClusterRoot)
	// The plan also contains the independent unowned volume-root candidate.
	require.Equal(t, oldCandidate.ObjectCount+1, plan.DeleteObjects)
}

func TestExecutorDeletesOldBackupAndKeepsYoungSiblingInOrphanCluster(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.August, 5, 12, 0, 0, 0, time.UTC)
	cluster := clusterPrefix()
	youngPrefix := cluster + "/postgresql/young"
	youngManifest := youngPrefix + "/" + domain.DefaultManifest
	store := volumeStore(t, now)

	filtered := make([]domain.Object, 0, len(store.current))
	for _, object := range store.current {
		if !containsPrefix(testOrphanRoot, object.Key) {
			filtered = append(filtered, object)
		}
	}

	filtered = append(filtered,
		domain.Object{
			Key:          youngManifest,
			Size:         100,
			ETag:         "young-manifest",
			LastModified: now.Add(-time.Hour),
		},
		domain.Object{
			Key:          youngPrefix + "/data",
			Size:         200,
			ETag:         "young-data",
			LastModified: now.Add(-time.Hour),
		},
	)
	store.current = filtered
	store.bodies[youngManifest] = backupManifestForNamespace(
		t,
		"repo",
		"dataflow-system",
		"young",
		"dataflow-system/test-db-"+testClusterUID+"/postgresql/young",
		now.Add(-time.Hour), domain.DeletionPolicyDelete,
	)

	plan, err := (Planner{Store: store, Now: func() time.Time { return now }}).Build(
		context.Background(), inventoryWithoutClusterBackups(), PlanOptions{
			Repository: "repo", Bucket: "bucket", MinAge: 7 * 24 * time.Hour,
			CaptureObjects: true, BucketVersioning: domain.BucketVersioningModeDisabled,
		},
	)
	require.NoError(t, err)

	execution, err := (Executor{
		Kube:  &fakeKube{inventory: inventoryWithoutClusterBackups()},
		Store: store,
	}).Run(context.Background(), plan, ExecuteOptions{Concurrency: 1})
	require.NoError(t, err)
	require.Equal(t, 2, execution.Results[0].ObjectsDeleted)
	require.Len(t, store.deleted, 2)

	for _, object := range store.deleted {
		require.False(t, containsPrefix(youngPrefix, object.Key))
	}
}

func TestVolumePlannerCapturesUnreferencedClusterVersions(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.August, 5, 12, 0, 0, 0, time.UTC)
	store := volumeStore(t, now)
	store.versions = append(
		append([]domain.Object(nil), store.current...),
		domain.Object{
			Key:  clusterPrefix() + "/postgresql/orphan/data",
			Size: 3, ETag: "historical", VersionID: "old",
			LastModified: now.Add(-60 * 24 * time.Hour),
		},
	)
	plan, err := (Planner{Store: store, Now: func() time.Time { return now }}).Build(
		context.Background(), inventoryWithoutClusterBackups(), PlanOptions{
			Repository: "repo", Bucket: "bucket", MinAge: 7 * 24 * time.Hour,
			CaptureObjects: true, PurgeVersions: true,
			BucketVersioning: domain.BucketVersioningModeEnabled,
		},
	)
	require.NoError(t, err)

	candidate := candidateByPrefix(t, plan, clusterPrefix()+"/postgresql/orphan")
	require.Equal(t, domain.StateOrphan, candidate.State)
	require.False(t, candidate.DeferredScan)
	require.Len(t, candidate.Objects, 3)
}

func TestVolumePlannerProtectsUnreferencedClusterContents(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.August, 5, 12, 0, 0, 0, time.UTC)
	old := now.Add(-30 * 24 * time.Hour)
	manifestKey := clusterPrefix() + "/postgresql/orphan/" + domain.DefaultManifest

	tests := []struct {
		name            string
		body            string
		includeRetained bool
		wantState       domain.CandidateState
	}{
		{
			name: "retained manifest",
			body: backupManifestForNamespace(
				t, "repo", "dataflow-system", "orphan",
				"dataflow-system/test-db-"+testClusterUID+"/postgresql/orphan",
				old, domain.DeletionPolicyRetain,
			),
			wantState: domain.StateRetained,
		},
		{
			name: "retained explicitly included",
			body: backupManifestForNamespace(
				t, "repo", "dataflow-system", "orphan",
				"dataflow-system/test-db-"+testClusterUID+"/postgresql/orphan",
				old, domain.DeletionPolicyRetain,
			),
			includeRetained: true,
			wantState:       domain.StateOrphan,
		},
		{name: "invalid manifest", body: "{", wantState: domain.StateInvalidManifest},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			store := volumeStore(t, now)
			store.bodies[manifestKey] = test.body
			plan, err := (Planner{Store: store, Now: func() time.Time { return now }}).Build(
				context.Background(), inventoryWithoutClusterBackups(), PlanOptions{
					Repository: "repo", Bucket: "bucket", MinAge: 7 * 24 * time.Hour,
					CaptureObjects: true, IncludeRetained: test.includeRetained,
					BucketVersioning: domain.BucketVersioningModeDisabled,
				},
			)
			require.NoError(t, err)
			require.Equal(
				t,
				test.wantState,
				candidateByPrefix(t, plan, clusterPrefix()+"/postgresql/orphan").State,
			)
		})
	}
}

func TestVolumePlannerProtectsYoungAndPartiallySelectedClusterRoot(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.August, 5, 12, 0, 0, 0, time.UTC)
	store := volumeStore(t, now)

	for i := range store.current {
		if store.current[i].Key == clusterPrefix()+"/postgresql/orphan/data" {
			store.current[i].LastModified = now
			break
		}
	}

	plan, err := (Planner{Store: store, Now: func() time.Time { return now }}).Build(
		context.Background(), inventoryWithoutClusterBackups(), PlanOptions{
			Repository: "repo", Bucket: "bucket", MinAge: 7 * 24 * time.Hour,
			CaptureObjects: true, BucketVersioning: domain.BucketVersioningModeDisabled,
		},
	)
	require.NoError(t, err)
	require.Equal(
		t,
		domain.StateTooYoung,
		candidateByPrefix(t, plan, clusterPrefix()+"/postgresql/orphan").State,
	)

	plan, err = (Planner{Store: volumeStore(t, now), Now: func() time.Time { return now }}).Build(
		context.Background(), inventoryWithoutClusterBackups(), PlanOptions{
			Repository: "repo", Bucket: "bucket",
			Prefix:           clusterPrefix() + "/postgresql",
			BucketVersioning: domain.BucketVersioningModeDisabled,
		},
	)
	require.NoError(t, err)
	partial := candidateByKind(t, plan, domain.CandidateOrphanClusterRoot)
	require.Equal(t, domain.StateProtected, partial.State)
	require.Contains(t, partial.Reason, "only part")
}

func TestVolumePlannerProtectsClusterRootUsedByPVC(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.August, 5, 12, 0, 0, 0, time.UTC)
	inventory := inventoryWithoutClusterBackups()
	inventory.Protections = []domain.Protection{{
		Prefix: clusterPrefix(), Kind: "pvc", Resource: "PVC dataflow-system/user-data",
	}}
	store := volumeStore(t, now)
	plan, err := (Planner{Store: store, Now: func() time.Time { return now }}).Build(
		context.Background(), inventory, PlanOptions{
			Repository: "repo", Bucket: "bucket",
			BucketVersioning: domain.BucketVersioningModeDisabled,
		},
	)
	require.NoError(t, err)
	candidate := candidateByKind(t, plan, domain.CandidateOrphanClusterRoot)
	require.Equal(t, domain.StateProtected, candidate.State)
	require.Contains(t, candidate.Reason, "PVC dataflow-system/user-data")
	requireClusterListSkipped(t, store)
}

func TestClusterBackupReferenceCoverage(t *testing.T) {
	t.Parallel()

	namespace := "dataflow-system"
	target := clusterPrefix()
	other := testRepositoryRoot + "/" + namespace + "/other-" + testOtherClusterUID
	key := domain.BackupKey{Namespace: namespace, Name: "live"}

	tests := []struct {
		name      string
		inventory domain.Inventory
		want      bool
	}{
		{
			name: "cluster UID",
			inventory: inventoryWithBackup(key, domain.Backup{
				ClusterUID: testClusterUID,
			}),
			want: true,
		},
		{
			name: "backup path",
			inventory: inventoryWithBackup(key, domain.Backup{
				Path: target + "/postgresql/live",
			}),
			want: true,
		},
		{
			name: "kopia path",
			inventory: inventoryWithBackup(key, domain.Backup{
				KopiaRepoPath: target + "/postgresql/_kopia",
			}),
			want: true,
		},
		{
			name:      "ambiguous backup",
			inventory: inventoryWithBackup(key, domain.Backup{}),
			want:      true,
		},
		{
			name: "invalid cluster UID is ambiguous",
			inventory: inventoryWithBackup(key, domain.Backup{
				ClusterUID: "invalid",
			}),
			want: true,
		},
		{
			name: "active restore reference",
			inventory: func() domain.Inventory {
				inventory := inventoryWithoutClusterBackups()
				inventory.ProtectedBackups[key] = "active Restore"

				return inventory
			}(),
			want: true,
		},
		{
			name: "live dependency",
			inventory: inventoryWithBackup(key, domain.Backup{
				ClusterUID: testOtherClusterUID, ParentBackupName: "base",
			}),
			want: true,
		},
		{
			name: "backup mapped to another cluster",
			inventory: inventoryWithBackup(key, domain.Backup{
				ClusterUID: testOtherClusterUID, Path: other + "/postgresql/live",
			}),
		},
		{
			name: "ambiguous backup in another namespace",
			inventory: inventoryWithBackup(
				domain.BackupKey{Namespace: "other", Name: "live"},
				domain.Backup{},
			),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			reason := clusterBackupReference(
				test.inventory,
				namespace,
				target,
				testClusterUID,
			)
			require.Equal(t, test.want, reason != "", reason)
		})
	}
}

func TestObjectPathClusterPrefixRequiresRepositoryShape(t *testing.T) {
	t.Parallel()

	prefix, ok := objectPathClusterPrefix(
		testRepositoryRoot,
		"dataflow-system",
		clusterPrefix()+"/postgresql/backup",
	)
	require.True(t, ok)
	require.Equal(t, clusterPrefix(), prefix)

	_, ok = objectPathClusterPrefix(
		testRepositoryRoot,
		"dataflow-system",
		testRepositoryRoot+"/other/test-db-"+testClusterUID+"/backup",
	)
	require.False(t, ok)
	_, ok = objectPathClusterPrefix(testRepositoryRoot, "dataflow-system", "other/root")
	require.False(t, ok)
}

func inventoryWithoutClusterBackups() domain.Inventory {
	inventory := volumeInventory()
	inventory.Backups = make(map[domain.BackupKey]domain.Backup)

	return inventory
}

func inventoryWithBackup(key domain.BackupKey, backup domain.Backup) domain.Inventory {
	inventory := inventoryWithoutClusterBackups()
	backup.Key = key
	backup.Repo = "repo"
	inventory.Backups[key] = backup

	return inventory
}

func historicalRepositoryInventory() domain.Inventory {
	inventory := inventoryWithoutClusterBackups()
	inventory.VolumeRoots[testHistoricalRoot] = domain.VolumeRoot{
		Prefix: testHistoricalRoot, Kind: domain.VolumeRootRepository,
		Namespace: "dataflow-system", Resource: "BackupRepo PV " + testHistoricalRoot,
	}

	return inventory
}

func historicalClusterPrefix() string {
	return testHistoricalRoot + "/dataflow-system/test-db-" + testClusterUID
}

func clusterPrefixForUID(clusterUID string) string {
	return testRepositoryRoot + "/dataflow-system/test-db-" + clusterUID
}

func clusterPrefix() string {
	return testRepositoryRoot + "/dataflow-system/test-db-" + testClusterUID
}

func candidateKinds(plan domain.Plan) []domain.CandidateKind {
	kinds := make([]domain.CandidateKind, 0, len(plan.Candidates))
	for _, candidate := range plan.Candidates {
		kinds = append(kinds, candidate.Kind)
	}

	return kinds
}

func requireClusterListSkipped(t *testing.T, store *memoryStore) {
	t.Helper()

	store.listMutex.Lock()
	calls := append([]string(nil), store.listCalls...)
	store.listMutex.Unlock()
	require.NotContains(t, calls, clusterPrefix())
	require.NotContains(t, calls, clusterPrefix()+"/")
}

func candidateByKind(
	t *testing.T,
	plan domain.Plan,
	kind domain.CandidateKind,
) domain.Candidate {
	t.Helper()

	for _, candidate := range plan.Candidates {
		if candidate.Kind == kind {
			return candidate
		}
	}

	t.Fatalf("candidate kind %q not found", kind)

	return domain.Candidate{}
}

func candidateByPrefix(t *testing.T, plan domain.Plan, prefix string) domain.Candidate {
	t.Helper()

	for _, candidate := range plan.Candidates {
		if candidate.Prefix == prefix {
			return candidate
		}
	}

	t.Fatalf("candidate prefix %q not found", prefix)

	return domain.Candidate{}
}

func candidateByPrefixAndKind(
	t *testing.T,
	plan domain.Plan,
	prefix string,
	kind domain.CandidateKind,
) domain.Candidate {
	t.Helper()

	for _, candidate := range plan.Candidates {
		if candidate.Prefix == prefix && candidate.Kind == kind {
			return candidate
		}
	}

	t.Fatalf("candidate prefix %q with kind %q not found", prefix, kind)

	return domain.Candidate{}
}
