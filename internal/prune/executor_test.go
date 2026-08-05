package prune //nolint:testpackage // White-box tests exercise executor race guards with package fakes.

import (
	"context"
	"errors"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/labring-sigs/kbbackup-prune/internal/domain"
	"github.com/stretchr/testify/require"
)

type fakeKube struct {
	mutex          sync.Mutex
	exists         map[domain.BackupKey]bool
	existsSeries   []bool
	existsCalls    int
	err            error
	inventory      domain.Inventory
	inventories    []domain.Inventory
	inventoryCalls int
	settings       domain.S3Settings
	inventoryErr   error
	namespace      string
}

func (k *fakeKube) Inventory(
	_ context.Context,
	_ string,
	namespace string,
	_ bool,
) (domain.Inventory, domain.S3Settings, error) {
	k.mutex.Lock()
	defer k.mutex.Unlock()

	k.namespace = namespace
	k.inventoryCalls++

	if len(k.inventories) > 0 {
		index := min(k.inventoryCalls-1, len(k.inventories)-1)
		return k.inventories[index], k.settings, k.inventoryErr
	}

	return k.inventory, k.settings, k.inventoryErr
}

func (k *fakeKube) BackupExists(_ context.Context, key domain.BackupKey) (bool, error) {
	k.mutex.Lock()
	defer k.mutex.Unlock()

	k.existsCalls++
	if len(k.existsSeries) > 0 {
		index := min(k.existsCalls-1, len(k.existsSeries)-1)
		return k.existsSeries[index], k.err
	}

	return k.exists[key], k.err
}

func executablePlan(now time.Time) domain.Plan {
	key := domain.BackupKey{Namespace: "ns", Name: "backup"}
	marker := domain.Object{
		Key:          "root/ns/backup/kubeblocks-backup.json",
		ETag:         "etag",
		LastModified: now,
		Size:         100,
	}

	return domain.Plan{
		Versioning: "Disabled", DeleteObjects: 2, DeleteBytes: 600,
		Candidates: []domain.Candidate{
			{
				Backup:       key,
				Prefix:       "root/ns/backup",
				ManifestKey:  marker.Key,
				ManifestETag: marker.ETag,
				LastModified: marker.LastModified,
				State:        domain.StateOrphan,
				ObjectCount:  2,
				Bytes:        600,
				Objects:      []domain.Object{marker, {Key: "root/ns/backup/data", Size: 500}},
			},
		},
	}
}

func TestExecutorDryRunAllowsBlockers(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	plan := executablePlan(now)
	plan.BlockingReasons = []string{"PVC cannot be mapped"}
	plan.Versioning = "Enabled"
	execution, err := (Executor{Kube: &fakeKube{}, Store: &memoryStore{}}).Run(
		context.Background(), plan, ExecuteOptions{DryRun: true},
	)
	require.NoError(t, err)
	require.True(t, execution.DryRun)
	require.Equal(t, 2, execution.Results[0].ObjectsDeleted)
}

func TestExecutorUsesBucketVersioningOverride(t *testing.T) {
	t.Parallel()

	plan := executablePlan(time.Now().UTC())
	plan.VersioningSource = domain.BucketVersioningSourceOverride
	store := &memoryStore{
		current:    append([]domain.Object(nil), plan.Candidates[0].Objects...),
		versioning: "error",
	}

	execution, err := (Executor{
		Kube:  &fakeKube{exists: map[domain.BackupKey]bool{}},
		Store: store,
	}).Run(context.Background(), plan, ExecuteOptions{
		Concurrency: 1,
	})
	require.NoError(t, err)
	require.Len(t, execution.Results, 1)
	require.Zero(t, store.versioningCalls)
	require.Len(t, store.deleted, 2)
}

func TestExecutorDeletesAfterRevalidation(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	plan := executablePlan(now)
	store := &memoryStore{current: append([]domain.Object(nil), plan.Candidates[0].Objects...)}
	execution, err := (Executor{Kube: &fakeKube{exists: map[domain.BackupKey]bool{}}, Store: store}).Run(
		context.Background(),
		plan,
		ExecuteOptions{
			Concurrency: 2,
		},
	)
	require.NoError(t, err)
	require.False(t, execution.DryRun)
	require.Len(t, store.deleted, 2)
	require.Len(t, store.deleteCalls, 2)
	require.Equal(t, "root/ns/backup/data", store.deleteCalls[0][0].Key)
	require.Equal(t, plan.Candidates[0].ManifestKey, store.deleteCalls[1][0].Key)
	require.Equal(t, 2, execution.Results[0].ObjectsDeleted)
	require.Equal(t, 1, store.versioningCalls)
}

func TestExecutorKeepsManifestWhenDataDeletionFails(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	plan := executablePlan(now)
	store := &memoryStore{
		current:     append([]domain.Object(nil), plan.Candidates[0].Objects...),
		deleteErr:   errors.New("data delete failed"),
		deleteErrAt: 1,
	}
	execution, err := (Executor{Kube: &fakeKube{}, Store: store}).Run(
		context.Background(),
		plan,
		ExecuteOptions{Concurrency: 1},
	)
	require.Error(t, err)
	require.ErrorContains(t, errors.New(execution.Results[0].Error), "data delete failed")
	require.Len(t, store.deleteCalls, 1)
	require.NotEqual(t, plan.Candidates[0].ManifestKey, store.deleteCalls[0][0].Key)
}

func TestExecutorDeletesManifestlessBackupFromExactSnapshot(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	key := domain.BackupKey{Namespace: "ns", Name: "historical"}
	objects := []domain.Object{
		{Key: "root/ns/cluster/postgresql/historical/", LastModified: now},
		{
			Key:  "root/ns/cluster/postgresql/historical/historical.tar.zst",
			Size: 500, ETag: "data", LastModified: now,
		},
	}
	plan := domain.Plan{
		Versioning:    domain.BucketVersioningDisabled,
		DeleteObjects: len(objects), DeleteBytes: 500,
		Candidates: []domain.Candidate{{
			Kind: domain.CandidateBackup, Backup: key,
			Prefix: "root/ns/cluster/postgresql/historical",
			State:  domain.StateOrphan, Objects: objects,
			ObjectCount: len(objects), Bytes: 500,
		}},
	}
	store := &memoryStore{
		current: append([]domain.Object(nil), objects...),
		statErr: errors.New("manifest stat must stay unused"),
	}

	execution, err := (Executor{
		Kube: &fakeKube{exists: map[domain.BackupKey]bool{}}, Store: store,
	}).Run(context.Background(), plan, ExecuteOptions{Concurrency: 1})
	require.NoError(t, err)
	require.Len(t, execution.Results, 1)
	require.Len(t, store.deleteCalls, 1)
	require.Equal(t, objects, store.deleteCalls[0])
}

func TestExecutorProtectsManifestlessBackupFromRaces(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	key := domain.BackupKey{Namespace: "ns", Name: "historical"}
	object := domain.Object{
		Key: "root/ns/cluster/postgresql/historical/data", Size: 500,
		ETag: "planned", LastModified: now,
	}
	plan := domain.Plan{
		Versioning:    domain.BucketVersioningDisabled,
		DeleteObjects: 1, DeleteBytes: 500,
		Candidates: []domain.Candidate{{
			Kind: domain.CandidateBackup, Backup: key,
			Prefix: "root/ns/cluster/postgresql/historical",
			State:  domain.StateOrphan, Objects: []domain.Object{object},
			ObjectCount: 1, Bytes: 500,
		}},
	}

	tests := []struct {
		name    string
		kube    *fakeKube
		store   *memoryStore
		wantErr string
	}{
		{
			name:    "Backup CR appeared",
			kube:    &fakeKube{exists: map[domain.BackupKey]bool{key: true}},
			store:   &memoryStore{current: []domain.Object{object}},
			wantErr: "Backup CR appeared",
		},
		{
			name: "object changed",
			kube: &fakeKube{exists: map[domain.BackupKey]bool{}},
			store: &memoryStore{current: []domain.Object{{
				Key: object.Key, Size: object.Size, ETag: "changed", LastModified: now,
			}}},
			wantErr: "prefix objects changed",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			execution, err := (Executor{Kube: test.kube, Store: test.store}).Run(
				context.Background(), plan, ExecuteOptions{Concurrency: 1},
			)
			require.Error(t, err)
			require.ErrorContains(t, errors.New(execution.Results[0].Error), test.wantErr)
			require.Empty(t, test.store.deleteCalls)
		})
	}
}

func TestExecutorCandidateRevalidationFailures(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()

	tests := []struct {
		name    string
		kube    *fakeKube
		store   *memoryStore
		wantErr string
	}{
		{
			name: "backup appeared",
			kube: &fakeKube{
				exists: map[domain.BackupKey]bool{{Namespace: "ns", Name: "backup"}: true},
			},
			store:   &memoryStore{},
			wantErr: "Backup CR appeared",
		},
		{
			name: "kube error", kube: &fakeKube{err: errors.New("API down")},
			store: &memoryStore{}, wantErr: "recheck Backup CR",
		},
		{
			name:    "marker missing",
			kube:    &fakeKube{},
			store:   &memoryStore{statErr: errors.New("gone")},
			wantErr: "recheck manifest",
		},
		{
			name: "marker changed",
			kube: &fakeKube{},
			store: &memoryStore{current: []domain.Object{{
				Key: "root/ns/backup/kubeblocks-backup.json", ETag: "different", LastModified: now,
			}}},
			wantErr: "manifest changed",
		},
		{
			name: "delete error", kube: &fakeKube{}, store: &memoryStore{
				current: append(
					[]domain.Object(nil),
					executablePlan(now).Candidates[0].Objects...,
				),
				deleteErr: errors.New("access denied"),
			}, wantErr: "access denied",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			execution, err := (Executor{Kube: test.kube, Store: test.store}).Run(
				context.Background(), executablePlan(now), ExecuteOptions{
					Concurrency: 1,
				},
			)
			require.Error(t, err)
			require.ErrorContains(t, errors.New(execution.Results[0].Error), test.wantErr)
		})
	}
}

func TestExecutorRejectsChangedObjectSnapshot(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	plan := executablePlan(now)
	store := &memoryStore{current: append([]domain.Object(nil), plan.Candidates[0].Objects...)}
	store.current[1].ETag = "new-data"

	execution, err := (Executor{Kube: &fakeKube{}, Store: store}).Run(
		context.Background(),
		plan,
		ExecuteOptions{Concurrency: 1},
	)
	require.Error(t, err)
	require.ErrorContains(t, errors.New(execution.Results[0].Error), "objects changed")
	require.Empty(t, store.deleteCalls)
}

func TestExecutorRechecksBackupImmediatelyBeforeDelete(t *testing.T) {
	t.Parallel()

	plan := executablePlan(time.Now().UTC())
	store := &memoryStore{current: append([]domain.Object(nil), plan.Candidates[0].Objects...)}
	kubeClient := &fakeKube{existsSeries: []bool{false, true}}
	execution, err := (Executor{Kube: kubeClient, Store: store}).Run(
		context.Background(),
		plan,
		ExecuteOptions{Concurrency: 1},
	)
	require.ErrorContains(t, err, "failed deletion")
	require.Len(t, execution.Results, 1)
	require.Contains(t, execution.Results[0].Error, "final Kubernetes recheck")
	require.Contains(t, execution.Results[0].Error, "final object snapshot")
	require.Empty(t, store.deleteCalls)
	require.Equal(t, 2, kubeClient.existsCalls)
}

func TestExecutorFinalRecheckProtectsChangedAggregateCandidates(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	tests := []struct {
		name       string
		build      func() (domain.Plan, domain.Inventory, domain.Inventory, []domain.Object)
		wantReason string
	}{
		{
			name: "repository root gains Backup reference",
			build: func() (domain.Plan, domain.Inventory, domain.Inventory, []domain.Object) {
				plan, safe, objects := executableOrphanRepositoryRootPlan(now)
				_, changed, _ := executableOrphanRepositoryRootPlan(now)
				key := domain.BackupKey{Namespace: "dataflow-system", Name: "new"}
				changed.Backups[key] = domain.Backup{Key: key, ClusterUID: testClusterUID}

				return plan, safe, changed, objects
			},
			wantReason: "orphan repository root",
		},
		{
			name: "cluster root gains Backup reference",
			build: func() (domain.Plan, domain.Inventory, domain.Inventory, []domain.Object) {
				plan, safe, objects := executableOrphanClusterPlan(now)
				_, changed, _ := executableOrphanClusterPlan(now)
				key := domain.BackupKey{Namespace: "dataflow-system", Name: "new"}
				changed.Backups[key] = domain.Backup{Key: key, ClusterUID: testClusterUID}

				return plan, safe, changed, objects
			},
			wantReason: "orphan cluster root",
		},
		{
			name: "volume root gains owner",
			build: func() (domain.Plan, domain.Inventory, domain.Inventory, []domain.Object) {
				root := "pvc-33333333-3333-4333-8333-333333333333"
				objects := []domain.Object{{
					Key: root + "/data", Size: 1, ETag: "data", LastModified: now,
				}}
				plan := domain.Plan{
					Repository: "repo", Bucket: "bucket",
					Versioning:      domain.BucketVersioningDisabled,
					VolumeDiscovery: true, DeleteObjects: 1, DeleteBytes: 1,
					Candidates: []domain.Candidate{{
						Kind: domain.CandidateOrphanVolumeRoot, Prefix: root,
						State: domain.StateOrphan, ObjectCount: 1, Bytes: 1, Objects: objects,
					}},
				}
				safe := domain.Inventory{Repo: domain.Repository{Name: "repo"}}
				changed := domain.Inventory{
					Repo: domain.Repository{Name: "repo"},
					VolumeRoots: map[string]domain.VolumeRoot{
						root: {
							Prefix: root, Kind: domain.VolumeRootUser,
							Resource: "PVC ns/new-owner", Current: true,
						},
					},
				}

				return plan, safe, changed, objects
			},
			wantReason: "now owned",
		},
		{
			name: "repository stray gains path reference",
			build: func() (domain.Plan, domain.Inventory, domain.Inventory, []domain.Object) {
				scope := testRepositoryRoot + "/dataflow-system/test-db-" + testClusterUID
				objects := []domain.Object{{
					Key: scope + "/unexpected", Size: 1, ETag: "stray", LastModified: now,
				}}
				plan := domain.Plan{
					Repository: "repo", Bucket: "bucket",
					Versioning:      domain.BucketVersioningDisabled,
					VolumeDiscovery: true, DeleteRepositoryStray: true,
					DeleteObjects: 1, DeleteBytes: 1,
					Candidates: []domain.Candidate{{
						Kind: domain.CandidateRepositoryStray, Prefix: scope,
						ScopePrefix: scope, State: domain.StateOrphan,
						ObjectCount: 1, Bytes: 1, Objects: objects,
					}},
				}
				safe := inventoryWithoutClusterBackups()
				changed := inventoryWithoutClusterBackups()
				key := domain.BackupKey{Namespace: "dataflow-system", Name: "new"}
				changed.Backups[key] = domain.Backup{Key: key, Path: scope}

				return plan, safe, changed, objects
			},
			wantReason: "repository stray",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			plan, safe, changed, objects := test.build()
			store := &memoryStore{current: append([]domain.Object(nil), objects...)}
			kubeClient := &fakeKube{inventories: []domain.Inventory{safe, changed}}
			execution, err := (Executor{Kube: kubeClient, Store: store}).Run(
				context.Background(), plan, ExecuteOptions{Concurrency: 1},
			)
			require.ErrorContains(t, err, "failed deletion")
			require.Len(t, execution.Results, 1)
			require.Contains(t, execution.Results[0].Error, "final Kubernetes recheck")
			require.Contains(t, execution.Results[0].Error, test.wantReason)
			require.Empty(t, store.deleteCalls)
			require.Equal(t, 2, kubeClient.inventoryCalls)
		})
	}
}

func TestCandidateSafetyValidatorSharesSufficientlyFreshInventory(t *testing.T) {
	t.Parallel()

	kubeClient := &fakeKube{inventory: domain.Inventory{Repo: domain.Repository{Name: "repo"}}}
	validator := candidateSafetyValidator{
		executor: Executor{Kube: kubeClient},
		plan:     domain.Plan{Repository: "repo"},
	}

	after := time.Now()
	for _, root := range []string{
		"pvc-11111111-1111-4111-8111-111111111111",
		"pvc-22222222-2222-4222-8222-222222222222",
	} {
		err := validator.validate(context.Background(), domain.Candidate{
			Kind: domain.CandidateOrphanVolumeRoot, Prefix: root,
		}, after)
		require.NoError(t, err)
	}

	require.Equal(t, 1, kubeClient.inventoryCalls)
}

func TestExecutorRefreshesKubernetesProtections(t *testing.T) {
	t.Parallel()

	plan := executablePlan(time.Now().UTC())
	plan.Repository = "repo"
	plan.RepositoryUID = "repo-uid"
	plan.RepositoryGeneration = 3
	plan.Bucket = "bucket"
	plan.Prefix = "root"
	key := plan.Candidates[0].Backup

	tests := []struct {
		name      string
		inventory domain.Inventory
		settings  domain.S3Settings
		err       error
		want      string
	}{
		{
			name: "active restore",
			inventory: domain.Inventory{
				Repo: domain.Repository{
					UID:        "repo-uid",
					Generation: 3,
					PathPrefix: "root",
				},
				ProtectedBackups: map[domain.BackupKey]string{key: "active Restore"},
			},
			settings: domain.S3Settings{Bucket: "bucket"},
			want:     "active Restore",
		},
		{
			name: "PVC",
			inventory: domain.Inventory{
				Repo: domain.Repository{UID: "repo-uid", Generation: 3, PathPrefix: "root"},
				Protections: []domain.Protection{{
					Prefix: plan.Candidates[0].Prefix, Resource: "PVC ns/data",
				}},
			},
			settings: domain.S3Settings{Bucket: "bucket"},
			want:     "PVC ns/data",
		},
		{
			name: "new incremental child",
			inventory: domain.Inventory{
				Repo: domain.Repository{UID: "repo-uid", Generation: 3, PathPrefix: "root"},
				Backups: map[domain.BackupKey]domain.Backup{
					{Namespace: "ns", Name: "child"}: {
						Key:              domain.BackupKey{Namespace: "ns", Name: "child"},
						ParentBackupName: key.Name,
					},
				},
			},
			settings: domain.S3Settings{Bucket: "bucket"},
			want:     "referenced by a live backup",
		},
		{
			name: "inventory blocker",
			inventory: domain.Inventory{
				Repo:            domain.Repository{UID: "repo-uid", Generation: 3},
				BlockingReasons: []string{"cannot list PVCs"},
			},
			want: "cannot list PVCs",
		},
		{
			name: "repo recreated",
			inventory: domain.Inventory{
				Repo: domain.Repository{UID: "new-uid", Generation: 3},
			},
			want: "recreated",
		},
		{
			name: "repo changed",
			inventory: domain.Inventory{
				Repo: domain.Repository{UID: "repo-uid", Generation: 4},
			},
			want: "specification changed",
		},
		{
			name: "bucket changed",
			inventory: domain.Inventory{
				Repo: domain.Repository{UID: "repo-uid", Generation: 3},
			},
			settings: domain.S3Settings{Bucket: "other"},
			want:     "bucket changed",
		},
		{name: "inventory error", err: errors.New("API unavailable"), want: "refresh Kubernetes"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			executor := Executor{Kube: &fakeKube{
				inventory: test.inventory, settings: test.settings, inventoryErr: test.err,
			}}
			err := executor.revalidateInventory(context.Background(), plan)
			require.ErrorContains(t, err, test.want)
		})
	}
}

func TestExecutorRejectsChangedRepositoryPVCObjectPrefix(t *testing.T) {
	t.Parallel()

	plan := executablePlan(time.Now().UTC())
	plan.Repository = "repo"
	plan.RepositoryUID = "repo-uid"
	plan.RepositoryGeneration = 3
	plan.Bucket = "bucket"
	plan.Namespace = "ns"
	plan.Prefix = "pvc-old/ns/backup"
	plan.Prefixes = []string{plan.Prefix}
	plan.ObjectPrefixes = map[string]string{"ns": "pvc-old"}

	kubeClient := &fakeKube{
		inventory: domain.Inventory{Repo: domain.Repository{
			UID:            "repo-uid",
			Generation:     3,
			ObjectPrefixes: map[string]string{"ns": "pvc-new"},
		}},
		settings: domain.S3Settings{Bucket: "bucket"},
	}
	err := (Executor{Kube: kubeClient}).revalidateInventory(context.Background(), plan)
	require.ErrorContains(t, err, "object prefix")
	require.ErrorContains(t, err, "changed after planning")
	require.Equal(t, "ns", kubeClient.namespace)
}

func TestExecutorDeletesCurrentManifestVersionLast(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	plan := executablePlan(now)
	plan.Versioning = domain.BucketVersioningEnabled
	candidate := &plan.Candidates[0]
	candidate.ManifestETag = "current-etag"
	candidate.Objects[0].ETag = "current-etag"
	candidate.Objects[0].VersionID = "current"
	candidate.Objects = append(candidate.Objects, domain.Object{
		Key: candidate.ManifestKey, ETag: "old-etag", VersionID: "old",
		LastModified: now.Add(-time.Hour),
	})
	candidate.ObjectCount++
	plan.DeleteObjects++
	store := &memoryStore{
		versioning: domain.BucketVersioningEnabled,
		current: []domain.Object{{
			Key: candidate.ManifestKey, ETag: "current-etag", VersionID: "current",
			LastModified: now,
		}},
		versions: append([]domain.Object(nil), candidate.Objects...),
	}

	execution, err := (Executor{Kube: &fakeKube{}, Store: store}).Run(
		context.Background(),
		plan,
		ExecuteOptions{Concurrency: 1, PurgeVersions: true},
	)
	require.NoError(t, err)
	require.Len(t, execution.Results, 1)
	require.Len(t, store.deleteCalls, 3)
	require.Equal(t, "data", path.Base(store.deleteCalls[0][0].Key))
	require.Equal(t, "old", store.deleteCalls[1][0].VersionID)
	require.Equal(t, "current", store.deleteCalls[2][0].VersionID)
}

func TestExecutorRejectsVersioningChangeAfterPlanning(t *testing.T) {
	t.Parallel()

	plan := executablePlan(time.Now().UTC())
	store := &memoryStore{
		current:    append([]domain.Object(nil), plan.Candidates[0].Objects...),
		versioning: domain.BucketVersioningEnabled,
	}
	execution, err := (Executor{Kube: &fakeKube{}, Store: store}).Run(
		context.Background(),
		plan,
		ExecuteOptions{Concurrency: 1},
	)
	require.ErrorContains(t, err, "bucket versioning changed after planning")
	require.Empty(t, execution.Results)
	require.Empty(t, store.deleteCalls)
}

func TestObjectSnapshotHelpers(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	objects := []domain.Object{{Key: "root/backup/data", ETag: "etag", LastModified: now}}
	require.True(t, sameObjectSnapshot(objects, append([]domain.Object(nil), objects...)))
	require.False(t, sameObjectSnapshot(objects, nil))
	require.False(t, sameObjectSnapshot(objects, append(objects, objects[0])))

	changed := append([]domain.Object(nil), objects...)
	changed[0].Size++
	require.False(t, sameObjectSnapshot(objects, changed))

	inside := objectsInsidePrefix(
		"root/backup",
		append(objects, domain.Object{Key: "root/backup-copy"}),
	)
	require.Equal(t, objects, inside)
}

func TestExecutorGlobalGuards(t *testing.T) {
	t.Parallel()

	plan := executablePlan(time.Now().UTC())
	executor := Executor{Kube: &fakeKube{}, Store: &memoryStore{}}

	tests := []struct {
		name    string
		mutate  func(*domain.Plan)
		opts    ExecuteOptions
		wantErr string
	}{
		{
			name:    "blocker",
			mutate:  func(p *domain.Plan) { p.BlockingReasons = []string{"unsafe PVC"} },
			wantErr: "execution blocked",
		},
		{
			name:    "versioning",
			mutate:  func(p *domain.Plan) { p.Versioning = "Enabled" },
			wantErr: "purge-versions",
		},
		{
			name:    "unknown versioning state",
			mutate:  func(p *domain.Plan) { p.Versioning = "Unknown" },
			wantErr: "unknown bucket versioning state",
		},
		{
			name: "unknown versioning source",
			mutate: func(p *domain.Plan) {
				p.VersioningSource = "unknown"
			},
			wantErr: "unknown bucket versioning source",
		},
		{
			name: "candidate snapshot count",
			mutate: func(p *domain.Plan) {
				p.Candidates[0].Objects = p.Candidates[0].Objects[:1]
			},
			wantErr: "object snapshot",
		},
		{
			name: "snapshot totals",
			mutate: func(p *domain.Plan) {
				p.DeleteBytes++
			},
			wantErr: "snapshot totals",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			candidatePlan := plan

			candidatePlan.Candidates = append([]domain.Candidate(nil), plan.Candidates...)
			for i := range candidatePlan.Candidates {
				candidatePlan.Candidates[i].Objects = append(
					[]domain.Object(nil),
					plan.Candidates[i].Objects...,
				)
			}

			if test.mutate != nil {
				test.mutate(&candidatePlan)
			}

			_, err := executor.Run(context.Background(), candidatePlan, test.opts)
			require.ErrorContains(t, err, test.wantErr)
		})
	}

	_, err := (Executor{}).Run(context.Background(), plan, ExecuteOptions{})
	require.ErrorContains(t, err, "clients are required")
}

func TestExecutorDeletesOrphanVolumeRootAfterSnapshotRevalidation(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	root := "pvc-33333333-3333-4333-8333-333333333333"
	objects := []domain.Object{
		{Key: root + "/old-a", Size: 10, ETag: "a", LastModified: now},
		{Key: root + "/old-b", Size: 20, ETag: "b", LastModified: now},
	}
	store := &memoryStore{current: objects}
	plan := domain.Plan{
		Repository: "repo", Bucket: "bucket", Versioning: domain.BucketVersioningDisabled,
		VolumeDiscovery: true, DeleteObjects: 2, DeleteBytes: 30,
		Candidates: []domain.Candidate{{
			Kind: domain.CandidateOrphanVolumeRoot, Prefix: root, ScopePrefix: root,
			State: domain.StateOrphan, ObjectCount: 2, Bytes: 30, Objects: objects,
		}},
	}
	execution, err := (Executor{Kube: &fakeKube{exists: map[domain.BackupKey]bool{}}, Store: store}).Run(
		context.Background(),
		plan,
		ExecuteOptions{Concurrency: 1},
	)
	require.NoError(t, err)
	require.Len(t, store.deleteCalls, 1)
	require.Len(t, store.deleteCalls[0], 2)
	require.Equal(t, 2, execution.Results[0].ObjectsDeleted)
}

func TestExecutorProtectsOrphanVolumeRootThatGetsBound(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	root := "pvc-33333333-3333-4333-8333-333333333333"
	objects := []domain.Object{{Key: root + "/data", Size: 1, ETag: "a", LastModified: now}}
	plan := domain.Plan{
		Repository: "repo", Bucket: "bucket", Versioning: domain.BucketVersioningDisabled,
		VolumeDiscovery: true, DeleteObjects: 1, DeleteBytes: 1,
		Candidates: []domain.Candidate{{
			Kind: domain.CandidateOrphanVolumeRoot, Prefix: root,
			State: domain.StateOrphan, ObjectCount: 1, Bytes: 1, Objects: objects,
		}},
	}
	kubeClient := &fakeKube{
		inventory: domain.Inventory{
			Repo: domain.Repository{Name: "repo"},
			VolumeRoots: map[string]domain.VolumeRoot{
				root: {Prefix: root, Kind: domain.VolumeRootUser, Resource: "PVC ns/data"},
			},
		},
	}
	_, err := (Executor{Kube: kubeClient, Store: &memoryStore{current: objects}}).Run(
		context.Background(), plan, ExecuteOptions{Concurrency: 1},
	)
	require.ErrorContains(t, err, "now owned")
}

func TestExecutorDeletesOrphanClusterRootAfterRevalidation(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	plan, inventory, objects := executableOrphanClusterPlan(now)
	store := &memoryStore{current: append([]domain.Object(nil), objects...)}
	execution, err := (Executor{
		Kube:  &fakeKube{inventory: inventory},
		Store: store,
	}).Run(context.Background(), plan, ExecuteOptions{Concurrency: 1})
	require.NoError(t, err)
	require.Len(t, execution.Results, 1)
	require.Equal(t, len(objects), execution.Results[0].ObjectsDeleted)
	require.Equal(t, objects, store.deleted)
}

func TestExecutorDeletesOrphanClusterFromHistoricalRepositoryRoot(t *testing.T) {
	t.Parallel()

	const historicalRoot = "pvc-44444444-4444-4444-8444-444444444444"

	now := time.Now().UTC()
	cluster := historicalRoot + "/dataflow-system/old-db-" + testOtherClusterUID
	objects := []domain.Object{
		{Key: cluster + "/postgresql/old/data", Size: 10, ETag: "a", LastModified: now},
	}
	inventory := inventoryWithoutClusterBackups()
	inventory.VolumeRoots[historicalRoot] = domain.VolumeRoot{
		Prefix: historicalRoot, Kind: domain.VolumeRootRepository,
		Namespace: "dataflow-system", Resource: "BackupRepo PV " + historicalRoot,
	}
	plan := domain.Plan{
		Repository: "repo", Bucket: "bucket", Versioning: domain.BucketVersioningDisabled,
		VolumeDiscovery: true, DeleteObjects: 1, DeleteBytes: 10,
		ObjectPrefixes: map[string]string{"dataflow-system": testRepositoryRoot},
		Candidates: []domain.Candidate{{
			Kind: domain.CandidateOrphanClusterRoot, Prefix: cluster, ScopePrefix: cluster,
			State: domain.StateOrphan, ObjectCount: 1, Bytes: 10, Objects: objects,
		}},
	}
	store := &memoryStore{current: append([]domain.Object(nil), objects...)}

	execution, err := (Executor{
		Kube: &fakeKube{inventory: inventory}, Store: store,
	}).Run(context.Background(), plan, ExecuteOptions{Concurrency: 1})
	require.NoError(t, err)
	require.Equal(t, 1, execution.Results[0].ObjectsDeleted)
	require.Equal(t, objects, store.deleted)
}

func TestExecutorDeletesOrphanRepositoryRootAfterRevalidation(t *testing.T) {
	t.Parallel()

	plan, inventory, objects := executableOrphanRepositoryRootPlan(time.Now().UTC())
	store := &memoryStore{current: append([]domain.Object(nil), objects...)}
	execution, err := (Executor{
		Kube: &fakeKube{inventory: inventory}, Store: store,
	}).Run(context.Background(), plan, ExecuteOptions{Concurrency: 1})
	require.NoError(t, err)
	require.Len(t, execution.Results, 1)
	require.Equal(t, len(objects), execution.Results[0].ObjectsDeleted)
	require.Equal(t, objects, store.deleted)
}

func TestExecutorRevalidatesOrphanRepositoryRootSafety(t *testing.T) {
	t.Parallel()

	key := domain.BackupKey{Namespace: "dataflow-system", Name: "new"}
	tests := []struct {
		name   string
		mutate func(*domain.Inventory, string)
	}{
		{
			name: "matching cluster UID",
			mutate: func(inventory *domain.Inventory, _ string) {
				inventory.Backups[key] = domain.Backup{Key: key, ClusterUID: testClusterUID}
			},
		},
		{
			name: "relative backup path",
			mutate: func(inventory *domain.Inventory, _ string) {
				inventory.Backups[key] = domain.Backup{
					Key:  key,
					Path: "dataflow-system/test-db-" + testClusterUID + "/postgresql/new",
				}
			},
		},
		{
			name: "absolute root path",
			mutate: func(inventory *domain.Inventory, root string) {
				inventory.Backups[key] = domain.Backup{Key: key, Path: root + "/unusual/path"}
			},
		},
		{
			name: "ambiguous backup",
			mutate: func(inventory *domain.Inventory, _ string) {
				inventory.Backups[key] = domain.Backup{Key: key}
			},
		},
		{
			name: "active restore",
			mutate: func(inventory *domain.Inventory, _ string) {
				inventory.ProtectedBackups[key] = "active Restore restore/new"
			},
		},
		{
			name: "storage protection",
			mutate: func(inventory *domain.Inventory, root string) {
				inventory.Protections = []domain.Protection{{
					Prefix: root, Kind: "pvc", Resource: "PVC dataflow-system/user",
				}}
			},
		},
		{
			name: "root became current",
			mutate: func(inventory *domain.Inventory, root string) {
				owner := inventory.VolumeRoots[root]
				owner.Current = true
				inventory.VolumeRoots[root] = owner
			},
		},
		{
			name: "root became user storage",
			mutate: func(inventory *domain.Inventory, root string) {
				owner := inventory.VolumeRoots[root]
				owner.Kind = domain.VolumeRootUser
				owner.Resource = "PVC dataflow-system/user"
				inventory.VolumeRoots[root] = owner
			},
		},
		{
			name: "historical PV disappeared",
			mutate: func(inventory *domain.Inventory, root string) {
				delete(inventory.VolumeRoots, root)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			plan, inventory, objects := executableOrphanRepositoryRootPlan(time.Now().UTC())
			test.mutate(&inventory, plan.Candidates[0].Prefix)
			_, err := (Executor{
				Kube:  &fakeKube{inventory: inventory},
				Store: &memoryStore{current: append([]domain.Object(nil), objects...)},
			}).Run(context.Background(), plan, ExecuteOptions{Concurrency: 1})
			require.Error(t, err)
			require.ErrorContains(t, err, "orphan repository root")
		})
	}
}

func TestExecutorAllowsUnrelatedBackupForOrphanRepositoryRoot(t *testing.T) {
	t.Parallel()

	plan, inventory, objects := executableOrphanRepositoryRootPlan(time.Now().UTC())
	key := domain.BackupKey{Namespace: "dataflow-system", Name: "other"}
	inventory.Backups[key] = domain.Backup{
		Key: key, ClusterUID: testOtherClusterUID,
		Path: clusterPrefixForUID(testOtherClusterUID) + "/postgresql/other",
	}
	store := &memoryStore{current: append([]domain.Object(nil), objects...)}
	_, err := (Executor{
		Kube: &fakeKube{inventory: inventory}, Store: store,
	}).Run(context.Background(), plan, ExecuteOptions{Concurrency: 1})
	require.NoError(t, err)
	require.Equal(t, objects, store.deleted)
}

func TestExecutorRejectsMalformedOrphanRepositoryRoot(t *testing.T) {
	t.Parallel()

	plan, inventory, objects := executableOrphanRepositoryRootPlan(time.Now().UTC())
	plan.Candidates[0].Prefix += "/namespace"
	_, err := (Executor{
		Kube: &fakeKube{inventory: inventory}, Store: &memoryStore{current: objects},
	}).Run(context.Background(), plan, ExecuteOptions{Concurrency: 1})
	require.ErrorContains(t, err, "invalid orphan repository root")
}

func TestExecutorRevalidatesOrphanClusterReferences(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	plan, _, objects := executableOrphanClusterPlan(now)
	key := domain.BackupKey{Namespace: "dataflow-system", Name: "new"}

	tests := []struct {
		name      string
		inventory domain.Inventory
	}{
		{
			name: "matching cluster UID",
			inventory: inventoryWithBackup(key, domain.Backup{
				ClusterUID: testClusterUID,
			}),
		},
		{
			name: "matching path",
			inventory: inventoryWithBackup(key, domain.Backup{
				Path: clusterPrefix() + "/postgresql/new",
			}),
		},
		{
			name:      "ambiguous Backup",
			inventory: inventoryWithBackup(key, domain.Backup{}),
		},
		{
			name: "active Restore",
			inventory: func() domain.Inventory {
				inventory := inventoryWithoutClusterBackups()
				inventory.ProtectedBackups[key] = "active Restore"

				return inventory
			}(),
		},
		{
			name: "live dependency",
			inventory: inventoryWithBackup(key, domain.Backup{
				ClusterUID:       testOtherClusterUID,
				ParentBackupName: "old-base",
			}),
		},
		{
			name: "PVC protection",
			inventory: func() domain.Inventory {
				inventory := inventoryWithoutClusterBackups()
				inventory.Protections = []domain.Protection{{
					Prefix: clusterPrefix(), Resource: "PVC dataflow-system/user-data",
				}}

				return inventory
			}(),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			store := &memoryStore{current: append([]domain.Object(nil), objects...)}
			execution, err := (Executor{
				Kube:  &fakeKube{inventory: test.inventory},
				Store: store,
			}).Run(context.Background(), plan, ExecuteOptions{Concurrency: 1})
			require.ErrorContains(t, err, "orphan cluster root")
			require.Empty(t, execution.Results)
			require.Empty(t, store.deleteCalls)
		})
	}
}

func TestExecutorRejectsDeferredOrChangedClusterSnapshot(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	plan, inventory, objects := executableOrphanClusterPlan(now)
	plan.Candidates[0].DeferredScan = true
	_, err := (Executor{Kube: &fakeKube{}, Store: &memoryStore{}}).Run(
		context.Background(),
		plan,
		ExecuteOptions{Concurrency: 1},
	)
	require.ErrorContains(t, err, "still deferred")

	dryRun, err := (Executor{Kube: &fakeKube{}, Store: &memoryStore{}}).Run(
		context.Background(),
		plan,
		ExecuteOptions{DryRun: true},
	)
	require.NoError(t, err)
	require.Len(t, dryRun.Results, 1)

	plan.Candidates[0].DeferredScan = false

	changed := append([]domain.Object(nil), objects...)
	changed[0].ETag = "changed"
	execution, err := (Executor{
		Kube:  &fakeKube{inventory: inventory},
		Store: &memoryStore{current: changed},
	}).Run(context.Background(), plan, ExecuteOptions{Concurrency: 1})
	require.ErrorContains(t, err, "cleanup candidates failed")
	require.Equal(t, "prefix objects changed after planning", execution.Results[0].Error)
}

func TestExecutorRejectsMalformedOrphanClusterRoot(t *testing.T) {
	t.Parallel()

	plan, inventory, objects := executableOrphanClusterPlan(time.Now().UTC())
	plan.Candidates[0].Prefix += "/postgresql"
	_, err := (Executor{
		Kube:  &fakeKube{inventory: inventory},
		Store: &memoryStore{current: objects},
	}).Run(context.Background(), plan, ExecuteOptions{Concurrency: 1})
	require.ErrorContains(t, err, "invalid orphan cluster root")
}

func executableOrphanClusterPlan(
	now time.Time,
) (domain.Plan, domain.Inventory, []domain.Object) {
	objects := []domain.Object{
		{Key: clusterPrefix() + "/postgresql/old/data", Size: 10, ETag: "a", LastModified: now},
		{Key: clusterPrefix() + "/postgresql/old/meta", Size: 20, ETag: "b", LastModified: now},
	}
	plan := domain.Plan{
		Repository: "repo", Bucket: "bucket", Versioning: domain.BucketVersioningDisabled,
		VolumeDiscovery: true, DeleteObjects: 2, DeleteBytes: 30,
		ObjectPrefixes: map[string]string{"dataflow-system": testRepositoryRoot},
		Candidates: []domain.Candidate{{
			Kind: domain.CandidateOrphanClusterRoot, Prefix: clusterPrefix(),
			ScopePrefix: clusterPrefix(), State: domain.StateOrphan,
			ObjectCount: len(objects), Bytes: 30, Objects: objects,
		}},
	}

	return plan, inventoryWithoutClusterBackups(), objects
}

func executableOrphanRepositoryRootPlan(
	now time.Time,
) (domain.Plan, domain.Inventory, []domain.Object) {
	const historicalRoot = "pvc-44444444-4444-4444-8444-444444444444"

	cluster := historicalClusterPrefix()
	objects := []domain.Object{
		{Key: cluster + "/postgresql/old/data", Size: 10, ETag: "a", LastModified: now},
		{Key: historicalRoot + "/loose", Size: 20, ETag: "b", LastModified: now},
	}
	inventory := historicalRepositoryInventory()
	plan := domain.Plan{
		Repository: "repo", Bucket: "bucket", Versioning: domain.BucketVersioningDisabled,
		VolumeDiscovery: true, DeleteObjects: 2, DeleteBytes: 30,
		ObjectPrefixes: map[string]string{"dataflow-system": testRepositoryRoot},
		Candidates: []domain.Candidate{{
			Kind: domain.CandidateOrphanRepositoryRoot, Prefix: historicalRoot,
			ScopePrefix: historicalRoot, State: domain.StateOrphan,
			ObjectCount: len(objects), Bytes: 30, Objects: objects,
		}},
	}

	return plan, inventory, objects
}

func TestExecutorDeletesOnlyPlannedRepositoryStrayObjects(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	scope := "pvc-11111111-1111-4111-8111-111111111111/ns/cluster-aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	stray := domain.Object{Key: scope + "/unexpected", Size: 3, ETag: "stray", LastModified: now}
	backupObject := domain.Object{
		Key:          scope + "/component/backup/data",
		Size:         7,
		ETag:         "live",
		LastModified: now,
	}
	store := &memoryStore{current: []domain.Object{stray, backupObject}}
	plan := domain.Plan{
		Repository: "repo", Bucket: "bucket", Versioning: domain.BucketVersioningDisabled,
		VolumeDiscovery: true, DeleteRepositoryStray: true, DeleteObjects: 1, DeleteBytes: 3,
		Candidates: []domain.Candidate{{
			Kind: domain.CandidateRepositoryStray, Prefix: scope, ScopePrefix: scope,
			State: domain.StateOrphan, ObjectCount: 1, Bytes: 3, Objects: []domain.Object{stray},
			FullScopeSnapshot: true,
			ScopeObjects:      []domain.Object{stray, backupObject},
		}},
	}
	execution, err := (Executor{Kube: &fakeKube{exists: map[domain.BackupKey]bool{}}, Store: store}).Run(
		context.Background(),
		plan,
		ExecuteOptions{Concurrency: 1},
	)
	require.NoError(t, err)
	require.Len(t, store.deleted, 1)
	require.Equal(t, stray.Key, store.deleted[0].Key)
	require.Equal(t, 1, execution.Results[0].ObjectsDeleted)
}

func TestExecutorRejectsUnauthorizedRepositoryStrayPlan(t *testing.T) {
	t.Parallel()

	plan := domain.Plan{
		Versioning: domain.BucketVersioningDisabled, VolumeDiscovery: true,
		Candidates: []domain.Candidate{{
			Kind: domain.CandidateRepositoryStray, Prefix: "pvc-root/stray",
			State: domain.StateOrphan, ObjectCount: 1,
			Objects: []domain.Object{{Key: "pvc-root/stray/data"}},
		}},
		DeleteObjects: 1,
	}
	_, err := (Executor{Kube: &fakeKube{}, Store: &memoryStore{}}).Run(
		context.Background(), plan, ExecuteOptions{Concurrency: 1},
	)
	require.ErrorContains(t, err, "explicit authorization")
}

func TestExecutorRevalidatesRepositoryStrayProtection(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	scope := "pvc-11111111-1111-4111-8111-111111111111/ns/cluster-aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	stray := domain.Object{Key: scope + "/unexpected", Size: 3, ETag: "stray", LastModified: now}
	plan := domain.Plan{
		Repository: "repo", Bucket: "bucket", Versioning: domain.BucketVersioningDisabled,
		VolumeDiscovery: true, DeleteRepositoryStray: true, DeleteObjects: 1, DeleteBytes: 3,
		Candidates: []domain.Candidate{{
			Kind: domain.CandidateRepositoryStray, Prefix: scope, ScopePrefix: scope,
			State: domain.StateOrphan, ObjectCount: 1, Bytes: 3, Objects: []domain.Object{stray},
		}},
	}
	kubeClient := &fakeKube{inventory: domain.Inventory{
		Backups: map[domain.BackupKey]domain.Backup{
			{Namespace: "ns", Name: "new"}: {Path: scope},
		},
	}}
	_, err := (Executor{Kube: kubeClient, Store: &memoryStore{current: []domain.Object{stray}}}).Run(
		context.Background(),
		plan,
		ExecuteOptions{Concurrency: 1},
	)
	require.ErrorContains(t, err, "repository stray")
	require.ErrorContains(t, err, "now protected")
}

func TestExecutorRejectsChangedRepositoryStraySnapshot(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	scope := "pvc-11111111-1111-4111-8111-111111111111/ns/cluster-aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	planned := domain.Object{Key: scope + "/unexpected", Size: 3, ETag: "before", LastModified: now}
	current := planned
	current.ETag = "after"
	plan := domain.Plan{
		Repository: "repo", Bucket: "bucket", Versioning: domain.BucketVersioningDisabled,
		VolumeDiscovery: true, DeleteRepositoryStray: true, DeleteObjects: 1, DeleteBytes: 3,
		Candidates: []domain.Candidate{{
			Kind: domain.CandidateRepositoryStray, Prefix: scope, ScopePrefix: scope,
			State: domain.StateOrphan, ObjectCount: 1, Bytes: 3, Objects: []domain.Object{planned},
		}},
	}
	execution, err := (Executor{
		Kube: &fakeKube{}, Store: &memoryStore{current: []domain.Object{current}},
	}).Run(context.Background(), plan, ExecuteOptions{Concurrency: 1})
	require.ErrorContains(t, err, "cleanup candidates failed")
	require.Equal(t, "repository stray objects changed after planning", execution.Results[0].Error)
}

func TestExecutorRejectsChangedRepositoryScope(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	scope := "pvc-11111111-1111-4111-8111-111111111111/ns/cluster-aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	stray := domain.Object{Key: scope + "/unexpected", Size: 3, ETag: "stray", LastModified: now}
	backupObject := domain.Object{
		Key:          scope + "/component/backup/data",
		Size:         7,
		ETag:         "live",
		LastModified: now,
	}
	newManifest := domain.Object{
		Key: scope + "/component/new/" + domain.DefaultManifest, Size: 5,
		ETag: "manifest", LastModified: now,
	}
	plan := domain.Plan{
		Repository: "repo", Bucket: "bucket", Versioning: domain.BucketVersioningDisabled,
		VolumeDiscovery: true, DeleteRepositoryStray: true, DeleteObjects: 1, DeleteBytes: 3,
		Candidates: []domain.Candidate{{
			Kind: domain.CandidateRepositoryStray, Prefix: scope, ScopePrefix: scope,
			State: domain.StateOrphan, ObjectCount: 1, Bytes: 3, Objects: []domain.Object{stray},
			FullScopeSnapshot: true,
			ScopeObjects:      []domain.Object{stray, backupObject},
		}},
	}
	execution, err := (Executor{
		Kube:  &fakeKube{},
		Store: &memoryStore{current: []domain.Object{stray, backupObject, newManifest}},
	}).Run(context.Background(), plan, ExecuteOptions{Concurrency: 1})
	require.ErrorContains(t, err, "cleanup candidates failed")
	require.Equal(t, "repository scope changed after planning", execution.Results[0].Error)
}
