package kube //nolint:testpackage // White-box tests cover unexported Kubernetes parsing and config helpers.

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/labring-sigs/kbbackup-prune/internal/domain"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
)

func repoObject() *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "dataprotection.kubeblocks.io/v1alpha1",
		"kind":       "BackupRepo",
		"metadata": map[string]any{
			"name": "repo", "uid": "repo-uid", "generation": int64(3),
		},
		"spec": map[string]any{
			"pathPrefix": "root",
			"config": map[string]any{
				"bucket":   "bucket",
				"endpoint": "http://minio:9000",
				"region":   "us-east-1",
				"insecure": "true",
			},
			"credential": map[string]any{"namespace": "secrets", "name": "repo-creds"},
		},
		"status": map[string]any{
			"generatedStorageClassName": "sc-repo", "backupPVCName": "pvc-repo",
			"generatedCSIDriverSecret": map[string]any{
				"namespace": "secrets", "name": "generated-repo-creds",
			},
		},
	}}
}

func backupObject(namespace, name string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "dataprotection.kubeblocks.io/v1alpha1",
		"kind":       "Backup",
		"metadata": map[string]any{
			"namespace": namespace, "name": name, "uid": "uid-" + name,
			"labels": map[string]any{
				domain.BackupRepoLabel: "repo",
				domain.ClusterUIDLabel: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
			},
		},
		"status": map[string]any{
			"backupRepoName": "repo", "path": "/root/" + namespace + "/cluster/component/" + name,
			"kopiaRepoPath":    "/root/" + namespace + "/cluster/component/_kopia",
			"parentBackupName": "parent", "baseBackupName": "base",
		},
	}}
}

func restoreObject(
	namespace, name, backupNamespace, backupName, phase string,
) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "dataprotection.kubeblocks.io/v1alpha1",
		"kind":       "Restore",
		"metadata":   map[string]any{"namespace": namespace, "name": name},
		"spec": map[string]any{
			"backup": map[string]any{"namespace": backupNamespace, "name": backupName},
		},
		"status": map[string]any{"phase": phase},
	}}
}

func dynamicClient(objects ...runtime.Object) *dynamicfake.FakeDynamicClient {
	return dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		runtime.NewScheme(),
		map[schema.GroupVersionResource]string{
			backupGVR: "BackupList", backupRepoGVR: "BackupRepoList", restoreGVR: "RestoreList",
		},
		objects...)
}

func TestInventoryLoadsBackupsCredentialsAndPVCProtections(t *testing.T) {
	t.Parallel()

	scName := "sc-repo"
	typed := kubernetesfake.NewClientset(
		&storagev1.StorageClass{
			ObjectMeta: metav1.ObjectMeta{Name: scName}, Provisioner: "ru.yandex.s3.csi",
			Parameters: map[string]string{"bucket": "bucket"},
		},
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Namespace: "secrets", Name: "repo-creds"},
			Data: map[string][]byte{
				"accessKeyId":     []byte("source-access"),
				"secretAccessKey": []byte("source-secret"),
			},
		},
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "secrets", Name: "generated-repo-creds",
			},
			Data: map[string][]byte{
				"accessKeyID":     []byte("generated-access"),
				"secretAccessKey": []byte("generated-secret"),
				"endpoint":        []byte("https://storage.googleapis.com"),
				"region":          []byte("generated-region"),
			},
		},
		&corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "ns",
				Name:      "pvc-repo",
				Labels:    map[string]string{domain.BackupRepoLabel: "repo"},
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				StorageClassName: &scName,
				VolumeName:       "pv-repo",
			},
			Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
		},
		&corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{Name: "pv-repo"},
			Spec: corev1.PersistentVolumeSpec{PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{
					Driver: domain.S3CSIDriverYandex, VolumeHandle: "bucket/repo-root",
				},
			}},
		},
		&corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "ns",
				Name:      "user-data",
				UID:       types.UID("claim-uid"),
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				StorageClassName: &scName,
				VolumeName:       "pv-user",
			},
			Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
		},
		&corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{Name: "pv-user"},
			Spec: corev1.PersistentVolumeSpec{PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{
					Driver: "ru.yandex.s3.csi", VolumeHandle: "pvc-volume-prefix",
					VolumeAttributes: map[string]string{"path": "s3://bucket/user/subdir"},
				},
			}},
		},
	)
	client := NewForClients(dynamicClient(
		repoObject(),
		backupObject("ns", "backup"),
		restoreObject("restore-ns", "active", "ns", "restore-source", "Running"),
		restoreObject("restore-ns", "done", "ns", "completed-source", "Completed"),
	), typed)
	inventory, settings, err := client.Inventory(context.Background(), "repo", "", true)
	require.NoError(t, err)
	require.Equal(t, "bucket", settings.Bucket)
	require.Equal(t, "repo-uid", inventory.Repo.UID)
	require.EqualValues(t, 3, inventory.Repo.Generation)
	require.Equal(t, "https://storage.googleapis.com", settings.Endpoint)
	require.Equal(t, "generated-region", settings.Region)
	require.Equal(t, "generated-access", settings.AccessKeyID)
	require.Equal(t, "generated-secret", settings.SecretAccessKey)
	require.Equal(t, domain.S3AuthSourceGenerated, settings.CredentialSource)
	require.Equal(
		t,
		&domain.SecretRef{Namespace: "secrets", Name: "generated-repo-creds"},
		settings.CredentialRef,
	)
	require.ElementsMatch(
		t,
		[]string{"accessKeyID", "endpoint", "region", "secretAccessKey"},
		settings.CredentialKeys,
	)
	require.True(t, settings.Insecure)
	require.Empty(t, inventory.BlockingReasons)
	require.Equal(t, map[string]string{"ns": "repo-root"}, inventory.Repo.ObjectPrefixes)
	require.Equal(
		t,
		"repo-root/root/ns/cluster/component/backup",
		inventory.Backups[domain.BackupKey{Namespace: "ns", Name: "backup"}].Path,
	)
	require.Equal(
		t,
		"root/ns/cluster/component/backup",
		inventory.Backups[domain.BackupKey{Namespace: "ns", Name: "backup"}].RawPath,
	)
	require.Equal(
		t,
		"root/ns/cluster/component/_kopia",
		inventory.Backups[domain.BackupKey{Namespace: "ns", Name: "backup"}].RawKopiaRepoPath,
	)
	require.Equal(
		t,
		"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
		inventory.Backups[domain.BackupKey{Namespace: "ns", Name: "backup"}].ClusterUID,
	)
	require.Contains(
		t,
		inventory.ProtectedBackups[domain.BackupKey{Namespace: "ns", Name: "restore-source"}],
		"active Restore restore-ns/active",
	)
	require.NotContains(
		t,
		inventory.ProtectedBackups,
		domain.BackupKey{Namespace: "ns", Name: "completed-source"},
	)

	prefixes := make(map[string]bool)
	for _, protection := range inventory.Protections {
		prefixes[protection.Prefix] = true
		require.Equal(t, "PVC ns/user-data", protection.Resource)
	}

	require.True(t, prefixes["claim-uid"])
	require.True(t, prefixes["pv-user"])
	require.True(t, prefixes["pvc-volume-prefix"])
	require.True(t, prefixes["user/subdir"])
}

func TestInventoryFallsBackToSpecCredential(t *testing.T) {
	t.Parallel()

	repo := repoObject()
	unstructured.RemoveNestedField(repo.Object, "status", "generatedCSIDriverSecret")
	unstructured.RemoveNestedField(repo.Object, "status", "generatedStorageClassName")

	typed := kubernetesfake.NewClientset(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: "secrets", Name: "repo-creds"},
		Data: map[string][]byte{
			"accessKeyId":     []byte("source-access"),
			"secretAccessKey": []byte("source-secret"),
		},
	})

	_, settings, err := NewForClients(dynamicClient(repo), typed).Inventory(
		context.Background(),
		"repo",
		"",
		true,
	)
	require.NoError(t, err)
	require.Equal(t, "source-access", settings.AccessKeyID)
	require.Equal(t, "source-secret", settings.SecretAccessKey)
	require.Equal(t, "http://minio:9000", settings.Endpoint)
	require.Equal(t, domain.S3AuthSourceSpec, settings.CredentialSource)
}

func TestInventoryRecordsPVCInspectionBlockers(t *testing.T) {
	t.Parallel()

	client := NewForClients(dynamicClient(repoObject()), kubernetesfake.NewClientset())
	inventory, _, err := client.Inventory(context.Background(), "repo", "", false)
	require.NoError(t, err)
	require.Len(t, inventory.BlockingReasons, 1)
	require.Contains(t, inventory.BlockingReasons[0], "StorageClass")
}

func TestInventoryBlocksUnmappableCSIPrefix(t *testing.T) {
	t.Parallel()

	scName := "sc-repo"
	typed := kubernetesfake.NewClientset(
		&storagev1.StorageClass{
			ObjectMeta: metav1.ObjectMeta{Name: scName}, Provisioner: "ru.yandex.s3.csi",
			Parameters: map[string]string{"bucket": "bucket"},
		},
		&corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "user-data"},
			Spec: corev1.PersistentVolumeClaimSpec{
				StorageClassName: &scName,
				VolumeName:       "pv-user",
			},
			Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
		},
		&corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{Name: "pv-user"},
			Spec: corev1.PersistentVolumeSpec{PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{
					Driver:       "ru.yandex.s3.csi",
					VolumeHandle: "https://unmappable.example/volume",
				},
			}},
		},
	)
	client := NewForClients(dynamicClient(repoObject()), typed)
	inventory, _, err := client.Inventory(context.Background(), "repo", "", false)
	require.NoError(t, err)
	require.Contains(
		t,
		inventory.BlockingReasons,
		"PVC ns/user-data has no safely mappable CSI object prefix",
	)
}

func TestInventoryBlocksUnsupportedCSIPrefixMapper(t *testing.T) {
	t.Parallel()

	scName := "sc-repo"
	typed := kubernetesfake.NewClientset(
		&storagev1.StorageClass{
			ObjectMeta:  metav1.ObjectMeta{Name: scName},
			Provisioner: "custom.s3.csi",
			Parameters:  map[string]string{"bucket": "bucket"},
		},
		&corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "user-data"},
			Spec: corev1.PersistentVolumeClaimSpec{
				StorageClassName: &scName,
				VolumeName:       "pv-user",
			},
			Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
		},
		&corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{Name: "pv-user"},
			Spec: corev1.PersistentVolumeSpec{PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{
					Driver: "custom.s3.csi", VolumeHandle: "opaque-id",
				},
			}},
		},
	)
	client := NewForClients(dynamicClient(repoObject()), typed)
	inventory, _, err := client.Inventory(context.Background(), "repo", "", false)
	require.NoError(t, err)
	require.Contains(
		t,
		inventory.BlockingReasons,
		`PVC ns/user-data uses unsupported CSI provisioner "custom.s3.csi"; object prefix mapping is unavailable`,
	)
}

func TestInventorySelectsRepositoryPVCObjectPrefixByNamespace(t *testing.T) {
	t.Parallel()

	scName := "sc-repo"
	typed := kubernetesfake.NewClientset(
		&storagev1.StorageClass{
			ObjectMeta:  metav1.ObjectMeta{Name: scName},
			Provisioner: domain.S3CSIDriverYandex,
			Parameters:  map[string]string{"bucket": "bucket"},
		},
		repositoryPVC("ns-a", "pv-a"),
		repositoryPVC("ns-b", "pv-b"),
		repositoryPV("pv-a", "bucket/pvc-a"),
		repositoryPV("pv-b", "bucket/pvc-b"),
	)

	inventory, _, err := NewForClients(dynamicClient(repoObject()), typed).Inventory(
		context.Background(),
		"repo",
		"ns-a",
		false,
	)
	require.NoError(t, err)
	require.Empty(t, inventory.BlockingReasons)
	require.Equal(t, map[string]string{"ns-a": "pvc-a"}, inventory.Repo.ObjectPrefixes)
}

func TestInventoryBlocksOverlappingRepositoryPVCObjectPrefixes(t *testing.T) {
	t.Parallel()

	scName := "sc-repo"
	typed := kubernetesfake.NewClientset(
		&storagev1.StorageClass{
			ObjectMeta:  metav1.ObjectMeta{Name: scName},
			Provisioner: domain.S3CSIDriverYandex,
			Parameters:  map[string]string{"bucket": "bucket"},
		},
		repositoryPVC("ns-a", "pv-a"),
		repositoryPVC("ns-b", "pv-b"),
		repositoryPV("pv-a", "bucket/shared"),
		repositoryPV("pv-b", "bucket/shared/nested"),
	)

	inventory, _, err := NewForClients(dynamicClient(repoObject()), typed).Inventory(
		context.Background(),
		"repo",
		"",
		false,
	)
	require.NoError(t, err)
	require.Len(t, inventory.BlockingReasons, 1)
	require.Contains(t, inventory.BlockingReasons[0], "object prefixes overlap")
}

func TestInventoryBlocksUnsafeRepositoryPVCMapping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		provisioner string
		volumeName  string
		driver      string
		handle      string
		want        string
	}{
		{
			name:        "unbound",
			provisioner: domain.S3CSIDriverYandex,
			want:        "is not bound to a PV",
		},
		{
			name:        "unsupported driver",
			provisioner: "custom.s3.csi",
			volumeName:  "pv-repo",
			driver:      "custom.s3.csi",
			handle:      "bucket/root",
			want:        "unsupported CSI provisioner",
		},
		{
			name:        "empty root",
			provisioner: domain.S3CSIDriverYandex,
			volumeName:  "pv-repo",
			driver:      domain.S3CSIDriverYandex,
			handle:      "bucket",
			want:        "non-empty CSI volumeHandle prefix",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			scName := "sc-repo"
			pvc := repositoryPVC("ns", test.volumeName)

			objects := []runtime.Object{
				&storagev1.StorageClass{
					ObjectMeta:  metav1.ObjectMeta{Name: scName},
					Provisioner: test.provisioner,
					Parameters:  map[string]string{"bucket": "bucket"},
				},
				pvc,
			}
			if test.volumeName != "" {
				objects = append(objects, &corev1.PersistentVolume{
					ObjectMeta: metav1.ObjectMeta{Name: test.volumeName},
					Spec: corev1.PersistentVolumeSpec{
						PersistentVolumeSource: corev1.PersistentVolumeSource{
							CSI: &corev1.CSIPersistentVolumeSource{
								Driver: test.driver, VolumeHandle: test.handle,
							},
						},
					},
				})
			}

			inventory, _, err := NewForClients(
				dynamicClient(repoObject()),
				kubernetesfake.NewClientset(objects...),
			).Inventory(context.Background(), "repo", "ns", false)
			require.NoError(t, err)
			require.Contains(t, strings.Join(inventory.BlockingReasons, "\n"), test.want)
		})
	}
}

func repositoryPVC(namespace, volumeName string) *corev1.PersistentVolumeClaim {
	storageClass := "sc-repo"

	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      "pvc-repo",
			Labels:    map[string]string{domain.BackupRepoLabel: "repo"},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: &storageClass,
			VolumeName:       volumeName,
		},
	}
}

func repositoryPV(name, handle string) *corev1.PersistentVolume {
	return &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: corev1.PersistentVolumeSpec{
			StorageClassName: "sc-repo",
			ClaimRef: &corev1.ObjectReference{
				Namespace: "ns", Name: "pvc-repo",
			},
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{
					Driver: domain.S3CSIDriverYandex, VolumeHandle: handle,
				},
			},
		},
		Status: corev1.PersistentVolumeStatus{Phase: corev1.VolumeBound},
	}
}

func TestInventoryRecordsIncompleteActiveRestore(t *testing.T) {
	t.Parallel()

	repo := repoObject()
	unstructured.RemoveNestedField(repo.Object, "status", "generatedStorageClassName")

	incompleteRestore := restoreObject("restore-ns", "incomplete", "", "", "Running")
	client := NewForClients(dynamicClient(repo, incompleteRestore), kubernetesfake.NewClientset())
	inventory, _, err := client.Inventory(context.Background(), "repo", "", false)
	require.NoError(t, err)
	require.Len(t, inventory.BlockingReasons, 1)
	require.Contains(t, inventory.BlockingReasons[0], "incomplete backup reference")
}

func TestInventoryErrors(t *testing.T) {
	t.Parallel()

	client := NewForClients(dynamicClient(), kubernetesfake.NewClientset())
	_, _, err := client.Inventory(context.Background(), "", "", false)
	require.ErrorContains(t, err, "name is required")
	_, _, err = client.Inventory(context.Background(), "missing", "", false)
	require.ErrorContains(t, err, "get BackupRepo")

	client = NewForClients(dynamicClient(repoObject()), kubernetesfake.NewClientset())
	_, _, err = client.Inventory(context.Background(), "repo", "", true)
	require.ErrorContains(t, err, "status.generatedCSIDriverSecret Secret")
}

func TestBackupExists(t *testing.T) {
	t.Parallel()

	client := NewForClients(
		dynamicClient(backupObject("ns", "backup")),
		kubernetesfake.NewClientset(),
	)
	exists, err := client.BackupExists(
		context.Background(),
		domain.BackupKey{Namespace: "ns", Name: "backup"},
	)
	require.NoError(t, err)
	require.True(t, exists)
	exists, err = client.BackupExists(
		context.Background(),
		domain.BackupKey{Namespace: "ns", Name: "missing"},
	)
	require.NoError(t, err)
	require.False(t, exists)
}

func TestInventoryUsesSpecParentBeforeStatusIsAvailable(t *testing.T) {
	t.Parallel()

	backup := backupObject("ns", "child")
	unstructured.RemoveNestedField(backup.Object, "status", "parentBackupName")
	require.NoError(
		t,
		unstructured.SetNestedField(backup.Object, "parent-from-spec", "spec", "parentBackupName"),
	)

	repo := repoObject()
	unstructured.RemoveNestedField(repo.Object, "status", "generatedStorageClassName")

	client := NewForClients(dynamicClient(repo, backup), kubernetesfake.NewClientset())
	inventory, _, err := client.Inventory(context.Background(), "repo", "", false)
	require.NoError(t, err)
	require.Equal(
		t,
		"parent-from-spec",
		inventory.Backups[domain.BackupKey{Namespace: "ns", Name: "child"}].ParentBackupName,
	)
}

func TestVolumePrefixParsing(t *testing.T) {
	t.Parallel()

	tests := []struct {
		value  string
		bucket string
		prefix string
		ok     bool
	}{
		{value: "s3://bucket/path/to/pvc", bucket: "bucket", prefix: "path/to/pvc", ok: true},
		{value: "s3://other/path", bucket: "bucket", ok: false},
		{value: "bucket/path", bucket: "bucket", prefix: "path", ok: true},
		{value: "bucket", bucket: "bucket", prefix: "", ok: true},
		{value: "https://example.com/path", bucket: "bucket", ok: false},
		{value: "", bucket: "bucket", ok: false},
	}
	for _, test := range tests {
		prefix, ok := normalizeVolumePrefix(test.value, test.bucket)
		require.Equal(t, test.ok, ok, test.value)
		require.Equal(t, test.prefix, prefix, test.value)
	}

	source := &corev1.CSIPersistentVolumeSource{
		Driver:       domain.S3CSIDriverYandex,
		VolumeHandle: "handle", VolumeAttributes: map[string]string{
			"options": "--prefix=from-option --path from-path", "subdir": "attribute",
		},
	}
	prefixes, supported := extractCSIPrefixes(source, "bucket")
	require.True(t, supported)
	require.ElementsMatch(
		t,
		[]string{"handle", "attribute", "from-option", "from-path"},
		prefixes,
	)

	_, supported = extractCSIPrefixes(
		&corev1.CSIPersistentVolumeSource{Driver: "example.csi.invalid", VolumeHandle: "opaque"},
		"bucket",
	)
	require.False(t, supported)
}

func TestInventoryIndexesRepositoryUserAndUnownedVolumeRoots(t *testing.T) {
	t.Parallel()

	const (
		repositoryRoot = "pvc-11111111-1111-4111-8111-111111111111"
		userRoot       = "pvc-22222222-2222-4222-8222-222222222222"
		unownedRoot    = "pvc-33333333-3333-4333-8333-333333333333"
	)

	scName := "sc-repo"
	userUID := types.UID("22222222-2222-4222-8222-222222222222")
	objects := []runtime.Object{
		&storagev1.StorageClass{
			ObjectMeta: metav1.ObjectMeta{Name: scName}, Provisioner: domain.S3CSIDriverYandex,
			Parameters: map[string]string{"bucket": "bucket"},
		},
		repositoryPVC("ns", "pv-repo"),
		repositoryPV("pv-repo", "bucket/"+repositoryRoot),
		&corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "user-data", UID: userUID},
			Spec: corev1.PersistentVolumeClaimSpec{
				StorageClassName: &scName,
				VolumeName:       "pv-user",
			},
		},
		&corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{Name: "pv-user"},
			Spec: corev1.PersistentVolumeSpec{PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{
					Driver:       domain.S3CSIDriverYandex,
					VolumeHandle: "bucket/" + userRoot,
				},
			}},
		},
		&corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{Name: unownedRoot},
			Spec: corev1.PersistentVolumeSpec{PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{
					Driver:       domain.S3CSIDriverYandex,
					VolumeHandle: "bucket/" + unownedRoot,
				},
			}},
		},
	}

	inventory, _, err := NewForClients(
		dynamicClient(repoObject()), kubernetesfake.NewClientset(objects...),
	).Inventory(context.Background(), "repo", "", false)
	require.NoError(t, err)
	require.Equal(t, domain.VolumeRootRepository, inventory.VolumeRoots[repositoryRoot].Kind)
	require.Equal(t, domain.VolumeRootUser, inventory.VolumeRoots[userRoot].Kind)
	require.Equal(t, domain.VolumeRootUser, inventory.VolumeRoots[unownedRoot].Kind)
	require.Equal(t, "ns", inventory.VolumeRoots[repositoryRoot].Namespace)
	require.Equal(t, "ns", inventory.VolumeRoots[userRoot].Namespace)
	require.Equal(t, map[string]string{"ns": repositoryRoot}, inventory.Repo.ObjectPrefixes)
}

func TestInventoryRecognizesReleasedBackupRepoPVsAsRepositoryRoots(t *testing.T) {
	t.Parallel()

	const (
		currentRoot    = "pvc-11111111-1111-4111-8111-111111111111"
		historicalRoot = "pvc-22222222-2222-4222-8222-222222222222"
		userRoot       = "pvc-33333333-3333-4333-8333-333333333333"
		preCheckRoot   = "pvc-44444444-4444-4444-8444-444444444444"
	)

	historical := repositoryPV(historicalRoot, "bucket/"+historicalRoot)
	historical.Spec.StorageClassName = "sc-repo"
	historical.Spec.ClaimRef = &corev1.ObjectReference{
		Namespace: "ns", Name: "pvc-repo",
	}
	historical.Status.Phase = corev1.VolumeReleased

	user := repositoryPV(userRoot, "bucket/"+userRoot)
	user.Spec.StorageClassName = "sc-repo"
	user.Spec.ClaimRef = &corev1.ObjectReference{
		Namespace: "ns", Name: "user-data",
	}
	user.Status.Phase = corev1.VolumeReleased

	preCheck := repositoryPV(preCheckRoot, "bucket/"+preCheckRoot)
	preCheck.Spec.StorageClassName = "sc-repo"
	preCheck.Spec.ClaimRef = &corev1.ObjectReference{
		Namespace: "controller", Name: "pre-check-repo-uid-repo",
	}
	preCheck.Status.Phase = corev1.VolumeReleased

	objects := []runtime.Object{
		&storagev1.StorageClass{
			ObjectMeta:  metav1.ObjectMeta{Name: "sc-repo"},
			Provisioner: domain.S3CSIDriverYandex,
			Parameters:  map[string]string{"bucket": "bucket"},
		},
		repositoryPVC("ns", "pv-current"),
		repositoryPV("pv-current", "bucket/"+currentRoot),
		historical,
		user,
		preCheck,
	}

	inventory, _, err := NewForClients(
		dynamicClient(repoObject()), kubernetesfake.NewClientset(objects...),
	).Inventory(context.Background(), "repo", "", false)
	require.NoError(t, err)
	require.Equal(
		t,
		domain.VolumeRootRepository,
		inventory.VolumeRoots[historicalRoot].Kind,
	)
	require.Equal(t, "ns", inventory.VolumeRoots[historicalRoot].Namespace)
	require.False(t, inventory.VolumeRoots[historicalRoot].Current)
	require.True(t, inventory.VolumeRoots[currentRoot].Current)
	require.Equal(t, domain.VolumeRootUser, inventory.VolumeRoots[userRoot].Kind)
	require.Equal(
		t,
		domain.VolumeRootRepository,
		inventory.VolumeRoots[preCheckRoot].Kind,
	)
	require.Equal(t, "controller", inventory.VolumeRoots[preCheckRoot].Namespace)
	require.False(t, inventory.VolumeRoots[preCheckRoot].Current)
	require.Equal(t, map[string]string{"ns": currentRoot}, inventory.Repo.ObjectPrefixes)
}

func TestInventoryKeepsBoundPreCheckPVCurrent(t *testing.T) {
	t.Parallel()

	const preCheckRoot = "pvc-44444444-4444-4444-8444-444444444444"

	preCheck := repositoryPV(preCheckRoot, "bucket/"+preCheckRoot)
	preCheck.Spec.StorageClassName = "sc-repo"
	preCheck.Spec.ClaimRef = &corev1.ObjectReference{
		Namespace: "controller", Name: "pre-check-repo-uid-repo",
	}
	preCheck.Status.Phase = corev1.VolumeBound

	inventory, _, err := NewForClients(
		dynamicClient(repoObject()),
		kubernetesfake.NewClientset(
			&storagev1.StorageClass{
				ObjectMeta:  metav1.ObjectMeta{Name: "sc-repo"},
				Provisioner: domain.S3CSIDriverYandex,
				Parameters:  map[string]string{"bucket": "bucket"},
			},
			preCheck,
		),
	).Inventory(context.Background(), "repo", "", false)
	require.NoError(t, err)
	require.Equal(t, domain.VolumeRootRepository, inventory.VolumeRoots[preCheckRoot].Kind)
	require.True(t, inventory.VolumeRoots[preCheckRoot].Current)
}

func TestInventoryUserVolumeWinsRepositoryRootCollision(t *testing.T) {
	t.Parallel()

	const (
		currentRoot = "pvc-11111111-1111-4111-8111-111111111111"
		sharedRoot  = "pvc-55555555-5555-4555-8555-555555555555"
	)

	historical := repositoryPV("pv-historical", "bucket/"+sharedRoot)
	historical.Status.Phase = corev1.VolumeReleased

	userStorageClass := "user-sc"
	userPV := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pv-user"},
		Spec: corev1.PersistentVolumeSpec{
			StorageClassName: userStorageClass,
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{
					Driver: domain.S3CSIDriverYandex, VolumeHandle: "bucket/" + sharedRoot,
				},
			},
		},
		Status: corev1.PersistentVolumeStatus{Phase: corev1.VolumeBound},
	}
	userPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "user-ns", Name: "user-data",
			UID: types.UID("55555555-5555-4555-8555-555555555555"),
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: &userStorageClass, VolumeName: userPV.Name,
		},
		Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
	}

	inventory, _, err := NewForClients(
		dynamicClient(repoObject()),
		kubernetesfake.NewClientset(
			&storagev1.StorageClass{
				ObjectMeta:  metav1.ObjectMeta{Name: "sc-repo"},
				Provisioner: domain.S3CSIDriverYandex,
				Parameters:  map[string]string{"bucket": "bucket"},
			},
			repositoryPVC("ns", "pv-current"),
			repositoryPV("pv-current", "bucket/"+currentRoot),
			historical,
			userPV,
			userPVC,
		),
	).Inventory(context.Background(), "repo", "", false)
	require.NoError(t, err)
	require.Empty(t, inventory.BlockingReasons)
	require.Equal(t, domain.VolumeRootUser, inventory.VolumeRoots[sharedRoot].Kind)
	require.True(t, inventory.VolumeRoots[sharedRoot].Current)
	require.Equal(t, "PVC user-ns/user-data", inventory.VolumeRoots[sharedRoot].Resource)
	require.Equal(t, domain.VolumeRootRepository, inventory.VolumeRoots[currentRoot].Kind)
}

func TestVolumeRootUserOwnershipPrecedenceIsOrderIndependent(t *testing.T) {
	t.Parallel()

	const root = "pvc-55555555-5555-4555-8555-555555555555"

	repository := domain.VolumeRoot{
		Prefix: root, Kind: domain.VolumeRootRepository,
		Resource: "BackupRepo PV historical", Current: false,
	}

	user := domain.VolumeRoot{
		Prefix: root, Kind: domain.VolumeRootUser, Namespace: "user-ns",
		Resource: "PVC user-ns/data", Current: true,
	}
	for name, roots := range map[string][]domain.VolumeRoot{
		"repository then user": {repository, user},
		"user then repository": {user, repository},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			inventory := domain.Inventory{}
			for _, owner := range roots {
				addVolumeRoot(&inventory, owner)
			}

			require.Equal(t, user, inventory.VolumeRoots[root])
		})
	}
}

func TestBackupRepoPVClaimNames(t *testing.T) {
	t.Parallel()

	repo := domain.Repository{
		Name: "repo", UID: "12345678-1234-1234-1234-123456789012", BackupPVCName: "repo-pvc",
	}
	require.True(t, isBackupRepoPVClaim(repo, "repo-pvc"))
	require.True(t, isBackupRepoPVClaim(repo, "pre-check-12345678-repo"))
	require.False(t, isBackupRepoPVClaim(repo, "pre-check-87654321-repo"))
	require.False(t, isBackupRepoPVClaim(domain.Repository{UID: "short"}, "pre-check-short-repo"))
}

func TestInventoryUsesStatusBackupPVCNameWithoutLabel(t *testing.T) {
	t.Parallel()

	const repositoryRoot = "pvc-11111111-1111-4111-8111-111111111111"

	pvc := repositoryPVC("ns", "pv-repo")
	pvc.Labels = nil
	typed := kubernetesfake.NewClientset(
		&storagev1.StorageClass{
			ObjectMeta:  metav1.ObjectMeta{Name: "sc-repo"},
			Provisioner: domain.S3CSIDriverYandex,
			Parameters:  map[string]string{"bucket": "bucket"},
		},
		pvc,
		repositoryPV("pv-repo", "bucket/"+repositoryRoot),
	)
	inventory, _, err := NewForClients(dynamicClient(repoObject()), typed).Inventory(
		context.Background(), "repo", "", false,
	)
	require.NoError(t, err)
	require.Empty(t, inventory.BlockingReasons)
	require.Equal(t, domain.VolumeRootRepository, inventory.VolumeRoots[repositoryRoot].Kind)
}

func TestInventoryBlocksBackupPVCLabelMismatch(t *testing.T) {
	t.Parallel()

	pvc := repositoryPVC("ns", "pv-repo")
	pvc.Labels[domain.BackupRepoLabel] = "other-repo"
	typed := kubernetesfake.NewClientset(
		&storagev1.StorageClass{
			ObjectMeta:  metav1.ObjectMeta{Name: "sc-repo"},
			Provisioner: domain.S3CSIDriverYandex,
			Parameters:  map[string]string{"bucket": "bucket"},
		},
		pvc,
		repositoryPV("pv-repo", "bucket/pvc-11111111-1111-4111-8111-111111111111"),
	)
	inventory, _, err := NewForClients(dynamicClient(repoObject()), typed).Inventory(
		context.Background(), "repo", "", false,
	)
	require.NoError(t, err)
	require.Contains(t, strings.Join(inventory.BlockingReasons, "\n"), "other-repo")
}

func TestCanonicalPVCRoot(t *testing.T) {
	t.Parallel()

	root, ok := canonicalPVCRoot("pvc-11111111-1111-4111-8111-111111111111")
	require.True(t, ok)
	require.Equal(t, "pvc-11111111-1111-4111-8111-111111111111", root)

	_, ok = canonicalPVCRoot("pvc-not-a-uuid")
	require.False(t, ok)
	_, ok = canonicalPVCRoot("prefix/pvc-11111111-1111-4111-8111-111111111111")
	require.False(t, ok)
}

func TestLoadRESTConfigFromExplicitKubeconfig(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	filename := filepath.Join(directory, "config")
	content := []byte(`apiVersion: v1
kind: Config
clusters:
- name: test
  cluster:
    server: https://kubernetes.example.test
    insecure-skip-tls-verify: true
users:
- name: test
  user:
    token: token
contexts:
- name: test
  context:
    cluster: test
    user: test
current-context: test
`)
	require.NoError(t, os.WriteFile(filename, content, 0o600))
	config, err := loadRESTConfig(ConfigOptions{
		Mode:       "kubeconfig",
		Kubeconfig: filename,
		Context:    "test",
		Timeout:    12 * time.Second,
		QPS:        9,
		Burst:      18,
	})
	require.NoError(t, err)
	require.Equal(t, "https://kubernetes.example.test", config.Host)
	require.Equal(t, "kbbackup-prune", config.UserAgent)
	require.Equal(t, 12*time.Second, config.Timeout)
	require.EqualValues(t, 9, config.QPS)
	require.Equal(t, 18, config.Burst)

	_, err = loadRESTConfig(ConfigOptions{Mode: "invalid"})
	require.ErrorContains(t, err, "invalid Kubernetes mode")
}
