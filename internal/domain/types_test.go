package domain //nolint:testpackage // White-box tests cover compact domain identity helpers.

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBackupIdentityAndManifestRepository(t *testing.T) {
	t.Parallel()

	key := BackupKey{Namespace: "ns", Name: "backup"}
	require.Equal(t, "ns/backup", key.String())

	manifest := BackupManifest{}
	manifest.Metadata.Namespace = key.Namespace
	manifest.Metadata.Name = key.Name
	manifest.Metadata.Labels = map[string]string{BackupRepoLabel: "label-repo"}
	require.Equal(t, key, manifest.Key())
	require.Equal(t, "label-repo", manifest.Repo())
	manifest.Status.BackupRepoName = "status-repo"
	require.Equal(t, "status-repo", manifest.Repo())
}
