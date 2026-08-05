package cli //nolint:testpackage // White-box tests verify credential redaction invariants.

import (
	"bytes"
	"testing"

	"github.com/labring-sigs/kbbackup-prune/internal/domain"
	"github.com/stretchr/testify/require"
)

func TestWriteDebugRedactsCredentials(t *testing.T) {
	t.Parallel()

	settings := domain.S3Settings{
		AccessKeyID:      "ACCESSKEY1234",
		SecretAccessKey:  "super-secret-value",
		SessionToken:     "session-token-value",
		CredentialSource: domain.S3AuthSourceGenerated,
		CredentialRef: &domain.SecretRef{
			Namespace: "sealos",
			Name:      "generated-secret",
		},
		CredentialKeys: []string{"accessKeyID", "endpoint", "secretAccessKey"},
	}

	var output bytes.Buffer

	//nolint:gosec // Credentials in this fixture verify URL and Secret redaction.
	require.NoError(t, writeDebug(&output, debugInfo{
		Settings:       settings,
		ObjectPrefixes: map[string]string{"ns": "pvc-root"},
		Bucket:         "bucket",
		Endpoint:       "https://user:password@storage.example.test/path?token=sensitive#fragment",
		Region:         "region",
		Prefix:         "root/ns",
		PathStyle:      true,
		VersioningMode: domain.BucketVersioningModeDisabled,
	}))

	debugOutput := output.String()
	require.Contains(t, debugOutput, `credential_source="status.generatedCSIDriverSecret"`)
	require.Contains(t, debugOutput, `credential_ref="sealos/generated-secret"`)
	require.Contains(t, debugOutput, `masked="ACCE...1234"`)
	require.Contains(t, debugOutput, "sha256=")
	require.Contains(t, debugOutput, `endpoint="https://storage.example.test/path"`)
	require.Contains(t, debugOutput, `bucket_versioning_mode="disabled"`)
	require.Contains(t, debugOutput, `repository_object_prefixes=1`)
	require.Contains(t, debugOutput, `repository_object_prefix_namespace="ns"`)
	require.Contains(t, debugOutput, `repository_object_prefix="pvc-root"`)
	require.NotContains(t, debugOutput, settings.AccessKeyID)
	require.NotContains(t, debugOutput, settings.SecretAccessKey)
	require.NotContains(t, debugOutput, settings.SessionToken)
	require.NotContains(t, debugOutput, "password")
	require.NotContains(t, debugOutput, "sensitive")
}

func TestDebugHelpersHandleEmptyAndShortValues(t *testing.T) {
	t.Parallel()

	require.Equal(t, "<empty>", maskCredential(""))
	require.Equal(t, "****", maskCredential("tiny"))
	require.Equal(t, "<empty>", credentialFingerprint(""))
	require.Equal(t, "<sdk-default>", redactEndpoint(""))
	require.Equal(t, "<invalid>", redactEndpoint(":"))
	require.NoError(t, writeDebug(nil, debugInfo{}))
}
