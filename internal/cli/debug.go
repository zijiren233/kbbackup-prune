package cli

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/url"
	"strings"

	"github.com/labring-sigs/kbbackup-prune/internal/domain"
)

type debugInfo struct {
	Settings       domain.S3Settings
	ObjectPrefixes map[string]string
	Bucket         string
	Endpoint       string
	Region         string
	Prefix         string
	PathStyle      bool
	VersioningMode string
	Insecure       bool
}

func writeDebug(writer io.Writer, info debugInfo) error {
	if writer == nil {
		writer = io.Discard
	}

	credentialRef := "<none>"
	if info.Settings.CredentialRef != nil {
		credentialRef = info.Settings.CredentialRef.Namespace + "/" +
			info.Settings.CredentialRef.Name
	}

	var output strings.Builder
	fmt.Fprintf(&output, "debug: credential_source=%q\n", info.Settings.CredentialSource)
	fmt.Fprintf(&output, "debug: credential_ref=%q\n", credentialRef)
	fmt.Fprintf(
		&output,
		"debug: credential_keys=%q\n",
		strings.Join(info.Settings.CredentialKeys, ","),
	)
	fmt.Fprintf(
		&output,
		"debug: access_key_id masked=%q bytes=%d sha256=%s\n",
		maskCredential(info.Settings.AccessKeyID),
		len(info.Settings.AccessKeyID),
		credentialFingerprint(info.Settings.AccessKeyID),
	)
	fmt.Fprintf(
		&output,
		"debug: secret_access_key present=%t bytes=%d sha256=%s\n",
		info.Settings.SecretAccessKey != "",
		len(info.Settings.SecretAccessKey),
		credentialFingerprint(info.Settings.SecretAccessKey),
	)
	fmt.Fprintf(
		&output,
		"debug: session_token present=%t bytes=%d sha256=%s\n",
		info.Settings.SessionToken != "",
		len(info.Settings.SessionToken),
		credentialFingerprint(info.Settings.SessionToken),
	)
	fmt.Fprintf(
		&output,
		"debug: s3 bucket=%q endpoint=%q region=%q path_style=%t insecure_tls=%t\n",
		info.Bucket,
		redactEndpoint(info.Endpoint),
		info.Region,
		info.PathStyle,
		info.Insecure,
	)
	fmt.Fprintf(&output, "debug: bucket_versioning_mode=%q\n", info.VersioningMode)
	fmt.Fprintf(&output, "debug: scan_prefix=%q\n", info.Prefix)
	fmt.Fprintf(&output, "debug: repository_object_prefixes=%d\n", len(info.ObjectPrefixes))

	if len(info.ObjectPrefixes) == 1 {
		for namespace, prefix := range info.ObjectPrefixes {
			fmt.Fprintf(&output, "debug: repository_object_prefix_namespace=%q\n", namespace)
			fmt.Fprintf(&output, "debug: repository_object_prefix=%q\n", prefix)
		}
	}

	_, err := io.WriteString(writer, output.String())

	return err
}

func maskCredential(value string) string {
	if value == "" {
		return "<empty>"
	}

	if len(value) <= 8 {
		return strings.Repeat("*", len(value))
	}

	return value[:4] + "..." + value[len(value)-4:]
}

func credentialFingerprint(value string) string {
	if value == "" {
		return "<empty>"
	}

	sum := sha256.Sum256([]byte(value))

	return hex.EncodeToString(sum[:6])
}

func redactEndpoint(value string) string {
	if value == "" {
		return "<sdk-default>"
	}

	parsed, err := url.Parse(value)
	if err != nil {
		return "<invalid>"
	}

	parsed.User = nil
	parsed.RawQuery = ""
	parsed.Fragment = ""

	return parsed.String()
}
