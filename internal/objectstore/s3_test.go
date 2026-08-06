package objectstore //nolint:testpackage // White-box tests cover TLS client construction.

import (
	"bytes"
	"context"
	"crypto/md5" //nolint:gosec // Content-MD5 is required by the S3 multi-delete protocol.
	"encoding/base64"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/labring-sigs/kbbackup-prune/internal/domain"
	"github.com/stretchr/testify/require"
)

func TestNewS3Validation(t *testing.T) {
	t.Parallel()

	_, err := NewS3(context.Background(), S3Options{})
	require.ErrorContains(t, err, "bucket is required")
	_, err = NewS3(context.Background(), S3Options{Bucket: "bucket", AccessKey: "only-access"})
	require.ErrorContains(t, err, "both access key and secret key")
}

func TestBuildHTTPClient(t *testing.T) {
	t.Parallel()

	client, err := buildHTTPClient("", true)
	require.NoError(t, err)
	require.NotNil(t, client)

	_, err = buildHTTPClient(filepath.Join(t.TempDir(), "missing.pem"), false)
	require.ErrorContains(t, err, "read CA file")

	filename := filepath.Join(t.TempDir(), "empty.pem")
	require.NoError(t, os.WriteFile(filename, []byte("not a certificate"), 0o600))
	_, err = buildHTTPClient(filename, false)
	require.ErrorContains(t, err, "contains no certificates")
}

func TestDeleteUsesBatchesWithContentMD5(t *testing.T) {
	t.Parallel()

	var (
		checks      []bool
		conditional []bool
		mutex       sync.Mutex
	)

	server := httptest.NewServer(
		http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			body, err := io.ReadAll(request.Body)
			if err != nil {
				http.Error(writer, err.Error(), http.StatusInternalServerError)

				return
			}

			digest := md5.Sum(body) //nolint:gosec // Required by S3 multi-delete.
			valid := request.Header.Get(
				"Content-MD5",
			) == base64.StdEncoding.EncodeToString(
				digest[:],
			)

			mutex.Lock()
			require.Empty(t, request.Header.Get("X-Amz-Checksum-Crc32"))
			require.Empty(t, request.Header.Get("X-Amz-Checksum-Crc32c"))
			require.Empty(t, request.Header.Get("X-Amz-Checksum-Sha1"))
			require.Empty(t, request.Header.Get("X-Amz-Checksum-Sha256"))
			require.NotEqual(t, "aws-chunked", request.Header.Get("Content-Encoding"))
			require.Empty(t, request.Header.Get("X-Amz-Trailer"))

			checks = append(checks, valid)
			conditional = append(
				conditional,
				bytes.Count(body, []byte("<ETag>")) == bytes.Count(body, []byte("<Object>")),
			)
			mutex.Unlock()
			writer.Header().Set("Content-Type", "application/xml")
			_, _ = writer.Write(
				[]byte(`<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"/>`),
			)
		}),
	)
	t.Cleanup(server.Close)

	store, err := NewS3(context.Background(), S3Options{
		Bucket: "bucket", Region: "us-east-1", Endpoint: server.URL, PathStyle: true,
		AccessKey: "access", SecretKey: "secret",
	})
	require.NoError(t, err)

	objects := make([]domain.Object, 1001)
	for i := range objects {
		objects[i].Key = "object-" + strconv.Itoa(i)
		objects[i].ETag = "etag-" + strconv.Itoa(i)
	}

	requireDeleteNoError(t, store, objects)

	mutex.Lock()
	defer mutex.Unlock()

	require.Equal(t, []bool{true, true}, checks)
	require.Equal(t, []bool{true, true}, conditional)
}

func TestDeleteGCSUsesGenerationInBatch(t *testing.T) {
	t.Parallel()

	var body []byte

	server := httptest.NewServer(
		http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			require.Equal(t, http.MethodPost, request.Method)
			body, _ = io.ReadAll(request.Body)

			writer.Header().Set("Content-Type", "application/xml")
			_, _ = writer.Write(
				[]byte(
					`<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Deleted><Key>object-a</Key><VersionId>generation-a</VersionId></Deleted><Deleted><Key>object-b</Key><VersionId>generation-b</VersionId></Deleted></DeleteResult>`,
				),
			)
		}),
	)
	t.Cleanup(server.Close)

	store, err := NewS3(context.Background(), S3Options{
		Bucket: "bucket", Region: "us-east-1", Endpoint: server.URL, PathStyle: true,
		AccessKey: "access", SecretKey: "secret",
	})
	require.NoError(t, err)

	store.gcsEndpoint = true

	requireDeleteNoError(t, store, []domain.Object{
		{Key: "object-a", Generation: "generation-a"},
		{Key: "object-b", Generation: "generation-b"},
	})
	require.Contains(t, string(body), "<VersionId>generation-a</VersionId>")
	require.NotContains(t, string(body), "<ETag>")
}

func TestDeleteGCSUsesStandardBatchForUnconditionalObjects(t *testing.T) {
	t.Parallel()

	var body []byte

	server := httptest.NewServer(
		http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			require.Equal(t, http.MethodPost, request.Method)

			var err error

			body, err = io.ReadAll(request.Body)
			require.NoError(t, err)
			writer.Header().Set("Content-Type", "application/xml")
			_, err = writer.Write(
				[]byte(`<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"/>`),
			)
			require.NoError(t, err)
		}),
	)
	t.Cleanup(server.Close)

	store, err := NewS3(context.Background(), S3Options{
		Bucket: "bucket", Region: "us-east-1", Endpoint: server.URL, PathStyle: true,
		AccessKey: "access", SecretKey: "secret",
	})
	require.NoError(t, err)

	store.gcsEndpoint = true

	requireDeleteNoError(t, store, []domain.Object{
		{Key: "object-a", VersionID: "version-a"},
		{Key: "object-b", VersionID: "version-b"},
	})
	require.NotContains(t, string(body), "<ETag>")
}

func TestDeleteReportsMalformedBatchWithoutUnsafeFallback(t *testing.T) {
	t.Parallel()

	var batchCalls int

	server := httptest.NewServer(
		http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			require.Equal(t, http.MethodPost, request.Method)

			batchCalls++

			writer.Header().Set("Content-Type", "application/xml")
			writer.WriteHeader(http.StatusBadRequest)
			_, _ = writer.Write([]byte(
				`<Error><Code>MalformedMultiObjectDeleteRequest</Code><Message>unsupported extension</Message></Error>`,
			))
		}),
	)
	t.Cleanup(server.Close)

	store, err := NewS3(context.Background(), S3Options{
		Bucket: "bucket", Region: "us-east-1", Endpoint: server.URL, PathStyle: true,
		AccessKey: "access", SecretKey: "secret",
	})
	require.NoError(t, err)

	_, err = store.Delete(context.Background(), []domain.Object{
		{Key: "object-a", ETag: "etag-a"},
		{Key: "object-b", ETag: `"etag-b"`},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "MalformedMultiObjectDeleteRequest")
	require.Equal(t, 1, batchCalls)
}

func TestDeleteReportsPartialBatchResults(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(
		http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
			writer.Header().Set("Content-Type", "application/xml")
			_, _ = writer.Write(
				[]byte(
					`<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Deleted><Key>ok</Key></Deleted><Error><Key>failed</Key><Code>AccessDenied</Code><Message>denied</Message></Error></DeleteResult>`,
				),
			)
		}),
	)
	t.Cleanup(server.Close)

	store, err := NewS3(context.Background(), S3Options{
		Bucket: "bucket", Region: "us-east-1", Endpoint: server.URL, PathStyle: true,
		AccessKey: "access", SecretKey: "secret",
	})
	require.NoError(t, err)

	report, err := store.Delete(context.Background(), []domain.Object{
		{Key: "ok", Size: 10, ETag: "ok-etag"},
		{Key: "failed", Size: 20, ETag: "failed-etag"},
	})
	require.ErrorContains(t, err, "AccessDenied")
	require.Len(t, report.Deleted, 1)
	require.Equal(t, "ok", report.Deleted[0].Key)
	require.Len(t, report.Failed, 1)
	require.Equal(t, "failed", report.Failed[0].Object.Key)
}

func TestListLevelPaginatesAndSortsPrefixes(t *testing.T) {
	t.Parallel()

	var calls int

	server := httptest.NewServer(
		http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			require.Equal(t, "/bucket", request.URL.Path)
			require.Equal(t, "root/", request.URL.Query().Get("prefix"))
			require.Equal(t, "/", request.URL.Query().Get("delimiter"))
			writer.Header().Set("Content-Type", "application/xml")

			calls++

			if request.URL.Query().Get("continuation-token") == "next" {
				_, _ = writer.Write(
					[]byte(`<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
<IsTruncated>false</IsTruncated>
<Contents><Key>root/loose-b</Key><LastModified>2026-08-01T00:00:00Z</LastModified><ETag>&quot;etag-b&quot;</ETag><Size>2</Size></Contents>
	<CommonPrefixes><Prefix>root/a/</Prefix></CommonPrefixes>
</ListBucketResult>`),
				)

				return
			}

			_, _ = writer.Write(
				[]byte(`<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
<IsTruncated>true</IsTruncated><NextContinuationToken>next</NextContinuationToken>
<Contents><Key>root/loose-a</Key><LastModified>2026-08-01T00:00:00Z</LastModified><ETag>&quot;etag-a&quot;</ETag><Size>1</Size></Contents>
<CommonPrefixes><Prefix>root/z/</Prefix></CommonPrefixes>
<CommonPrefixes><Prefix>root/a/</Prefix></CommonPrefixes>
</ListBucketResult>`),
			)
		}),
	)
	t.Cleanup(server.Close)

	store, err := NewS3(context.Background(), S3Options{
		Bucket: "bucket", Region: "us-east-1", Endpoint: server.URL, PathStyle: true,
		AccessKey: "access", SecretKey: "secret",
	})
	require.NoError(t, err)
	level, err := store.ListLevel(context.Background(), "root/", "/", false)
	require.NoError(t, err)
	require.Equal(t, 2, calls)
	require.Equal(t, []string{"root/a/", "root/z/"}, level.Prefixes)
	require.Equal(t, []string{"root/loose-a", "root/loose-b"}, []string{
		level.Objects[0].Key,
		level.Objects[1].Key,
	})
	require.Equal(t, "etag-a", level.Objects[0].ETag)
	require.False(t, strings.Contains(level.Objects[0].ETag, `"`))
}

func TestListVersionLevelIncludesDeleteMarkersAndHiddenPrefixes(t *testing.T) {
	t.Parallel()

	var calls int

	server := httptest.NewServer(
		http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			require.Equal(t, "/bucket", request.URL.Path)
			require.True(t, request.URL.Query().Has("versions"))
			require.Equal(t, "root/", request.URL.Query().Get("prefix"))
			require.Equal(t, "/", request.URL.Query().Get("delimiter"))
			writer.Header().Set("Content-Type", "application/xml")

			calls++

			if request.URL.Query().Get("key-marker") == "root/loose" {
				require.Equal(t, "delete-v2", request.URL.Query().Get("version-id-marker"))

				_, _ = writer.Write([]byte(
					`<ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
<IsTruncated>false</IsTruncated>
<Version><Key>root/loose</Key><VersionId>v1</VersionId><IsLatest>false</IsLatest><LastModified>2026-08-01T00:00:00Z</LastModified><ETag>&quot;etag-v1&quot;</ETag><Size>2</Size></Version>
<CommonPrefixes><Prefix>root/a/</Prefix></CommonPrefixes>
</ListVersionsResult>`,
				))

				return
			}

			_, _ = writer.Write([]byte(
				`<ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
<IsTruncated>true</IsTruncated><NextKeyMarker>root/loose</NextKeyMarker><NextVersionIdMarker>delete-v2</NextVersionIdMarker>
<DeleteMarker><Key>root/loose</Key><VersionId>delete-v2</VersionId><IsLatest>true</IsLatest><LastModified>2026-08-02T00:00:00Z</LastModified></DeleteMarker>
<CommonPrefixes><Prefix>root/z/</Prefix></CommonPrefixes>
</ListVersionsResult>`,
			))
		}),
	)
	t.Cleanup(server.Close)

	store, err := NewS3(context.Background(), S3Options{
		Bucket: "bucket", Region: "us-east-1", Endpoint: server.URL, PathStyle: true,
		AccessKey: "access", SecretKey: "secret",
	})
	require.NoError(t, err)
	level, err := store.ListLevel(context.Background(), "root/", "/", true)
	require.NoError(t, err)
	require.Equal(t, 2, calls)
	require.Equal(t, []string{"root/a/", "root/z/"}, level.Prefixes)
	require.Len(t, level.Objects, 2)
	require.True(t, level.Objects[0].DeleteMarker)
	require.Equal(t, "delete-v2", level.Objects[0].VersionID)
	require.Equal(t, "etag-v1", level.Objects[1].ETag)
	require.Equal(t, "v1", level.Objects[1].VersionID)
}

func TestDeleteRequiresIdentityForUnversionedObjects(t *testing.T) {
	t.Parallel()

	store, err := NewS3(context.Background(), S3Options{
		Bucket: "bucket", Region: "us-east-1", Endpoint: "http://127.0.0.1:1", PathStyle: true,
		AccessKey: "access", SecretKey: "secret",
	})
	require.NoError(t, err)
	require.ErrorContains(t, store.Walk(context.Background(), "", false, nil), "visitor")
	_, err = store.Delete(context.Background(), []domain.Object{{Key: "object"}})
	require.ErrorContains(t, err, "has no ETag")

	store.gcsEndpoint = true
	_, err = store.Delete(context.Background(), []domain.Object{{Key: "object", ETag: "etag"}})
	require.ErrorContains(t, err, "has no GCS generation")
}

func requireDeleteNoError(t *testing.T, store *S3, objects []domain.Object) {
	t.Helper()

	_, err := store.Delete(context.Background(), objects)
	require.NoError(t, err)
}

func TestVersioningPermissionHint(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(
		http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
			writer.Header().Set("Content-Type", "application/xml")
			writer.WriteHeader(http.StatusForbidden)
			_, _ = writer.Write([]byte(
				`<Error><Code>SignatureDoesNotMatch</Code><Message>Access denied.</Message></Error>`,
			))
		}),
	)
	t.Cleanup(server.Close)

	store, err := NewS3(context.Background(), S3Options{
		Bucket: "bucket", Region: "us-east-1", Endpoint: server.URL, PathStyle: true,
		AccessKey: "access", SecretKey: "secret",
	})
	require.NoError(t, err)

	_, err = store.Versioning(context.Background())
	require.ErrorContains(t, err, "s3:GetBucketVersioning")
	require.ErrorContains(t, err, "storage.buckets.get")
}

func TestSigningDebugRedactsCredentials(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(
		http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
			writer.Header().Set("Content-Type", "application/xml")
			_, _ = writer.Write([]byte(
				`<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><KeyCount>0</KeyCount></ListBucketResult>`,
			))
		}),
	)
	t.Cleanup(server.Close)

	var debug bytes.Buffer

	//nolint:gosec // Static test-only credentials verify signing log redaction.
	store, err := NewS3(context.Background(), S3Options{
		Bucket: "bucket", Region: "us-east-1", Endpoint: server.URL, PathStyle: true,
		AccessKey: "access-key-sensitive", SecretKey: "secret-key-sensitive",
		SigningDebug: &debug,
	})
	require.NoError(t, err)
	require.NoError(t, store.Walk(context.Background(), "", false, func(domain.Object) error {
		return nil
	}))

	require.Contains(t, debug.String(), "CANONICAL STRING")
	require.Contains(t, debug.String(), "STRING TO SIGN")
	require.NotContains(t, debug.String(), "access-key-sensitive")
	require.NotContains(t, debug.String(), "secret-key-sensitive")
	require.NotContains(t, debug.String(), "Signature=")
}
