package objectstore //nolint:testpackage // White-box tests verify provider-specific signing middleware.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func TestGoogleCloudStorageEndpointDetection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		endpoint string
		want     bool
	}{
		{endpoint: "https://storage.googleapis.com", want: true},
		{endpoint: "https://storage.googleapis.com.", want: true},
		{endpoint: "https://storage.mtls.googleapis.com", want: true},
		{endpoint: "https://storage-download.googleapis.com", want: true},
		{endpoint: "https://bucket.storage.googleapis.com", want: true},
		{endpoint: "https://storage.googleapis.com.example.test"},
		{endpoint: "https://storageinsights.googleapis.com"},
		{endpoint: "https://googleapis.com"},
		{endpoint: "://invalid"},
		{},
	}
	for _, test := range tests {
		t.Run(test.endpoint, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, test.want, isGoogleCloudStorageEndpoint(test.endpoint))
		})
	}
}

func TestGCSSigningCompatibility(t *testing.T) {
	t.Parallel()

	var captured http.Header

	server := httptest.NewServer(
		http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			captured = request.Header.Clone()
			writer.Header().Set("Content-Type", "application/xml")
			_, _ = writer.Write([]byte(
				`<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><KeyCount>0</KeyCount></ListBucketResult>`,
			))
		}),
	)
	t.Cleanup(server.Close)

	client := s3.NewFromConfig(aws.Config{
		Region:      "us-east-1",
		Credentials: credentials.NewStaticCredentialsProvider("access", "secret", ""),
		HTTPClient:  server.Client(),
	}, func(options *s3.Options) {
		options.BaseEndpoint = aws.String(server.URL)
		options.UsePathStyle = true
		options.APIOptions = append(options.APIOptions, addGCSSigningCompatibility)
	})

	_, err := client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
		Bucket: aws.String("bucket"),
	})
	require.NoError(t, err)
	require.NotNil(t, captured)

	authorization := strings.ToLower(captured.Get("Authorization"))
	for _, header := range gcsIncompatibleSignedHeaders {
		require.NotContains(t, authorization, strings.ToLower(header))
	}

	require.Empty(t, captured.Get("Amz-Sdk-Invocation-Id"))
	require.Empty(t, captured.Get("Amz-Sdk-Request"))
	require.Equal(t, "identity", captured.Get("Accept-Encoding"))
}

func TestGCSListCapturesGenerationAndInteropHeader(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(
		http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			writer.Header().Set("Content-Type", "application/xml")

			if request.URL.Query().Has("versions") {
				require.Equal(
					t,
					"enabled",
					request.Header.Get("X-Goog-Interop-List-Objects-Format"),
				)
				require.Contains(
					t,
					strings.ToLower(request.Header.Get("Authorization")),
					"x-goog-interop-list-objects-format",
				)

				_, _ = writer.Write(
					[]byte(
						`<ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><IsTruncated>false</IsTruncated><Version><Key>root/data</Key><VersionId>generation-123</VersionId><LastModified>2026-08-01T00:00:00Z</LastModified><ETag>&quot;etag&quot;</ETag><Size>4</Size></Version></ListVersionsResult>`,
					),
				)

				return
			}

			if request.Method == http.MethodHead {
				writer.Header().Set("X-Goog-Generation", "123")
				writer.WriteHeader(http.StatusOK)

				return
			}

			_, _ = writer.Write(
				[]byte(
					`<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><IsTruncated>false</IsTruncated><Contents><Key>root/data</Key><Generation>123</Generation><LastModified>2026-08-01T00:00:00Z</LastModified><ETag>&quot;etag&quot;</ETag><Size>4</Size></Contents></ListBucketResult>`,
				),
			)
		}),
	)
	t.Cleanup(server.Close)
	local, err := url.Parse(server.URL)
	require.NoError(t, err)

	httpClient := &http.Client{
		Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			clone := request.Clone(request.Context())
			clone.URL.Scheme = local.Scheme
			clone.URL.Host = local.Host
			clone.Host = local.Host

			return server.Client().Transport.RoundTrip(clone)
		}),
	}

	store, err := NewS3(context.Background(), S3Options{
		Bucket: "bucket", Region: "us-east-1", Endpoint: "https://storage.googleapis.com",
		AccessKey: "access", SecretKey: "secret", HTTPClient: httpClient,
	})
	require.NoError(t, err)

	objects, err := store.List(context.Background(), "root/", false)
	require.NoError(t, err)
	require.Len(t, objects, 1)
	require.Equal(t, "123", objects[0].Generation)

	stat, err := store.Stat(context.Background(), "root/data")
	require.NoError(t, err)
	require.Equal(t, "123", stat.Generation)

	versionLevel, err := store.ListLevel(context.Background(), "root/", "/", true)
	require.NoError(t, err)
	require.Len(t, versionLevel.Objects, 1)
	require.Equal(t, "generation-123", versionLevel.Objects[0].VersionID)
}
