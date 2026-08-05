package objectstore //nolint:testpackage // White-box tests verify provider-specific signing middleware.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
)

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
