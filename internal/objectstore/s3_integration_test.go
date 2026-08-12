package objectstore //nolint:testpackage // Integration tests inspect the concrete S3 adapter.

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/labring-sigs/kbbackup-prune/internal/domain"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	miniotest "github.com/testcontainers/testcontainers-go/modules/minio"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	testUsername = "test-access-key"
	testPassword = "test-secret-key"
	testBucket   = "backups"
)

func TestS3MinIO(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	endpoint := startMinIO(t, ctx, "minio/minio:RELEASE.2024-01-16T16-07-38Z")
	runS3CompatibilitySuite(t, ctx, endpoint, checksumCompatibility{
		defaultSDK:  false,
		explicitMD5: false,
	})
}

func TestS3CurrentMinIO(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	endpoint := startMinIO(t, ctx, "minio/minio:RELEASE.2025-09-07T16-13-09Z")
	runS3CompatibilitySuite(t, ctx, endpoint, checksumCompatibility{
		defaultSDK:  true,
		explicitMD5: false,
	})
}

func TestS3RustFS(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	container, err := testcontainers.Run(
		ctx,
		"rustfs/rustfs:1.0.0-beta.12",
		testcontainers.WithExposedPorts("9000/tcp"),
		testcontainers.WithEnv(map[string]string{
			"RUSTFS_ACCESS_KEY": testUsername,
			"RUSTFS_SECRET_KEY": testPassword,
		}),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("9000/tcp").WithStartupTimeout(2*time.Minute),
		),
	)
	containerOrSkip(t, err, "RustFS")
	t.Cleanup(func() {
		require.NoError(t, container.Terminate(context.Background()))
	})

	host, err := container.Host(ctx)
	require.NoError(t, err)
	port, err := container.MappedPort(ctx, "9000/tcp")
	require.NoError(t, err)

	endpoint := "http://" + net.JoinHostPort(host, port.Port())

	runS3CompatibilitySuite(t, ctx, endpoint, checksumCompatibility{
		defaultSDK:  true,
		explicitMD5: false,
	})
}

type checksumCompatibility struct {
	defaultSDK  bool
	explicitMD5 bool
}

func startMinIO(t *testing.T, ctx context.Context, image string) string {
	t.Helper()

	container, err := miniotest.Run(
		ctx,
		image,
		miniotest.WithUsername(testUsername),
		miniotest.WithPassword(testPassword),
	)
	containerOrSkip(t, err, "MinIO")
	t.Cleanup(func() {
		require.NoError(t, container.Terminate(context.Background()))
	})

	endpoint, err := container.ConnectionString(ctx)
	require.NoError(t, err)

	if !strings.Contains(endpoint, "://") {
		endpoint = "http://" + endpoint
	}

	return endpoint
}

func containerOrSkip(t *testing.T, err error, service string) {
	t.Helper()

	if err == nil {
		return
	}

	if os.Getenv("REQUIRE_TESTCONTAINERS") == "true" {
		require.NoError(t, err)
	}

	t.Skipf("%s testcontainer unavailable: %v", service, err)
}

func runS3CompatibilitySuite(
	t *testing.T,
	ctx context.Context,
	endpoint string,
	expect checksumCompatibility,
) {
	t.Helper()

	store, err := NewS3(ctx, S3Options{
		Bucket: testBucket, Region: "us-east-1", Endpoint: endpoint, PathStyle: true,
		AccessKey: testUsername, SecretKey: testPassword,
	})
	require.NoError(t, err)
	admin := s3TestClient(t, ctx, endpoint)
	_, err = admin.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(testBucket)})
	require.NoError(t, err)

	require.Equal(
		t,
		expect.defaultSDK,
		deleteWithSDKChecksum(t, ctx, admin, "checksum-default", ""),
	)
	require.Equal(
		t,
		expect.explicitMD5,
		deleteWithSDKChecksum(t, ctx, admin, "checksum-md5", types.ChecksumAlgorithmMd5),
	)
	putObject(t, ctx, admin, "root/backup/kubeblocks-backup.json", "manifest-v1")
	putObject(t, ctx, admin, "root/backup/data", "payload")
	objects, err := store.List(ctx, "root", false)
	require.NoError(t, err)
	require.Len(t, objects, 2)

	level, err := store.ListLevel(ctx, "root/", "/", false)
	require.NoError(t, err)
	require.Empty(t, level.Objects)
	require.Equal(t, []string{"root/backup/"}, level.Prefixes)
	require.Equal(t, "Disabled", mustVersioning(t, ctx, store))

	body, err := store.Open(ctx, "root/backup/data", 1024)
	require.NoError(t, err)
	content, err := io.ReadAll(body)
	require.NoError(t, err)
	require.NoError(t, body.Close())
	require.Equal(t, "payload", string(content))

	_, err = store.Open(ctx, "root/backup/data", 1)
	require.ErrorContains(t, err, "exceeds")

	stat, err := store.Stat(ctx, "root/backup/data")
	require.NoError(t, err)
	require.NotEmpty(t, stat.ETag)
	require.EqualValues(t, len("payload"), stat.Size)

	_, err = store.Delete(ctx, objects)
	require.NoError(t, err)
	objects, err = store.List(ctx, "root", false)
	require.NoError(t, err)
	require.Empty(t, objects)

	_, err = admin.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
		Bucket: aws.String(testBucket),
		VersioningConfiguration: &types.VersioningConfiguration{
			Status: types.BucketVersioningStatusEnabled,
		},
	})
	require.NoError(t, err)
	putObject(t, ctx, admin, "versioned/data", "v1")
	putObject(t, ctx, admin, "versioned/data", "v2")
	require.Equal(t, "Enabled", mustVersioning(t, ctx, store))
	versions, err := store.List(ctx, "versioned", true)
	require.NoError(t, err)
	require.Len(t, versions, 2)
	_, err = store.Delete(ctx, versions)
	require.NoError(t, err)
	versions, err = store.List(ctx, "versioned", true)
	require.NoError(t, err)
	require.Empty(t, versions)

	putObject(t, ctx, admin, "hidden-root/data", "hidden-v1")
	_, err = admin.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(testBucket), Key: aws.String("hidden-root/data"),
	})
	require.NoError(t, err)
	current, err := store.List(ctx, "hidden-root", false)
	require.NoError(t, err)
	require.Empty(t, current)

	versionLevel, err := store.ListLevel(ctx, "", "/", true)
	require.NoError(t, err)
	require.Contains(t, versionLevel.Prefixes, "hidden-root/")

	hiddenLevel, err := store.ListLevel(ctx, "hidden-root/", "/", true)
	require.NoError(t, err)
	require.Len(t, hiddenLevel.Objects, 2)
	require.Equal(t, 1, countDeleteMarkers(hiddenLevel.Objects))

	hiddenVersions, err := store.List(ctx, "hidden-root", true)
	require.NoError(t, err)
	require.Len(t, hiddenVersions, 2)
	_, err = store.Delete(ctx, hiddenVersions)
	require.NoError(t, err)
	hiddenVersions, err = store.List(ctx, "hidden-root", true)
	require.NoError(t, err)
	require.Empty(t, hiddenVersions)
}

func countDeleteMarkers(objects []domain.Object) int {
	count := 0
	for _, object := range objects {
		if object.DeleteMarker {
			count++
		}
	}

	return count
}

func deleteWithSDKChecksum(
	t *testing.T,
	ctx context.Context,
	client *s3.Client,
	key string,
	algorithm types.ChecksumAlgorithm,
) bool {
	t.Helper()

	putObject(t, ctx, client, key, "probe")

	_, err := client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket:            aws.String(testBucket),
		ChecksumAlgorithm: algorithm,
		Delete: &types.Delete{
			Objects: []types.ObjectIdentifier{{Key: aws.String(key)}},
		},
	})
	if err == nil {
		return true
	}

	if algorithm == types.ChecksumAlgorithmMd5 {
		require.ErrorContains(t, err, "unknown checksum algorithm, MD5")

		return false
	}

	var apiError smithy.APIError
	require.True(t, errors.As(err, &apiError), err)
	require.Equal(t, "MissingContentMD5", apiError.ErrorCode())

	return false
}

func s3TestClient(t *testing.T, ctx context.Context, endpoint string) *s3.Client {
	t.Helper()

	config, err := awsconfig.LoadDefaultConfig(
		ctx,
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(testUsername, testPassword, ""),
		),
	)
	require.NoError(t, err)

	return s3.NewFromConfig(config, func(options *s3.Options) {
		options.BaseEndpoint = aws.String(endpoint)
		options.UsePathStyle = true
	})
}

func putObject(t *testing.T, ctx context.Context, client *s3.Client, key, value string) {
	t.Helper()

	_, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(testBucket), Key: aws.String(key), Body: strings.NewReader(value),
	})
	require.NoError(t, err)
}

func mustVersioning(t *testing.T, ctx context.Context, store *S3) string {
	t.Helper()

	versioning, err := store.Versioning(ctx)
	require.NoError(t, err)

	return versioning
}
