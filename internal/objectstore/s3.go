package objectstore

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go/logging"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/labring-sigs/kbbackup-prune/internal/domain"
)

type S3Options struct {
	Bucket       string
	Region       string
	Endpoint     string
	PathStyle    bool
	CAFile       string
	Insecure     bool
	AccessKey    string
	SecretKey    string
	SessionToken string
	SigningDebug io.Writer
	HTTPClient   *http.Client
}

type S3 struct {
	client      *s3.Client
	bucket      string
	gcsEndpoint bool
}

func NewS3(ctx context.Context, opts S3Options) (*S3, error) {
	if opts.Bucket == "" {
		return nil, errors.New("S3 bucket is required")
	}

	if opts.Region == "" {
		opts.Region = "us-east-1"
	}

	gcsEndpoint := isGoogleCloudStorageEndpoint(opts.Endpoint)

	loadOptions := []func(*awsconfig.LoadOptions) error{awsconfig.WithRegion(opts.Region)}
	if opts.SigningDebug != nil {
		loadOptions = append(
			loadOptions,
			awsconfig.WithClientLogMode(aws.LogSigning),
			awsconfig.WithLogger(logging.NewStandardLogger(opts.SigningDebug)),
		)
	}

	if opts.AccessKey != "" || opts.SecretKey != "" {
		if opts.AccessKey == "" || opts.SecretKey == "" {
			return nil, errors.New(
				"both access key and secret key are required for static credentials",
			)
		}

		provider := credentials.NewStaticCredentialsProvider(
			opts.AccessKey,
			opts.SecretKey,
			opts.SessionToken,
		)
		loadOptions = append(loadOptions, awsconfig.WithCredentialsProvider(provider))
	}

	httpClient := opts.HTTPClient

	var err error
	if httpClient == nil {
		httpClient, err = buildHTTPClient(opts.CAFile, opts.Insecure)
		if err != nil {
			return nil, err
		}
	}

	if gcsEndpoint {
		if httpClient == nil {
			httpClient = &http.Client{}
		}

		httpClient = wrapGCSHTTPClient(httpClient)
	}

	if httpClient != nil {
		loadOptions = append(loadOptions, awsconfig.WithHTTPClient(httpClient))
	}

	config, err := awsconfig.LoadDefaultConfig(ctx, loadOptions...)
	if err != nil {
		return nil, fmt.Errorf("load AWS configuration: %w", err)
	}

	client := s3.NewFromConfig(config, func(options *s3.Options) {
		options.UsePathStyle = opts.PathStyle
		if opts.Endpoint != "" {
			options.BaseEndpoint = aws.String(opts.Endpoint)
		}

		if gcsEndpoint {
			options.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
			options.APIOptions = append(options.APIOptions, addGCSSigningCompatibility)
		}
	})

	return &S3{client: client, bucket: opts.Bucket, gcsEndpoint: gcsEndpoint}, nil
}

func buildHTTPClient(caFile string, insecure bool) (*http.Client, error) {
	if caFile == "" && !insecure {
		return nil, nil
	}

	transport, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		return nil, errors.New("default HTTP transport has unexpected type")
	}

	transport = transport.Clone()

	tlsConfig := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: insecure, //nolint:gosec // This value comes from the explicit operator TLS option.
	}
	if caFile != "" {
		pem, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("read CA file: %w", err)
		}

		roots, err := x509.SystemCertPool()
		if err != nil {
			return nil, fmt.Errorf("load system CA pool: %w", err)
		}

		if !roots.AppendCertsFromPEM(pem) {
			return nil, errors.New("CA file contains no certificates")
		}

		tlsConfig.RootCAs = roots
	}

	transport.TLSClientConfig = tlsConfig

	return &http.Client{Transport: transport}, nil
}

func (s *S3) List(ctx context.Context, prefix string, versions bool) ([]domain.Object, error) {
	var objects []domain.Object

	err := s.Walk(ctx, prefix, versions, func(object domain.Object) error {
		objects = append(objects, object)
		return nil
	})

	return objects, err
}

func (s *S3) ListLevel(
	ctx context.Context,
	prefix string,
	delimiter string,
	versions bool,
) (domain.ObjectLevel, error) {
	if delimiter == "" {
		return domain.ObjectLevel{}, errors.New("object delimiter is required")
	}

	if versions {
		return s.listVersionLevel(ctx, prefix, delimiter)
	}

	result := domain.ObjectLevel{}
	capture := newGCSGenerationCapture()
	ctx = context.WithValue(ctx, gcsGenerationCaptureKey{}, capture)
	prefixes := make(map[string]struct{})
	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket), Prefix: aws.String(prefix), Delimiter: aws.String(delimiter),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return domain.ObjectLevel{}, err
		}

		for _, object := range page.Contents {
			key := aws.ToString(object.Key)
			result.Objects = append(result.Objects, domain.Object{
				Key:          key,
				Size:         aws.ToInt64(object.Size),
				LastModified: aws.ToTime(object.LastModified),
				ETag:         strings.Trim(aws.ToString(object.ETag), "\""),
				Generation:   capture.get(key),
			})
		}

		for _, commonPrefix := range page.CommonPrefixes {
			value := aws.ToString(commonPrefix.Prefix)
			if value != "" {
				prefixes[value] = struct{}{}
			}
		}
	}

	finalizeObjectLevel(&result, prefixes)

	return result, nil
}

func (s *S3) listVersionLevel(
	ctx context.Context,
	prefix string,
	delimiter string,
) (domain.ObjectLevel, error) {
	result := domain.ObjectLevel{}
	prefixes := make(map[string]struct{})
	paginator := s3.NewListObjectVersionsPaginator(s.client, &s3.ListObjectVersionsInput{
		Bucket: aws.String(s.bucket), Prefix: aws.String(prefix), Delimiter: aws.String(delimiter),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return domain.ObjectLevel{}, err
		}

		for _, object := range page.Versions {
			result.Objects = append(result.Objects, domain.Object{
				Key:          aws.ToString(object.Key),
				Size:         aws.ToInt64(object.Size),
				LastModified: aws.ToTime(object.LastModified),
				ETag:         strings.Trim(aws.ToString(object.ETag), "\""),
				VersionID:    aws.ToString(object.VersionId),
			})
		}

		for _, marker := range page.DeleteMarkers {
			result.Objects = append(result.Objects, domain.Object{
				Key:          aws.ToString(marker.Key),
				LastModified: aws.ToTime(marker.LastModified),
				VersionID:    aws.ToString(marker.VersionId),
				DeleteMarker: true,
			})
		}

		for _, commonPrefix := range page.CommonPrefixes {
			value := aws.ToString(commonPrefix.Prefix)
			if value != "" {
				prefixes[value] = struct{}{}
			}
		}
	}

	finalizeObjectLevel(&result, prefixes)

	return result, nil
}

func finalizeObjectLevel(result *domain.ObjectLevel, prefixes map[string]struct{}) {
	result.Prefixes = make([]string, 0, len(prefixes))
	for value := range prefixes {
		result.Prefixes = append(result.Prefixes, value)
	}

	sort.Strings(result.Prefixes)
	sort.Slice(result.Objects, func(i, j int) bool {
		if result.Objects[i].Key == result.Objects[j].Key {
			return result.Objects[i].VersionID < result.Objects[j].VersionID
		}

		return result.Objects[i].Key < result.Objects[j].Key
	})
}

func (s *S3) Walk(
	ctx context.Context,
	prefix string,
	versions bool,
	visit func(domain.Object) error,
) error {
	if visit == nil {
		return errors.New("object visitor is required")
	}

	if versions {
		return s.walkVersions(ctx, prefix, visit)
	}

	capture := newGCSGenerationCapture()
	ctx = context.WithValue(ctx, gcsGenerationCaptureKey{}, capture)
	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket), Prefix: aws.String(prefix),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return err
		}

		for _, object := range page.Contents {
			key := aws.ToString(object.Key)
			if err := visit(domain.Object{
				Key:  key,
				Size: aws.ToInt64(object.Size),
				LastModified: aws.ToTime(
					object.LastModified,
				),
				ETag:       strings.Trim(aws.ToString(object.ETag), "\""),
				Generation: capture.get(key),
			}); err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *S3) walkVersions(
	ctx context.Context,
	prefix string,
	visit func(domain.Object) error,
) error {
	paginator := s3.NewListObjectVersionsPaginator(s.client, &s3.ListObjectVersionsInput{
		Bucket: aws.String(s.bucket), Prefix: aws.String(prefix),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return err
		}

		for _, object := range page.Versions {
			if err := visit(domain.Object{
				Key:  aws.ToString(object.Key),
				Size: aws.ToInt64(object.Size),
				LastModified: aws.ToTime(
					object.LastModified,
				),
				ETag:      strings.Trim(aws.ToString(object.ETag), "\""),
				VersionID: aws.ToString(object.VersionId),
			}); err != nil {
				return err
			}
		}

		for _, marker := range page.DeleteMarkers {
			if err := visit(domain.Object{
				Key: aws.ToString(marker.Key), LastModified: aws.ToTime(marker.LastModified),
				VersionID: aws.ToString(marker.VersionId), DeleteMarker: true,
			}); err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *S3) Open(ctx context.Context, key string, maxBytes int64) (io.ReadCloser, error) {
	output, err := s.client.GetObject(
		ctx,
		&s3.GetObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)},
	)
	if err != nil {
		return nil, err
	}

	if maxBytes > 0 && aws.ToInt64(output.ContentLength) > maxBytes {
		_ = output.Body.Close()
		return nil, fmt.Errorf("object %q exceeds %d bytes", key, maxBytes)
	}

	return output.Body, nil
}

func (s *S3) Stat(ctx context.Context, key string) (domain.Object, error) {
	capture := newGCSGenerationCapture()
	ctx = context.WithValue(ctx, gcsGenerationCaptureKey{}, capture)
	ctx = context.WithValue(ctx, gcsHeadObjectKey{}, key)

	output, err := s.client.HeadObject(
		ctx,
		&s3.HeadObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)},
	)
	if err != nil {
		return domain.Object{}, err
	}

	return domain.Object{
		Key:          key,
		Size:         aws.ToInt64(output.ContentLength),
		LastModified: aws.ToTime(output.LastModified),
		ETag: strings.Trim(
			aws.ToString(output.ETag),
			"\"",
		),
		VersionID:  aws.ToString(output.VersionId),
		Generation: capture.get(key),
	}, nil
}

func (s *S3) Delete(ctx context.Context, objects []domain.Object) (domain.DeleteReport, error) {
	const batchSize = 1000

	var report domain.DeleteReport
	for start := 0; start < len(objects); start += batchSize {
		end := min(start+batchSize, len(objects))

		batch := objects[start:end]
		batchReport, err := s.deleteBatch(ctx, batch, !s.gcsEndpoint)
		report.Deleted = append(report.Deleted, batchReport.Deleted...)

		report.Failed = append(report.Failed, batchReport.Failed...)
		if err != nil {
			return report, err
		}
	}

	return report, nil
}

func (s *S3) deleteBatch(
	ctx context.Context,
	objects []domain.Object,
	conditional bool,
) (domain.DeleteReport, error) {
	identifiers := make([]types.ObjectIdentifier, 0, len(objects))
	for _, object := range objects {
		identifier := types.ObjectIdentifier{Key: aws.String(object.Key)}
		switch {
		case deleteVersionID(object) != "":
			identifier.VersionId = aws.String(deleteVersionID(object))
		case !conditional:
			return domain.DeleteReport{}, fmt.Errorf(
				"object %q has no GCS generation for conditional deletion", object.Key,
			)
		case object.ETag == "":
			return domain.DeleteReport{}, fmt.Errorf(
				"object %q has no ETag for conditional deletion",
				object.Key,
			)
		default:
			identifier.ETag = aws.String(object.ETag)
		}

		identifiers = append(identifiers, identifier)
	}

	output, err := s.client.DeleteObjects(
		ctx,
		&s3.DeleteObjectsInput{
			Bucket: aws.String(s.bucket),
			Delete: &types.Delete{Objects: identifiers, Quiet: aws.Bool(false)},
		},
		func(options *s3.Options) {
			options.APIOptions = append(
				options.APIOptions,
				addContentMD5Only,
			)
		},
	)
	if err != nil {
		return domain.DeleteReport{}, err
	}

	report := domain.DeleteReport{}
	byIdentity := make(map[objectIdentity]domain.Object, len(objects))

	byKey := make(map[string]domain.Object, len(objects))
	for _, object := range objects {
		byIdentity[objectIdentity{key: object.Key, versionID: deleteVersionID(object)}] = object
		byKey[object.Key] = object
	}

	for _, deleted := range output.Deleted {
		object, ok := byIdentity[objectIdentity{
			key:       aws.ToString(deleted.Key),
			versionID: aws.ToString(deleted.VersionId),
		}]
		if !ok {
			object = byKey[aws.ToString(deleted.Key)]
		}

		if object.Key != "" {
			report.Deleted = append(report.Deleted, object)
		}
	}

	for _, failure := range output.Errors {
		object, ok := byIdentity[objectIdentity{
			key:       aws.ToString(failure.Key),
			versionID: aws.ToString(failure.VersionId),
		}]
		if !ok {
			object = byKey[aws.ToString(failure.Key)]
		}

		report.Failed = append(report.Failed, domain.DeleteFailure{
			Object: object,
			Code:   aws.ToString(failure.Code), Message: aws.ToString(failure.Message),
		})
	}

	if len(output.Deleted) == 0 && len(output.Errors) == 0 {
		// A few compatible servers return an empty DeleteResult on success.
		report.Deleted = append(report.Deleted, objects...)
	}

	if len(report.Failed) > 0 {
		first := report.Failed[0]
		return report, fmt.Errorf("delete %d objects: %s (%s)",
			len(report.Failed), first.Message, first.Code)
	}

	return report, nil
}

func addContentMD5Only(stack *middleware.Stack) error {
	_, _ = stack.Initialize.Remove("AWSChecksum:SetupInputContext")
	_, _ = stack.Build.Remove("AWSChecksum:RequestMetricsTracking")
	_, _ = stack.Finalize.Remove("AWSChecksum:ComputeInputPayloadChecksum")
	_, _ = stack.Finalize.Remove("addInputChecksumTrailer")
	_, _ = stack.Build.Remove("ContentChecksum")

	return smithyhttp.AddContentChecksumMiddleware(stack)
}

func deleteVersionID(object domain.Object) string {
	if object.VersionID != "" {
		return object.VersionID
	}

	return object.Generation
}

type objectIdentity struct {
	key       string
	versionID string
}

func (s *S3) Versioning(ctx context.Context) (string, error) {
	output, err := s.client.GetBucketVersioning(
		ctx,
		&s3.GetBucketVersioningInput{Bucket: aws.String(s.bucket)},
	)
	if err != nil {
		return "", fmt.Errorf(
			"query bucket versioning (requires s3:GetBucketVersioning; "+
				"GCS HMAC identity requires storage.buckets.get): %w",
			err,
		)
	}

	if output.Status == "" {
		return domain.BucketVersioningDisabled, nil
	}

	return string(output.Status), nil
}
