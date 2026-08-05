package objectstore

import (
	"context"
	"net/url"
	"slices"
	"strings"

	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

var gcsIncompatibleSignedHeaders = []string{
	"Amz-Sdk-Invocation-Id",
	"Amz-Sdk-Request",
	"Accept-Encoding",
}

// addGCSSigningCompatibility matches GCS's SigV4 canonicalization while retaining
// the AWS SDK v2 request pipeline.
func addGCSSigningCompatibility(stack *middleware.Stack) error {
	strip := middleware.FinalizeMiddlewareFunc(
		"kbbackup-prune/strip-gcs-incompatible-signed-headers",
		func(
			ctx context.Context,
			input middleware.FinalizeInput,
			next middleware.FinalizeHandler,
		) (middleware.FinalizeOutput, middleware.Metadata, error) {
			if request, ok := input.Request.(*smithyhttp.Request); ok {
				for _, header := range gcsIncompatibleSignedHeaders {
					request.Header.Del(header)
				}
			}

			return next.HandleFinalize(ctx, input)
		},
	)
	restore := middleware.FinalizeMiddlewareFunc(
		"kbbackup-prune/restore-accept-encoding-identity",
		func(
			ctx context.Context,
			input middleware.FinalizeInput,
			next middleware.FinalizeHandler,
		) (middleware.FinalizeOutput, middleware.Metadata, error) {
			if request, ok := input.Request.(*smithyhttp.Request); ok {
				request.Header.Set("Accept-Encoding", "identity")
			}

			return next.HandleFinalize(ctx, input)
		},
	)

	if err := stack.Finalize.Insert(strip, "Signing", middleware.Before); err != nil {
		if err := stack.Finalize.Add(strip, middleware.Before); err != nil {
			return err
		}
	}

	if err := stack.Finalize.Insert(restore, "Signing", middleware.After); err != nil {
		return stack.Finalize.Add(restore, middleware.After)
	}

	return nil
}

func isGoogleCloudStorageEndpoint(endpoint string) bool {
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Hostname() == "" {
		return false
	}

	host := strings.TrimSuffix(strings.ToLower(parsed.Hostname()), ".")

	labels := strings.Split(host, ".")
	if len(labels) < 3 || labels[len(labels)-2] != "googleapis" || labels[len(labels)-1] != "com" {
		return false
	}

	for _, label := range labels[:len(labels)-2] {
		if slices.Contains(strings.Split(label, "-"), "storage") {
			return true
		}
	}

	return false
}
