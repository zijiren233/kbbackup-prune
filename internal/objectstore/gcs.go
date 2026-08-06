package objectstore

import (
	"bytes"
	"context"
	"encoding/xml"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"

	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

type (
	gcsGenerationCaptureKey struct{}
	gcsHeadObjectKey        struct{}
)

type gcsGenerationCapture struct {
	mu          sync.Mutex
	generations map[string]string
}

func newGCSGenerationCapture() *gcsGenerationCapture {
	return &gcsGenerationCapture{generations: make(map[string]string)}
}

func (c *gcsGenerationCapture) add(objects []gcsListObject) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, object := range objects {
		if object.Key != "" && object.Generation != "" {
			c.generations[object.Key] = object.Generation
		}
	}
}

func (c *gcsGenerationCapture) get(key string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.generations[key]
}

func (c *gcsGenerationCapture) set(key, generation string) {
	if key == "" || generation == "" {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.generations[key] = generation
}

type gcsListObject struct {
	Key        string `xml:"Key"`
	Generation string `xml:"Generation"`
}

type gcsListResponse struct {
	Contents []gcsListObject `xml:"Contents"`
}

type gcsGenerationTransport struct {
	base http.RoundTripper
}

func (t *gcsGenerationTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	response, err := t.base.RoundTrip(request)
	if err != nil || response == nil {
		return response, err
	}

	capture, ok := request.Context().Value(gcsGenerationCaptureKey{}).(*gcsGenerationCapture)
	if !ok || capture == nil {
		return response, nil
	}

	if request.Method == http.MethodHead {
		key, _ := request.Context().Value(gcsHeadObjectKey{}).(string)
		capture.set(key, response.Header.Get("X-Goog-Generation"))
		return response, nil
	}

	if response.Body == nil || request.URL.Query().Get("list-type") != "2" {
		return response, nil
	}

	body, readErr := io.ReadAll(response.Body)
	_ = response.Body.Close()

	response.Body = io.NopCloser(bytes.NewReader(body))
	if readErr != nil {
		return response, nil
	}

	var listing gcsListResponse
	if err := xml.Unmarshal(body, &listing); err == nil {
		capture.add(listing.Contents)
	}

	return response, nil
}

func wrapGCSHTTPClient(client *http.Client) *http.Client {
	clone := *client

	transport := client.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}

	clone.Transport = &gcsGenerationTransport{base: transport}

	return &clone
}

var gcsIncompatibleSignedHeaders = []string{
	"Amz-Sdk-Invocation-Id",
	"Amz-Sdk-Request",
	"Accept-Encoding",
}

// addGCSSigningCompatibility matches GCS's SigV4 canonicalization while retaining
// the AWS SDK v2 request pipeline.
func addGCSSigningCompatibility(stack *middleware.Stack) error {
	interop := middleware.FinalizeMiddlewareFunc(
		"kbbackup-prune/add-gcs-interop-version-header",
		func(
			ctx context.Context,
			input middleware.FinalizeInput,
			next middleware.FinalizeHandler,
		) (middleware.FinalizeOutput, middleware.Metadata, error) {
			if request, ok := input.Request.(*smithyhttp.Request); ok &&
				request.URL.Query().Has("versions") {
				request.Header.Set("X-Goog-Interop-List-Objects-Format", "enabled")
			}

			return next.HandleFinalize(ctx, input)
		},
	)
	if err := stack.Finalize.Insert(interop, "Signing", middleware.Before); err != nil {
		if err := stack.Finalize.Add(interop, middleware.Before); err != nil {
			return err
		}
	}

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
