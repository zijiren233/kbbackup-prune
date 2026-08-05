package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"strings"
	"time"

	"github.com/labring-sigs/kbbackup-prune/internal/domain"
	"github.com/labring-sigs/kbbackup-prune/internal/kube"
	"github.com/labring-sigs/kbbackup-prune/internal/objectstore"
	"github.com/labring-sigs/kbbackup-prune/internal/prune"
	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/util/validation"
)

type options struct {
	debug          bool
	kubeMode       string
	kubeconfig     string
	kubeContext    string
	namespace      string
	requestTimeout time.Duration
	timeout        time.Duration
	backupRepo     string
	useRepoSecret  bool

	bucket           string
	endpoint         string
	region           string
	prefix           string
	pathStyle        bool
	bucketVersioning string
	caFile           string
	insecureTLS      bool

	manifest        string
	minAge          time.Duration
	includeRetained bool
	deleteRepoStray bool
	purgeVersions   bool
	showAll         bool
	output          string
	failOnOrphans   bool

	dryRun      bool
	confirm     string
	concurrency int
}

type App struct {
	Version string
	Out     io.Writer
	ErrOut  io.Writer
}

type ExitError struct {
	Code int
	Err  error
}

func (e *ExitError) Error() string { return e.Err.Error() }
func (e *ExitError) Unwrap() error { return e.Err }

func (a App) Command() *cobra.Command {
	opts := &options{}

	command := &cobra.Command{
		Use:           "kbbackup-prune",
		Short:         "Safely find and remove orphaned KubeBlocks backups from S3",
		SilenceUsage:  true,
		SilenceErrors: true,
		Version:       a.Version,
	}
	if a.Out == nil {
		a.Out = os.Stdout
	}

	if a.ErrOut == nil {
		a.ErrOut = os.Stderr
	}

	command.SetOut(a.Out)
	command.SetErr(a.ErrOut)
	bindPersistentFlags(command, opts)
	command.AddCommand(a.planCommand(opts), a.pruneCommand(opts))

	return command
}

func bindPersistentFlags(command *cobra.Command, opts *options) {
	flags := command.PersistentFlags()
	flags.BoolVar(&opts.debug, "debug", false, "print redacted connection diagnostics to stderr")
	flags.StringVar(
		&opts.kubeMode,
		"kube-mode",
		env("KBBACKUP_PRUNE_KUBE_MODE", "auto"),
		"Kubernetes client mode: auto, kubeconfig, or in-cluster",
	)
	flags.StringVar(
		&opts.kubeconfig,
		"kubeconfig",
		"",
		"kubeconfig path; default loading honors KUBECONFIG",
	)
	flags.StringVar(&opts.kubeContext, "context", "", "kubeconfig context")
	flags.StringVarP(
		&opts.namespace,
		"namespace",
		"n",
		env("KBBACKUP_PRUNE_NAMESPACE", ""),
		"limit scanning to the BackupRepo PVC object root for one Kubernetes namespace",
	)
	flags.DurationVar(
		&opts.requestTimeout,
		"request-timeout",
		30*time.Second,
		"timeout for each Kubernetes request",
	)
	flags.DurationVar(&opts.timeout, "timeout", 30*time.Minute, "overall command timeout")
	flags.StringVar(
		&opts.backupRepo,
		"backup-repo",
		env("KBBACKUP_PRUNE_BACKUP_REPO", ""),
		"KubeBlocks BackupRepo name (required)",
	)
	flags.BoolVar(
		&opts.useRepoSecret,
		"use-backup-repo-credentials",
		true,
		"read S3 settings from BackupRepo generated CSI Secret or spec.credential",
	)

	flags.StringVar(
		&opts.bucket,
		"bucket",
		env("KBBACKUP_PRUNE_BUCKET", ""),
		"S3 bucket; defaults to BackupRepo config",
	)
	flags.StringVar(
		&opts.endpoint,
		"endpoint",
		env("KBBACKUP_PRUNE_ENDPOINT", ""),
		"S3-compatible endpoint URL",
	)
	flags.StringVar(
		&opts.region,
		"region",
		env("AWS_REGION", env("AWS_DEFAULT_REGION", "")),
		"S3 region",
	)
	flags.StringVar(
		&opts.prefix,
		"prefix",
		env("KBBACKUP_PRUNE_PREFIX", ""),
		"narrow scanning to a full object prefix inside the selected BackupRepo scope",
	)
	flags.BoolVar(&opts.pathStyle, "path-style", false, "use S3 path-style addressing")
	flags.StringVar(
		&opts.bucketVersioning,
		"bucket-versioning",
		domain.BucketVersioningModeAuto,
		"bucket versioning mode: auto, disabled, enabled, or suspended",
	)
	flags.StringVar(&opts.caFile, "ca-file", "", "PEM CA bundle for the S3 endpoint")
	flags.BoolVar(
		&opts.insecureTLS,
		"insecure-skip-tls-verify",
		false,
		"skip S3 TLS certificate verification",
	)

	flags.StringVar(
		&opts.manifest,
		"manifest-name",
		domain.DefaultManifest,
		"KubeBlocks backup manifest file name",
	)
	flags.DurationVar(
		&opts.minAge,
		"min-age",
		7*24*time.Hour,
		"minimum age before an orphan becomes eligible",
	)
	flags.BoolVar(
		&opts.includeRetained,
		"include-retained",
		false,
		"include manifests whose embedded deletionPolicy is Retain",
	)
	flags.BoolVar(
		&opts.deleteRepoStray,
		"delete-repository-stray",
		false,
		"delete old unrecognized objects inside current BackupRepo volume roots",
	)
	flags.BoolVar(
		&opts.purgeVersions,
		"purge-versions",
		false,
		"permanently remove all versions under eligible backup prefixes",
	)
	flags.BoolVar(
		&opts.showAll,
		"show-all",
		false,
		"show live and hard-protected resources in addition to cleanup candidates",
	)
	flags.StringVarP(&opts.output, "output", "o", "table", "output format: table or json")
}

func (a App) planCommand(opts *options) *cobra.Command {
	command := &cobra.Command{
		Use:   "plan",
		Short: "Build a read-only orphan cleanup plan",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			ctx, cancel, err := commandContext(command.Context(), opts.timeout)
			if err != nil {
				return err
			}
			defer cancel()

			plan, _, err := a.buildPlan(ctx, opts, false)
			if err != nil {
				return err
			}

			plan.DryRun = true
			if err := writeOutput(
				command.OutOrStdout(),
				opts.output,
				plan,
				nil,
				opts.showAll,
			); err != nil {
				return err
			}

			if opts.failOnOrphans && hasOrphanCandidates(plan) {
				return &ExitError{
					Code: 2,
					Err: fmt.Errorf(
						"plan contains orphan candidates covering %d enumerated objects",
						plan.DeleteObjects,
					),
				}
			}

			return nil
		},
	}
	command.Flags().
		BoolVar(&opts.failOnOrphans, "fail-on-orphans", false, "exit with code 2 when eligible orphans are found")

	return command
}

func (a App) pruneCommand(opts *options) *cobra.Command {
	command := &cobra.Command{
		Use:   "prune",
		Short: "Preview or execute an orphan cleanup plan",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			if opts.concurrency < 1 {
				return errors.New("--concurrency must be greater than zero")
			}

			if err := validateConfirmation(opts); err != nil {
				return err
			}

			ctx, cancel, err := commandContext(command.Context(), opts.timeout)
			if err != nil {
				return err
			}
			defer cancel()

			plan, clients, err := a.buildPlan(ctx, opts, !opts.dryRun)
			if err != nil {
				return err
			}

			plan.DryRun = opts.dryRun

			executor := prune.Executor{Kube: clients.kube, Store: clients.store}

			execution, executeErr := executor.Run(ctx, plan, prune.ExecuteOptions{
				DryRun: opts.dryRun, Concurrency: opts.concurrency,
				PurgeVersions: opts.purgeVersions,
			})
			if outputErr := writeOutput(
				command.OutOrStdout(),
				opts.output,
				plan,
				&execution,
				opts.showAll,
			); outputErr != nil {
				return errors.Join(executeErr, outputErr)
			}

			return executeErr
		},
	}
	flags := command.Flags()
	flags.BoolVar(&opts.dryRun, "dry-run", true, "preview deletions without modifying S3")
	flags.StringVar(
		&opts.confirm,
		"confirm",
		"",
		"confirmation token required when dry-run is disabled",
	)
	flags.IntVar(&opts.concurrency, "concurrency", 4, "maximum concurrent backup deletions")

	return command
}

type builtClients struct {
	kube  *kube.Client
	store *objectstore.S3
}

func (a App) buildPlan(
	ctx context.Context,
	opts *options,
	captureObjects bool,
) (domain.Plan, builtClients, error) {
	if opts.backupRepo == "" {
		return domain.Plan{}, builtClients{}, errors.New("--backup-repo is required")
	}

	if opts.minAge < 0 {
		return domain.Plan{}, builtClients{}, errors.New("--min-age must be zero or greater")
	}

	if err := prune.ValidateBucketVersioningMode(opts.bucketVersioning); err != nil {
		return domain.Plan{}, builtClients{}, err
	}

	if opts.requestTimeout <= 0 {
		return domain.Plan{}, builtClients{}, errors.New(
			"--request-timeout must be greater than zero",
		)
	}

	if opts.namespace != "" {
		if validationErrors := validation.IsDNS1123Label(
			opts.namespace,
		); len(
			validationErrors,
		) > 0 {
			return domain.Plan{}, builtClients{}, fmt.Errorf(
				"invalid --namespace %q: %s",
				opts.namespace,
				strings.Join(validationErrors, "; "),
			)
		}
	}

	kubeClient, err := kube.New(kube.ConfigOptions{
		Mode: opts.kubeMode, Kubeconfig: opts.kubeconfig, Context: opts.kubeContext,
		Timeout: opts.requestTimeout, QPS: 20, Burst: 40,
	})
	if err != nil {
		return domain.Plan{}, builtClients{}, err
	}

	inventory, repoS3, err := kubeClient.Inventory(
		ctx,
		opts.backupRepo,
		opts.namespace,
		opts.useRepoSecret,
	)
	if err != nil {
		return domain.Plan{}, builtClients{}, err
	}

	if opts.bucket != "" && repoS3.Bucket != "" && opts.bucket != repoS3.Bucket {
		return domain.Plan{}, builtClients{}, fmt.Errorf(
			"--bucket %q differs from BackupRepo bucket %q",
			opts.bucket,
			repoS3.Bucket,
		)
	}

	bucket := coalesce(opts.bucket, repoS3.Bucket)
	endpoint := coalesce(opts.endpoint, repoS3.Endpoint)
	region := coalesce(opts.region, repoS3.Region, "us-east-1")

	if endpoint != "" {
		parsed, parseErr := url.ParseRequestURI(endpoint)
		if parseErr != nil || parsed.Scheme == "" || parsed.Host == "" {
			return domain.Plan{}, builtClients{}, fmt.Errorf("invalid S3 endpoint URL %q", endpoint)
		}
	}

	store, err := objectstore.NewS3(ctx, objectstore.S3Options{
		Bucket:       bucket,
		Region:       region,
		Endpoint:     endpoint,
		PathStyle:    opts.pathStyle,
		CAFile:       opts.caFile,
		Insecure:     opts.insecureTLS || repoS3.Insecure,
		AccessKey:    repoS3.AccessKeyID,
		SecretKey:    repoS3.SecretAccessKey,
		SessionToken: repoS3.SessionToken,
		SigningDebug: debugWriter(opts.debug, a.ErrOut),
	})
	if err != nil {
		return domain.Plan{}, builtClients{}, err
	}

	if opts.debug {
		if err := writeDebug(a.ErrOut, debugInfo{
			Settings:       repoS3,
			ObjectPrefixes: inventory.Repo.ObjectPrefixes,
			Bucket:         bucket,
			Endpoint:       endpoint,
			Region:         region,
			Prefix:         opts.prefix,
			PathStyle:      opts.pathStyle,
			VersioningMode: opts.bucketVersioning,
			Insecure:       opts.insecureTLS || repoS3.Insecure,
		}); err != nil {
			return domain.Plan{}, builtClients{}, fmt.Errorf("write debug output: %w", err)
		}
	}

	planner := prune.Planner{Store: store}
	plan, err := planner.Build(ctx, inventory, prune.PlanOptions{
		Repository:            opts.backupRepo,
		Bucket:                bucket,
		Namespace:             opts.namespace,
		Prefix:                opts.prefix,
		ManifestName:          opts.manifest,
		MinAge:                opts.minAge,
		IncludeRetained:       opts.includeRetained,
		PurgeVersions:         opts.purgeVersions,
		CaptureObjects:        captureObjects,
		BucketVersioning:      opts.bucketVersioning,
		DeleteRepositoryStray: opts.deleteRepoStray,
	})

	return plan, builtClients{kube: kubeClient, store: store}, err
}

func debugWriter(enabled bool, writer io.Writer) io.Writer {
	if !enabled {
		return nil
	}

	return writer
}

func commandContext(
	parent context.Context,
	timeout time.Duration,
) (context.Context, context.CancelFunc, error) {
	if timeout <= 0 {
		return nil, nil, errors.New("--timeout must be greater than zero")
	}

	ctx, cancel := context.WithTimeout(parent, timeout)

	return ctx, cancel, nil
}

func validateConfirmation(opts *options) error {
	if opts.dryRun {
		return nil
	}

	expected := "DELETE"
	switch {
	case opts.includeRetained && opts.deleteRepoStray:
		expected = "DELETE-RETAINED-AND-STRAY"
	case opts.includeRetained:
		expected = "DELETE-RETAINED"
	case opts.deleteRepoStray:
		expected = "DELETE-STRAY"
	}

	if opts.confirm != expected {
		return fmt.Errorf("live deletion requires --confirm %s", expected)
	}

	return nil
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func coalesce(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}

	return ""
}

func hasOrphanCandidates(plan domain.Plan) bool {
	for _, candidate := range plan.Candidates {
		if candidate.State == domain.StateOrphan {
			return true
		}
	}

	return false
}

func prefixContains(parent, child string) bool {
	parent = strings.Trim(path.Clean("/"+parent), "/")
	child = strings.Trim(path.Clean("/"+child), "/")
	return parent == "" || child == parent || strings.HasPrefix(child, parent+"/")
}
