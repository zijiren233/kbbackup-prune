package cli //nolint:testpackage // White-box tests cover parsing and deletion confirmation helpers.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/labring-sigs/kbbackup-prune/internal/domain"
	"github.com/stretchr/testify/require"
)

func TestFormatBytes(t *testing.T) {
	t.Parallel()

	require.Equal(t, "12 B", humanBytes(12))
	require.Equal(t, "1.5 KiB", humanBytes(1536))
}

func TestConfirmation(t *testing.T) {
	t.Parallel()
	require.NoError(t, validateConfirmation(&options{dryRun: true}))
	require.NoError(t, validateConfirmation(&options{confirm: "DELETE"}))
	require.ErrorContains(t, validateConfirmation(&options{}), "--confirm DELETE")
	require.NoError(
		t,
		validateConfirmation(&options{includeRetained: true, confirm: "DELETE-RETAINED"}),
	)
	require.ErrorContains(
		t,
		validateConfirmation(&options{includeRetained: true, confirm: "DELETE"}),
		"DELETE-RETAINED",
	)
	require.NoError(
		t,
		validateConfirmation(&options{deleteRepoStray: true, confirm: "DELETE-STRAY"}),
	)
	require.ErrorContains(
		t,
		validateConfirmation(&options{deleteRepoStray: true, confirm: "DELETE"}),
		"DELETE-STRAY",
	)
	require.NoError(t, validateConfirmation(&options{
		includeRetained: true, deleteRepoStray: true, confirm: "DELETE-RETAINED-AND-STRAY",
	}))
}

func TestOutput(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.August, 5, 12, 0, 0, 0, time.UTC)
	plan := domain.Plan{
		GeneratedAt: now, Repository: "repo", Bucket: "bucket", Versioning: "Disabled",
		ScannedObjects: 2, ScannedBytes: 1000, DeleteObjects: 2, DeleteBytes: 1000,
		Prefixes:       []string{"pvc-repository", "pvc-user"},
		ObjectPrefixes: map[string]string{"ns": "pvc-repository"},
		VolumeRootCounts: &domain.VolumeRootCounts{
			Total: 2, Repository: 1, ProtectedUser: 1,
		},
		Candidates: []domain.Candidate{
			{
				Kind:   domain.CandidateBackup,
				Backup: domain.BackupKey{Namespace: "ns", Name: "backup"},
				Prefix: "root/ns/backup",
				CreatedAt: now.Add(
					-10 * 24 * time.Hour,
				),
				State:       domain.StateOrphan,
				Reason:      "eligible",
				ObjectCount: 2,
				Bytes:       1000,
			},
			{
				Kind:   domain.CandidateBackup,
				Backup: domain.BackupKey{Namespace: "ns", Name: "live-backup"},
				Prefix: "root/ns/live-backup", State: domain.StateLive,
				Reason: "Backup CR exists",
			},
			{
				Kind: domain.CandidateOrphanClusterRoot, Prefix: "root/ns/cluster-uuid",
				State: domain.StateOrphan, DeferredScan: true,
				Reason: "cluster object enumeration is deferred",
			},
			{
				Kind: domain.CandidateProtectedUserVolume, Prefix: "pvc-user",
				State: domain.StateProtected, Reason: "current user PVC",
			},
		},
		BlockingReasons: []string{"example blocker"},
	}
	execution := &domain.Execution{DryRun: true, Results: []domain.DeleteResult{{
		Prefix: "root/ns/backup", ObjectsDeleted: 2, BytesDeleted: 1000,
	}}}

	var table bytes.Buffer
	require.NoError(t, writeOutput(&table, "table", plan, execution, false))
	require.Contains(t, table.String(), "Mode: dry-run")
	require.Contains(
		t,
		table.String(),
		"Discovered PVC roots: 2 (repository: 1, protected user: 1, unowned: 0, other: 0)",
	)
	require.Contains(t, table.String(), "ns/backup")
	require.Contains(t, table.String(), "orphan-cluster-root")
	require.NotContains(t, table.String(), "live-backup")
	require.NotContains(t, table.String(), "Backup CR exists")
	require.NotContains(t, table.String(), "pvc-user")
	require.NotContains(t, table.String(), "backup-orphan")
	require.Contains(t, table.String(), "deferred")
	require.Contains(t, table.String(), "Would delete: 2 objects")

	var jsonOutput bytes.Buffer
	require.NoError(t, writeOutput(&jsonOutput, "json", plan, execution, false))

	var decoded commandOutput
	require.NoError(t, json.Unmarshal(jsonOutput.Bytes(), &decoded))
	require.Equal(t, "repo", decoded.Plan.Repository)
	require.Len(t, decoded.Plan.Candidates, 2)
	require.Equal(t, domain.CandidateBackup, decoded.Plan.Candidates[0].Kind)
	require.NotContains(t, decoded.Plan.StateCounts, domain.StateLive)
	require.True(t, decoded.Execution.DryRun)

	var showAllOutput bytes.Buffer
	require.NoError(t, writeOutput(&showAllOutput, "json", plan, execution, true))
	require.NoError(t, json.Unmarshal(showAllOutput.Bytes(), &decoded))
	require.Len(t, decoded.Plan.Candidates, 4)
	require.Contains(t, decoded.Plan.StateCounts, domain.StateLive)

	require.ErrorContains(
		t,
		writeOutput(&bytes.Buffer{}, "yaml", plan, nil, false),
		"unsupported output",
	)
}

func TestOutputShowsOrphanRepositoryRootAsSingleDeferredCandidate(t *testing.T) {
	t.Parallel()

	root := "pvc-44444444-4444-4444-8444-444444444444"
	plan := domain.Plan{
		Repository: "repo", Bucket: "bucket", Versioning: domain.BucketVersioningDisabled,
		VolumeRootCounts: &domain.VolumeRootCounts{Total: 1, Repository: 1},
		Candidates: []domain.Candidate{{
			Kind: domain.CandidateOrphanRepositoryRoot, Prefix: root,
			ScopePrefix: root, State: domain.StateOrphan, DeferredScan: true,
			Reason: "historical BackupRepo PVC root has no current backup or storage reference",
		}},
	}

	var table bytes.Buffer
	require.NoError(t, writeOutput(&table, "table", plan, nil, false))
	require.Contains(t, table.String(), "orphan-repository-root")
	require.Contains(t, table.String(), root)
	require.Contains(t, table.String(), "deferred")
	require.NotContains(t, table.String(), "orphan-cluster-root")
	require.NotContains(t, table.String(), "repository-stray")
}

func TestOutputVolumeRootCountsFallback(t *testing.T) {
	t.Parallel()

	require.Equal(
		t,
		domain.VolumeRootCounts{Total: 2, Other: 2},
		outputVolumeRootCounts(domain.Plan{Prefixes: []string{"one", "two"}}),
	)
}

func TestVisibleCandidate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		candidate domain.Candidate
		want      bool
	}{
		{
			name: "eligible orphan", candidate: domain.Candidate{State: domain.StateOrphan},
			want: true,
		},
		{
			name: "retained policy", candidate: domain.Candidate{State: domain.StateRetained},
			want: true,
		},
		{
			name: "minimum age", candidate: domain.Candidate{State: domain.StateTooYoung},
			want: true,
		},
		{
			name: "configurable protection",
			candidate: domain.Candidate{
				State: domain.StateProtected, DeletionConfigurable: true,
			},
			want: true,
		},
		{name: "live", candidate: domain.Candidate{State: domain.StateLive}},
		{
			name:      "hard protection",
			candidate: domain.Candidate{State: domain.StateProtected},
		},
		{name: "dependency", candidate: domain.Candidate{State: domain.StateDependency}},
		{
			name:      "invalid manifest",
			candidate: domain.Candidate{State: domain.StateInvalidManifest},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, test.want, visibleCandidate(test.candidate))
		})
	}
}

func TestCommandDefaultsAndHelpers(t *testing.T) {
	t.Parallel()

	command := (App{Version: "test"}).Command()
	require.Equal(t, "test", command.Version)
	pruneCommand, _, err := command.Find([]string{"prune"})
	require.NoError(t, err)
	require.Equal(t, "true", pruneCommand.Flags().Lookup("dry-run").DefValue)
	require.Nil(t, pruneCommand.Flags().Lookup("allow-unversioned-delete"))
	require.Nil(t, pruneCommand.Flags().Lookup("max-delete-bytes"))
	require.Nil(t, pruneCommand.Flags().Lookup("max-delete-objects"))
	require.Equal(t, "false", command.PersistentFlags().Lookup("debug").DefValue)
	require.Equal(t, "false", command.PersistentFlags().Lookup("show-all").DefValue)
	require.Equal(
		t,
		"true",
		command.PersistentFlags().Lookup("use-backup-repo-credentials").DefValue,
	)
	require.Equal(t, "false", command.PersistentFlags().Lookup("path-style").DefValue)
	require.Equal(t, "false", command.PersistentFlags().Lookup("delete-repository-stray").DefValue)
	require.Equal(
		t,
		domain.BucketVersioningModeAuto,
		command.PersistentFlags().Lookup("bucket-versioning").DefValue,
	)
	require.Equal(t, "", coalesce("", ""))
	require.Equal(t, "second", coalesce("", "second"))
	require.True(t, prefixContains("root/ns", "root/ns/backup"))
	require.False(t, prefixContains("root/ns", "root/other"))
	require.Equal(t, "0s", shortDuration(-time.Second))
	require.Equal(t, "2h", shortDuration(2*time.Hour))
	require.True(t, hasOrphanCandidates(domain.Plan{Candidates: []domain.Candidate{{
		State: domain.StateOrphan, DeferredScan: true,
	}}}))
	require.False(t, hasOrphanCandidates(domain.Plan{Candidates: []domain.Candidate{{
		State: domain.StateProtected,
	}}}))

	exitErr := &ExitError{Code: 2, Err: errors.New("orphans")}
	require.Equal(t, "orphans", exitErr.Error())
	require.ErrorIs(t, exitErr, exitErr.Err)
}

func TestCommandEarlyValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		args    []string
		wantErr string
	}{
		{args: []string{"plan"}, wantErr: "--backup-repo is required"},
		{
			args:    []string{"plan", "--timeout=0", "--backup-repo=repo"},
			wantErr: "--timeout must be greater than zero",
		},
		{
			args:    []string{"plan", "--min-age=-1h", "--backup-repo=repo"},
			wantErr: "--min-age must be zero or greater",
		},
		{
			args:    []string{"plan", "--request-timeout=0", "--backup-repo=repo"},
			wantErr: "--request-timeout must be greater than zero",
		},
		{
			args:    []string{"plan", "--backup-repo=repo", "--namespace=../unsafe"},
			wantErr: "invalid --namespace",
		},
		{
			args:    []string{"plan", "--backup-repo=repo", "--path-style", "false"},
			wantErr: "unknown command \"false\"",
		},
		{
			args: []string{
				"plan", "--backup-repo=repo", "--bucket-versioning=unknown",
			},
			wantErr: "invalid --bucket-versioning",
		},
		{
			args:    []string{"prune", "--dry-run=false", "--backup-repo=repo"},
			wantErr: "--confirm DELETE",
		},
		{
			args:    []string{"prune", "--backup-repo=repo", "--concurrency=0"},
			wantErr: "--concurrency must be greater than zero",
		},
	}
	for _, test := range tests {
		command := (App{Version: "test"}).Command()
		command.SetArgs(test.args)
		err := command.Execute()
		require.ErrorContains(t, err, test.wantErr)
	}

	ctx, cancel, err := commandContext(context.Background(), time.Second)
	require.NoError(t, err)
	require.NotNil(t, ctx)
	cancel()
}

func TestEnvironmentDefault(t *testing.T) {
	t.Setenv("KBBACKUP_PRUNE_TEST", "configured")
	require.Equal(t, "configured", env("KBBACKUP_PRUNE_TEST", "fallback"))
	require.Equal(t, "fallback", env("KBBACKUP_PRUNE_MISSING", "fallback"))
}
