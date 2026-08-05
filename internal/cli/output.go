package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"text/tabwriter"
	"time"

	"github.com/labring-sigs/kbbackup-prune/internal/domain"
)

type commandOutput struct {
	Plan      domain.Plan       `json:"plan"`
	Execution *domain.Execution `json:"execution,omitempty"`
}

func writeOutput(
	writer io.Writer,
	format string,
	plan domain.Plan,
	execution *domain.Execution,
	showAll bool,
) error {
	rootCounts := outputVolumeRootCounts(plan)
	plan = outputPlan(plan, showAll)

	if format == "json" {
		encoder := json.NewEncoder(writer)
		encoder.SetIndent("", "  ")
		return encoder.Encode(commandOutput{Plan: plan, Execution: execution})
	}

	if format != "table" {
		return fmt.Errorf("unsupported output format %q; use table or json", format)
	}

	mode := "plan"
	if execution != nil && execution.DryRun {
		mode = "dry-run"
	}

	if execution != nil && !execution.DryRun {
		mode = "delete"
	}

	versioning := plan.Versioning
	if plan.VersioningSource != "" {
		versioning += " (" + plan.VersioningSource + ")"
	}

	fmt.Fprintf(
		writer,
		"Mode: %s  Repository: %s  Bucket: %s  Versioning: %s\n",
		mode,
		plan.Repository,
		plan.Bucket,
		versioning,
	)

	if len(plan.Prefixes) > 1 {
		fmt.Fprintf(
			writer,
			"Discovered PVC roots: %d (repository: %d, protected user: %d, "+
				"unowned: %d, other: %d)\n",
			rootCounts.Total,
			rootCounts.Repository,
			rootCounts.ProtectedUser,
			rootCounts.Unowned,
			rootCounts.Other,
		)
	} else if plan.Prefix != "" {
		fmt.Fprintf(writer, "Scan prefix: %s\n", plan.Prefix)
	}

	fmt.Fprintf(
		writer,
		"Scanned: %d objects / %s  Eligible: %d objects / %s  Unclassified: %d objects / %s\n\n",
		plan.ScannedObjects,
		humanBytes(plan.ScannedBytes),
		plan.DeleteObjects,
		humanBytes(plan.DeleteBytes),
		plan.UnclassifiedObjects,
		humanBytes(plan.UnclassifiedBytes),
	)
	table := tabwriter.NewWriter(writer, 2, 4, 2, ' ', 0)

	_, _ = fmt.Fprintln(table, "STATE\tTYPE\tBACKUP\tAGE\tOBJECTS\tSIZE\tPREFIX\tREASON")
	for _, candidate := range plan.Candidates {
		age := "unknown"
		if !candidate.CreatedAt.IsZero() {
			age = shortDuration(plan.GeneratedAt.Sub(candidate.CreatedAt))
		}

		kind := candidate.Kind
		if kind == "" {
			kind = domain.CandidateBackup
		}

		backup := candidate.Backup.String()
		if candidate.Backup.Namespace == "" && candidate.Backup.Name == "" {
			backup = "-"
		}

		objects := strconv.Itoa(candidate.ObjectCount)
		if candidate.DeferredScan {
			objects = "deferred"
		}

		_, _ = fmt.Fprintf(table, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			candidate.State, kind, backup, age, objects,
			humanBytes(candidate.Bytes), candidate.Prefix, candidate.Reason)
	}

	if err := table.Flush(); err != nil {
		return err
	}

	if len(plan.BlockingReasons) > 0 {
		_, _ = fmt.Fprintln(writer, "\nExecution blockers:")
		for _, reason := range plan.BlockingReasons {
			_, _ = fmt.Fprintf(writer, "- %s\n", reason)
		}
	}

	if execution != nil {
		failed := 0
		deletedObjects := 0

		deletedBytes := int64(0)
		for _, result := range execution.Results {
			deletedObjects += result.ObjectsDeleted

			deletedBytes += result.BytesDeleted
			if result.Error != "" {
				failed++
				_, _ = fmt.Fprintf(writer, "FAILED %s: %s\n", result.Prefix, result.Error)
			}
		}

		action := "Would delete"
		if !execution.DryRun {
			action = "Deleted"
		}

		_, _ = fmt.Fprintf(writer, "\n%s: %d objects / %s across %d candidates; failures: %d\n",
			action, deletedObjects, humanBytes(deletedBytes), len(execution.Results), failed)
	}

	return nil
}

func outputVolumeRootCounts(plan domain.Plan) domain.VolumeRootCounts {
	if plan.VolumeRootCounts != nil {
		return *plan.VolumeRootCounts
	}

	return domain.VolumeRootCounts{Total: len(plan.Prefixes), Other: len(plan.Prefixes)}
}

func outputPlan(plan domain.Plan, showAll bool) domain.Plan {
	candidates := make([]domain.Candidate, 0, len(plan.Candidates))
	stateCounts := make(map[domain.CandidateState]int)

	for _, candidate := range plan.Candidates {
		if !showAll && !visibleCandidate(candidate) {
			continue
		}

		candidates = append(candidates, candidate)
		stateCounts[candidate.State]++
	}

	plan.Candidates = candidates
	plan.StateCounts = stateCounts

	return plan
}

func visibleCandidate(candidate domain.Candidate) bool {
	switch candidate.State {
	case domain.StateOrphan, domain.StateRetained, domain.StateTooYoung:
		return true
	default:
		return candidate.DeletionConfigurable
	}
}

func shortDuration(duration time.Duration) string {
	if duration < 0 {
		return "0s"
	}

	days := int(duration.Hours()) / 24
	if days > 0 {
		return fmt.Sprintf("%dd", days)
	}

	if hours := int(duration.Hours()); hours > 0 {
		return fmt.Sprintf("%dh", hours)
	}

	return duration.Round(time.Minute).String()
}
