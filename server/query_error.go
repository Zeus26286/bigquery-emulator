package server

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	bigqueryv2 "google.golang.org/api/bigquery/v2"

	"github.com/goccy/bigquery-emulator/internal/metadata"
)

// googlesqlite reports SQLite errors with the internal names of the tables
// (project_dataset_table) and the formatted internal query. The errors a
// BigQuery client can act on are rewritten here into BigQuery's.
//
// TODO(googlesqlite): report CREATE TABLE on an existing table and NOT NULL
// violations with the GoogleSQL names, and suggest the GoogleSQL name of a
// table in "Did you mean" (no upstream issues yet).

var (
	alreadyExistsPattern = regexp.MustCompile("(?:table|view) `([^`]+)` already exists")
	notNullPattern       = regexp.MustCompile(`NOT NULL constraint failed: [^\s.]+\.([^\s,;]+)`)
	didYouMeanPattern    = regexp.MustCompile(`; Did you mean ([^?]+)\?`)
)

// translateQueryError rewrites an error of googlesqlite into BigQuery's. It
// is pure: it reads no metadata, since the job transaction may still be open.
// ddl is the analysis of the query, which maps the internal name of a table
// back to the table.
func translateQueryError(err error, projectID string, ddl *ddlInfo) error {
	if err == nil {
		return nil
	}
	msg := err.Error()
	if m := alreadyExistsPattern.FindStringSubmatch(msg); m != nil && ddl != nil {
		for i := len(ddl.creates) - 1; i >= 0; i-- {
			ref := ddl.creates[i].ref
			if strings.Join([]string{ref.ProjectId, ref.DatasetId, ref.TableId}, "_") == m[1] {
				return errDuplicate(fmt.Sprintf("Already Exists: Table %s:%s.%s", ref.ProjectId, ref.DatasetId, ref.TableId))
			}
		}
		return err
	}
	if m := notNullPattern.FindStringSubmatch(msg); m != nil {
		column := strings.Trim(m[1], "`\"[]")
		return fmt.Errorf("Required field %s cannot be null", column)
	}
	if strings.Contains(msg, "Table not found:") {
		if loc := didYouMeanPattern.FindStringSubmatchIndex(msg); loc != nil {
			suggestion := msg[loc[2]:loc[3]]
			replacement := ""
			if mapped, ok := mapSuggestion(suggestion, projectID); ok {
				replacement = fmt.Sprintf("; Did you mean %s?", mapped)
			}
			return errors.New(msg[:loc[0]] + replacement + msg[loc[1]:])
		}
	}
	return err
}

// mapSuggestion maps a table suggested by googlesqlite, which names it by its
// internal name (ds.project_ds_table), back to the name the query would use
// (ds.table).
func mapSuggestion(suggestion, projectID string) (string, bool) {
	i := strings.LastIndex(suggestion, ".")
	if i < 0 {
		return "", false
	}
	prefix, last := suggestion[:i], suggestion[i+1:]
	var storagePrefix string
	switch parts := strings.Split(prefix, "."); len(parts) {
	case 1:
		storagePrefix = projectID + "_" + parts[0] + "_"
	case 2:
		storagePrefix = parts[0] + "_" + parts[1] + "_"
	default:
		return "", false
	}
	if !strings.HasPrefix(last, storagePrefix) || len(last) == len(storagePrefix) {
		return "", false
	}
	return prefix + "." + last[len(storagePrefix):], true
}

// jobErrorProto returns the error of a failed query job, keeping the reason
// of a typed error.
func jobErrorProto(err error) *bigqueryv2.ErrorProto {
	var serr *ServerError
	if errors.As(err, &serr) {
		return serr.ErrorProto()
	}
	return errJobInternalError(err.Error()).ErrorProto()
}

// queryServerError returns the HTTP error of a failed jobs.query, keeping
// the status and reason of a typed error.
func queryServerError(err error) *ServerError {
	var serr *ServerError
	if errors.As(err, &serr) {
		return serr
	}
	return errJobInternalError(err.Error())
}

// jobResultError returns the HTTP error of jobs.getQueryResults for a
// failed job, keeping the status and reason the job failed with.
func jobResultError(job *metadata.Job, err error) *ServerError {
	if content := job.Content(); content != nil && content.Status != nil && content.Status.ErrorResult != nil {
		switch ErrorReason(content.Status.ErrorResult.Reason) {
		case Duplicate:
			return errDuplicate(content.Status.ErrorResult.Message)
		case NotFound:
			return errNotFound(content.Status.ErrorResult.Message)
		}
	}
	return errJobInternalError(err.Error())
}

// storedJobError is the error recorded with a failed job: the message of a
// typed error, without its reason prefix.
func storedJobError(err error) error {
	var serr *ServerError
	if errors.As(err, &serr) {
		return errors.New(serr.Message)
	}
	return err
}
