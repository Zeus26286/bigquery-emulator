package server_test

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"
)

// TestUnusedQueryParametersAreIgnored checks that query parameters the
// query never references are ignored, as BigQuery does, rather than
// failing the query, and that parameters it does reference still bind.
func TestUnusedQueryParametersAreIgnored(t *testing.T) {
	env := newDDLTestEnv(t)
	cases := []struct {
		name   string
		sql    string
		params []bigquery.QueryParameter
		want   [][]bigquery.Value
	}{
		{
			name:   "named_unused",
			sql:    "SELECT 1",
			params: []bigquery.QueryParameter{{Name: "a", Value: 1}},
			want:   [][]bigquery.Value{{int64(1)}},
		},
		{
			name:   "named_two_unused",
			sql:    "SELECT 1",
			params: []bigquery.QueryParameter{{Name: "a", Value: 1}, {Name: "b", Value: "x"}},
			want:   [][]bigquery.Value{{int64(1)}},
		},
		{
			name:   "positional_unused",
			sql:    "SELECT 1",
			params: []bigquery.QueryParameter{{Value: 1}},
			want:   [][]bigquery.Value{{int64(1)}},
		},
		{
			name:   "positional_two_unused",
			sql:    "SELECT 1",
			params: []bigquery.QueryParameter{{Value: 1}, {Value: "x"}},
			want:   [][]bigquery.Value{{int64(1)}},
		},
		{
			name:   "named_partially_used",
			sql:    "SELECT @a",
			params: []bigquery.QueryParameter{{Name: "a", Value: 7}, {Name: "b", Value: "x"}},
			want:   [][]bigquery.Value{{int64(7)}},
		},
		{
			name:   "positional_partially_used",
			sql:    "SELECT ?",
			params: []bigquery.QueryParameter{{Value: 7}, {Value: 8}},
			want:   [][]bigquery.Value{{int64(7)}},
		},
		{
			name:   "script_declare_named",
			sql:    "DECLARE x INT64 DEFAULT @p; SELECT x",
			params: []bigquery.QueryParameter{{Name: "p", Value: 4}, {Name: "q", Value: 1}},
			want:   [][]bigquery.Value{{int64(4)}},
		},
		// Script variables can leak between queries on the shared env,
		// so each script case declares its own variable names.
		{
			name:   "script_declare_positional",
			sql:    "DECLARE d INT64 DEFAULT ?; SELECT ?, d",
			params: []bigquery.QueryParameter{{Value: 4}, {Value: 5}},
			want:   [][]bigquery.Value{{int64(5), int64(4)}},
		},
		{
			name:   "script_declare_bool",
			sql:    "DECLARE flag BOOL DEFAULT @b; SELECT flag AND TRUE",
			params: []bigquery.QueryParameter{{Name: "b", Value: true}},
			want:   [][]bigquery.Value{{true}},
		},
	}
	for _, mode := range ddlModes {
		for _, tc := range cases {
			t.Run(mode+"/"+tc.name, func(t *testing.T) {
				rows, err := env.readParamQuery(mode, tc.sql, tc.params)
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(rows, tc.want) {
					t.Fatalf("got rows %v, want %v", rows, tc.want)
				}
			})
		}
	}
}

// TestUnusedQueryParametersInDDLAndDML runs DDL and DML statements with a
// query parameter they never reference.
func TestUnusedQueryParametersInDDLAndDML(t *testing.T) {
	env := newDDLTestEnv(t)
	unused := []bigquery.QueryParameter{{Name: "unused", Value: 1}}
	for _, mode := range ddlModes {
		t.Run(mode, func(t *testing.T) {
			table := "dataset1.unused_params_" + strings.ReplaceAll(mode, ".", "_")
			for _, sql := range []string{
				fmt.Sprintf("CREATE TABLE %s (a INT64)", table),
				fmt.Sprintf("INSERT INTO %s (a) VALUES (1)", table),
			} {
				if _, err := env.readParamQuery(mode, sql, unused); err != nil {
					t.Fatalf("%s: %v", sql, err)
				}
			}
			rows := env.mustRead(t, fmt.Sprintf("SELECT a FROM %s", table))
			if want := [][]bigquery.Value{{int64(1)}}; !reflect.DeepEqual(rows, want) {
				t.Fatalf("got rows %v, want %v", rows, want)
			}
		})
	}
}

// readParamQuery runs sql with params through the API named by mode and
// returns its rows.
func (e *ddlTestEnv) readParamQuery(mode, sql string, params []bigquery.QueryParameter) ([][]bigquery.Value, error) {
	q := e.client.Query(sql)
	q.Parameters = params
	var it *bigquery.RowIterator
	if mode == "jobs.insert" {
		job, err := q.Run(e.ctx)
		if err != nil {
			return nil, err
		}
		status, err := job.Wait(e.ctx)
		if err != nil {
			return nil, err
		}
		if err := status.Err(); err != nil {
			return nil, err
		}
		if it, err = job.Read(e.ctx); err != nil {
			return nil, err
		}
	} else {
		var err error
		if it, err = q.Read(e.ctx); err != nil {
			return nil, err
		}
	}
	var rows [][]bigquery.Value
	for {
		var row []bigquery.Value
		err := it.Next(&row)
		if err == iterator.Done {
			return rows, nil
		}
		if err != nil {
			return rows, err
		}
		rows = append(rows, row)
	}
}
