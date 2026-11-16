package server_test

import (
	"reflect"
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
	}
	for _, mode := range ddlModes {
		for _, tc := range cases {
			t.Run(mode+"/"+tc.name, func(t *testing.T) {
				q := env.client.Query(tc.sql)
				q.Parameters = tc.params
				var it *bigquery.RowIterator
				if mode == "jobs.insert" {
					job, err := q.Run(env.ctx)
					if err != nil {
						t.Fatal(err)
					}
					status, err := job.Wait(env.ctx)
					if err != nil {
						t.Fatal(err)
					}
					if err := status.Err(); err != nil {
						t.Fatal(err)
					}
					it, err = job.Read(env.ctx)
					if err != nil {
						t.Fatal(err)
					}
				} else {
					var err error
					it, err = q.Read(env.ctx)
					if err != nil {
						t.Fatal(err)
					}
				}
				var rows [][]bigquery.Value
				for {
					var row []bigquery.Value
					err := it.Next(&row)
					if err == iterator.Done {
						break
					}
					if err != nil {
						t.Fatal(err)
					}
					rows = append(rows, row)
				}
				if !reflect.DeepEqual(rows, tc.want) {
					t.Fatalf("got rows %v, want %v", rows, tc.want)
				}
			})
		}
	}
}
