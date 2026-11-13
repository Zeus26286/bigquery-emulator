package server

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"go.uber.org/zap"
	bigqueryv2 "google.golang.org/api/bigquery/v2"

	"github.com/goccy/bigquery-emulator/internal/contentdata"
	"github.com/goccy/bigquery-emulator/internal/logger"
)

func likeSourceTable() *bigqueryv2.Table {
	return &bigqueryv2.Table{
		Id:             "p:ds.src",
		Etag:           "etag",
		CreationTime:   1,
		NumRows:        10,
		Type:           "TABLE",
		TableReference: &bigqueryv2.TableReference{ProjectId: "p", DatasetId: "ds", TableId: "src"},
		Schema: &bigqueryv2.TableSchema{Fields: []*bigqueryv2.TableFieldSchema{
			{Name: "id", Type: "INTEGER", Mode: "REQUIRED", Description: "i"},
			{Name: "s", Type: "STRING", MaxLength: 10, DefaultValueExpression: "'x'"},
			{Name: "d", Type: "DATE"},
			{Name: "r", Type: "RECORD", Fields: []*bigqueryv2.TableFieldSchema{
				{Name: "a", Type: "INTEGER", Mode: "REQUIRED", Description: "nested"},
			}},
		}},
		TimePartitioning:       &bigqueryv2.TimePartitioning{Type: "DAY", Field: "d", ExpirationMs: 86400000},
		Clustering:             &bigqueryv2.Clustering{Fields: []string{"id"}},
		RequirePartitionFilter: true,
		Description:            "t",
		FriendlyName:           "f",
		Labels:                 map[string]string{"k": "v"},
		ExpirationTime:         1893456000000,
		TableConstraints:       &bigqueryv2.TableConstraints{PrimaryKey: &bigqueryv2.TableConstraintsPrimaryKey{Columns: []string{"id"}}},
	}
}

func TestBuildLikeTable(t *testing.T) {
	ctx := logger.WithLogger(context.Background(), zap.NewNop())
	dst := &bigqueryv2.TableReference{ProjectId: "p", DatasetId: "ds2", TableId: "dst"}

	t.Run("copy", func(t *testing.T) {
		src := likeSourceTable()
		got, err := buildLikeTable(ctx, src, &createLikeDDL{ref: dst})
		if err != nil {
			t.Fatal(err)
		}
		want := likeSourceTable()
		want.Id, want.Etag, want.CreationTime, want.NumRows, want.Type = "", "", 0, 0, ""
		want.TableReference = dst
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got  %+v\nwant %+v", got, want)
		}
		// The copy is deep.
		got.Schema.Fields[3].Fields[0].Description = "changed"
		got.Labels["k"] = "changed"
		if src.Schema.Fields[3].Fields[0].Description != "nested" || src.Labels["k"] != "v" {
			t.Fatal("the source table changed")
		}
	})

	t.Run("time_to_range", func(t *testing.T) {
		got, err := buildLikeTable(ctx, likeSourceTable(), &createLikeDDL{
			ref:       dst,
			partition: &partitionSpec{kind: "range", column: "ID", start: 0, end: 100, interval: 10},
		})
		if err != nil {
			t.Fatal(err)
		}
		if got.TimePartitioning != nil || got.RangePartitioning == nil || got.RangePartitioning.Field != "id" || got.RangePartitioning.Range.Interval != 10 {
			t.Fatalf("got time %+v, range %+v", got.TimePartitioning, got.RangePartitioning)
		}
	})

	t.Run("time_keeps_expiration", func(t *testing.T) {
		got, err := buildLikeTable(ctx, likeSourceTable(), &createLikeDDL{
			ref:       dst,
			partition: &partitionSpec{kind: "time", unit: "DAY"},
		})
		if err != nil {
			t.Fatal(err)
		}
		if tp := got.TimePartitioning; tp == nil || tp.Field != "" || tp.Type != "DAY" || tp.ExpirationMs != 86400000 {
			t.Fatalf("got %+v", got.TimePartitioning)
		}
	})

	t.Run("options", func(t *testing.T) {
		got, err := buildLikeTable(ctx, likeSourceTable(), &createLikeDDL{
			ref: dst,
			options: []*optionValue{
				{name: "description", value: "n", isLiteral: true, assign: true},
				{name: "labels", value: map[string]string{"x": "y"}, isLiteral: true, assign: true},
				{name: "partition_expiration_days", value: int64(2), isLiteral: true, assign: true},
				{name: "kms_key_name", value: "k", isLiteral: true, assign: true},
				{name: "friendly_name", exprText: "f()", assign: true},
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		if got.Description != "n" || !reflect.DeepEqual(got.Labels, map[string]string{"x": "y"}) || got.FriendlyName != "f" || got.TimePartitioning.ExpirationMs != 2*86400000 {
			t.Fatalf("got description %q, labels %v, friendlyName %q, partitioning %+v", got.Description, got.Labels, got.FriendlyName, got.TimePartitioning)
		}
	})

	t.Run("cluster_by", func(t *testing.T) {
		got, err := buildLikeTable(ctx, likeSourceTable(), &createLikeDDL{ref: dst, clustering: []string{"S", "d"}})
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got.Clustering.Fields, []string{"s", "d"}) {
			t.Fatalf("got %+v", got.Clustering)
		}
	})

	for _, c := range []struct {
		name string
		like *createLikeDDL
		want string
	}{
		{"unknown_partition_column", &createLikeDDL{ref: dst, partition: &partitionSpec{kind: "time", unit: "DAY", column: "nope"}}, "Unrecognized name: nope"},
		{"partition_column_type", &createLikeDDL{ref: dst, partition: &partitionSpec{kind: "time", unit: "DAY", column: "s"}}, "PARTITION BY expression does not apply to column s of type STRING"},
		{"partition_unrecognized", &createLikeDDL{ref: dst, partitionUnrecognized: true}, "PARTITION BY expression is not supported"},
		{"unknown_cluster_column", &createLikeDDL{ref: dst, clustering: []string{"nope"}}, "Invalid field: nope"},
		{"cluster_expression", &createLikeDDL{ref: dst, clusteringInvalid: true}, "CLUSTER BY supports only column names"},
		{"too_many_cluster_columns", &createLikeDDL{ref: dst, clustering: []string{"id", "s", "d", "r", "id"}}, "Too many clustering fields: 5 clustering fields specified, exceeding the limit of 4"},
		{"bad_option", &createLikeDDL{ref: dst, options: []*optionValue{{name: "description", value: int64(1), isLiteral: true, assign: true}}}, "description must be a STRING"},
	} {
		t.Run(c.name, func(t *testing.T) {
			_, err := buildLikeTable(ctx, likeSourceTable(), c.like)
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("got %v, want an error containing %q", err, c.want)
			}
		})
	}
}

func TestLikeColumnDefinitions(t *testing.T) {
	got, err := likeColumnDefinitions(&bigqueryv2.TableSchema{Fields: []*bigqueryv2.TableFieldSchema{
		{Name: "id", Type: "INTEGER", Mode: "REQUIRED"},
		{Name: "s", Type: "STRING", MaxLength: 10, DefaultValueExpression: "'x'"},
		{Name: "arr", Type: "INTEGER", Mode: "REPEATED"},
		{Name: "r", Type: "RECORD", Mode: "REQUIRED", Fields: []*bigqueryv2.TableFieldSchema{
			{Name: "a", Type: "INTEGER", Mode: "REQUIRED"},
			{Name: "b", Type: "STRING", Mode: "REPEATED"},
		}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	want := []contentdata.ColumnDefinition{
		{Name: "id", Type: "INT64", NotNull: true},
		{Name: "s", Type: "STRING", Default: "'x'"},
		{Name: "arr", Type: "ARRAY<INT64>"},
		{Name: "r", Type: "STRUCT<`a` INT64, `b` ARRAY<STRING>>", NotNull: true},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got  %+v\nwant %+v", got, want)
	}

	_, err = likeColumnDefinitions(&bigqueryv2.TableSchema{Fields: []*bigqueryv2.TableFieldSchema{{Name: "rg", Type: "RANGE"}}})
	if err == nil || err.Error() != "CREATE TABLE LIKE is not supported by the emulator for column rg of type RANGE" {
		t.Fatalf("got %v", err)
	}
}
