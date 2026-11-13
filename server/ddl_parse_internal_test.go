package server

import (
	"errors"
	"net/http"
	"reflect"
	"strings"
	"testing"

	bigqueryv2 "google.golang.org/api/bigquery/v2"
)

func TestAnalyzeDDLCreateTable(t *testing.T) {
	type column struct {
		description string
		required    bool
		defaultText string
	}
	type create struct {
		ref        string // project.dataset.table
		isView     bool
		columns    map[string]column
		partition  *partitionSpec
		clustering []string
		options    map[string]any // decoded literals; source text for other values
		primaryKey []string
	}
	for _, c := range []struct {
		name  string
		query string
		want  []create
	}{
		{
			name:  "columns",
			query: "CREATE TABLE ds.t (a INT64 NOT NULL OPTIONS(description='col a'), b STRING DEFAULT 'x', ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP(), c STRING DEFAULT CONCAT('a','b'), arr ARRAY<INT64> NOT NULL)",
			want: []create{{ref: "p.ds.t", columns: map[string]column{
				"a":   {description: "col a", required: true},
				"b":   {defaultText: "'x'"},
				"ts":  {defaultText: "CURRENT_TIMESTAMP()"},
				"c":   {defaultText: "CONCAT('a','b')"},
				"arr": {},
			}}},
		},
		{
			name:  "partition_date_column",
			query: "CREATE TABLE ds.t (d DATE) PARTITION BY d",
			want:  []create{{ref: "p.ds.t", columns: map[string]column{"d": {}}, partition: &partitionSpec{kind: "time", unit: "DAY", column: "d"}}},
		},
		{
			name:  "partition_date_function",
			query: "CREATE TABLE ds.t (ts TIMESTAMP) PARTITION BY DATE(ts)",
			want:  []create{{ref: "p.ds.t", columns: map[string]column{"ts": {}}, partition: &partitionSpec{kind: "time", unit: "DAY", column: "ts", function: "DATE"}}},
		},
		{
			name:  "partition_timestamp_trunc",
			query: "CREATE TABLE ds.t (ts TIMESTAMP) PARTITION BY timestamp_trunc(ts, month)",
			want:  []create{{ref: "p.ds.t", columns: map[string]column{"ts": {}}, partition: &partitionSpec{kind: "time", unit: "MONTH", column: "ts", function: "TIMESTAMP_TRUNC"}}},
		},
		{
			name:  "partition_datetime_trunc",
			query: "CREATE TABLE ds.t (dt DATETIME) PARTITION BY DATETIME_TRUNC(dt, YEAR)",
			want:  []create{{ref: "p.ds.t", columns: map[string]column{"dt": {}}, partition: &partitionSpec{kind: "time", unit: "YEAR", column: "dt", function: "DATETIME_TRUNC"}}},
		},
		{
			name:  "partition_date_trunc",
			query: "CREATE TABLE ds.t (d DATE) PARTITION BY DATE_TRUNC(d, MONTH)",
			want:  []create{{ref: "p.ds.t", columns: map[string]column{"d": {}}, partition: &partitionSpec{kind: "time", unit: "MONTH", column: "d", function: "DATE_TRUNC"}}},
		},
		{
			name:  "partition_ingestion_date",
			query: "CREATE TABLE ds.t (a INT64) PARTITION BY _PARTITIONDATE",
			want:  []create{{ref: "p.ds.t", columns: map[string]column{"a": {}}, partition: &partitionSpec{kind: "time", unit: "DAY"}}},
		},
		{
			name:  "partition_ingestion_time",
			query: "CREATE TABLE ds.t (a INT64) PARTITION BY DATE(_PARTITIONTIME)",
			want:  []create{{ref: "p.ds.t", columns: map[string]column{"a": {}}, partition: &partitionSpec{kind: "time", unit: "DAY", function: "DATE"}}},
		},
		{
			name:  "partition_ingestion_hour",
			query: "CREATE TABLE ds.t (a INT64) PARTITION BY TIMESTAMP_TRUNC(_PARTITIONTIME, HOUR)",
			want:  []create{{ref: "p.ds.t", columns: map[string]column{"a": {}}, partition: &partitionSpec{kind: "time", unit: "HOUR", function: "TIMESTAMP_TRUNC"}}},
		},
		{
			name:  "partition_range",
			query: "CREATE TABLE ds.t (id INT64) PARTITION BY RANGE_BUCKET(id, GENERATE_ARRAY(-10, 100))",
			want:  []create{{ref: "p.ds.t", columns: map[string]column{"id": {}}, partition: &partitionSpec{kind: "range", column: "id", start: -10, end: 100, interval: 1}}},
		},
		{
			name:  "partition_range_interval",
			query: "CREATE TABLE ds.t (id INT64) PARTITION BY RANGE_BUCKET(id, GENERATE_ARRAY(0, 100, 10))",
			want:  []create{{ref: "p.ds.t", columns: map[string]column{"id": {}}, partition: &partitionSpec{kind: "range", column: "id", start: 0, end: 100, interval: 10}}},
		},
		{
			name:  "partition_ingestion_month",
			query: "CREATE TABLE ds.t (a INT64) PARTITION BY DATE_TRUNC(_PARTITIONDATE, MONTH)",
			want:  []create{{ref: "p.ds.t", columns: map[string]column{"a": {}}, partition: &partitionSpec{kind: "time", unit: "MONTH", function: "DATE_TRUNC"}}},
		},
		{
			name:  "partition_date_trunc_hour",
			query: "CREATE TABLE ds.t (d DATE) PARTITION BY DATE_TRUNC(d, HOUR)",
			want:  []create{{ref: "p.ds.t", columns: map[string]column{"d": {}}}},
		},
		{
			name:  "partition_datetime_trunc_ingestion",
			query: "CREATE TABLE ds.t (a INT64) PARTITION BY DATETIME_TRUNC(_PARTITIONTIME, DAY)",
			want:  []create{{ref: "p.ds.t", columns: map[string]column{"a": {}}}},
		},
		{
			name:  "partition_unrecognised",
			query: "CREATE TABLE ds.t (id INT64) PARTITION BY MOD(id, 2)",
			want:  []create{{ref: "p.ds.t", columns: map[string]column{"id": {}}}},
		},
		{
			name:  "ctas",
			query: "CREATE TABLE ds.t PARTITION BY DATE(ts) CLUSTER BY name, id OPTIONS(description='d', labels=[('k','v'), STRUCT('k2' AS a, 'v2' AS b)], partition_expiration_days=1.5, require_partition_filter=true, friendly_name=NULL, expiration_timestamp=TIMESTAMP '2030-01-01 00:00:00 UTC') AS SELECT 1 AS id, 'a' AS name, CURRENT_TIMESTAMP() AS ts",
			want: []create{{
				ref:        "p.ds.t",
				columns:    map[string]column{},
				partition:  &partitionSpec{kind: "time", unit: "DAY", column: "ts", function: "DATE"},
				clustering: []string{"name", "id"},
				options: map[string]any{
					"description":               "d",
					"labels":                    map[string]string{"k": "v", "k2": "v2"},
					"partition_expiration_days": 1.5,
					"require_partition_filter":  true,
					"friendly_name":             nil,
					"expiration_timestamp":      "TIMESTAMP '2030-01-01 00:00:00 UTC'",
				},
			}},
		},
		{
			name:  "ctas_column_list",
			query: "CREATE OR REPLACE TABLE ds.t (id INT64 NOT NULL OPTIONS(description='i'), name STRING) AS SELECT 1, 'a'",
			want:  []create{{ref: "p.ds.t", columns: map[string]column{"id": {description: "i", required: true}, "name": {}}}},
		},
		{
			name:  "primary_key",
			query: "CREATE TABLE ds.t (a INT64, b INT64, PRIMARY KEY (a, b) NOT ENFORCED)",
			want:  []create{{ref: "p.ds.t", columns: map[string]column{"a": {}, "b": {}}, primaryKey: []string{"a", "b"}}},
		},
		{
			name:  "column_primary_key",
			query: "CREATE TABLE ds.t (a INT64 PRIMARY KEY NOT ENFORCED, b INT64)",
			want:  []create{{ref: "p.ds.t", columns: map[string]column{"a": {}, "b": {}}, primaryKey: []string{"a"}}},
		},
		{
			name:  "backticked_path",
			query: "CREATE TABLE `proj.ds.t` (a INT64)",
			want:  []create{{ref: "proj.ds.t", columns: map[string]column{"a": {}}}},
		},
		{
			name:  "unqualified_with_default_dataset",
			query: "CREATE TABLE t (a INT64)",
			want:  []create{{ref: "p.def.t", columns: map[string]column{"a": {}}}},
		},
		{
			name:  "temp_table_ignored",
			query: "CREATE TEMP TABLE t (a INT64); SELECT * FROM t",
		},
		{
			name:  "script_with_two_creates",
			query: "CREATE TABLE ds.a (x INT64) OPTIONS(description='first'); INSERT ds.a VALUES (1); CREATE VIEW ds.v OPTIONS(description='view') AS SELECT 1 AS y",
			want: []create{
				{ref: "p.ds.a", columns: map[string]column{"x": {}}, options: map[string]any{"description": "first"}},
				{ref: "p.ds.v", isView: true, options: map[string]any{"description": "view"}},
			},
		},
		{
			name:  "create_in_if_branch",
			query: "IF TRUE THEN CREATE TABLE ds.c (a INT64 NOT NULL); END IF",
			want:  []create{{ref: "p.ds.c", columns: map[string]column{"a": {required: true}}}},
		},
		{
			// The table of CREATE TABLE ... LIKE is recorded for
			// checkDatasets and translateQueryError (see
			// TestAnalyzeDDLCreateTableLike).
			name:  "like",
			query: "CREATE TABLE ds.t LIKE ds.src",
			want:  []create{{ref: "p.ds.t", columns: map[string]column{}}},
		},
		{
			name:  "unparseable",
			query: "CREATE TABLE ds.t (a INT64",
		},
		{
			name:  "no_ddl",
			query: "SELECT 1",
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			info, err := analyzeDDL(c.query, "p", "def")
			if err != nil {
				t.Fatal(err)
			}
			var got []create
			if info != nil {
				for _, cr := range info.creates {
					g := create{
						ref:        cr.ref.ProjectId + "." + cr.ref.DatasetId + "." + cr.ref.TableId,
						isView:     cr.isView,
						partition:  cr.partition,
						clustering: cr.clustering,
						primaryKey: cr.primaryKey,
					}
					if cr.columns != nil {
						g.columns = map[string]column{}
						for name, a := range cr.columns {
							g.columns[name] = column{description: a.description, required: a.required, defaultText: a.defaultText}
						}
					}
					if len(cr.options) != 0 {
						g.options = map[string]any{}
						for _, o := range cr.options {
							if o.isLiteral {
								g.options[o.name] = o.value
							} else {
								g.options[o.name] = o.exprText
							}
						}
					}
					got = append(got, g)
				}
			}
			if !reflect.DeepEqual(got, c.want) {
				t.Fatalf("got  %+v\nwant %+v", got, c.want)
			}
		})
	}
}

func TestAnalyzeDDLRejects(t *testing.T) {
	const qualify = `Table "nodataset" must be qualified with a dataset (e.g. dataset.table).`
	for _, c := range []struct {
		query          string
		defaultDataset string
		want           error
		wantMessage    string
	}{
		{query: "CREATE TABLE nodataset (a INT64)", wantMessage: qualify},
		{query: "CREATE OR REPLACE TABLE nodataset AS SELECT 1 AS a", wantMessage: qualify},
		{query: "SELECT 1; CREATE TABLE IF NOT EXISTS nodataset (a INT64)", wantMessage: qualify},
		{query: "CREATE VIEW nodataset AS SELECT 1 AS a", wantMessage: qualify},
		{query: "CREATE TABLE nodataset (a INT64)", defaultDataset: "ds"},
		{query: "CREATE TEMP TABLE nodataset (a INT64); SELECT 1"},
		{query: "CREATE TABLE ds.t (a INT64)"},
		{query: "ALTER TABLE ds.t ADD COLUMN b STRING; SELECT 1", want: errAlterInScript},
		{query: "SELECT 1; ALTER TABLE ds.t ADD COLUMN b STRING", want: errAlterInScript},
		{query: "BEGIN ALTER TABLE ds.t ADD COLUMN b STRING; END", want: errAlterInScript},
		{query: "IF TRUE THEN ALTER TABLE ds.t ADD COLUMN b STRING; END IF", want: errAlterInScript},
		{query: "ALTER TABLE ds.t ADD COLUMN b STRING;"},
		{query: "ALTER TABLE ds.t ADD COLUMN b STRING"},
		{query: "SELECT 1; CREATE TABLE ds.a LIKE ds.b", want: errLikeInScript},
		{query: "CREATE TABLE ds.a LIKE ds.b; SELECT 1", want: errLikeInScript},
		{query: "BEGIN CREATE TABLE ds.a LIKE ds.b; END", want: errLikeInScript},
		{query: "IF TRUE THEN CREATE TABLE ds.a LIKE ds.b; END IF", want: errLikeInScript},
		{query: "CREATE TABLE nodataset LIKE ds.b", wantMessage: qualify},
		{query: "CREATE TABLE ds.a LIKE nodataset", wantMessage: qualify},
		{query: "CREATE TEMP TABLE a LIKE ds.b", wantMessage: "CREATE TEMP TABLE ... LIKE is not supported by the emulator"},
		{query: "CREATE TABLE ds.a (x INT64) LIKE ds.b", wantMessage: "CREATE TABLE ... LIKE: a column list is not supported by the emulator"},
		{query: "CREATE TABLE ds.a LIKE ds.b WITH CONNECTION `p.us.c`", wantMessage: "CREATE TABLE ... LIKE: WITH CONNECTION is not supported by the emulator"},
		{query: "CREATE TABLE ds.a LIKE ds.b DEFAULT COLLATE 'und:ci'", wantMessage: "CREATE TABLE ... LIKE: DEFAULT COLLATE is not supported by the emulator"},
		{query: "CREATE TABLE ds.a LIKE ds.b;"},
		{query: "CREATE TABLE nodataset LIKE b", defaultDataset: "ds"},
	} {
		t.Run(c.query, func(t *testing.T) {
			_, err := analyzeDDL(c.query, "p", c.defaultDataset)
			switch {
			case c.want != nil:
				if !errors.Is(err, c.want) {
					t.Fatalf("got %v, want %v", err, c.want)
				}
			case c.wantMessage != "":
				if err == nil || err.Error() != c.wantMessage {
					t.Fatalf("got %v, want %q", err, c.wantMessage)
				}
			case err != nil:
				t.Fatalf("got %v, want no error", err)
			}
		})
	}
}

func TestAnalyzeDDLCreateTableLike(t *testing.T) {
	type like struct {
		ref, srcRef           string // project.dataset.table
		orReplace             bool
		ifNotExists           bool
		partition             *partitionSpec
		partitionUnrecognized bool
		clustering            []string
		clusteringInvalid     bool
		options               map[string]any // decoded literals; source text for other values
		queryText             string
	}
	for _, c := range []struct {
		name           string
		query          string
		defaultDataset string
		want           like
	}{
		{name: "two_part", query: "CREATE TABLE ds.a LIKE ds2.b", want: like{ref: "p.ds.a", srcRef: "p.ds2.b"}},
		{name: "three_part", query: "CREATE TABLE `q.ds.a` LIKE q2.ds2.b", want: like{ref: "q.ds.a", srcRef: "q2.ds2.b"}},
		{name: "default_dataset", query: "CREATE TABLE a LIKE b", defaultDataset: "def", want: like{ref: "p.def.a", srcRef: "p.def.b"}},
		{name: "or_replace", query: "CREATE OR REPLACE TABLE ds.a LIKE ds.b", want: like{ref: "p.ds.a", srcRef: "p.ds.b", orReplace: true}},
		{name: "if_not_exists", query: "CREATE TABLE IF NOT EXISTS ds.a LIKE ds.b", want: like{ref: "p.ds.a", srcRef: "p.ds.b", ifNotExists: true}},
		{
			name:  "partition_column",
			query: "CREATE TABLE ds.a LIKE ds.b PARTITION BY d",
			want:  like{ref: "p.ds.a", srcRef: "p.ds.b", partition: &partitionSpec{kind: "time", unit: "DAY", column: "d"}},
		},
		{
			name:  "partition_date",
			query: "CREATE TABLE ds.a LIKE ds.b PARTITION BY DATE(ts)",
			want:  like{ref: "p.ds.a", srcRef: "p.ds.b", partition: &partitionSpec{kind: "time", unit: "DAY", column: "ts", function: "DATE"}},
		},
		{
			name:  "partition_ingestion",
			query: "CREATE TABLE ds.a LIKE ds.b PARTITION BY _PARTITIONDATE",
			want:  like{ref: "p.ds.a", srcRef: "p.ds.b", partition: &partitionSpec{kind: "time", unit: "DAY"}},
		},
		{
			name:  "partition_range",
			query: "CREATE TABLE ds.a LIKE ds.b PARTITION BY RANGE_BUCKET(id, GENERATE_ARRAY(0, 100, 10))",
			want:  like{ref: "p.ds.a", srcRef: "p.ds.b", partition: &partitionSpec{kind: "range", column: "id", start: 0, end: 100, interval: 10}},
		},
		{
			name:  "partition_unrecognized",
			query: "CREATE TABLE ds.a LIKE ds.b PARTITION BY MOD(id, 2)",
			want:  like{ref: "p.ds.a", srcRef: "p.ds.b", partitionUnrecognized: true},
		},
		{
			name:  "cluster_by",
			query: "CREATE TABLE ds.a LIKE ds.b CLUSTER BY x, y",
			want:  like{ref: "p.ds.a", srcRef: "p.ds.b", clustering: []string{"x", "y"}},
		},
		{
			name:  "cluster_by_expression",
			query: "CREATE TABLE ds.a LIKE ds.b CLUSTER BY x, LOWER(y)",
			want:  like{ref: "p.ds.a", srcRef: "p.ds.b", clusteringInvalid: true},
		},
		{
			name:  "options",
			query: "CREATE TABLE ds.a LIKE ds.b OPTIONS(description='d', labels=[('k','v')], expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY))",
			want: like{ref: "p.ds.a", srcRef: "p.ds.b", options: map[string]any{
				"description":          "d",
				"labels":               map[string]string{"k": "v"},
				"expiration_timestamp": "TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)",
			}},
		},
		{
			name:  "as_query",
			query: "CREATE TABLE ds.a LIKE ds.b AS WITH x AS (SELECT 1 AS a) SELECT * FROM x;",
			want:  like{ref: "p.ds.a", srcRef: "p.ds.b", queryText: "WITH x AS (SELECT 1 AS a) SELECT * FROM x"},
		},
		{
			name:  "as_query_union",
			query: "CREATE TABLE ds.a LIKE ds.b AS (SELECT 1 AS a) UNION ALL (SELECT 2)",
			want:  like{ref: "p.ds.a", srcRef: "p.ds.b", queryText: "(SELECT 1 AS a) UNION ALL (SELECT 2)"},
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			info, err := analyzeDDL(c.query, "p", c.defaultDataset)
			if err != nil {
				t.Fatal(err)
			}
			if info == nil || info.like == nil {
				t.Fatalf("got no CREATE TABLE LIKE: %+v", info)
			}
			l := info.like
			got := like{
				ref:                   l.ref.ProjectId + "." + l.ref.DatasetId + "." + l.ref.TableId,
				srcRef:                l.srcRef.ProjectId + "." + l.srcRef.DatasetId + "." + l.srcRef.TableId,
				orReplace:             l.orReplace,
				ifNotExists:           l.ifNotExists,
				partition:             l.partition,
				partitionUnrecognized: l.partitionUnrecognized,
				clustering:            l.clustering,
				clusteringInvalid:     l.clusteringInvalid,
				queryText:             l.queryText,
			}
			if len(l.options) != 0 {
				got.options = map[string]any{}
				for _, o := range l.options {
					if o.isLiteral {
						got.options[o.name] = o.value
					} else {
						got.options[o.name] = o.exprText
					}
				}
			}
			if !reflect.DeepEqual(got, c.want) {
				t.Fatalf("got  %+v\nwant %+v", got, c.want)
			}
			// The table is also recorded as created.
			if len(info.creates) != 1 || info.creates[0].ref != l.ref {
				t.Fatalf("creates: got %+v", info.creates)
			}
		})
	}
}

func TestTranslateQueryError(t *testing.T) {
	ddl, err := analyzeDDL("SELECT 1; CREATE TABLE ds.c1 (a INT64); CREATE VIEW proj.ds.v1 AS SELECT 1 AS a", "test", "")
	if err != nil || ddl == nil {
		t.Fatalf("analyzeDDL: %v, %v", ddl, err)
	}
	for _, c := range []struct {
		name       string
		err        string
		ddl        *ddlInfo
		want       string
		wantReason ErrorReason
		wantStatus int
	}{
		{
			name:       "duplicate_table",
			err:        "failed to exec CREATE TABLE ds.c1 (a INT64): sqlite3: SQL logic error: table `test_ds_c1` already exists",
			ddl:        ddl,
			want:       "Already Exists: Table test:ds.c1",
			wantReason: Duplicate,
			wantStatus: http.StatusConflict,
		},
		{
			name:       "duplicate_view",
			err:        "failed to exec SELECT 1 AS `a#1` : sqlite3: SQL logic error: view `proj_ds_v1` already exists",
			ddl:        ddl,
			want:       "Already Exists: Table proj:ds.v1",
			wantReason: Duplicate,
			wantStatus: http.StatusConflict,
		},
		{
			name: "duplicate_unknown_table",
			err:  "sqlite3: SQL logic error: table `test_ds_other` already exists",
			ddl:  ddl,
			want: "sqlite3: SQL logic error: table `test_ds_other` already exists",
		},
		{
			name: "not_null",
			err:  "failed to exec INSERT INTO `test_ds_c6` (`a`,`b`) VALUES (null,\"x\"): sqlite3: constraint failed: NOT NULL constraint failed: test_ds_c6.a",
			want: "Required field a cannot be null",
		},
		{
			name: "not_null_domain_scoped_project",
			err:  "sqlite3: constraint failed: NOT NULL constraint failed: google.com:p_ds_t.col_a",
			want: "Required field col_a cannot be null",
		},
		{
			name: "did_you_mean_dataset",
			err:  "failed to analyze: Table not found: ds.cp1; Did you mean ds.test_ds_c1? [at 1:15]",
			want: "failed to analyze: Table not found: ds.cp1; Did you mean ds.c1? [at 1:15]",
		},
		{
			name: "did_you_mean_project",
			err:  "failed to analyze: Table not found: proj.ds.cp1; Did you mean proj.ds.proj_ds_c1? [at 1:15]",
			want: "failed to analyze: Table not found: proj.ds.cp1; Did you mean proj.ds.c1? [at 1:15]",
		},
		{
			name: "did_you_mean_unmappable",
			err:  "failed to analyze: Table not found: cp1; Did you mean test_ds_c1? [at 1:15]",
			want: "failed to analyze: Table not found: cp1 [at 1:15]",
		},
		{
			name: "unrelated",
			err:  "failed to analyze: Unrecognized name: b [at 1:8]",
			want: "failed to analyze: Unrecognized name: b [at 1:8]",
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			got := translateQueryError(errors.New(c.err), "test", c.ddl)
			var serr *ServerError
			if c.wantReason != "" {
				if !errors.As(got, &serr) || serr.Reason != c.wantReason || serr.Status != c.wantStatus || serr.Message != c.want {
					t.Fatalf("got %#v, want %s %d %q", got, c.wantReason, c.wantStatus, c.want)
				}
				return
			}
			if errors.As(got, &serr) {
				t.Fatalf("got typed error %v", got)
			}
			if got.Error() != c.want {
				t.Fatalf("got %q, want %q", got, c.want)
			}
		})
	}
	if translateQueryError(nil, "test", ddl) != nil {
		t.Fatal("nil error translated")
	}
}

func TestApplyTableOption(t *testing.T) {
	table := &bigqueryv2.Table{TimePartitioning: &bigqueryv2.TimePartitioning{Type: "DAY"}}
	for _, o := range []*optionValue{
		{name: "description", value: "d"},
		{name: "friendly_name", value: "f"},
		{name: "labels", value: map[string]string{"k": "v"}},
		{name: "expiration_timestamp", value: int64(1893456000000)},
		{name: "require_partition_filter", value: true},
		{name: "partition_expiration_days", value: int64(3)},
	} {
		if err := applyTableOption(table, o); err != nil {
			t.Fatalf("%s: %v", o.name, err)
		}
	}
	if table.Description != "d" || table.FriendlyName != "f" || table.Labels["k"] != "v" || table.ExpirationTime != 1893456000000 ||
		!table.RequirePartitionFilter || table.TimePartitioning.ExpirationMs != 3*86400000 {
		t.Fatalf("got %+v", table)
	}
	for _, name := range []string{"description", "friendly_name", "labels", "expiration_timestamp", "require_partition_filter", "partition_expiration_days"} {
		if err := applyTableOption(table, &optionValue{name: name}); err != nil {
			t.Fatalf("%s=NULL: %v", name, err)
		}
	}
	if table.Description != "" || table.FriendlyName != "" || table.Labels != nil || table.ExpirationTime != 0 ||
		table.RequirePartitionFilter || table.TimePartitioning.ExpirationMs != 0 {
		t.Fatalf("after NULL: got %+v", table)
	}
	for _, o := range []*optionValue{
		{name: "description", value: int64(1)},
		{name: "labels", value: "x"},
		{name: "require_partition_filter", value: "x"},
		{name: "kms_key_name", value: "x"},
	} {
		if err := applyTableOption(table, o); err == nil {
			t.Errorf("%s=%v: want an error", o.name, o.value)
		}
	}
}

func TestDDLParserOptionsParseAlterTable(t *testing.T) {
	// The emulator's parse of ALTER TABLE moved to the shared parser
	// options; the actions it supported before must parse as before.
	stmt, ok, err := parseAlterTable("ALTER TABLE ds.t ADD COLUMN IF NOT EXISTS b STRUCT<x INT64 NOT NULL OPTIONS(description='xd')> OPTIONS(description='bd'), DROP COLUMN IF EXISTS c, RENAME COLUMN d TO e")
	if err != nil || !ok {
		t.Fatalf("parseAlterTable: %v, %v", ok, err)
	}
	if !reflect.DeepEqual(stmt.path, []string{"ds", "t"}) || len(stmt.actions) != 3 {
		t.Fatalf("got %+v", stmt)
	}
	add, drop, rename := stmt.actions[0], stmt.actions[1], stmt.actions[2]
	if add.kind != alterAddColumn || add.name != "b" || add.typeText != "STRUCT<`x` INT64>" || !add.ifExists || add.annotation.description != "bd" || !add.annotation.fields["x"].required || add.annotation.fields["x"].description != "xd" {
		t.Fatalf("ADD COLUMN: got %+v", add)
	}
	if drop.kind != alterDropColumn || drop.name != "c" || !drop.ifExists {
		t.Fatalf("DROP COLUMN: got %+v", drop)
	}
	if rename.kind != alterRenameColumn || rename.name != "d" || rename.newName != "e" {
		t.Fatalf("RENAME COLUMN: got %+v", rename)
	}
	stmt, ok, err = parseAlterTable("ALTER TABLE IF EXISTS `p.ds.t` RENAME TO t2")
	if err != nil || !ok || !stmt.ifExists || stmt.actions[0].kind != alterRenameTo || !reflect.DeepEqual(stmt.actions[0].newPath, []string{"t2"}) {
		t.Fatalf("RENAME TO: got %+v, %v, %v", stmt, ok, err)
	}
	if _, _, err := parseAlterTable("ALTER TABLE ds.t ADD COLUMN c STRING DEFAULT 'x'"); err == nil || !strings.Contains(err.Error(), "DEFAULT is not supported") {
		t.Fatalf("ADD COLUMN DEFAULT: got %v", err)
	}
}

func TestPartitionColumnTypeOK(t *testing.T) {
	for _, c := range []struct {
		spec    *partitionSpec
		typ     string
		mode    string
		allowed bool
	}{
		{&partitionSpec{kind: "time", function: ""}, "DATE", "", true},
		{&partitionSpec{kind: "time", function: ""}, "INTEGER", "", false},
		{&partitionSpec{kind: "time", function: ""}, "TIMESTAMP", "", false},
		{&partitionSpec{kind: "time", function: "DATE"}, "TIMESTAMP", "", true},
		{&partitionSpec{kind: "time", function: "DATE"}, "DATETIME", "", true},
		{&partitionSpec{kind: "time", function: "DATE"}, "DATE", "", false},
		{&partitionSpec{kind: "time", function: "TIMESTAMP_TRUNC"}, "TIMESTAMP", "", true},
		{&partitionSpec{kind: "time", function: "TIMESTAMP_TRUNC"}, "DATETIME", "", false},
		{&partitionSpec{kind: "time", function: "DATETIME_TRUNC"}, "DATETIME", "", true},
		{&partitionSpec{kind: "time", function: "DATE_TRUNC"}, "DATE", "", true},
		{&partitionSpec{kind: "time", function: "DATE_TRUNC"}, "TIMESTAMP", "", false},
		{&partitionSpec{kind: "time", function: ""}, "DATE", "REPEATED", false},
		{&partitionSpec{kind: "range"}, "INTEGER", "", true},
		{&partitionSpec{kind: "range"}, "STRING", "", false},
	} {
		field := &bigqueryv2.TableFieldSchema{Name: "c", Type: c.typ, Mode: c.mode}
		if got := partitionColumnTypeOK(c.spec, field); got != c.allowed {
			t.Errorf("%+v on %s %s: got %v, want %v", c.spec, c.typ, c.mode, got, c.allowed)
		}
	}
}
