package server_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
)

// execIn runs sql with defaultDataset as the default dataset through the API
// named by mode.
func (e *ddlTestEnv) execIn(t *testing.T, mode, defaultDataset, sql string) error {
	t.Helper()
	q := e.client.Query(sql)
	q.DefaultDatasetID = defaultDataset
	if mode == "jobs.insert" {
		job, err := q.Run(e.ctx)
		if err != nil {
			return err
		}
		status, err := job.Wait(e.ctx)
		if err != nil {
			return err
		}
		return status.Err()
	}
	it, err := q.Read(e.ctx)
	if err != nil {
		return err
	}
	var row []bigquery.Value
	for {
		if err := it.Next(&row); err != nil {
			if err == iterator.Done {
				return nil
			}
			return err
		}
	}
}

// queryError runs sql through raw REST on the API named by mode and returns
// the HTTP status and the error's reason and message. For jobs.insert the
// error is the job's status.errorResult.
func (e *ddlTestEnv) queryError(t *testing.T, mode, sql string) (int, string, string) {
	t.Helper()
	var url, body string
	if mode == "jobs.insert" {
		url = e.url + "/bigquery/v2/projects/test/jobs"
		body = fmt.Sprintf(`{"jobReference":{"projectId":"test","jobId":"job_%d"},"configuration":{"query":{"query":%q,"useLegacySql":false}}}`, time.Now().UnixNano(), sql)
	} else {
		url = e.url + "/bigquery/v2/projects/test/queries"
		body = fmt.Sprintf(`{"query":%q,"useLegacySql":false}`, sql)
	}
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	type errorProto struct {
		Reason  string `json:"reason"`
		Message string `json:"message"`
	}
	var res struct {
		Error *struct {
			Errors []errorProto `json:"errors"`
		} `json:"error"`
		Status *struct {
			ErrorResult *errorProto `json:"errorResult"`
		} `json:"status"`
	}
	if err := json.Unmarshal(b, &res); err != nil {
		t.Fatalf("decode %s: %v", b, err)
	}
	switch {
	case res.Error != nil && len(res.Error.Errors) != 0:
		return resp.StatusCode, res.Error.Errors[0].Reason, res.Error.Errors[0].Message
	case res.Status != nil && res.Status.ErrorResult != nil:
		return resp.StatusCode, res.Status.ErrorResult.Reason, res.Status.ErrorResult.Message
	}
	return resp.StatusCode, "", ""
}

// TestDDLMetadata checks the metadata of tables created and altered by DDL.
// All cases share one server, since every server allocates its own
// googlesqlite catalog in the googlesql wasm module, whose memory is limited
// for the whole test binary. Each mode works in its own dataset.
func TestDDLMetadata(t *testing.T) {
	env := newDDLTestEnv(t)
	datasets := map[string]string{"jobs.insert": "dataset1", "jobs.query": "dataset2"}
	for _, c := range []struct {
		name string
		test func(t *testing.T, env *ddlTestEnv, mode, ds string)
	}{
		{"create_table_partitioning", testCreateTablePartitioningMetadata},
		{"create_table_partitioning_types", testCreateTablePartitioningTypes},
		{"create_table_clustering", testCreateTableClusteringMetadata},
		{"create_table_options", testCreateTableOptionsMetadata},
		{"create_table_column", testCreateTableColumnMetadata},
		{"create_table_primary_key", testCreateTablePrimaryKeyMetadata},
		{"create_table_as_select", testCreateTableAsSelectMetadata},
		{"create_or_replace_resets", testCreateOrReplaceTableResetsMetadata},
		{"create_if_not_exists_keeps", testCreateTableIfNotExistsKeepsMetadata},
		{"create_in_script", testCreateTableMetadataInScript},
		{"create_requires_dataset", testCreateTableRequiresDataset},
		{"not_null_violation", testNotNullViolationMessage},
		{"alter_in_script_rejected", testAlterTableInScriptRejected},
		{"alter_set_options", testAlterTableSetOptions},
		{"alter_column_actions", testAlterColumnActions},
		{"alter_primary_key", testAlterTablePrimaryKey},
		{"alter_rebuild_keeps_constraints", testAlterRebuildKeepsConstraints},
		{"create_table_like_copies", testCreateTableLikeCopies},
		{"create_table_like_overrides", testCreateTableLikeOverrides},
		{"create_table_like_as_query", testCreateTableLikeAsQuery},
		{"create_table_like_or_replace", testCreateTableLikeOrReplace},
		{"create_table_like_if_not_exists", testCreateTableLikeIfNotExists},
	} {
		for _, mode := range ddlModes {
			t.Run(c.name+"/"+mode, func(t *testing.T) {
				c.test(t, env, mode, datasets[mode])
			})
		}
	}
	for _, c := range []struct {
		name string
		test func(t *testing.T, env *ddlTestEnv)
	}{
		{"create_already_exists", testCreateTableAlreadyExists},
		{"table_not_found_suggestion", testTableNotFoundSuggestion},
		{"alter_rebuild_of_api_table", testAlterRebuildOfAPITable},
		{"alter_key_columns_of_ddl_table", testAlterKeyColumnsOfDDLTable},
		{"create_table_like_errors", testCreateTableLikeErrors},
		{"create_table_like_api_table", testCreateTableLikeOfAPITable},
		{"rewrite_failing_after_run", testRewriteFailingAfterRun},
	} {
		t.Run(c.name, func(t *testing.T) {
			c.test(t, env)
		})
	}
}

func testCreateTablePartitioningMetadata(t *testing.T, env *ddlTestEnv, mode, ds string) {
	for _, c := range []struct {
		table, ddl string
		typ        bigquery.TimePartitioningType
		field      string
	}{
		{"p1", "(id INT64, d DATE) PARTITION BY d", bigquery.DayPartitioningType, "d"},
		{"p2", "(id INT64, ts TIMESTAMP) PARTITION BY DATE(ts)", bigquery.DayPartitioningType, "ts"},
		{"p3", "(id INT64, ts TIMESTAMP) PARTITION BY TIMESTAMP_TRUNC(ts, MONTH)", bigquery.MonthPartitioningType, "ts"},
		{"p4", "(id INT64, dt DATETIME) PARTITION BY DATETIME_TRUNC(dt, YEAR)", bigquery.YearPartitioningType, "dt"},
		{"p5", "(id INT64) PARTITION BY _PARTITIONDATE", bigquery.DayPartitioningType, ""},
		{"p6", "(id INT64) PARTITION BY DATE(_PARTITIONTIME)", bigquery.DayPartitioningType, ""},
		{"p7", "(id INT64, TS TIMESTAMP) PARTITION BY TIMESTAMP_TRUNC(ts, HOUR)", bigquery.HourPartitioningType, "TS"},
	} {
		env.mustExec(t, mode, "CREATE TABLE "+ds+"."+c.table+" "+c.ddl)
		md := env.metadata(t, ds, c.table)
		if md.TimePartitioning == nil || md.TimePartitioning.Type != c.typ || md.TimePartitioning.Field != c.field {
			t.Fatalf("%s: timePartitioning: got %+v, want %s %q", c.table, md.TimePartitioning, c.typ, c.field)
		}
		if md.RangePartitioning != nil {
			t.Fatalf("%s: rangePartitioning: got %+v", c.table, md.RangePartitioning)
		}
	}

	env.mustExec(t, mode, "CREATE TABLE "+ds+".r1 (id INT64) PARTITION BY RANGE_BUCKET(id, GENERATE_ARRAY(-10, 100, 5))")
	md := env.metadata(t, ds, "r1")
	if rp := md.RangePartitioning; rp == nil || rp.Field != "id" || rp.Range == nil || rp.Range.Start != -10 || rp.Range.End != 100 || rp.Range.Interval != 5 {
		t.Fatalf("rangePartitioning: got %+v", md.RangePartitioning)
	}
	if md.TimePartitioning != nil {
		t.Fatalf("timePartitioning: got %+v", md.TimePartitioning)
	}

	env.mustExec(t, mode, "CREATE TABLE "+ds+".pe (ts TIMESTAMP) PARTITION BY DATE(ts) OPTIONS(partition_expiration_days=3, require_partition_filter=true)")
	md = env.metadata(t, ds, "pe")
	if md.TimePartitioning == nil || md.TimePartitioning.Expiration != 72*time.Hour {
		t.Fatalf("timePartitioning: got %+v, want an expiration of 72h", md.TimePartitioning)
	}
	if !md.RequirePartitionFilter {
		t.Fatal("requirePartitionFilter: got false")
	}
}

// testCreateTablePartitioningTypes checks that a PARTITION BY that does not
// apply to the type of its column is not recorded.
func testCreateTablePartitioningTypes(t *testing.T, env *ddlTestEnv, mode, ds string) {
	for table, ddl := range map[string]string{
		"pt1": "(x INT64) PARTITION BY x",
		"pt2": "(d DATE) PARTITION BY DATE_TRUNC(d, HOUR)",
		"pt3": "(ts TIMESTAMP) PARTITION BY DATETIME_TRUNC(ts, DAY)",
		"pt4": "(d DATE) PARTITION BY DATE(d)",
		"pt5": "(s STRING) PARTITION BY RANGE_BUCKET(s, GENERATE_ARRAY(0, 10))",
	} {
		// googlesqlite accepts some of these; when it does, nothing is
		// recorded.
		if err := env.exec(t, mode, "CREATE TABLE "+ds+"."+table+" "+ddl); err != nil {
			continue
		}
		md := env.metadata(t, ds, table)
		if md.TimePartitioning != nil || md.RangePartitioning != nil {
			t.Fatalf("%s: got timePartitioning %+v, rangePartitioning %+v", table, md.TimePartitioning, md.RangePartitioning)
		}
	}
	env.mustExec(t, mode, "CREATE TABLE "+ds+".pt6 (a INT64) PARTITION BY DATE_TRUNC(_PARTITIONDATE, MONTH)")
	if md := env.metadata(t, ds, "pt6"); md.TimePartitioning == nil || md.TimePartitioning.Type != bigquery.MonthPartitioningType || md.TimePartitioning.Field != "" {
		t.Fatalf("pt6: got %+v", md.TimePartitioning)
	}
}

func testCreateTableClusteringMetadata(t *testing.T, env *ddlTestEnv, mode, ds string) {
	env.mustExec(t, mode, "CREATE TABLE "+ds+".cl (id INT64, name STRING, ts TIMESTAMP) PARTITION BY DATE(ts) CLUSTER BY name, id")
	md := env.metadata(t, ds, "cl")
	if md.Clustering == nil || !reflect.DeepEqual(md.Clustering.Fields, []string{"name", "id"}) {
		t.Fatalf("clustering: got %+v", md.Clustering)
	}
	if md.TimePartitioning == nil || md.TimePartitioning.Field != "ts" {
		t.Fatalf("timePartitioning: got %+v", md.TimePartitioning)
	}
}

func testCreateTableOptionsMetadata(t *testing.T, env *ddlTestEnv, mode, ds string) {
	env.mustExec(t, mode, "CREATE TABLE "+ds+".o1 (a INT64) OPTIONS(description='tbl', friendly_name='One', labels=[('env','dev'), ('team','x')], expiration_timestamp=TIMESTAMP '2030-01-01 00:00:00 UTC')")
	md := env.metadata(t, ds, "o1")
	if md.Description != "tbl" || md.Name != "One" || !reflect.DeepEqual(md.Labels, map[string]string{"env": "dev", "team": "x"}) {
		t.Fatalf("got description %q, friendlyName %q, labels %v", md.Description, md.Name, md.Labels)
	}
	if want := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC); !md.ExpirationTime.Equal(want) {
		t.Fatalf("expirationTime: got %v, want %v", md.ExpirationTime, want)
	}

	env.mustExec(t, mode, "CREATE TABLE "+ds+".o2 (a INT64) OPTIONS(expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY))")
	md = env.metadata(t, ds, "o2")
	want := time.Now().Add(24 * time.Hour)
	if d := md.ExpirationTime.Sub(want); d < -5*time.Minute || d > 5*time.Minute {
		t.Fatalf("expirationTime: got %v, want about %v", md.ExpirationTime, want)
	}
}

func testCreateTableColumnMetadata(t *testing.T, env *ddlTestEnv, mode, ds string) {
	env.mustExec(t, mode, "CREATE TABLE "+ds+".cm (a INT64 NOT NULL OPTIONS(description='col a'), b STRING DEFAULT 'x', ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP(), arr ARRAY<INT64> NOT NULL)")
	md := env.metadata(t, ds, "cm")
	if len(md.Schema) != 4 {
		t.Fatalf("schema: got %+v", md.Schema)
	}
	if a := md.Schema[0]; !a.Required || a.Description != "col a" || a.DefaultValueExpression != "" {
		t.Fatalf("a: got %+v", a)
	}
	if b := md.Schema[1]; b.Required || b.DefaultValueExpression != "'x'" {
		t.Fatalf("b: got %+v", b)
	}
	if ts := md.Schema[2]; ts.DefaultValueExpression != "CURRENT_TIMESTAMP()" {
		t.Fatalf("ts: got %+v", ts)
	}
	if arr := md.Schema[3]; !arr.Repeated || arr.Required {
		t.Fatalf("arr: got %+v", arr)
	}
	env.mustExec(t, mode, "INSERT INTO "+ds+".cm (a, arr) VALUES (1, [])")
	if rows := env.mustRead(t, "SELECT b, ts IS NOT NULL FROM "+ds+".cm"); len(rows) != 1 || rows[0][0] != "x" || rows[0][1] != true {
		t.Fatalf("rows: got %v", rows)
	}
}

func testCreateTablePrimaryKeyMetadata(t *testing.T, env *ddlTestEnv, mode, ds string) {
	env.mustExec(t, mode, "CREATE TABLE "+ds+".k1 (id INT64, b INT64, PRIMARY KEY (id, b) NOT ENFORCED)")
	env.mustExec(t, mode, "CREATE TABLE "+ds+".k2 (id INT64 PRIMARY KEY NOT ENFORCED, b STRING)")
	for table, want := range map[string][]string{"k1": {"id", "b"}, "k2": {"id"}} {
		md := env.metadata(t, ds, table)
		if md.TableConstraints == nil || md.TableConstraints.PrimaryKey == nil || !reflect.DeepEqual(md.TableConstraints.PrimaryKey.Columns, want) {
			t.Fatalf("%s: tableConstraints: got %+v, want %v", table, md.TableConstraints, want)
		}
	}
}

func testCreateTableAsSelectMetadata(t *testing.T, env *ddlTestEnv, mode, ds string) {
	env.mustExec(t, mode, "CREATE TABLE "+ds+".c1 PARTITION BY DATE(ts) CLUSTER BY name OPTIONS(description='d', labels=[('k','v')]) AS SELECT 1 AS id, 'a' AS name, TIMESTAMP '2024-01-01' AS ts")
	md := env.metadata(t, ds, "c1")
	if md.Description != "d" || md.Labels["k"] != "v" {
		t.Fatalf("description %q, labels %v", md.Description, md.Labels)
	}
	if md.TimePartitioning == nil || md.TimePartitioning.Field != "ts" || md.Clustering == nil || !reflect.DeepEqual(md.Clustering.Fields, []string{"name"}) {
		t.Fatalf("partitioning %+v, clustering %+v", md.TimePartitioning, md.Clustering)
	}

	env.mustExec(t, mode, "CREATE TABLE "+ds+".c2 (id INT64 NOT NULL OPTIONS(description='i'), name STRING) AS SELECT 1, 'a'")
	md = env.metadata(t, ds, "c2")
	if len(md.Schema) != 2 || !md.Schema[0].Required || md.Schema[0].Description != "i" || md.Schema[1].Required {
		t.Fatalf("schema: got %+v", md.Schema)
	}
}

func testCreateOrReplaceTableResetsMetadata(t *testing.T, env *ddlTestEnv, mode, ds string) {
	env.mustExec(t, mode, "CREATE TABLE "+ds+".r (a INT64) PARTITION BY _PARTITIONDATE OPTIONS(description='old', labels=[('k','v')])")
	env.mustExec(t, mode, "CREATE OR REPLACE TABLE "+ds+".r (a INT64)")
	md := env.metadata(t, ds, "r")
	if md.Description != "" || len(md.Labels) != 0 || md.TimePartitioning != nil {
		t.Fatalf("got description %q, labels %v, partitioning %+v", md.Description, md.Labels, md.TimePartitioning)
	}
	env.mustExec(t, mode, "CREATE OR REPLACE TABLE "+ds+".r (a INT64) OPTIONS(description='new')")
	if md := env.metadata(t, ds, "r"); md.Description != "new" {
		t.Fatalf("description: got %q", md.Description)
	}
}

func testCreateTableIfNotExistsKeepsMetadata(t *testing.T, env *ddlTestEnv, mode, ds string) {
	env.mustExec(t, mode, "CREATE TABLE "+ds+".ine (a INT64, ts TIMESTAMP) PARTITION BY DATE(ts) OPTIONS(description='keep')")
	env.mustExec(t, mode, "CREATE TABLE IF NOT EXISTS "+ds+".ine (z BOOL) PARTITION BY _PARTITIONDATE OPTIONS(description='new')")
	md := env.metadata(t, ds, "ine")
	assertNames(t, "schema", env.tableSchemaNames(t, ds, "ine"), "a", "ts")
	if md.Description != "keep" || md.TimePartitioning == nil || md.TimePartitioning.Field != "ts" {
		t.Fatalf("got description %q, partitioning %+v", md.Description, md.TimePartitioning)
	}
	env.mustExec(t, mode, "CREATE TABLE IF NOT EXISTS "+ds+".ine2 (z BOOL) OPTIONS(description='new')")
	if md := env.metadata(t, ds, "ine2"); md.Description != "new" {
		t.Fatalf("ine2 description: got %q", md.Description)
	}
}

func testCreateTableMetadataInScript(t *testing.T, env *ddlTestEnv, mode, ds string) {
	env.mustExec(t, mode, "CREATE TABLE "+ds+".s1 (a INT64 OPTIONS(description='v')) OPTIONS(description='sc'); INSERT INTO "+ds+".s1 (a) VALUES (1); SELECT * FROM "+ds+".s1")
	md := env.metadata(t, ds, "s1")
	if md.Description != "sc" || len(md.Schema) != 1 || md.Schema[0].Description != "v" {
		t.Fatalf("s1: got description %q, schema %+v", md.Description, md.Schema)
	}

	env.mustExec(t, mode, "CREATE TABLE "+ds+".s2 (b STRING)")
	env.mustExec(t, mode, "DROP TABLE IF EXISTS "+ds+".s2; CREATE TABLE "+ds+".s2 (a INT64 NOT NULL) CLUSTER BY a")
	md = env.metadata(t, ds, "s2")
	if len(md.Schema) != 1 || !md.Schema[0].Required || md.Clustering == nil || !reflect.DeepEqual(md.Clustering.Fields, []string{"a"}) {
		t.Fatalf("s2: got schema %+v, clustering %+v", md.Schema, md.Clustering)
	}

	assertErrContains(t, env.exec(t, mode, "CREATE TABLE "+ds+".s3 (a INT64) OPTIONS(description='x'); SELECT ERROR('boom')"), "boom")
	if md := env.metadata(t, ds, "s3"); md.Description != "x" {
		t.Fatalf("s3 description: got %q", md.Description)
	}
}

func testCreateTableAlreadyExists(t *testing.T, env *ddlTestEnv) {
	env.mustExec(t, "jobs.insert", "CREATE TABLE dataset1.dup (a INT64)")
	env.mustExec(t, "jobs.insert", "CREATE VIEW dataset1.dupv AS SELECT 1 AS a")
	for _, c := range []struct {
		sql, table string
	}{
		{"CREATE TABLE dataset1.dup (a INT64)", "dup"},
		{"CREATE VIEW dataset1.dupv AS SELECT 2 AS a", "dupv"},
		{"SELECT 1; CREATE TABLE dataset1.dup (b STRING)", "dup"},
	} {
		want := "Already Exists: Table test:dataset1." + c.table
		code, reason, msg := env.queryError(t, "jobs.query", c.sql)
		if code != http.StatusConflict || reason != "duplicate" || msg != want {
			t.Fatalf("jobs.query %s: got HTTP %d %q %q", c.sql, code, reason, msg)
		}
		code, reason, msg = env.queryError(t, "jobs.insert", c.sql)
		if code != http.StatusOK || reason != "duplicate" || msg != want {
			t.Fatalf("jobs.insert %s: got HTTP %d %q %q", c.sql, code, reason, msg)
		}
	}

	_, err := env.readQuery(t, "CREATE TABLE dataset1.dup (a INT64)")
	var gerr *googleapi.Error
	if !errors.As(err, &gerr) || gerr.Code != http.StatusConflict {
		t.Fatalf("Read: want a 409, got %v", err)
	}
	// The Go client reads the error of a job from jobs.getQueryResults.
	err = env.runQueryJob(t, "CREATE TABLE dataset1.dup (a INT64)")
	if !errors.As(err, &gerr) || gerr.Code != http.StatusConflict || len(gerr.Errors) != 1 || gerr.Errors[0].Reason != "duplicate" || gerr.Errors[0].Message != "Already Exists: Table test:dataset1.dup" {
		t.Fatalf("Run: want a 409 duplicate error, got %#v", err)
	}
	assertNames(t, "schema", env.tableSchemaNames(t, "dataset1", "dup"), "a")
}

func testCreateTableRequiresDataset(t *testing.T, env *ddlTestEnv, mode, ds string) {
	const qualify = `Table "nodataset" must be qualified with a dataset (e.g. dataset.table).`
	assertErrContains(t, env.execIn(t, mode, "", "CREATE TABLE nodataset (a INT64)"), qualify)
	assertErrContains(t, env.execIn(t, mode, "", "CREATE VIEW nodataset AS SELECT 1 AS a"), qualify)
	// Nothing was created: the name is still free.
	for _, id := range env.tableIDs(t, ds) {
		if id == "nodataset" {
			t.Fatalf("nodataset was created in %s", ds)
		}
	}
	env.mustExec(t, mode, "CREATE TABLE "+ds+".nodataset (b STRING)")
	assertNames(t, "nodataset", env.tableSchemaNames(t, ds, "nodataset"), "b")

	if err := env.execIn(t, mode, ds, "CREATE TABLE uq (a INT64) OPTIONS(description='u')"); err != nil {
		t.Fatal(err)
	}
	if md := env.metadata(t, ds, "uq"); md.Description != "u" {
		t.Fatalf("uq description: got %q", md.Description)
	}

	assertErrContains(t, env.exec(t, mode, "CREATE TABLE nods.t (a INT64)"), "Not found: Dataset test:nods")
	code, reason, msg := env.queryError(t, mode, "CREATE TABLE nods.t (a INT64)")
	wantCode := http.StatusOK
	if mode == "jobs.query" {
		wantCode = http.StatusNotFound
	}
	if code != wantCode || reason != "notFound" || msg != "Not found: Dataset test:nods" {
		t.Fatalf("got HTTP %d %q %q", code, reason, msg)
	}
	// TEMP tables belong to no dataset.
	if err := env.execIn(t, mode, "", "CREATE TEMP TABLE tt AS SELECT 1 AS a; SELECT a FROM tt"); err != nil {
		t.Fatal(err)
	}
}

func testNotNullViolationMessage(t *testing.T, env *ddlTestEnv, mode, ds string) {
	env.mustExec(t, mode, "CREATE TABLE "+ds+".nn (a INT64 NOT NULL, b STRING)")
	env.mustExec(t, mode, "INSERT INTO "+ds+".nn (a, b) VALUES (1, 'x')")
	for _, sql := range []string{
		"INSERT INTO " + ds + ".nn (a, b) VALUES (NULL, 'y')",
		"INSERT INTO " + ds + ".nn (b) VALUES ('y')",
		"UPDATE " + ds + ".nn SET a = NULL WHERE TRUE",
	} {
		err := env.exec(t, mode, sql)
		assertErrContains(t, err, "Required field a cannot be null")
		if strings.Contains(err.Error(), "test_"+ds+"_") || strings.Contains(err.Error(), "failed to exec") {
			t.Fatalf("%s: error exposes internals: %v", sql, err)
		}
	}
}

func testTableNotFoundSuggestion(t *testing.T, env *ddlTestEnv) {
	env.mustExec(t, "jobs.insert", "CREATE TABLE dataset1.sugg (a INT64)")
	_, err := env.readQuery(t, "SELECT * FROM dataset1.suggx")
	if err == nil {
		t.Fatal("expected an error")
	}
	if strings.Contains(err.Error(), "test_dataset1_") {
		t.Fatalf("error exposes the internal table name: %v", err)
	}
	if strings.Contains(err.Error(), "Did you mean") && !strings.Contains(err.Error(), "Did you mean dataset1.sugg?") {
		t.Fatalf("unexpected suggestion: %v", err)
	}
}

func testAlterTableInScriptRejected(t *testing.T, env *ddlTestEnv, mode, ds string) {
	const want = "ALTER TABLE inside a multi-statement script is not supported by the emulator"
	env.mustExec(t, mode, "CREATE TABLE "+ds+".sc (a INT64)")
	for _, sql := range []string{
		"ALTER TABLE " + ds + ".sc ADD COLUMN b STRING; SELECT 1",
		"SELECT 1; ALTER TABLE " + ds + ".sc ADD COLUMN b STRING",
		"BEGIN ALTER TABLE " + ds + ".sc ADD COLUMN b STRING; END",
		"IF TRUE THEN ALTER TABLE " + ds + ".sc ADD COLUMN b STRING; END IF",
	} {
		assertErrContains(t, env.exec(t, mode, sql), want)
		assertNames(t, "schema", env.tableSchemaNames(t, ds, "sc"), "a")
	}
	env.mustExec(t, mode, "ALTER TABLE "+ds+".sc ADD COLUMN b STRING;")
	assertNames(t, "schema", env.tableSchemaNames(t, ds, "sc"), "a", "b")
}

func testAlterTableSetOptions(t *testing.T, env *ddlTestEnv, mode, ds string) {
	env.mustExec(t, mode, "CREATE TABLE "+ds+".so (a INT64, ts TIMESTAMP) PARTITION BY DATE(ts)")
	env.mustExec(t, mode, "INSERT INTO "+ds+".so (a) VALUES (1)")
	env.mustExec(t, mode, "ALTER TABLE "+ds+".so SET OPTIONS(description='d', labels=[('k','v')], friendly_name='F', expiration_timestamp=TIMESTAMP '2031-01-01 00:00:00 UTC', require_partition_filter=true, partition_expiration_days=2)")
	md := env.metadata(t, ds, "so")
	if md.Description != "d" || md.Labels["k"] != "v" || md.Name != "F" || !md.RequirePartitionFilter ||
		!md.ExpirationTime.Equal(time.Date(2031, 1, 1, 0, 0, 0, 0, time.UTC)) ||
		md.TimePartitioning == nil || md.TimePartitioning.Expiration != 48*time.Hour {
		t.Fatalf("got %+v", md)
	}

	env.mustExec(t, mode, "ALTER TABLE "+ds+".so SET OPTIONS(description=NULL, labels=NULL)")
	md = env.metadata(t, ds, "so")
	if md.Description != "" || len(md.Labels) != 0 || md.Name != "F" {
		t.Fatalf("after NULL: description %q, labels %v, friendlyName %q", md.Description, md.Labels, md.Name)
	}

	env.mustExec(t, mode, "ALTER TABLE "+ds+".so SET OPTIONS(expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY))")
	md = env.metadata(t, ds, "so")
	if d := md.ExpirationTime.Sub(time.Now().Add(24 * time.Hour)); d < -5*time.Minute || d > 5*time.Minute {
		t.Fatalf("expirationTime: got %v", md.ExpirationTime)
	}

	assertErrContains(t, env.exec(t, mode, "ALTER TABLE "+ds+".so SET OPTIONS(kms_key_name='x', description='no')"), "option kms_key_name is not supported")
	if md := env.metadata(t, ds, "so"); md.Description != "" {
		t.Fatalf("description changed by a failed ALTER: %q", md.Description)
	}

	env.mustExec(t, mode, "ALTER TABLE "+ds+".so ADD COLUMN x STRING, SET OPTIONS(description='t')")
	md = env.metadata(t, ds, "so")
	if md.Description != "t" || len(md.Schema) != 3 || md.Schema[2].Name != "x" {
		t.Fatalf("combined: description %q, schema %+v", md.Description, md.Schema)
	}
	if rows := env.mustRead(t, "SELECT a FROM "+ds+".so"); len(rows) != 1 || rows[0][0] != int64(1) {
		t.Fatalf("rows: got %v", rows)
	}

	assertErrContains(t, env.exec(t, mode, "ALTER TABLE "+ds+".so ADD COLUMN y STRING, SET OPTIONS(description='u'), DROP CONSTRAINT c"), "ALTER TABLE DROP CONSTRAINT is not supported")
	md = env.metadata(t, ds, "so")
	if md.Description != "t" || len(md.Schema) != 3 {
		t.Fatalf("after failed ALTER: description %q, schema %+v", md.Description, md.Schema)
	}
	if rows := env.mustRead(t, "SELECT * FROM "+ds+".so"); len(rows) != 1 || len(rows[0]) != 3 {
		t.Fatalf("rows: got %v", rows)
	}
}

func testAlterColumnActions(t *testing.T, env *ddlTestEnv, mode, ds string) {
	env.mustExec(t, mode, "CREATE TABLE "+ds+".ac (a INT64 NOT NULL, b STRING, n INT64, s STRING)")
	env.mustExec(t, mode, "INSERT INTO "+ds+".ac (a, b, n, s) VALUES (1, 'x', 7, 's')")

	env.mustExec(t, mode, "ALTER TABLE "+ds+".ac ALTER COLUMN a SET OPTIONS(description='d')")
	if a := env.metadata(t, ds, "ac").Schema[0]; a.Description != "d" || !a.Required {
		t.Fatalf("a: got %+v", a)
	}

	env.mustExec(t, mode, "ALTER TABLE "+ds+".ac ALTER COLUMN a DROP NOT NULL")
	if a := env.metadata(t, ds, "ac").Schema[0]; a.Required || a.Description != "d" {
		t.Fatalf("a after DROP NOT NULL: got %+v", a)
	}
	env.mustExec(t, mode, "INSERT INTO "+ds+".ac (b) VALUES ('y')")

	env.mustExec(t, mode, "ALTER TABLE "+ds+".ac ALTER COLUMN n SET DATA TYPE NUMERIC")
	if n := env.metadata(t, ds, "ac").Schema[2]; n.Type != bigquery.NumericFieldType {
		t.Fatalf("n: got %+v", n)
	}
	if rows := env.mustRead(t, "SELECT CAST(n AS STRING) FROM "+ds+".ac WHERE a = 1"); len(rows) != 1 || rows[0][0] != "7" {
		t.Fatalf("n rows: got %v", rows)
	}
	assertErrContains(t, env.exec(t, mode, "ALTER TABLE "+ds+".ac ALTER COLUMN s SET DATA TYPE INT64"), "ALTER TABLE ALTER COLUMN SET DATA TYPE requires that the existing column type (STRING) is assignable to the new type (INT64)")

	env.mustExec(t, mode, "ALTER TABLE "+ds+".ac ALTER COLUMN b SET DEFAULT 'z'")
	if b := env.metadata(t, ds, "ac").Schema[1]; b.DefaultValueExpression != "'z'" {
		t.Fatalf("b: got %+v", b)
	}
	env.mustExec(t, mode, "INSERT INTO "+ds+".ac (a) VALUES (2)")
	if rows := env.mustRead(t, "SELECT b FROM "+ds+".ac WHERE a = 2"); len(rows) != 1 || rows[0][0] != "z" {
		t.Fatalf("default not applied: got %v", rows)
	}
	env.mustExec(t, mode, "ALTER TABLE "+ds+".ac ALTER COLUMN b DROP DEFAULT")
	if b := env.metadata(t, ds, "ac").Schema[1]; b.DefaultValueExpression != "" {
		t.Fatalf("b after DROP DEFAULT: got %+v", b)
	}
	env.mustExec(t, mode, "INSERT INTO "+ds+".ac (a) VALUES (3)")
	if rows := env.mustRead(t, "SELECT b FROM "+ds+".ac WHERE a = 3"); len(rows) != 1 || rows[0][0] != nil {
		t.Fatalf("default still applied: got %v", rows)
	}

	env.mustExec(t, mode, "ALTER TABLE "+ds+".ac ALTER COLUMN IF EXISTS nope SET OPTIONS(description='x')")
	assertErrContains(t, env.exec(t, mode, "ALTER TABLE "+ds+".ac ALTER COLUMN nope DROP NOT NULL"), "Column not found: nope")
	if rows := env.mustRead(t, "SELECT * FROM "+ds+".ac"); len(rows) != 4 {
		t.Fatalf("rows: got %v", rows)
	}
}

func testAlterTablePrimaryKey(t *testing.T, env *ddlTestEnv, mode, ds string) {
	env.mustExec(t, mode, "CREATE TABLE "+ds+".pk (id INT64, ref INT64, v STRING)")
	primaryKey := func() []string {
		md := env.metadata(t, ds, "pk")
		if md.TableConstraints == nil || md.TableConstraints.PrimaryKey == nil {
			return nil
		}
		return md.TableConstraints.PrimaryKey.Columns
	}
	env.mustExec(t, mode, "ALTER TABLE "+ds+".pk ADD PRIMARY KEY (id) NOT ENFORCED")
	if got := primaryKey(); !reflect.DeepEqual(got, []string{"id"}) {
		t.Fatalf("primary key: got %v", got)
	}
	assertErrContains(t, env.exec(t, mode, "ALTER TABLE "+ds+".pk ADD PRIMARY KEY (ref) NOT ENFORCED"), "already has a primary key")
	assertErrContains(t, env.exec(t, mode, "ALTER TABLE "+ds+".pk DROP COLUMN id"), "part of the primary key")
	env.mustExec(t, mode, "ALTER TABLE "+ds+".pk RENAME COLUMN id TO pid")
	if got := primaryKey(); !reflect.DeepEqual(got, []string{"pid"}) {
		t.Fatalf("primary key after rename: got %v", got)
	}
	env.mustExec(t, mode, "ALTER TABLE "+ds+".pk DROP PRIMARY KEY")
	if got := primaryKey(); got != nil {
		t.Fatalf("primary key after DROP: got %v", got)
	}
	assertErrContains(t, env.exec(t, mode, "ALTER TABLE "+ds+".pk DROP PRIMARY KEY"), "does not have a primary key")
	env.mustExec(t, mode, "ALTER TABLE "+ds+".pk DROP PRIMARY KEY IF EXISTS")
	assertErrContains(t, env.exec(t, mode, "ALTER TABLE "+ds+".pk ADD PRIMARY KEY (nope) NOT ENFORCED"), "Column not found: nope")
	assertErrContains(t, env.exec(t, mode, "ALTER TABLE "+ds+".pk ADD CONSTRAINT fk FOREIGN KEY (ref) REFERENCES "+ds+".pk(pid) NOT ENFORCED"), "ALTER TABLE ADD CONSTRAINT is not supported by the emulator")
}

func testAlterRebuildKeepsConstraints(t *testing.T, env *ddlTestEnv, mode, ds string) {
	env.mustExec(t, mode, "CREATE TABLE "+ds+".rk (a INT64 NOT NULL, b STRING DEFAULT 'q')")
	env.mustExec(t, mode, "INSERT INTO "+ds+".rk (a) VALUES (0)")
	env.mustExec(t, mode, "ALTER TABLE "+ds+".rk ADD COLUMN c INT64")
	env.mustExec(t, mode, "INSERT INTO "+ds+".rk (a) VALUES (1)")
	if rows := env.mustRead(t, "SELECT b FROM "+ds+".rk ORDER BY a"); len(rows) != 2 || rows[0][0] != "q" || rows[1][0] != "q" {
		t.Fatalf("rows: got %v", rows)
	}
	assertErrContains(t, env.exec(t, mode, "INSERT INTO "+ds+".rk (c) VALUES (1)"), "Required field a cannot be null")
	md := env.metadata(t, ds, "rk")
	if len(md.Schema) != 3 || !md.Schema[0].Required || md.Schema[1].DefaultValueExpression != "'q'" {
		t.Fatalf("schema: got %+v", md.Schema)
	}

	// RENAME TO keeps them too.
	env.mustExec(t, mode, "ALTER TABLE "+ds+".rk RENAME TO rk2")
	env.mustExec(t, mode, "INSERT INTO "+ds+".rk2 (a) VALUES (2)")
	if rows := env.mustRead(t, "SELECT b FROM "+ds+".rk2 WHERE a = 2"); len(rows) != 1 || rows[0][0] != "q" {
		t.Fatalf("rows after rename: got %v", rows)
	}
	assertErrContains(t, env.exec(t, mode, "INSERT INTO "+ds+".rk2 (c) VALUES (1)"), "Required field a cannot be null")
}

// TestAlterRebuildOfAPITable checks that a rebuild does not make a table
// created by tables.insert stricter: its REQUIRED columns have no NOT NULL
// constraint in SQLite and may hold NULLs.
func testAlterRebuildOfAPITable(t *testing.T, env *ddlTestEnv) {
	if err := env.client.Dataset("dataset1").Table("api").Create(env.ctx, &bigquery.TableMetadata{
		Schema: bigquery.Schema{
			{Name: "a", Type: bigquery.IntegerFieldType, Required: true},
			{Name: "b", Type: bigquery.StringFieldType, DefaultValueExpression: "'d'"},
		},
	}); err != nil {
		t.Fatal(err)
	}
	env.mustExec(t, "jobs.insert", "INSERT INTO dataset1.api (b) VALUES ('x')")
	env.mustExec(t, "jobs.insert", "ALTER TABLE dataset1.api ADD COLUMN c STRING")
	env.mustExec(t, "jobs.insert", "INSERT INTO dataset1.api (b) VALUES ('y')")
	md := env.metadata(t, "dataset1", "api")
	if len(md.Schema) != 3 || !md.Schema[0].Required || md.Schema[1].DefaultValueExpression != "'d'" {
		t.Fatalf("schema: got %+v", md.Schema)
	}
	if rows := env.mustRead(t, "SELECT COUNT(*) FROM dataset1.api WHERE a IS NULL"); len(rows) != 1 || rows[0][0] != int64(2) {
		t.Fatalf("rows: got %v", rows)
	}
}

func testAlterKeyColumnsOfDDLTable(t *testing.T, env *ddlTestEnv) {
	env.mustExec(t, "jobs.insert", "CREATE TABLE dataset1.kc (ts TIMESTAMP, c STRING, n INT64) PARTITION BY DATE(ts) CLUSTER BY c")
	assertErrContains(t, env.runQueryJob(t, "ALTER TABLE dataset1.kc DROP COLUMN ts"), "partitioning column")
	assertErrContains(t, env.runQueryJob(t, "ALTER TABLE dataset1.kc RENAME COLUMN ts TO ts2"), "partitioning column")
	assertErrContains(t, env.runQueryJob(t, "ALTER TABLE dataset1.kc DROP COLUMN c"), "clustering column")
	assertErrContains(t, env.runQueryJob(t, "ALTER TABLE dataset1.kc RENAME COLUMN c TO c2"), "clustering column")
	env.mustExec(t, "jobs.insert", "ALTER TABLE dataset1.kc DROP COLUMN n")
	assertNames(t, "schema", env.tableSchemaNames(t, "dataset1", "kc"), "ts", "c")
}

func testCreateTableLikeCopies(t *testing.T, env *ddlTestEnv, mode, ds string) {
	env.mustExec(t, mode, "CREATE TABLE "+ds+".lsrc (id INT64 NOT NULL OPTIONS(description='i'), s STRING DEFAULT 'x', r STRUCT<a INT64, b ARRAY<STRING>>, arr ARRAY<INT64>, d DATE, PRIMARY KEY (id) NOT ENFORCED) "+
		"PARTITION BY d CLUSTER BY id OPTIONS(description='t', friendly_name='f', labels=[('k','v')], require_partition_filter=true, partition_expiration_days=2)")
	env.mustExec(t, mode, "INSERT INTO "+ds+".lsrc (id, d) VALUES (1, '2024-01-01')")
	env.mustExec(t, mode, "CREATE TABLE "+ds+".l1 LIKE "+ds+".lsrc")
	src, md := env.metadata(t, ds, "lsrc"), env.metadata(t, ds, "l1")
	if !reflect.DeepEqual(md.Schema, src.Schema) {
		t.Fatalf("schema: got %+v, want %+v", md.Schema, src.Schema)
	}
	if id, s := md.Schema[0], md.Schema[1]; !id.Required || id.Description != "i" || s.DefaultValueExpression != "'x'" {
		t.Fatalf("columns: got %+v, %+v", id, s)
	}
	if md.TimePartitioning == nil || md.TimePartitioning.Field != "d" || md.TimePartitioning.Expiration != 48*time.Hour {
		t.Fatalf("timePartitioning: got %+v", md.TimePartitioning)
	}
	if md.Clustering == nil || !reflect.DeepEqual(md.Clustering.Fields, []string{"id"}) {
		t.Fatalf("clustering: got %+v", md.Clustering)
	}
	if md.Description != "t" || md.Name != "f" || !reflect.DeepEqual(md.Labels, map[string]string{"k": "v"}) || !md.RequirePartitionFilter {
		t.Fatalf("options: got description %q, friendlyName %q, labels %v, requirePartitionFilter %v", md.Description, md.Name, md.Labels, md.RequirePartitionFilter)
	}
	if md.TableConstraints == nil || md.TableConstraints.PrimaryKey == nil || !reflect.DeepEqual(md.TableConstraints.PrimaryKey.Columns, []string{"id"}) {
		t.Fatalf("tableConstraints: got %+v", md.TableConstraints)
	}
	if rows := env.mustRead(t, "SELECT COUNT(*) FROM "+ds+".l1"); len(rows) != 1 || rows[0][0] != int64(0) {
		t.Fatalf("rows: got %v", rows)
	}
	// The new table enforces the NOT NULL and DEFAULT of the source table.
	assertErrContains(t, env.exec(t, mode, "INSERT INTO "+ds+".l1 (s) VALUES ('y')"), "Required field id cannot be null")
	env.mustExec(t, mode, "INSERT INTO "+ds+".l1 (id, r) VALUES (1, STRUCT(2 AS a, ['b'] AS b))")
	if rows := env.mustRead(t, "SELECT s, r.a FROM "+ds+".l1"); len(rows) != 1 || rows[0][0] != "x" || rows[0][1] != int64(2) {
		t.Fatalf("rows: got %v", rows)
	}
}

func testCreateTableLikeOverrides(t *testing.T, env *ddlTestEnv, mode, ds string) {
	env.mustExec(t, mode, "CREATE TABLE "+ds+".lov (id INT64, s STRING, d DATE) PARTITION BY d CLUSTER BY id OPTIONS(description='t', friendly_name='f', labels=[('k','v')])")
	env.mustExec(t, mode, "CREATE TABLE "+ds+".lov1 LIKE "+ds+".lov PARTITION BY RANGE_BUCKET(id, GENERATE_ARRAY(0, 100, 10)) CLUSTER BY s OPTIONS(description='n')")
	md := env.metadata(t, ds, "lov1")
	if rp := md.RangePartitioning; rp == nil || rp.Field != "id" || rp.Range == nil || rp.Range.End != 100 || rp.Range.Interval != 10 {
		t.Fatalf("rangePartitioning: got %+v", md.RangePartitioning)
	}
	if md.TimePartitioning != nil {
		t.Fatalf("timePartitioning: got %+v", md.TimePartitioning)
	}
	if md.Clustering == nil || !reflect.DeepEqual(md.Clustering.Fields, []string{"s"}) {
		t.Fatalf("clustering: got %+v", md.Clustering)
	}
	if md.Description != "n" || md.Name != "f" || !reflect.DeepEqual(md.Labels, map[string]string{"k": "v"}) {
		t.Fatalf("options: got description %q, friendlyName %q, labels %v", md.Description, md.Name, md.Labels)
	}

	env.mustExec(t, mode, "CREATE TABLE "+ds+".lpd (a INT64) PARTITION BY _PARTITIONDATE")
	env.mustExec(t, mode, "CREATE TABLE "+ds+".lpd1 LIKE "+ds+".lpd")
	if tp := env.metadata(t, ds, "lpd1").TimePartitioning; tp == nil || tp.Type != bigquery.DayPartitioningType || tp.Field != "" {
		t.Fatalf("lpd1 timePartitioning: got %+v", tp)
	}

	assertErrContains(t, env.exec(t, mode, "CREATE TABLE "+ds+".lov2 LIKE "+ds+".lov PARTITION BY nope"), "Unrecognized name: nope")
	assertErrContains(t, env.exec(t, mode, "CREATE TABLE "+ds+".lov2 LIKE "+ds+".lov CLUSTER BY nope"), "Invalid field: nope")
	env.assertTableNotFound(t, ds, "lov2")
}

func testCreateTableLikeAsQuery(t *testing.T, env *ddlTestEnv, mode, ds string) {
	env.mustExec(t, mode, "CREATE TABLE "+ds+".laq (id INT64 NOT NULL, s STRING DEFAULT 'x', n NUMERIC)")
	env.mustExec(t, mode, "CREATE TABLE "+ds+".laq1 LIKE "+ds+".laq AS SELECT 1 AS id, 'a' AS s, 2 AS n UNION ALL SELECT 2, 'b', 3")
	if rows := env.mustRead(t, "SELECT id, s, CAST(n AS STRING) FROM "+ds+".laq1 ORDER BY id"); !reflect.DeepEqual(rows, [][]bigquery.Value{{int64(1), "a", "2"}, {int64(2), "b", "3"}}) {
		t.Fatalf("laq1 rows: got %v", rows)
	}
	if md := env.metadata(t, ds, "laq1"); len(md.Schema) != 3 || !md.Schema[0].Required || md.Schema[2].Type != bigquery.NumericFieldType {
		t.Fatalf("laq1 schema: got %+v", md.Schema)
	}

	// The columns of the query are matched by name.
	env.mustExec(t, mode, "CREATE TABLE "+ds+".laq2 LIKE "+ds+".laq AS SELECT 'c' AS S, 5 AS n, 3 AS ID")
	if rows := env.mustRead(t, "SELECT id, s FROM "+ds+".laq2"); !reflect.DeepEqual(rows, [][]bigquery.Value{{int64(3), "c"}}) {
		t.Fatalf("laq2 rows: got %v", rows)
	}
	assertNames(t, "laq2 schema", env.tableSchemaNames(t, ds, "laq2"), "id", "s", "n")

	assertErrContains(t, env.exec(t, mode, "CREATE TABLE "+ds+".laq3 LIKE "+ds+".laq AS SELECT 1 AS id, 'a' AS x, 2 AS n"), "do not match the columns of table test:"+ds+".laq")
	env.assertTableNotFound(t, ds, "laq3")

	// A failed insert leaves nothing behind, and the name stays free.
	assertErrContains(t, env.exec(t, mode, "CREATE TABLE "+ds+".laq4 LIKE "+ds+".laq AS SELECT CAST(NULL AS INT64) AS id, 'a' AS s, 1 AS n"), "Required field id cannot be null")
	env.assertTableNotFound(t, ds, "laq4")
	for _, id := range env.tableIDs(t, ds) {
		if strings.HasPrefix(id, "__bqemu_like_") {
			t.Fatalf("table %s left behind", id)
		}
	}
	env.mustExec(t, mode, "CREATE TABLE "+ds+".laq4 LIKE "+ds+".laq")
	env.mustExec(t, mode, "INSERT INTO "+ds+".laq4 (id) VALUES (1)")
	if rows := env.mustRead(t, "SELECT s FROM "+ds+".laq4"); len(rows) != 1 || rows[0][0] != "x" {
		t.Fatalf("laq4 rows: got %v", rows)
	}
}

func testCreateTableLikeOrReplace(t *testing.T, env *ddlTestEnv, mode, ds string) {
	env.mustExec(t, mode, "CREATE TABLE "+ds+".lrs (id INT64 NOT NULL, s STRING DEFAULT 'x') OPTIONS(description='src')")
	env.mustExec(t, mode, "INSERT INTO "+ds+".lrs (id) VALUES (1)")
	env.mustExec(t, mode, "CREATE TABLE "+ds+".lrd (z BOOL NOT NULL, w STRING DEFAULT 'w') OPTIONS(description='old')")
	env.mustExec(t, mode, "INSERT INTO "+ds+".lrd (z) VALUES (TRUE)")

	// A failed CREATE OR REPLACE keeps the replaced table as it was.
	assertErrContains(t, env.exec(t, mode, "CREATE OR REPLACE TABLE "+ds+".lrd LIKE "+ds+".lrs AS SELECT CAST(NULL AS INT64) AS id, 'a' AS s"), "Required field id cannot be null")
	assertNames(t, "lrd schema", env.tableSchemaNames(t, ds, "lrd"), "z", "w")
	if md := env.metadata(t, ds, "lrd"); md.Description != "old" || !md.Schema[0].Required || md.Schema[1].DefaultValueExpression != "'w'" {
		t.Fatalf("lrd metadata: got description %q, schema %+v", md.Description, md.Schema)
	}
	env.mustExec(t, mode, "INSERT INTO "+ds+".lrd (z) VALUES (FALSE)")
	if rows := env.mustRead(t, "SELECT z, w FROM "+ds+".lrd ORDER BY z"); !reflect.DeepEqual(rows, [][]bigquery.Value{{false, "w"}, {true, "w"}}) {
		t.Fatalf("lrd rows: got %v", rows)
	}
	assertErrContains(t, env.exec(t, mode, "INSERT INTO "+ds+".lrd (w) VALUES ('q')"), "Required field z cannot be null")

	env.mustExec(t, mode, "CREATE OR REPLACE TABLE "+ds+".lrd LIKE "+ds+".lrs")
	assertNames(t, "lrd schema", env.tableSchemaNames(t, ds, "lrd"), "id", "s")
	if md := env.metadata(t, ds, "lrd"); md.Description != "src" {
		t.Fatalf("lrd description: got %q", md.Description)
	}
	if rows := env.mustRead(t, "SELECT COUNT(*) FROM "+ds+".lrd"); len(rows) != 1 || rows[0][0] != int64(0) {
		t.Fatalf("lrd rows: got %v", rows)
	}
	env.mustExec(t, mode, "INSERT INTO "+ds+".lrd (id) VALUES (5)")
	if rows := env.mustRead(t, "SELECT id, s FROM "+ds+".lrd"); !reflect.DeepEqual(rows, [][]bigquery.Value{{int64(5), "x"}}) {
		t.Fatalf("lrd rows: got %v", rows)
	}

	// The query runs before the table is replaced.
	env.mustExec(t, mode, "CREATE OR REPLACE TABLE "+ds+".lrs LIKE "+ds+".lrs AS SELECT * FROM "+ds+".lrs")
	if rows := env.mustRead(t, "SELECT id, s FROM "+ds+".lrs"); !reflect.DeepEqual(rows, [][]bigquery.Value{{int64(1), "x"}}) {
		t.Fatalf("lrs rows: got %v", rows)
	}
	if md := env.metadata(t, ds, "lrs"); md.Description != "src" {
		t.Fatalf("lrs description: got %q", md.Description)
	}
}

func testCreateTableLikeIfNotExists(t *testing.T, env *ddlTestEnv, mode, ds string) {
	env.mustExec(t, mode, "CREATE TABLE "+ds+".lin (a INT64)")
	env.mustExec(t, mode, "INSERT INTO "+ds+".lin (a) VALUES (1)")
	env.mustExec(t, mode, "CREATE TABLE "+ds+".linsrc (b STRING)")
	env.mustExec(t, mode, "CREATE TABLE IF NOT EXISTS "+ds+".lin LIKE "+ds+".linsrc")
	assertNames(t, "lin schema", env.tableSchemaNames(t, ds, "lin"), "a")
	if rows := env.mustRead(t, "SELECT a FROM "+ds+".lin"); len(rows) != 1 || rows[0][0] != int64(1) {
		t.Fatalf("lin rows: got %v", rows)
	}
	env.mustExec(t, mode, "CREATE TABLE IF NOT EXISTS "+ds+".lin2 LIKE "+ds+".linsrc")
	assertNames(t, "lin2 schema", env.tableSchemaNames(t, ds, "lin2"), "b")
}

func testCreateTableLikeErrors(t *testing.T, env *ddlTestEnv) {
	env.mustExec(t, "jobs.insert", "CREATE TABLE dataset1.le (a INT64)")
	env.mustExec(t, "jobs.insert", "CREATE VIEW dataset1.lev AS SELECT 1 AS a")
	env.mustExec(t, "jobs.insert", "CREATE TABLE dataset2.lcross (q STRING)")
	for _, mode := range ddlModes {
		for _, c := range []struct {
			sql, reason, msg string
			status           int
		}{
			{"CREATE TABLE dataset1.lx LIKE dataset1.nosuch", "notFound", "Not found: Table test:dataset1.nosuch", http.StatusNotFound},
			{"CREATE TABLE nods.lx LIKE dataset1.le", "notFound", "Not found: Dataset test:nods", http.StatusNotFound},
			{"CREATE TABLE dataset1.lx LIKE nods.le", "notFound", "Not found: Dataset test:nods", http.StatusNotFound},
			{"CREATE TABLE dataset1.le LIKE dataset1.le", "duplicate", "Already Exists: Table test:dataset1.le", http.StatusConflict},
		} {
			code, reason, msg := env.queryError(t, mode, c.sql)
			wantCode := c.status
			if mode == "jobs.insert" {
				wantCode = http.StatusOK
			}
			if code != wantCode || reason != c.reason || msg != c.msg {
				t.Fatalf("%s %s: got HTTP %d %q %q", mode, c.sql, code, reason, msg)
			}
		}
		assertErrContains(t, env.exec(t, mode, "CREATE TABLE dataset1.lx LIKE dataset1.lev"), "not supported by the emulator for view test:dataset1.lev")
		assertErrContains(t, env.exec(t, mode, "SELECT 1; CREATE TABLE dataset1.lx LIKE dataset1.le"), "CREATE TABLE ... LIKE inside a multi-statement script is not supported by the emulator")
		assertErrContains(t, env.execIn(t, mode, "", "CREATE TABLE lx LIKE dataset1.le"), `Table "lx" must be qualified with a dataset (e.g. dataset.table).`)
		env.assertTableNotFound(t, "dataset1", "lx")

		table := "lq_" + strings.ReplaceAll(mode, ".", "_")
		if err := env.execIn(t, mode, "dataset1", "CREATE TABLE "+table+" LIKE le"); err != nil {
			t.Fatal(err)
		}
		assertNames(t, table, env.tableSchemaNames(t, "dataset1", table), "a")
		table = "lc_" + strings.ReplaceAll(mode, ".", "_")
		env.mustExec(t, mode, "CREATE TABLE dataset1."+table+" LIKE dataset2.lcross")
		assertNames(t, table, env.tableSchemaNames(t, "dataset1", table), "q")
	}

	// A dry run checks the statement and creates nothing.
	dryRun := func(sql string) error {
		q := env.client.Query(sql)
		q.DryRun = true
		job, err := q.Run(env.ctx)
		if err != nil {
			return err
		}
		return job.LastStatus().Err()
	}
	if err := dryRun("CREATE TABLE dataset1.ldry LIKE dataset1.le AS SELECT 1 AS a"); err != nil {
		t.Fatalf("dry run: %v", err)
	}
	assertErrContains(t, dryRun("CREATE TABLE dataset1.ldry LIKE dataset1.le AS SELECT 1 AS b"), "do not match the columns")
	assertErrContains(t, dryRun("CREATE TABLE dataset1.ldry LIKE dataset1.nosuch"), "Not found: Table test:dataset1.nosuch")
	env.assertTableNotFound(t, "dataset1", "ldry")

	// The Go client reads the error of a job from jobs.getQueryResults.
	var gerr *googleapi.Error
	if err := env.runQueryJob(t, "CREATE TABLE dataset1.le LIKE dataset1.le"); !errors.As(err, &gerr) || gerr.Code != http.StatusConflict {
		t.Fatalf("want a 409, got %v", err)
	}
	if err := env.runQueryJob(t, "CREATE TABLE dataset1.lx LIKE dataset1.nosuch"); !errors.As(err, &gerr) || gerr.Code != http.StatusNotFound {
		t.Fatalf("want a 404, got %v", err)
	}
}

// testCreateTableLikeOfAPITable copies a table created by tables.insert,
// whose metadata says more than DDL can: nested modes and descriptions,
// type parameters and range partitioning.
func testCreateTableLikeOfAPITable(t *testing.T, env *ddlTestEnv) {
	if err := env.client.Dataset("dataset1").Table("lapi").Create(env.ctx, &bigquery.TableMetadata{
		Schema: bigquery.Schema{
			{Name: "id", Type: bigquery.IntegerFieldType, Required: true},
			{Name: "s", Type: bigquery.StringFieldType, MaxLength: 5},
			{Name: "n", Type: bigquery.NumericFieldType, Precision: 10, Scale: 2},
			{Name: "r", Type: bigquery.RecordFieldType, Description: "rec", Schema: bigquery.Schema{
				{Name: "a", Type: bigquery.IntegerFieldType, Required: true, Description: "nested"},
				{Name: "b", Type: bigquery.StringFieldType, Repeated: true},
			}},
		},
		RangePartitioning: &bigquery.RangePartitioning{Field: "id", Range: &bigquery.RangePartitioningRange{Start: 0, End: 100, Interval: 10}},
	}); err != nil {
		t.Fatal(err)
	}
	env.mustExec(t, "jobs.insert", "CREATE TABLE dataset1.lapi2 LIKE dataset1.lapi")
	src, md := env.metadata(t, "dataset1", "lapi"), env.metadata(t, "dataset1", "lapi2")
	if !reflect.DeepEqual(md.Schema, src.Schema) {
		t.Fatalf("schema: got %+v, want %+v", md.Schema, src.Schema)
	}
	if r := md.Schema[3]; r.Description != "rec" || !r.Schema[0].Required || r.Schema[0].Description != "nested" || md.Schema[1].MaxLength != 5 || md.Schema[2].Precision != 10 || md.Schema[2].Scale != 2 {
		t.Fatalf("schema: got %+v", md.Schema)
	}
	if !reflect.DeepEqual(md.RangePartitioning, src.RangePartitioning) {
		t.Fatalf("rangePartitioning: got %+v, want %+v", md.RangePartitioning, src.RangePartitioning)
	}
	env.mustExec(t, "jobs.insert", "INSERT INTO dataset1.lapi2 (id, r) VALUES (1, STRUCT(2 AS a, ['x'] AS b))")
	if rows := env.mustRead(t, "SELECT r.a, r.b[OFFSET(0)] FROM dataset1.lapi2"); !reflect.DeepEqual(rows, [][]bigquery.Value{{int64(2), "x"}}) {
		t.Fatalf("rows: got %v", rows)
	}
}

// testRewriteFailingAfterRun checks that an ALTER TABLE or CREATE TABLE ...
// LIKE whose job fails after its steps ran leaves the table as it was: the
// steps are rolled back and googlesqlite's in-memory catalog is repaired. A
// job ID in use is rejected before anything runs.
func testRewriteFailingAfterRun(t *testing.T, env *ddlTestEnv) {
	post := func(t *testing.T, body string) (int, string) {
		t.Helper()
		resp, err := http.Post(env.url+"/bigquery/v2/projects/test/jobs", "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		return resp.StatusCode, string(b)
	}
	env.mustExec(t, "jobs.insert", "CREATE TABLE dataset1.rfsrc (id INT64 NOT NULL, s STRING)")
	check := func(t *testing.T, table string) {
		t.Helper()
		assertNames(t, table+" schema", env.tableSchemaNames(t, "dataset1", table), "x", "y")
		if rows := env.mustRead(t, "SELECT * FROM dataset1."+table); !reflect.DeepEqual(rows, [][]bigquery.Value{{"a", "b"}}) {
			t.Fatalf("%s rows: got %v", table, rows)
		}
		assertErrContains(t, env.exec(t, "jobs.insert", "INSERT INTO dataset1."+table+" (y) VALUES ('c')"), "Required field x cannot be null")
		for _, id := range env.tableIDs(t, "dataset1") {
			if strings.HasPrefix(id, "__bqemu_") {
				t.Fatalf("table %s left behind", id)
			}
		}
		if rows := env.mustRead(t, "SELECT COUNT(*) FROM dataset1.INFORMATION_SCHEMA.TABLES WHERE STARTS_WITH(table_name, '__bqemu_')"); len(rows) != 1 || rows[0][0] != int64(0) {
			t.Fatalf("INFORMATION_SCHEMA.TABLES: got %v", rows)
		}
	}
	for i, sql := range []string{
		"CREATE OR REPLACE TABLE dataset1.%s LIKE dataset1.rfsrc",
		"CREATE OR REPLACE TABLE dataset1.%s LIKE dataset1.rfsrc AS SELECT 1 AS id, 'q' AS s",
		"ALTER TABLE dataset1.%s DROP COLUMN y",
		"ALTER TABLE dataset1.%s ADD COLUMN z STRING",
	} {
		table := fmt.Sprintf("rf%d", i)
		env.mustExec(t, "jobs.insert", "CREATE TABLE dataset1."+table+" (x STRING NOT NULL, y STRING)")
		env.mustExec(t, "jobs.insert", "INSERT INTO dataset1."+table+" (x, y) VALUES ('a', 'b')")
		sql = fmt.Sprintf(sql, table)

		// A job ID in use.
		jobID := fmt.Sprintf("rf_dup_%d_%d", i, time.Now().UnixNano())
		if code, body := post(t, fmt.Sprintf(`{"jobReference":{"projectId":"test","jobId":%q},"configuration":{"query":{"query":"SELECT 1","useLegacySql":false}}}`, jobID)); code != http.StatusOK {
			t.Fatalf("first job: HTTP %d %s", code, body)
		}
		code, body := post(t, fmt.Sprintf(`{"jobReference":{"projectId":"test","jobId":%q},"configuration":{"query":{"query":%q,"useLegacySql":false}}}`, jobID, sql))
		if code != http.StatusConflict || !strings.Contains(body, "Already Exists: Job test:"+jobID) {
			t.Fatalf("%s with a job ID in use: got HTTP %d %s", sql, code, body)
		}
		check(t, table)

		// A failure after the steps ran: the destination table of the
		// job is missing and may not be created.
		code, body = post(t, fmt.Sprintf(`{"jobReference":{"projectId":"test","jobId":"rf_dest_%d_%d"},"configuration":{"query":{"query":%q,"useLegacySql":false,`+
			`"destinationTable":{"projectId":"test","datasetId":"dataset1","tableId":"rf_nodest"},"createDisposition":"CREATE_NEVER"}}}`, i, time.Now().UnixNano(), sql))
		if code == http.StatusOK {
			t.Fatalf("%s with a missing destination table: got HTTP %d %s", sql, code, body)
		}
		check(t, table)
	}
}
