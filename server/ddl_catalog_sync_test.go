package server_test

import (
	"context"
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
	"google.golang.org/api/option"

	"github.com/goccy/bigquery-emulator/server"
	"github.com/goccy/bigquery-emulator/types"
)

// ddlTestEnv is a server with the datasets dataset1 and dataset2 in project
// "test", plus a client for it.
type ddlTestEnv struct {
	ctx    context.Context
	url    string
	client *bigquery.Client
}

func newDDLTestEnv(t *testing.T) *ddlTestEnv {
	t.Helper()
	ctx := context.Background()
	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	if err := bqServer.Load(server.StructSource(types.NewProject("test", types.NewDataset("dataset1"), types.NewDataset("dataset2")))); err != nil {
		t.Fatal(err)
	}
	testServer := bqServer.TestServer()
	client, err := bigquery.NewClient(
		ctx,
		"test",
		option.WithEndpoint(testServer.URL),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		client.Close()
		testServer.Close()
		bqServer.Stop(ctx)
	})
	return &ddlTestEnv{ctx: ctx, url: testServer.URL, client: client}
}

// runQueryJob runs sql through jobs.insert and returns the job's error.
func (e *ddlTestEnv) runQueryJob(t *testing.T, sql string) error {
	t.Helper()
	job, err := e.client.Query(sql).Run(e.ctx)
	if err != nil {
		return err
	}
	status, err := job.Wait(e.ctx)
	if err != nil {
		return err
	}
	return status.Err()
}

// readQuery runs sql through jobs.query and returns its rows.
func (e *ddlTestEnv) readQuery(t *testing.T, sql string) ([][]bigquery.Value, error) {
	t.Helper()
	it, err := e.client.Query(sql).Read(e.ctx)
	if err != nil {
		return nil, err
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

// exec runs sql through the API named by mode.
func (e *ddlTestEnv) exec(t *testing.T, mode, sql string) error {
	t.Helper()
	if mode == "jobs.insert" {
		return e.runQueryJob(t, sql)
	}
	_, err := e.readQuery(t, sql)
	return err
}

func (e *ddlTestEnv) mustExec(t *testing.T, mode, sql string) {
	t.Helper()
	if err := e.exec(t, mode, sql); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
}

func (e *ddlTestEnv) mustRead(t *testing.T, sql string) [][]bigquery.Value {
	t.Helper()
	rows, err := e.readQuery(t, sql)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return rows
}

func (e *ddlTestEnv) metadata(t *testing.T, datasetID, tableID string) *bigquery.TableMetadata {
	t.Helper()
	md, err := e.client.Dataset(datasetID).Table(tableID).Metadata(e.ctx)
	if err != nil {
		t.Fatalf("tables.get %s.%s: %v", datasetID, tableID, err)
	}
	return md
}

func (e *ddlTestEnv) tableSchemaNames(t *testing.T, datasetID, tableID string) []string {
	t.Helper()
	var names []string
	for _, field := range e.metadata(t, datasetID, tableID).Schema {
		names = append(names, field.Name)
	}
	return names
}

func (e *ddlTestEnv) assertTableNotFound(t *testing.T, datasetID, tableID string) {
	t.Helper()
	_, err := e.client.Dataset(datasetID).Table(tableID).Metadata(e.ctx)
	var gerr *googleapi.Error
	if !errors.As(err, &gerr) || gerr.Code != http.StatusNotFound {
		t.Fatalf("tables.get %s.%s: want 404, got %v", datasetID, tableID, err)
	}
}

// postJob inserts a query job through raw REST, so that a 5xx is observed
// instead of being retried away by the Go client. It returns the HTTP status
// and the job's status.errorResult message.
func (e *ddlTestEnv) postJob(t *testing.T, jobID, sql string) (int, string) {
	t.Helper()
	body := fmt.Sprintf(
		`{"jobReference":{"projectId":"test","jobId":%q},"configuration":{"query":{"query":%q,"useLegacySql":false}}}`,
		jobID, sql,
	)
	resp, err := http.Post(e.url+"/bigquery/v2/projects/test/jobs", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	var job struct {
		Status struct {
			ErrorResult *struct {
				Message string `json:"message"`
			} `json:"errorResult"`
		} `json:"status"`
	}
	if resp.StatusCode != http.StatusOK {
		return resp.StatusCode, string(b)
	}
	if err := json.Unmarshal(b, &job); err != nil {
		t.Fatalf("decode %s: %v", b, err)
	}
	if job.Status.ErrorResult != nil {
		return resp.StatusCode, job.Status.ErrorResult.Message
	}
	return resp.StatusCode, ""
}

func (e *ddlTestEnv) tableIDs(t *testing.T, datasetID string) []string {
	t.Helper()
	var ids []string
	it := e.client.Dataset(datasetID).Tables(e.ctx)
	for {
		table, err := it.Next()
		if err == iterator.Done {
			return ids
		}
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, table.TableID)
	}
}

func assertNames(t *testing.T, what string, got []string, want ...string) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%s: got %v, want %v", what, got, want)
	}
}

func assertErrContains(t *testing.T, err error, want string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected an error containing %q, got nil", want)
	}
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("expected an error containing %q, got %v", want, err)
	}
}

var ddlModes = []string{"jobs.insert", "jobs.query"}

func TestCreateOrReplaceTableSyncsCatalog(t *testing.T) {
	for _, mode := range ddlModes {
		t.Run(mode, func(t *testing.T) {
			env := newDDLTestEnv(t)

			t.Run("column_list", func(t *testing.T) {
				env.mustExec(t, mode, "CREATE TABLE dataset1.cor_cols (a INT64)")
				env.mustExec(t, mode, "CREATE OR REPLACE TABLE dataset1.cor_cols (a INT64, b STRING)")
				assertNames(t, "schema", env.tableSchemaNames(t, "dataset1", "cor_cols"), "a", "b")
				env.mustExec(t, mode, "INSERT INTO dataset1.cor_cols (a, b) VALUES (1, 'x')")
				rows := env.mustRead(t, "SELECT * FROM dataset1.cor_cols")
				if len(rows) != 1 || len(rows[0]) != 2 {
					t.Fatalf("SELECT *: got %v, want 1 row of 2 columns", rows)
				}
			})

			t.Run("ctas_different_schema", func(t *testing.T) {
				env.mustExec(t, mode, "CREATE OR REPLACE TABLE dataset1.cor_ctas AS SELECT 1 AS id")
				env.mustExec(t, mode, "CREATE OR REPLACE TABLE dataset1.cor_ctas AS SELECT 'z' AS c UNION ALL SELECT 'y'")
				assertNames(t, "schema", env.tableSchemaNames(t, "dataset1", "cor_ctas"), "c")
				if got := env.metadata(t, "dataset1", "cor_ctas").NumRows; got != 2 {
					t.Fatalf("numRows: got %d, want 2", got)
				}
				if rows := env.mustRead(t, "SELECT * FROM dataset1.cor_ctas"); len(rows) != 2 || len(rows[0]) != 1 {
					t.Fatalf("SELECT *: got %v", rows)
				}
				if rows := env.mustRead(t, "SELECT c FROM dataset1.cor_ctas ORDER BY c"); len(rows) != 2 || rows[0][0] != "y" {
					t.Fatalf("SELECT c: got %v", rows)
				}
			})

			t.Run("replace_view", func(t *testing.T) {
				env.mustExec(t, mode, "CREATE VIEW dataset1.v AS SELECT 1 AS a")
				env.mustExec(t, mode, "CREATE OR REPLACE VIEW dataset1.v AS SELECT 'x' AS b")
				md := env.metadata(t, "dataset1", "v")
				if md.Type != bigquery.ViewTable {
					t.Fatalf("type: got %s, want VIEW", md.Type)
				}
				assertNames(t, "view schema", env.tableSchemaNames(t, "dataset1", "v"), "b")
				if rows := env.mustRead(t, "SELECT b FROM dataset1.v"); len(rows) != 1 || rows[0][0] != "x" {
					t.Fatalf("SELECT b: got %v", rows)
				}
			})

			t.Run("plain_create_on_existing_fails", func(t *testing.T) {
				if err := env.exec(t, mode, "CREATE TABLE dataset1.cor_cols (z INT64)"); err == nil {
					t.Fatal("expected CREATE TABLE on an existing table to fail")
				}
				assertNames(t, "schema", env.tableSchemaNames(t, "dataset1", "cor_cols"), "a", "b")
			})

			t.Run("drop_create_cycle", func(t *testing.T) {
				for i := 0; i < 3; i++ {
					env.mustExec(t, mode, "CREATE TABLE dataset1.cor_cycle (a INT64)")
					env.mustExec(t, mode, "DROP TABLE dataset1.cor_cycle")
				}
				env.assertTableNotFound(t, "dataset1", "cor_cycle")
			})

			t.Run("same_name_other_dataset", func(t *testing.T) {
				env.mustExec(t, mode, "CREATE TABLE dataset1.same (a INT64)")
				env.mustExec(t, mode, "CREATE TABLE dataset2.same (b STRING)")
				env.mustExec(t, mode, "CREATE OR REPLACE TABLE dataset2.same (b STRING, c INT64)")
				assertNames(t, "dataset1.same", env.tableSchemaNames(t, "dataset1", "same"), "a")
				assertNames(t, "dataset2.same", env.tableSchemaNames(t, "dataset2", "same"), "b", "c")
				env.mustExec(t, mode, "INSERT INTO dataset1.same (a) VALUES (1)")
				env.mustExec(t, mode, "INSERT INTO dataset2.same (b, c) VALUES ('x', 2)")
				if rows := env.mustRead(t, "SELECT * FROM dataset1.same"); len(rows) != 1 || len(rows[0]) != 1 {
					t.Fatalf("dataset1.same: got %v", rows)
				}
				if rows := env.mustRead(t, "SELECT * FROM dataset2.same"); len(rows) != 1 || len(rows[0]) != 2 {
					t.Fatalf("dataset2.same: got %v", rows)
				}
			})
		})
	}
}

func TestCreateTableIfNotExistsKeepsExistingTable(t *testing.T) {
	env := newDDLTestEnv(t)
	env.mustExec(t, "jobs.insert", "CREATE TABLE dataset1.ine (a INT64)")
	env.mustExec(t, "jobs.insert", "INSERT INTO dataset1.ine (a) VALUES (1)")
	env.mustExec(t, "jobs.insert", "CREATE TABLE IF NOT EXISTS dataset1.ine (z INT64)")
	assertNames(t, "schema", env.tableSchemaNames(t, "dataset1", "ine"), "a")

	// A DROP makes googlesqlite rebuild its analyzer catalog from the stored
	// table specs. The spec of ine must still describe column a.
	env.mustExec(t, "jobs.insert", "CREATE TABLE dataset1.tmp_x (q INT64)")
	env.mustExec(t, "jobs.insert", "DROP TABLE dataset1.tmp_x")
	rows := env.mustRead(t, "SELECT * FROM dataset1.ine")
	if len(rows) != 1 || len(rows[0]) != 1 || rows[0][0] != int64(1) {
		t.Fatalf("SELECT *: got %v, want [[1]]", rows)
	}
	if rows := env.mustRead(t, "SELECT a FROM dataset1.ine"); len(rows) != 1 {
		t.Fatalf("SELECT a: got %v", rows)
	}

	env.mustExec(t, "jobs.insert", "CREATE TABLE IF NOT EXISTS dataset1.ine_new (a INT64)")
	assertNames(t, "ine_new", env.tableSchemaNames(t, "dataset1", "ine_new"), "a")
}

func TestAlterTableSyncsCatalog(t *testing.T) {
	env := newDDLTestEnv(t)

	for _, mode := range ddlModes {
		t.Run("add_column/"+mode, func(t *testing.T) {
			table := "dataset1.add_" + strings.ReplaceAll(mode, ".", "_")
			env.mustExec(t, mode, "CREATE TABLE "+table+" (a INT64)")
			env.mustExec(t, mode, "INSERT INTO "+table+" (a) VALUES (1)")
			env.mustExec(t, mode, "ALTER TABLE "+table+" ADD COLUMN b STRING")
			md := env.metadata(t, "dataset1", strings.TrimPrefix(table, "dataset1."))
			if len(md.Schema) != 2 || md.Schema[1].Name != "b" || md.Schema[1].Type != bigquery.StringFieldType || md.Schema[1].Required || md.Schema[1].Repeated {
				t.Fatalf("schema: got %+v", md.Schema)
			}
			env.mustExec(t, mode, "INSERT INTO "+table+" (a, b) VALUES (2, 'x')")
			rows := env.mustRead(t, "SELECT a, b FROM "+table+" ORDER BY a")
			if len(rows) != 2 || rows[0][1] != nil || rows[1][1] != "x" {
				t.Fatalf("rows: got %v", rows)
			}
		})
	}

	t.Run("add_column_if_not_exists_existing", func(t *testing.T) {
		env.mustExec(t, "jobs.insert", "CREATE TABLE dataset1.add_ine (a INT64)")
		env.mustExec(t, "jobs.insert", "ALTER TABLE dataset1.add_ine ADD COLUMN IF NOT EXISTS a STRING")
		md := env.metadata(t, "dataset1", "add_ine")
		if len(md.Schema) != 1 || md.Schema[0].Type != bigquery.IntegerFieldType {
			t.Fatalf("schema: got %+v", md.Schema)
		}
	})

	t.Run("add_existing_column_fails", func(t *testing.T) {
		env.mustExec(t, "jobs.insert", "CREATE TABLE dataset1.add_dup (a INT64)")
		assertErrContains(t, env.runQueryJob(t, "ALTER TABLE dataset1.add_dup ADD COLUMN a STRING"), "already exists")
	})

	t.Run("add_multiple_columns", func(t *testing.T) {
		env.mustExec(t, "jobs.insert", "CREATE TABLE dataset1.add_multi (a INT64)")
		env.mustExec(t, "jobs.insert", "ALTER TABLE dataset1.add_multi ADD COLUMN b STRING, ADD COLUMN c ARRAY<INT64>")
		md := env.metadata(t, "dataset1", "add_multi")
		if len(md.Schema) != 3 {
			t.Fatalf("schema: got %+v", md.Schema)
		}
		if md.Schema[1].Type != bigquery.StringFieldType {
			t.Fatalf("b: got %+v", md.Schema[1])
		}
		if md.Schema[2].Type != bigquery.IntegerFieldType || !md.Schema[2].Repeated {
			t.Fatalf("c: got %+v", md.Schema[2])
		}
		env.mustExec(t, "jobs.insert", "INSERT INTO dataset1.add_multi (a, b, c) VALUES (1, 'x', [1, 2])")
		if rows := env.mustRead(t, "SELECT c FROM dataset1.add_multi"); len(rows) != 1 {
			t.Fatalf("rows: got %v", rows)
		}
	})

	t.Run("add_struct_column", func(t *testing.T) {
		env.mustExec(t, "jobs.insert", "CREATE TABLE dataset1.add_struct (a INT64)")
		env.mustExec(t, "jobs.insert", "ALTER TABLE dataset1.add_struct ADD COLUMN s STRUCT<x INT64, y STRING>")
		md := env.metadata(t, "dataset1", "add_struct")
		if len(md.Schema) != 2 || md.Schema[1].Type != bigquery.RecordFieldType || len(md.Schema[1].Schema) != 2 {
			t.Fatalf("schema: got %+v", md.Schema)
		}
		env.mustExec(t, "jobs.insert", "INSERT INTO dataset1.add_struct (a, s) VALUES (1, STRUCT(2, 'y'))")
		if rows := env.mustRead(t, "SELECT s.y FROM dataset1.add_struct"); len(rows) != 1 || rows[0][0] != "y" {
			t.Fatalf("rows: got %v", rows)
		}
	})

	t.Run("drop_column", func(t *testing.T) {
		env.mustExec(t, "jobs.insert", "CREATE TABLE dataset1.drop_col (a INT64, b STRING, c INT64)")
		env.mustExec(t, "jobs.insert", "INSERT INTO dataset1.drop_col (a, b, c) VALUES (1, 'x', 3)")
		env.mustExec(t, "jobs.insert", "ALTER TABLE dataset1.drop_col DROP COLUMN b")
		assertNames(t, "schema", env.tableSchemaNames(t, "dataset1", "drop_col"), "a", "c")
		rows := env.mustRead(t, "SELECT * FROM dataset1.drop_col")
		if len(rows) != 1 || !reflect.DeepEqual(rows[0], []bigquery.Value{int64(1), int64(3)}) {
			t.Fatalf("rows: got %v", rows)
		}
		if _, err := env.readQuery(t, "SELECT b FROM dataset1.drop_col"); err == nil {
			t.Fatal("expected SELECT of a dropped column to fail")
		}
	})

	t.Run("drop_column_if_exists_missing", func(t *testing.T) {
		env.mustExec(t, "jobs.insert", "CREATE TABLE dataset1.drop_ie (a INT64)")
		env.mustExec(t, "jobs.insert", "ALTER TABLE dataset1.drop_ie DROP COLUMN IF EXISTS nope")
		assertNames(t, "schema", env.tableSchemaNames(t, "dataset1", "drop_ie"), "a")
		assertErrContains(t, env.runQueryJob(t, "ALTER TABLE dataset1.drop_ie DROP COLUMN nope"), "not found")
	})

	t.Run("rename_column", func(t *testing.T) {
		env.mustExec(t, "jobs.insert", "CREATE TABLE dataset1.ren_col (a INT64, b STRING, c BOOL)")
		env.mustExec(t, "jobs.insert", "INSERT INTO dataset1.ren_col (a, b, c) VALUES (1, 'x', true)")
		env.mustExec(t, "jobs.insert", "ALTER TABLE dataset1.ren_col RENAME COLUMN b TO bb")
		assertNames(t, "schema", env.tableSchemaNames(t, "dataset1", "ren_col"), "a", "bb", "c")
		rows := env.mustRead(t, "SELECT * FROM dataset1.ren_col")
		if len(rows) != 1 || !reflect.DeepEqual(rows[0], []bigquery.Value{int64(1), "x", true}) {
			t.Fatalf("rows: got %v", rows)
		}
	})

	t.Run("rename_table", func(t *testing.T) {
		env.mustExec(t, "jobs.insert", "CREATE TABLE dataset1.ren_old (a INT64, b STRING)")
		env.mustExec(t, "jobs.insert", "INSERT INTO dataset1.ren_old (a, b) VALUES (1, 'x'), (2, 'y')")
		env.mustExec(t, "jobs.insert", "ALTER TABLE dataset1.ren_old RENAME TO ren_new")
		env.assertTableNotFound(t, "dataset1", "ren_old")
		assertNames(t, "schema", env.tableSchemaNames(t, "dataset1", "ren_new"), "a", "b")
		if got := env.metadata(t, "dataset1", "ren_new").NumRows; got != 2 {
			t.Fatalf("numRows: got %d, want 2", got)
		}
		if rows := env.mustRead(t, "SELECT * FROM dataset1.ren_new"); len(rows) != 2 {
			t.Fatalf("rows: got %v", rows)
		}
		env.mustExec(t, "jobs.insert", "CREATE TABLE dataset1.ren_old (z INT64)")
		assertNames(t, "recreated", env.tableSchemaNames(t, "dataset1", "ren_old"), "z")
	})

	t.Run("alter_if_exists_missing_table", func(t *testing.T) {
		env.mustExec(t, "jobs.insert", "ALTER TABLE IF EXISTS dataset1.nope ADD COLUMN b STRING")
		env.mustExec(t, "jobs.query", "ALTER TABLE IF EXISTS dataset1.nope ADD COLUMN b STRING")
	})

	t.Run("alter_missing_table_fails", func(t *testing.T) {
		assertErrContains(t, env.runQueryJob(t, "ALTER TABLE dataset1.nope ADD COLUMN b STRING"), "Not found")
	})

	t.Run("preserves_column_description", func(t *testing.T) {
		if err := env.client.Dataset("dataset1").Table("desc").Create(env.ctx, &bigquery.TableMetadata{
			Schema: bigquery.Schema{
				{Name: "a", Type: bigquery.IntegerFieldType, Description: "keep me"},
			},
		}); err != nil {
			t.Fatal(err)
		}
		env.mustExec(t, "jobs.insert", "ALTER TABLE dataset1.desc ADD COLUMN b STRING")
		md := env.metadata(t, "dataset1", "desc")
		if len(md.Schema) != 2 || md.Schema[0].Description != "keep me" {
			t.Fatalf("schema: got %+v", md.Schema)
		}
	})

	t.Run("other_dataset_untouched", func(t *testing.T) {
		env.mustExec(t, "jobs.insert", "CREATE TABLE dataset1.same_alter (a INT64)")
		env.mustExec(t, "jobs.insert", "CREATE TABLE dataset2.same_alter (a INT64)")
		env.mustExec(t, "jobs.insert", "INSERT INTO dataset1.same_alter (a) VALUES (1)")
		env.mustExec(t, "jobs.insert", "ALTER TABLE dataset2.same_alter ADD COLUMN b STRING")
		assertNames(t, "dataset1", env.tableSchemaNames(t, "dataset1", "same_alter"), "a")
		assertNames(t, "dataset2", env.tableSchemaNames(t, "dataset2", "same_alter"), "a", "b")
		if rows := env.mustRead(t, "SELECT * FROM dataset1.same_alter"); len(rows) != 1 || len(rows[0]) != 1 {
			t.Fatalf("dataset1 rows: got %v", rows)
		}
	})
}

func TestAlterTableEdgeCases(t *testing.T) {
	env := newDDLTestEnv(t)

	t.Run("add_column_with_options", func(t *testing.T) {
		env.mustExec(t, "jobs.insert", "CREATE TABLE dataset1.opt (a INT64)")
		env.mustExec(t, "jobs.insert", "ALTER TABLE dataset1.opt ADD COLUMN b STRING OPTIONS(description='bd')")
		env.mustExec(t, "jobs.query", "ALTER TABLE dataset1.opt ADD COLUMN s STRUCT<x INT64 NOT NULL OPTIONS(description='xd'), y ARRAY<STRING>> OPTIONS(description='sd')")
		md := env.metadata(t, "dataset1", "opt")
		if len(md.Schema) != 3 {
			t.Fatalf("schema: got %+v", md.Schema)
		}
		if b := md.Schema[1]; b.Name != "b" || b.Type != bigquery.StringFieldType || b.Description != "bd" {
			t.Fatalf("b: got %+v", b)
		}
		st := md.Schema[2]
		if st.Type != bigquery.RecordFieldType || st.Description != "sd" || len(st.Schema) != 2 {
			t.Fatalf("s: got %+v", st)
		}
		if x := st.Schema[0]; x.Name != "x" || x.Description != "xd" || !x.Required {
			t.Fatalf("s.x: got %+v", x)
		}
		if y := st.Schema[1]; y.Name != "y" || !y.Repeated || y.Type != bigquery.StringFieldType {
			t.Fatalf("s.y: got %+v", y)
		}
		env.mustExec(t, "jobs.insert", "INSERT INTO dataset1.opt (a, b, s) VALUES (1, 'x', STRUCT(2, ['p']))")
		if rows := env.mustRead(t, "SELECT b, s.x FROM dataset1.opt"); len(rows) != 1 || rows[0][0] != "x" || rows[0][1] != int64(2) {
			t.Fatalf("rows: got %v", rows)
		}
	})

	t.Run("add_column_type_parameters", func(t *testing.T) {
		env.mustExec(t, "jobs.insert", "CREATE TABLE dataset1.params (a INT64)")
		env.mustExec(t, "jobs.insert", "ALTER TABLE dataset1.params ADD COLUMN s STRING(10), ADD COLUMN n NUMERIC(10, 2)")
		assertNames(t, "schema", env.tableSchemaNames(t, "dataset1", "params"), "a", "s", "n")
	})

	t.Run("add_column_default_unsupported", func(t *testing.T) {
		env.mustExec(t, "jobs.insert", "CREATE TABLE dataset1.def (a INT64)")
		assertErrContains(t, env.runQueryJob(t, "ALTER TABLE dataset1.def ADD COLUMN c STRING DEFAULT 'x'"), "DEFAULT is not supported")
		assertNames(t, "schema", env.tableSchemaNames(t, "dataset1", "def"), "a")
	})

	for _, mode := range ddlModes {
		t.Run("failed_rebuild_rolls_back/"+mode, func(t *testing.T) {
			table := "fail_" + strings.ReplaceAll(mode, ".", "_")
			env.mustExec(t, mode, "CREATE TABLE dataset1."+table+" (a INT64)")
			env.mustExec(t, mode, "INSERT INTO dataset1."+table+" (a) VALUES (1)")
			if err := env.exec(t, mode, "ALTER TABLE dataset1."+table+" ADD COLUMN b NO_SUCH_TYPE"); err == nil {
				t.Fatal("expected ALTER with an unknown type to fail")
			}
			assertNames(t, "schema", env.tableSchemaNames(t, "dataset1", table), "a")
			if rows := env.mustRead(t, "SELECT * FROM dataset1."+table); len(rows) != 1 || len(rows[0]) != 1 {
				t.Fatalf("rows: got %v", rows)
			}
			for _, id := range env.tableIDs(t, "dataset1") {
				if strings.HasPrefix(id, "__bqemu_alter_") {
					t.Fatalf("rebuild table %s left behind", id)
				}
			}
			env.mustExec(t, mode, "ALTER TABLE dataset1."+table+" ADD COLUMN b STRING")
			assertNames(t, "after retry", env.tableSchemaNames(t, "dataset1", table), "a", "b")
		})
	}

	t.Run("rename_via_jobs_query", func(t *testing.T) {
		env.mustExec(t, "jobs.query", "CREATE TABLE dataset1.rq (a INT64, b STRING)")
		env.mustExec(t, "jobs.query", "INSERT INTO dataset1.rq (a, b) VALUES (1, 'x')")
		env.mustExec(t, "jobs.query", "ALTER TABLE dataset1.rq RENAME COLUMN b TO c")
		assertNames(t, "schema", env.tableSchemaNames(t, "dataset1", "rq"), "a", "c")
		env.mustExec(t, "jobs.query", "ALTER TABLE dataset1.rq RENAME TO rq2")
		env.assertTableNotFound(t, "dataset1", "rq")
		if rows := env.mustRead(t, "SELECT c FROM dataset1.rq2"); len(rows) != 1 || rows[0][0] != "x" {
			t.Fatalf("rows: got %v", rows)
		}
	})

	t.Run("rename_table_keeps_metadata", func(t *testing.T) {
		if err := env.client.Dataset("dataset1").Table("keep").Create(env.ctx, &bigquery.TableMetadata{
			Description: "tdesc",
			Schema:      bigquery.Schema{{Name: "a", Type: bigquery.IntegerFieldType}},
		}); err != nil {
			t.Fatal(err)
		}
		created := env.metadata(t, "dataset1", "keep").CreationTime
		// Creation times have a resolution of one second.
		time.Sleep(1100 * time.Millisecond)
		env.mustExec(t, "jobs.insert", "ALTER TABLE dataset1.keep RENAME TO keep2")
		md := env.metadata(t, "dataset1", "keep2")
		if !md.CreationTime.Equal(created) {
			t.Fatalf("creationTime: got %v, want %v", md.CreationTime, created)
		}
		if md.Description != "tdesc" {
			t.Fatalf("description: got %q", md.Description)
		}
	})

	t.Run("partitioning_and_clustering_columns", func(t *testing.T) {
		if err := env.client.Dataset("dataset1").Table("pt").Create(env.ctx, &bigquery.TableMetadata{
			Schema: bigquery.Schema{
				{Name: "ts", Type: bigquery.TimestampFieldType},
				{Name: "c", Type: bigquery.StringFieldType},
				{Name: "v", Type: bigquery.IntegerFieldType},
			},
			TimePartitioning: &bigquery.TimePartitioning{Field: "ts", Type: bigquery.DayPartitioningType},
			Clustering:       &bigquery.Clustering{Fields: []string{"c"}},
		}); err != nil {
			t.Fatal(err)
		}
		assertErrContains(t, env.runQueryJob(t, "ALTER TABLE dataset1.pt DROP COLUMN ts"), "partitioning column")
		assertErrContains(t, env.runQueryJob(t, "ALTER TABLE dataset1.pt RENAME COLUMN TS TO ts2"), "partitioning column")
		assertErrContains(t, env.runQueryJob(t, "ALTER TABLE dataset1.pt DROP COLUMN c"), "clustering column")
		assertErrContains(t, env.runQueryJob(t, "ALTER TABLE dataset1.pt RENAME COLUMN c TO c2"), "clustering column")
		env.mustExec(t, "jobs.insert", "ALTER TABLE dataset1.pt DROP COLUMN v, ADD COLUMN z INT64")
		md := env.metadata(t, "dataset1", "pt")
		assertNames(t, "schema", env.tableSchemaNames(t, "dataset1", "pt"), "ts", "c", "z")
		if md.TimePartitioning == nil || md.TimePartitioning.Field != "ts" {
			t.Fatalf("timePartitioning: got %+v", md.TimePartitioning)
		}
		if md.Clustering == nil || !reflect.DeepEqual(md.Clustering.Fields, []string{"c"}) {
			t.Fatalf("clustering: got %+v", md.Clustering)
		}
	})

	t.Run("leading_comment", func(t *testing.T) {
		env.mustExec(t, "jobs.insert", "CREATE TABLE dataset1.cmt (a INT64)")
		env.mustExec(t, "jobs.insert", "-- add b\n/* and c */ alter table dataset1.cmt add column b STRING, add column c INT64")
		assertNames(t, "schema", env.tableSchemaNames(t, "dataset1", "cmt"), "a", "b", "c")
	})

	t.Run("multi_statement_script_is_noop", func(t *testing.T) {
		// Documented limitation: ALTER TABLE inside a multi-statement
		// script is left to googlesqlite, which runs it as a no-op.
		env.mustExec(t, "jobs.insert", "CREATE TABLE dataset1.ms (a INT64)")
		env.mustExec(t, "jobs.insert", "ALTER TABLE dataset1.ms ADD COLUMN b STRING; INSERT INTO dataset1.ms (a) VALUES (7)")
		assertNames(t, "schema", env.tableSchemaNames(t, "dataset1", "ms"), "a")
		if rows := env.mustRead(t, "SELECT * FROM dataset1.ms"); len(rows) != 1 || len(rows[0]) != 1 {
			t.Fatalf("rows: got %v", rows)
		}
	})

	t.Run("concurrent_alters", func(t *testing.T) {
		env.mustExec(t, "jobs.insert", "CREATE TABLE dataset1.conc (a INT64)")
		env.mustExec(t, "jobs.insert", "INSERT INTO dataset1.conc (a) VALUES (1)")
		const n = 4
		errs := make(chan error, n)
		for i := 0; i < n; i++ {
			go func(i int) {
				errs <- env.runQueryJob(t, fmt.Sprintf("ALTER TABLE dataset1.conc ADD COLUMN c%d STRING", i))
			}(i)
		}
		for i := 0; i < n; i++ {
			if err := <-errs; err != nil {
				t.Fatal(err)
			}
		}
		if names := env.tableSchemaNames(t, "dataset1", "conc"); len(names) != n+1 {
			t.Fatalf("schema: got %v", names)
		}
		if rows := env.mustRead(t, "SELECT * FROM dataset1.conc"); len(rows) != 1 || len(rows[0]) != n+1 {
			t.Fatalf("rows: got %v", rows)
		}
	})
}

// TestJobsQuerySingleFailingStatementIsAtomic checks that committing a
// failed query on jobs.query keeps only the statements before the failure:
// a single statement that fails part way through its rows changes nothing.
func TestJobsQuerySingleFailingStatementIsAtomic(t *testing.T) {
	for _, mode := range ddlModes {
		t.Run(mode, func(t *testing.T) {
			env := newDDLTestEnv(t)
			env.mustExec(t, mode, "CREATE TABLE dataset1.atomic (a INT64)")
			env.mustExec(t, mode, "INSERT INTO dataset1.atomic (a) VALUES (1), (2), (3)")
			want := [][]bigquery.Value{{int64(1)}, {int64(2)}, {int64(3)}}
			for _, sql := range []string{
				"INSERT INTO dataset1.atomic (a) SELECT IF(x = 3, ERROR('boom'), x + 100) FROM UNNEST([1, 2, 3]) AS x",
				"UPDATE dataset1.atomic SET a = IF(a = 3, ERROR('boom'), a + 10) WHERE TRUE",
				"DELETE FROM dataset1.atomic WHERE IF(a = 3, ERROR('boom'), TRUE)",
			} {
				assertErrContains(t, env.exec(t, mode, sql), "boom")
				if rows := env.mustRead(t, "SELECT a FROM dataset1.atomic ORDER BY a"); !reflect.DeepEqual(rows, want) {
					t.Fatalf("after %s: got %v, want %v", sql, rows, want)
				}
			}
		})
	}
}

// postQuery runs sql through jobs.query over raw REST and returns the HTTP
// status and the error reason, if any.
func (e *ddlTestEnv) postQuery(t *testing.T, sql string) (int, string) {
	t.Helper()
	body := fmt.Sprintf(`{"query":%q,"useLegacySql":false}`, sql)
	resp, err := http.Post(e.url+"/bigquery/v2/projects/test/queries", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	var res struct {
		Error *struct {
			Errors []struct {
				Reason string `json:"reason"`
			} `json:"errors"`
		} `json:"error"`
	}
	if err := json.Unmarshal(b, &res); err != nil {
		t.Fatalf("decode %s: %v", b, err)
	}
	if res.Error == nil || len(res.Error.Errors) == 0 {
		return resp.StatusCode, ""
	}
	return resp.StatusCode, res.Error.Errors[0].Reason
}

func (e *ddlTestEnv) jobCount(t *testing.T) int {
	t.Helper()
	n := 0
	it := e.client.Jobs(e.ctx)
	for {
		_, err := it.Next()
		if err == iterator.Done {
			return n
		}
		if err != nil {
			t.Fatal(err)
		}
		n++
	}
}

// TestAlterPlanErrorsLikeQueryErrors checks that an ALTER TABLE rejected by
// the emulator is reported like any other invalid query, on both endpoints.
func TestAlterPlanErrorsLikeQueryErrors(t *testing.T) {
	env := newDDLTestEnv(t)
	env.mustExec(t, "jobs.insert", "CREATE TABLE dataset1.perr (a INT64)")
	invalid := "SELEC 1"
	for n, rejected := range []string{
		"ALTER TABLE dataset1.nope ADD COLUMN b STRING",
		"ALTER TABLE dataset1.perr ADD COLUMN a STRING",
		"ALTER TABLE dataset1.perr ADD COLUMN c STRING DEFAULT 'x'",
	} {
		t.Run(rejected, func(t *testing.T) {
			// jobs.insert: a failed job is recorded for both.
			for i, sql := range []string{invalid, rejected} {
				code, msg := env.postJob(t, fmt.Sprintf("perr_%d_%d", n, i), sql)
				if code != http.StatusOK || msg == "" {
					t.Fatalf("jobs.insert %q: got HTTP %d %q, want 200 with an errorResult", sql, code, msg)
				}
			}
			// jobs.query: the same HTTP error, and the same effect on the
			// job list.
			before := env.jobCount(t)
			invalidCode, invalidReason := env.postQuery(t, invalid)
			afterInvalid := env.jobCount(t)
			rejectedCode, rejectedReason := env.postQuery(t, rejected)
			afterRejected := env.jobCount(t)
			if invalidCode == http.StatusOK || invalidCode != rejectedCode || invalidReason != rejectedReason {
				t.Fatalf("jobs.query: %q got HTTP %d %q, %q got HTTP %d %q", invalid, invalidCode, invalidReason, rejected, rejectedCode, rejectedReason)
			}
			if afterInvalid-before != afterRejected-afterInvalid {
				t.Fatalf("jobs.query registered %d job(s) for %q and %d for %q", afterInvalid-before, invalid, afterRejected-afterInvalid, rejected)
			}
		})
	}
}

func TestFailedScriptSyncsCatalog(t *testing.T) {
	for _, mode := range ddlModes {
		t.Run("create_then_error/"+mode, func(t *testing.T) {
			env := newDDLTestEnv(t)
			assertErrContains(t, env.exec(t, mode, "CREATE TABLE dataset1.fs_a (x INT64); SELECT ERROR('boom')"), "boom")
			assertNames(t, "schema", env.tableSchemaNames(t, "dataset1", "fs_a"), "x")
			code, msg := env.postJob(t, "drop_fs_a", "DROP TABLE dataset1.fs_a")
			if code != http.StatusOK || msg != "" {
				t.Fatalf("DROP TABLE: got HTTP %d %q", code, msg)
			}
			env.assertTableNotFound(t, "dataset1", "fs_a")
		})

		t.Run("drop_then_error/"+mode, func(t *testing.T) {
			env := newDDLTestEnv(t)
			env.mustExec(t, mode, "CREATE TABLE dataset1.fs_d (x INT64)")
			assertErrContains(t, env.exec(t, mode, "DROP TABLE dataset1.fs_d; SELECT ERROR('boom')"), "boom")
			env.assertTableNotFound(t, "dataset1", "fs_d")
			env.mustExec(t, mode, "CREATE TABLE dataset1.fs_d (x INT64)")
			assertNames(t, "schema", env.tableSchemaNames(t, "dataset1", "fs_d"), "x")
		})
	}

	t.Run("mid_script_failure_self_heals", func(t *testing.T) {
		env := newDDLTestEnv(t)
		if err := env.runQueryJob(t, "CREATE TABLE dataset1.fs_m (x INT64); INSERT INTO dataset1.fs_m SELECT IF(TRUE, ERROR('boom'), 1); SELECT 1"); err == nil {
			t.Fatal("expected the script to fail")
		}
		code, msg := env.postJob(t, "drop_fs_m", "DROP TABLE dataset1.fs_m")
		if code != http.StatusOK || msg != "" {
			t.Fatalf("DROP TABLE: got HTTP %d %q", code, msg)
		}
		for _, id := range env.tableIDs(t, "dataset1") {
			if id == "fs_m" {
				t.Fatal("fs_m is still listed after DROP TABLE")
			}
		}
	})

	t.Run("failed_job_reports_failure", func(t *testing.T) {
		env := newDDLTestEnv(t)
		job, err := env.client.Query("CREATE TABLE dataset1.fs_g (x INT64); SELECT ERROR('boom')").Run(env.ctx)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = job.Wait(env.ctx)
		// The stored job must still carry the query's error after the
		// partial catalog changes were synced.
		got, err := env.client.JobFromID(env.ctx, job.ID())
		if err != nil {
			t.Fatal(err)
		}
		_, err = got.Read(env.ctx)
		assertErrContains(t, err, "boom")
		assertNames(t, "schema", env.tableSchemaNames(t, "dataset1", "fs_g"), "x")
	})
}
