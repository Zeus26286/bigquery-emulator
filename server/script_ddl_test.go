package server_test

import (
	"reflect"
	"strings"
	"testing"

	"cloud.google.com/go/bigquery"
)

func TestScriptCreateTableLikeAndAlterTable(t *testing.T) {
	env := newDDLTestEnv(t)
	for _, mode := range ddlModes {
		suffix := "_" + strings.ReplaceAll(mode, ".", "_")
		src, dst := "dataset1.ssrc"+suffix, "dataset1.sdst"+suffix

		t.Run("like_then_alter/"+mode, func(t *testing.T) {
			env.mustExec(t, mode, "CREATE TABLE "+src+" (id INT64 NOT NULL OPTIONS(description='i'), d DATE) PARTITION BY d CLUSTER BY id OPTIONS(description='t')")
			env.mustExec(t, mode, `
CREATE TABLE `+dst+` LIKE `+src+`;
ALTER TABLE `+dst+` ADD COLUMN c STRING OPTIONS(description='c');
INSERT INTO `+dst+` (id, d, c) VALUES (1, '2024-01-01', 'x')`)
			md := env.metadata(t, "dataset1", strings.TrimPrefix(dst, "dataset1."))
			assertNames(t, "schema", schemaNames(md.Schema), "id", "d", "c")
			if id, c := md.Schema[0], md.Schema[2]; !id.Required || id.Description != "i" || c.Description != "c" {
				t.Fatalf("columns: got %+v, %+v", id, c)
			}
			if md.TimePartitioning == nil || md.TimePartitioning.Field != "d" {
				t.Fatalf("timePartitioning: got %+v", md.TimePartitioning)
			}
			if md.Clustering == nil || !reflect.DeepEqual(md.Clustering.Fields, []string{"id"}) {
				t.Fatalf("clustering: got %+v", md.Clustering)
			}
			if md.Description != "t" {
				t.Fatalf("description: got %q", md.Description)
			}
			if rows := env.mustRead(t, "SELECT id, c FROM "+dst); len(rows) != 1 || rows[0][1] != "x" {
				t.Fatalf("rows: got %v", rows)
			}
		})

		t.Run("statements_after_see_the_table/"+mode, func(t *testing.T) {
			table := "dataset1.sseen" + suffix
			env.mustExec(t, mode, "CREATE TABLE "+table+" (a INT64)")
			env.mustExec(t, mode, "INSERT INTO "+table+" (a) VALUES (1)")
			rows, err := env.readQuery(t, "ALTER TABLE "+table+" RENAME COLUMN a TO b; SELECT b FROM "+table)
			if err != nil {
				t.Fatal(err)
			}
			if len(rows) != 1 || rows[0][0] != int64(1) {
				t.Fatalf("rows: got %v", rows)
			}
			assertNames(t, "schema", env.tableSchemaNames(t, "dataset1", strings.TrimPrefix(table, "dataset1.")), "b")
		})

		t.Run("failed_statement_stops_the_script/"+mode, func(t *testing.T) {
			table := "dataset1.sfail" + suffix
			env.mustExec(t, mode, "CREATE TABLE "+table+" (a INT64)")
			err := env.exec(t, mode, `
INSERT INTO `+table+` (a) VALUES (1);
ALTER TABLE `+table+` ADD COLUMN a STRING;
INSERT INTO `+table+` (a) VALUES (2)`)
			assertErrContains(t, err, "already exists")
			assertNames(t, "schema", env.tableSchemaNames(t, "dataset1", strings.TrimPrefix(table, "dataset1.")), "a")
			// The statement before the failure is committed, as in
			// BigQuery; the one after it did not run.
			if rows := env.mustRead(t, "SELECT a FROM "+table); len(rows) != 1 || rows[0][0] != int64(1) {
				t.Fatalf("rows: got %v", rows)
			}
			env.mustExec(t, mode, "INSERT INTO "+table+" (a) VALUES (3)")
		})

		t.Run("later_statement_replaces_the_table/"+mode, func(t *testing.T) {
			table := "dataset1.srepl" + suffix
			env.mustExec(t, mode, "CREATE TABLE "+table+"_src (a INT64 OPTIONS(description='a'))")
			env.mustExec(t, mode, `
CREATE TABLE `+table+` LIKE `+table+`_src;
CREATE OR REPLACE TABLE `+table+` (x STRING)`)
			assertNames(t, "schema", env.tableSchemaNames(t, "dataset1", strings.TrimPrefix(table, "dataset1.")), "x")
		})
	}

	t.Run("tables_created_earlier_in_the_script", func(t *testing.T) {
		env.mustExec(t, "jobs.insert", `
CREATE TABLE dataset1.searly (a INT64 OPTIONS(description='a')) OPTIONS(description='t');
ALTER TABLE dataset1.searly SET OPTIONS (friendly_name = 'f');
CREATE TABLE dataset1.searly2 LIKE dataset1.searly;
ALTER TABLE dataset1.searly2 ADD COLUMN b STRING`)
		md := env.metadata(t, "dataset1", "searly")
		if md.Description != "t" || md.Name != "f" || md.Schema[0].Description != "a" {
			t.Fatalf("searly: got description %q, friendlyName %q, schema %+v", md.Description, md.Name, md.Schema)
		}
		md = env.metadata(t, "dataset1", "searly2")
		assertNames(t, "schema", schemaNames(md.Schema), "a", "b")
		if md.Description != "t" || md.Name != "f" || md.Schema[0].Description != "a" {
			t.Fatalf("searly2: got description %q, friendlyName %q, schema %+v", md.Description, md.Name, md.Schema)
		}
	})

	t.Run("every_alter_action", func(t *testing.T) {
		env.mustExec(t, "jobs.insert", "CREATE TABLE dataset1.sall (a INT64 NOT NULL, b INT64, c STRING, d STRING DEFAULT 'x')")
		for _, action := range []string{
			"ADD COLUMN e STRING",
			"DROP COLUMN e",
			"RENAME COLUMN c TO c2",
			"ALTER COLUMN b SET DATA TYPE NUMERIC",
			"ALTER COLUMN a DROP NOT NULL",
			"ALTER COLUMN c2 SET DEFAULT 'y'",
			"ALTER COLUMN d DROP DEFAULT",
			"SET OPTIONS (description = 'desc')",
			"ALTER COLUMN a SET OPTIONS (description = 'col')",
			"ADD PRIMARY KEY (a) NOT ENFORCED",
			"DROP PRIMARY KEY",
		} {
			if err := env.runQueryJob(t, "SELECT 1; ALTER TABLE dataset1.sall "+action); err != nil {
				t.Fatalf("%s: %v", action, err)
			}
		}
		md := env.metadata(t, "dataset1", "sall")
		assertNames(t, "schema", schemaNames(md.Schema), "a", "b", "c2", "d")
		if md.Description != "desc" || md.Schema[0].Description != "col" || md.Schema[0].Required || md.Schema[1].Type != bigquery.NumericFieldType {
			t.Fatalf("metadata: got %+v", md)
		}
		env.mustExec(t, "jobs.insert", "SELECT 1; ALTER TABLE dataset1.sall RENAME TO sall2")
		assertNames(t, "schema", env.tableSchemaNames(t, "dataset1", "sall2"), "a", "b", "c2", "d")
		env.assertTableNotFound(t, "dataset1", "sall")
	})

	t.Run("dry_run", func(t *testing.T) {
		env.mustExec(t, "jobs.insert", "CREATE TABLE dataset1.sdry_src (a INT64)")
		q := env.client.Query("SELECT 1; CREATE TABLE dataset1.sdry LIKE dataset1.sdry_src")
		q.DryRun = true
		job, err := q.Run(env.ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := job.LastStatus().Err(); err != nil {
			t.Fatal(err)
		}
		env.assertTableNotFound(t, "dataset1", "sdry")
	})
}

func schemaNames(schema bigquery.Schema) []string {
	var names []string
	for _, field := range schema {
		names = append(names, field.Name)
	}
	return names
}
