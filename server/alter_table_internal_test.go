package server

import (
	"context"
	"fmt"
	"reflect"
	"runtime/debug"
	"strings"
	"testing"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	"github.com/goccy/bigquery-emulator/internal/logger"
	"github.com/goccy/bigquery-emulator/types"
)

func TestLooksLikeAlter(t *testing.T) {
	for query, want := range map[string]bool{
		"ALTER TABLE t ADD COLUMN b STRING":            true,
		"alter\n\ttable t add column b string":         true,
		"-- c\n/* d */ ALTER  TABLE `t` DROP COLUMN b": true,
		"# c\nAlter Table t RENAME TO t2":              true,
		"ALTER TABLE\nt RENAME TO t2":                  true,
		"ALTER VIEW v SET OPTIONS(description='x')":    false,
		"ALTER SCHEMA s SET OPTIONS(description='x')":  false,
		"ALTERTABLE t":                                   false,
		"ALTER TABLES t":                                 false,
		"SELECT 'ALTER TABLE t'":                         false,
		"-- ALTER TABLE t ADD COLUMN b STRING":           false,
		"/* unterminated ALTER TABLE t ADD COLUMN b INT": false,
	} {
		if got := looksLikeAlter(query); got != want {
			t.Errorf("looksLikeAlter(%q) = %v, want %v", query, got, want)
		}
	}
}

type alterGuardEnv struct {
	ctx    context.Context
	server *Server
	client *bigquery.Client
}

func newAlterGuardEnv(t *testing.T) *alterGuardEnv {
	t.Helper()
	ctx := context.Background()
	s, err := New(TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Load(StructSource(types.NewProject("test", types.NewDataset("dataset1")))); err != nil {
		t.Fatal(err)
	}
	testServer := s.TestServer()
	client, err := bigquery.NewClient(ctx, "test", option.WithEndpoint(testServer.URL), option.WithoutAuthentication())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		setAlterBeforeRebuildHook(nil)
		client.Close()
		testServer.Close()
		s.Stop(ctx)
	})
	return &alterGuardEnv{ctx: ctx, server: s, client: client}
}

func (e *alterGuardEnv) exec(mode, sql string) error {
	if mode == "jobs.insert" {
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
	_, err := e.read(sql)
	return err
}

func (e *alterGuardEnv) read(sql string) ([][]bigquery.Value, error) {
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
			return nil, err
		}
		rows = append(rows, row)
	}
}

// execDirect runs sql the way jobs.query does, but without going through
// the HTTP handler. REST requests are served one at a time
// (sequentialAccessMiddleware), so a concurrent change to a table during an
// ALTER TABLE can only come from outside that path; a hook that issued a
// request would wait for the ALTER request forever.
func (e *alterGuardEnv) execDirect(sql string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v\n%s", r, debug.Stack())
		}
	}()
	ctx := logger.WithLogger(e.ctx, e.server.logger)
	conn, err := e.server.connMgr.Connection(ctx, "test", "")
	if err != nil {
		return err
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.RollbackIfNotCommitted()
	response, err := e.server.contentRepo.Query(ctx, tx, "test", "", sql, nil)
	if err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return syncQueryCatalog(ctx, e.server, "test", "", nil, response, nil, nil, nil)
}

func (e *alterGuardEnv) mustExec(t *testing.T, sql string) {
	t.Helper()
	if err := e.exec("jobs.insert", sql); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
}

// TestAlterTableGuardsAgainstConcurrentChange changes the table between
// planning an ALTER TABLE and running its rebuild. The ALTER must fail with
// a concurrent-modification error and leave the changed table as it is.
func TestAlterTableGuardsAgainstConcurrentChange(t *testing.T) {
	for _, mode := range []string{"jobs.insert", "jobs.query"} {
		t.Run("create_or_replace/"+mode, func(t *testing.T) {
			env := newAlterGuardEnv(t)
			env.mustExec(t, "CREATE TABLE dataset1.g (a INT64)")
			env.mustExec(t, "INSERT INTO dataset1.g (a) VALUES (1)")
			hookErrs := make(chan error, 1)
			setAlterBeforeRebuildHook(func(*alterTablePlan) {
				setAlterBeforeRebuildHook(nil)
				hookErrs <- env.execDirect("CREATE OR REPLACE TABLE dataset1.g AS SELECT 'new' AS x UNION ALL SELECT 'newer'")
			})
			err := env.exec(mode, "ALTER TABLE dataset1.g ADD COLUMN b STRING")
			select {
			case hookErr := <-hookErrs:
				if hookErr != nil {
					t.Fatalf("CREATE OR REPLACE in the hook: %v", hookErr)
				}
			default:
				t.Fatalf("the hook did not run; ALTER returned %v", err)
			}
			if err == nil || !strings.Contains(err.Error(), "modified concurrently") {
				t.Fatalf("ALTER: want a concurrent-modification error, got %v", err)
			}
			md, err := env.client.Dataset("dataset1").Table("g").Metadata(env.ctx)
			if err != nil {
				t.Fatal(err)
			}
			if len(md.Schema) != 1 || md.Schema[0].Name != "x" {
				t.Fatalf("schema: got %+v, want [x]", md.Schema)
			}
			rows, err := env.read("SELECT x FROM dataset1.g ORDER BY x")
			if err != nil {
				t.Fatal(err)
			}
			if len(rows) != 2 || rows[0][0] != "new" || rows[1][0] != "newer" {
				t.Fatalf("rows: got %v", rows)
			}
			it := env.client.Dataset("dataset1").Tables(env.ctx)
			for {
				table, err := it.Next()
				if err == iterator.Done {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
				if strings.HasPrefix(table.TableID, alterTmpTablePrefix) {
					t.Fatalf("rebuild table %s left behind", table.TableID)
				}
			}
			// The ALTER succeeds once retried against the new table.
			if err := env.exec(mode, "ALTER TABLE dataset1.g ADD COLUMN b STRING"); err != nil {
				t.Fatalf("retry: %v", err)
			}
		})
	}

	// Without the guard, this case loses data: the planned projection
	// still resolves against the replaced table and drops its new column.
	t.Run("create_or_replace_adds_column", func(t *testing.T) {
		env := newAlterGuardEnv(t)
		env.mustExec(t, "CREATE TABLE dataset1.s (a INT64)")
		hookErrs := make(chan error, 1)
		setAlterBeforeRebuildHook(func(*alterTablePlan) {
			setAlterBeforeRebuildHook(nil)
			hookErrs <- env.execDirect("CREATE OR REPLACE TABLE dataset1.s AS SELECT 1 AS a, 'keep' AS extra")
		})
		err := env.exec("jobs.insert", "ALTER TABLE dataset1.s ADD COLUMN b STRING")
		select {
		case hookErr := <-hookErrs:
			if hookErr != nil {
				t.Fatalf("CREATE OR REPLACE in the hook: %v", hookErr)
			}
		default:
			t.Fatalf("the hook did not run; ALTER returned %v", err)
		}
		if err == nil || !strings.Contains(err.Error(), "modified concurrently") {
			t.Fatalf("ALTER: want a concurrent-modification error, got %v", err)
		}
		rows, err := env.read("SELECT a, extra FROM dataset1.s")
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) != 1 || rows[0][1] != "keep" {
			t.Fatalf("rows: got %v", rows)
		}
	})

	t.Run("drop", func(t *testing.T) {
		env := newAlterGuardEnv(t)
		env.mustExec(t, "CREATE TABLE dataset1.d (a INT64)")
		hookErrs := make(chan error, 1)
		setAlterBeforeRebuildHook(func(*alterTablePlan) {
			setAlterBeforeRebuildHook(nil)
			hookErrs <- env.execDirect("DROP TABLE dataset1.d")
		})
		err := env.exec("jobs.insert", "ALTER TABLE dataset1.d RENAME COLUMN a TO b")
		select {
		case hookErr := <-hookErrs:
			if hookErr != nil {
				t.Fatalf("DROP in the hook: %v", hookErr)
			}
		default:
			t.Fatalf("the hook did not run; ALTER returned %v", err)
		}
		if err == nil || !strings.Contains(err.Error(), "modified concurrently") {
			t.Fatalf("ALTER: want a concurrent-modification error, got %v", err)
		}
	})
}

// TestAlterTableTempTableName checks that the rebuild's temporary table has
// a bounded name, whatever the length of the table name.
func TestAlterTableTempTableName(t *testing.T) {
	env := newAlterGuardEnv(t)
	long := "t" + strings.Repeat("x", 300)
	env.mustExec(t, "CREATE TABLE dataset1."+long+" (a INT64)")
	tmpIDs := make(chan string, 1)
	setAlterBeforeRebuildHook(func(plan *alterTablePlan) {
		setAlterBeforeRebuildHook(nil)
		tmpIDs <- plan.tmpRef.TableId
	})
	env.mustExec(t, "ALTER TABLE dataset1."+long+" ADD COLUMN b STRING")
	var tmpID string
	select {
	case tmpID = <-tmpIDs:
	default:
		t.Fatal("the hook did not run")
	}
	if !strings.HasPrefix(tmpID, alterTmpTablePrefix) || len(tmpID) > 64 || strings.Contains(tmpID, long) {
		t.Fatalf("temp table name %q", tmpID)
	}
}

func TestParseAlterTableNewActions(t *testing.T) {
	for _, c := range []struct {
		query string
		check func(*alterAction) bool
	}{
		{"ALTER TABLE ds.t SET OPTIONS(description='d', labels=[('k','v')])", func(a *alterAction) bool {
			return a.kind == alterSetTableOptions && len(a.options) == 2 && a.options[0].name == "description" && a.options[0].value == "d" && a.options[1].name == "labels"
		}},
		{"ALTER TABLE ds.t SET OPTIONS(expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY))", func(a *alterAction) bool {
			return a.kind == alterSetTableOptions && !a.options[0].isLiteral && a.options[0].exprText == "TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)"
		}},
		{"ALTER TABLE ds.t ALTER COLUMN IF EXISTS a SET OPTIONS(description=NULL)", func(a *alterAction) bool {
			return a.kind == alterSetColumnOptions && a.name == "a" && a.ifExists && a.options[0].isLiteral && a.options[0].value == nil
		}},
		{"ALTER TABLE ds.t ALTER COLUMN a DROP NOT NULL", func(a *alterAction) bool {
			return a.kind == alterDropNotNull && a.name == "a"
		}},
		{"ALTER TABLE ds.t ALTER COLUMN a SET DATA TYPE NUMERIC(10, 2)", func(a *alterAction) bool {
			return a.kind == alterSetDataType && a.newType.name == "NUMERIC" && reflect.DeepEqual(a.newType.params, []int64{10, 2})
		}},
		{"ALTER TABLE ds.t ALTER COLUMN a SET DATA TYPE integer", func(a *alterAction) bool {
			return a.kind == alterSetDataType && a.newType.name == "INT64" && len(a.newType.params) == 0
		}},
		{"ALTER TABLE ds.t ALTER COLUMN a SET DEFAULT CONCAT('a', 'b')", func(a *alterAction) bool {
			return a.kind == alterSetDefault && a.defaultText == "CONCAT('a', 'b')"
		}},
		{"ALTER TABLE ds.t ALTER COLUMN a DROP DEFAULT", func(a *alterAction) bool {
			return a.kind == alterDropDefault && a.name == "a"
		}},
		{"ALTER TABLE ds.t ADD PRIMARY KEY (a, b) NOT ENFORCED", func(a *alterAction) bool {
			return a.kind == alterAddPrimaryKey && reflect.DeepEqual(a.columns, []string{"a", "b"})
		}},
		{"ALTER TABLE ds.t DROP PRIMARY KEY IF EXISTS", func(a *alterAction) bool {
			return a.kind == alterDropPrimaryKey && a.ifExists
		}},
	} {
		stmt, ok, err := parseAlterTable(c.query)
		if err != nil || !ok || len(stmt.actions) != 1 || !c.check(stmt.actions[0]) {
			var action *alterAction
			if stmt != nil && len(stmt.actions) != 0 {
				action = stmt.actions[0]
			}
			t.Errorf("%s: got %+v, %v, %v", c.query, action, ok, err)
		}
	}
	for query, want := range map[string]string{
		"ALTER TABLE ds.t ADD CONSTRAINT fk FOREIGN KEY (a) REFERENCES ds.k(id) NOT ENFORCED": "ALTER TABLE ADD CONSTRAINT is not supported by the emulator",
		"ALTER TABLE ds.t DROP CONSTRAINT fk":                                                 "ALTER TABLE DROP CONSTRAINT is not supported by the emulator",
		"ALTER TABLE ds.t ADD PRIMARY KEY (a)":                                                "PRIMARY KEY must be NOT ENFORCED",
		"ALTER TABLE ds.t ALTER COLUMN a SET DATA TYPE ARRAY<INT64>":                          "SET DATA TYPE: this type is not supported",
		"ALTER TABLE ds.t ADD COLUMN b STRING, SET DEFAULT COLLATE 'und:ci'":                  "ALTER TABLE SET DEFAULT COLLATE is not supported by the emulator",
		"ALTER TABLE ds.t ALTER CONSTRAINT x NOT ENFORCED":                                    "is not supported by the emulator",
		"ALTER TABLE ds.t ADD COLUMN b STRING FILL USING 1":                                   "FILL USING is not supported",
	} {
		_, ok, err := parseAlterTable(query)
		if ok || err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: got %v, %v; want an error containing %q", query, ok, err, want)
		}
	}
	// SET OPTIONS with += is parsed; planAlterTable rejects it.
	stmt, ok, err := parseAlterTable("ALTER TABLE ds.t SET OPTIONS(labels += [('k','v')])")
	if err != nil || !ok || stmt.actions[0].options[0].assign {
		t.Fatalf("labels +=: got %+v, %v, %v", stmt, ok, err)
	}
	if err := checkAlterOption("ALTER TABLE SET OPTIONS", stmt.actions[0].options[0], tableOptionNames); err == nil {
		t.Fatal("checkAlterOption accepted +=")
	}
	// A parse failure is left to googlesqlite.
	if _, ok, err := parseAlterTable("ALTER TABLE ds.t ADD COLUMN"); ok || err != nil {
		t.Fatalf("syntax error: got %v, %v", ok, err)
	}
}

func TestSetDataTypeAllowed(t *testing.T) {
	typ := func(name string, params ...int64) *columnTypeName {
		return &columnTypeName{name: name, params: params}
	}
	for _, c := range []struct {
		from, to      *columnTypeName
		allowed, same bool
	}{
		{typ("INT64"), typ("NUMERIC"), true, false},
		{typ("INT64"), typ("BIGNUMERIC"), true, false},
		{typ("INT64"), typ("FLOAT64"), true, false},
		{typ("NUMERIC"), typ("BIGNUMERIC"), true, false},
		{typ("NUMERIC"), typ("FLOAT64"), true, false},
		{typ("INT64"), typ("INT64"), true, true},
		{typ("STRING", 10), typ("STRING", 20), true, false},
		{typ("STRING", 10), typ("STRING"), true, false},
		{typ("STRING", 10), typ("STRING", 10), true, true},
		{typ("BYTES", 10), typ("BYTES", 5), false, false},
		{typ("STRING"), typ("STRING", 10), false, false},
		{typ("NUMERIC", 10, 2), typ("NUMERIC", 12, 3), true, false},
		{typ("NUMERIC", 10, 2), typ("NUMERIC", 10, 3), false, false},
		{typ("NUMERIC", 10, 2), typ("NUMERIC"), true, false},
		{typ("STRING"), typ("INT64"), false, false},
		{typ("FLOAT64"), typ("INT64"), false, false},
		{typ("NUMERIC"), typ("INT64"), false, false},
		{typ("INT64"), typ("NUMERIC", 20, 0), false, false},
		{typ("TIMESTAMP"), typ("DATE"), false, false},
	} {
		allowed, same := setDataTypeAllowed(c.from, c.to)
		if allowed != c.allowed || same != c.same {
			t.Errorf("%s -> %s: got allowed=%v same=%v, want %v %v", c.from, c.to, allowed, same, c.allowed, c.same)
		}
	}
}

func TestValidateDefaultExpression(t *testing.T) {
	// A trailing comment is allowed: the generated SQL closes the
	// expression on a new line.
	for _, text := range []string{"'x'", "CURRENT_TIMESTAMP()", "CONCAT('a', 'b')", "1 + 2", "1 --", "1 #", "1 /* x */"} {
		if err := validateDefaultExpression(text); err != nil {
			t.Errorf("%s: %v", text, err)
		}
	}
	for _, text := range []string{"", "1); DROP TABLE x; SELECT (1", "1) NOT NULL, `zz` INT64 DEFAULT (2", "1, 2", "SELECT 1", "(SELECT 1)", "1 + (SELECT MAX(a) FROM t)", "1 /* x"} {
		if err := validateDefaultExpression(text); err == nil {
			t.Errorf("%q: want an error", text)
		}
	}
}

// TestAlterRebuildFailureKeepsTable checks that a failed ALTER TABLE leaves
// the table usable: rolling back a rebuild whose first step ran must not
// leave googlesqlite's analyzer catalog without the table.
func TestAlterRebuildFailureKeepsTable(t *testing.T) {
	env := newAlterGuardEnv(t)
	t.Cleanup(func() { setAlterFinishHook(nil) })
	check := func(t *testing.T, table string, wantRows int) {
		t.Helper()
		rows, err := env.read("SELECT a, b FROM dataset1." + table + " ORDER BY a")
		if err != nil || len(rows) != wantRows {
			t.Fatalf("SELECT: got %v, %v; want %d rows", rows, err, wantRows)
		}
		// The table keeps its DEFAULT and NOT NULL constraints.
		if err := env.exec("jobs.insert", fmt.Sprintf("INSERT INTO dataset1.%s (a) VALUES (%d)", table, 100+wantRows)); err != nil {
			t.Fatalf("INSERT: %v", err)
		}
		rows, err = env.read(fmt.Sprintf("SELECT b FROM dataset1.%s WHERE a = %d", table, 100+wantRows))
		if err != nil || len(rows) != 1 || rows[0][0] != "q" {
			t.Fatalf("DEFAULT: got %v, %v", rows, err)
		}
		if err := env.exec("jobs.insert", "INSERT INTO dataset1."+table+" (b) VALUES ('n')"); err == nil || !strings.Contains(err.Error(), "Required field a cannot be null") {
			t.Fatalf("NOT NULL: got %v", err)
		}
		it := env.client.Dataset("dataset1").Tables(env.ctx)
		for {
			tbl, err := it.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			if strings.HasPrefix(tbl.TableID, alterTmpTablePrefix) {
				t.Fatalf("rebuild table %s left behind", tbl.TableID)
			}
		}
	}
	for _, mode := range []string{"jobs.insert", "jobs.query"} {
		t.Run("wrong_type_default/"+mode, func(t *testing.T) {
			table := "rbd_" + strings.ReplaceAll(mode, ".", "_")
			env.mustExec(t, "CREATE TABLE dataset1."+table+" (a INT64 NOT NULL, b STRING DEFAULT 'q')")
			env.mustExec(t, "INSERT INTO dataset1."+table+" (a, b) VALUES (1, 'x'), (2, 'y')")
			err := env.exec(mode, "ALTER TABLE dataset1."+table+" ALTER COLUMN a SET DEFAULT 'abc'")
			if err == nil || !strings.Contains(err.Error(), "Invalid default value expression for column a: an expression of type STRING cannot be assigned to a column of type INT64") {
				t.Fatalf("ALTER: got %v", err)
			}
			check(t, table, 2)
			if err := env.exec(mode, "ALTER TABLE dataset1."+table+" ADD COLUMN c INT64"); err != nil {
				t.Fatalf("next ALTER: %v", err)
			}
			check(t, table, 3)
		})

		t.Run("second_step_fails/"+mode, func(t *testing.T) {
			table := "rbf_" + strings.ReplaceAll(mode, ".", "_")
			env.mustExec(t, "CREATE TABLE dataset1."+table+" (a INT64 NOT NULL, b STRING DEFAULT 'q')")
			env.mustExec(t, "INSERT INTO dataset1."+table+" (a, b) VALUES (1, 'x'), (2, 'y')")
			setAlterFinishHook(func(string) string {
				setAlterFinishHook(nil)
				return "SELECT ERROR('second step failed')"
			})
			err := env.exec(mode, "ALTER TABLE dataset1."+table+" ADD COLUMN c INT64")
			if err == nil || !strings.Contains(err.Error(), "second step failed") {
				t.Fatalf("ALTER: got %v", err)
			}
			md, err := env.client.Dataset("dataset1").Table(table).Metadata(env.ctx)
			if err != nil || len(md.Schema) != 2 {
				t.Fatalf("metadata: got %+v, %v", md, err)
			}
			check(t, table, 2)
			if err := env.exec(mode, "ALTER TABLE dataset1."+table+" ADD COLUMN c INT64"); err != nil {
				t.Fatalf("next ALTER: %v", err)
			}
			check(t, table, 3)
		})
	}

	// A defaultValueExpression of the metadata that is not a valid DEFAULT
	// (tables.patch input) is dropped instead of failing the rebuild.
	for i, expr := range []string{"1 --", "1 #", "(SELECT 1)", "1) NOT NULL, `zz` INT64 DEFAULT (2", "'abc'"} {
		t.Run(fmt.Sprintf("patched_default_%d", i), func(t *testing.T) {
			table := fmt.Sprintf("rbp_%d", i)
			env.mustExec(t, "CREATE TABLE dataset1."+table+" (a INT64 DEFAULT 5, b STRING)")
			env.mustExec(t, "INSERT INTO dataset1."+table+" (b) VALUES ('x')")
			tbl := env.client.Dataset("dataset1").Table(table)
			md, err := tbl.Metadata(env.ctx)
			if err != nil {
				t.Fatal(err)
			}
			schema := md.Schema
			schema[0].DefaultValueExpression = expr
			if _, err := tbl.Update(env.ctx, bigquery.TableMetadataToUpdate{Schema: schema}, ""); err != nil {
				t.Fatal(err)
			}
			env.mustExec(t, "ALTER TABLE dataset1."+table+" ADD COLUMN c INT64")
			env.mustExec(t, "INSERT INTO dataset1."+table+" (b) VALUES ('y')")
			rows, err := env.read("SELECT a, b, c FROM dataset1." + table + " ORDER BY b")
			if err != nil || len(rows) != 2 || rows[0][0] != int64(5) {
				t.Fatalf("rows: got %v, %v", rows, err)
			}
			// "1 --" and "1 #" are valid expressions and stay the
			// DEFAULT; the others are dropped.
			if want := map[int]any{0: int64(1), 1: int64(1)}[i]; rows[1][0] != want {
				t.Fatalf("default after the rebuild: got %v, want %v", rows[1][0], want)
			}
			if rows, err := env.read("SELECT * FROM dataset1." + table); err != nil || len(rows) != 2 || len(rows[0]) != 3 {
				t.Fatalf("SELECT *: got %v, %v", rows, err)
			}
		})
	}
}
