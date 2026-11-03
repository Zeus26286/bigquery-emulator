package server

import (
	"context"
	"fmt"
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
	return syncQueryCatalog(ctx, e.server, "test", "", response, nil, nil)
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
