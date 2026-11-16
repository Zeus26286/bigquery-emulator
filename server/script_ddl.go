package server

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	googlesql "github.com/goccy/go-googlesql"
	"github.com/goccy/googlesqlite"
	bigqueryv2 "google.golang.org/api/bigquery/v2"

	"github.com/goccy/bigquery-emulator/internal/connection"
)

// A CREATE TABLE ... LIKE or ALTER TABLE at the top level of a multi-statement
// script runs at its place in the script: googlesqlite hands it to the
// statement handlers registered below, which run the same plans as for a
// standalone statement (create_table_like.go, alter_table.go) on the
// connection of the script, inside the job transaction. So the statement sees
// what the statements before it did, and the statements after it see its
// table.
//
// Unlike a standalone statement, whose metadata is written after the job
// committed, the handler writes the metadata in the job transaction, so that
// a later statement of the script finds it (e.g. ALTER TABLE on a table that
// CREATE TABLE ... LIKE created). The catalog sync after the job then leaves
// these tables alone, unless a later statement of the script replaced them.
//
// The metadata of the tables that the statements before a handled statement
// created or changed is written only after the job. So that the handled
// statement finds them (e.g. ALTER TABLE on a table the script created), the
// handler first syncs the catalog changes of those statements, in the job
// transaction (syncBefore).
//
// A handled statement is atomic, as when standalone: it runs in a savepoint,
// which a failure rolls back. googlesqlite's in-memory catalog does not roll
// back (goccy/googlesqlite#104), so the catalog of a failed statement is
// repaired once the job transaction ended (repairFailed).

type scriptDDLKey struct{}

// scriptDDL is the state of the statement handlers for one query.
type scriptDDL struct {
	server    *Server
	projectID string
	datasetID string
	params    []*bigqueryv2.QueryParameter
	dryRun    bool
	// ddl is the analysis of the script's text.
	ddl *ddlInfo

	mu sync.Mutex
	// handled are the statements the handlers ran, in order.
	handled []handledScriptDDL
	// failed are the plans of the statements that failed after they
	// started changing tables.
	failed []catalogRepairer
}

type handledScriptDDL struct {
	plan ddlRewritePlan
	// skip reports the tables whose metadata the statement wrote.
	skip skipTableFunc
	// specs are the table specs the statement added or updated.
	specs []*googlesqlite.TableSpec
	// before are the table specs that the statements before it added or
	// updated.
	before map[*googlesqlite.TableSpec]bool
}

type catalogRepairer interface {
	repairCatalog(ctx context.Context, server *Server)
}

func newScriptDDL(server *Server, projectID, datasetID string, params []*bigqueryv2.QueryParameter, dryRun bool, ddl *ddlInfo) *scriptDDL {
	return &scriptDDL{server: server, projectID: projectID, datasetID: datasetID, params: params, dryRun: dryRun, ddl: ddl}
}

// withScriptDDL makes the statement handlers run the CREATE TABLE ... LIKE and
// ALTER TABLE statements of a script queried with ctx.
func withScriptDDL(ctx context.Context, s *scriptDDL) context.Context {
	return context.WithValue(ctx, scriptDDLKey{}, s)
}

func scriptDDLFromContext(ctx context.Context) *scriptDDL {
	s, _ := ctx.Value(scriptDDLKey{}).(*scriptDDL)
	return s
}

func init() {
	googlesqlite.RegisterStatementHandler(googlesql.ResolvedNodeKindResolvedAlterTableStmt, handleScriptAlterTable)
	googlesqlite.RegisterStatementHandler(googlesql.ResolvedNodeKindResolvedCreateTableStmt, handleScriptCreateTableLike)
}

func handleScriptAlterTable(ctx context.Context, conn *sql.Conn, stmt *googlesqlite.Statement) error {
	s := scriptDDLFromContext(ctx)
	// A standalone ALTER TABLE reaches googlesqlite only when the emulator
	// left it to googlesqlite. A dry run analyzes ALTER TABLE only, as
	// when standalone.
	if s == nil || !stmt.InScript || s.dryRun {
		return googlesqlite.ErrStatementNotHandled
	}
	ctx = withScriptConn(ctx, conn)
	if err := s.syncBefore(ctx, stmt.Changed); err != nil {
		return err
	}
	plan, err := planAlterTable(ctx, s.server, s.projectID, s.datasetID, stmt.SQL, s.params)
	if err != nil {
		return err
	}
	if plan == nil {
		return googlesqlite.ErrStatementNotHandled
	}
	return s.run(ctx, plan, stmt.Changed, func(tx *rewriteTx) (*googlesqlite.ChangedCatalog, error) {
		var cat *googlesqlite.ChangedCatalog
		if plan.sql != "" {
			if err := plan.checkUnchanged(ctx, s.server, tx.tx); err != nil {
				return nil, err
			}
			response, err := plan.run(ctx, s.server, tx.tx, s.projectID, s.datasetID, s.params)
			if err != nil {
				return nil, tx.fail(err)
			}
			cat = response.ChangedCatalog
		}
		return cat, plan.applyMetadata(ctx, s.server, cat)
	})
}

func handleScriptCreateTableLike(ctx context.Context, conn *sql.Conn, stmt *googlesqlite.Statement) error {
	s := scriptDDLFromContext(ctx)
	if s == nil || !stmt.InScript {
		return googlesqlite.ErrStatementNotHandled
	}
	node, ok := stmt.Node.(*googlesql.ResolvedCreateTableStmt)
	if !ok {
		return googlesqlite.ErrStatementNotHandled
	}
	if like, _ := node.LikeTable(); like == nil {
		return googlesqlite.ErrStatementNotHandled
	}
	ddl, err := analyzeDDL(stmt.SQL, s.projectID, s.datasetID)
	if err != nil {
		return err
	}
	if ddl == nil || ddl.like == nil {
		return fmt.Errorf("CREATE TABLE ... LIKE: the emulator cannot parse %q", stmt.SQL)
	}
	ctx = withScriptConn(ctx, conn)
	if err := s.syncBefore(ctx, stmt.Changed); err != nil {
		return err
	}
	plan, err := planCreateTableLike(ctx, s.server, s.projectID, s.datasetID, ddl.like, s.params)
	if err != nil {
		return err
	}
	if s.dryRun {
		tx, err := beginDDLTx(ctx, s.server, s.projectID, s.datasetID)
		if err != nil {
			return err
		}
		defer tx.RollbackIfNotCommitted()
		_, err = plan.dryRun(ctx, s.server, tx, s.projectID, s.datasetID, s.params)
		return err
	}
	return s.run(ctx, plan, stmt.Changed, func(tx *rewriteTx) (*googlesqlite.ChangedCatalog, error) {
		response, err := plan.run(ctx, s.server, tx.tx, s.projectID, s.datasetID, s.params)
		if err != nil {
			return nil, tx.fail(err)
		}
		return response.ChangedCatalog, plan.applyMetadata(ctx, s.server)
	})
}

// scriptPlan is a plan the handlers run.
type scriptPlan interface {
	ddlRewritePlan
	catalogRepairer
}

// rewriteTx is the savepoint a handled statement runs in.
type rewriteTx struct {
	tx     *connection.Tx
	failed bool
}

// fail records that the statement failed after it started changing tables,
// so that the catalog is repaired after the job.
func (t *rewriteTx) fail(err error) error {
	t.failed = true
	return err
}

// syncBefore writes the metadata of the tables that the statements before a
// handled statement changed, in the job transaction. changed are the catalog
// changes of those statements. The metadata of the tables of earlier handled
// statements is already written.
func (s *scriptDDL) syncBefore(ctx context.Context, changed *googlesqlite.ChangedCatalog) error {
	if changed == nil || changed.Table == nil || !changed.Table.Changed() {
		return nil
	}
	if s.ddl != nil && len(changed.Table.Added)+len(changed.Table.Updated) != 0 {
		for _, create := range s.ddl.creates {
			create.evalOptions(ctx, s.server, s.projectID, s.datasetID, s.params)
		}
	}
	var skip skipTableFunc
	if plan := s.rewritePlan(changed); plan != nil {
		skip = plan.syncSkip()
	}
	_, err := syncCatalog(ctx, s.server, changed, skip, s.ddl)
	return err
}

// run runs a handled statement in a savepoint of the script's transaction:
// fn runs the plan and writes its metadata, and returns the catalog change
// of the plan. before are the catalog changes of the statements before it.
func (s *scriptDDL) run(ctx context.Context, plan scriptPlan, before *googlesqlite.ChangedCatalog, fn func(tx *rewriteTx) (*googlesqlite.ChangedCatalog, error)) error {
	tx, err := beginDDLTx(ctx, s.server, s.projectID, s.datasetID)
	if err != nil {
		return err
	}
	defer tx.RollbackIfNotCommitted()
	rtx := &rewriteTx{tx: tx}
	cat, err := fn(rtx)
	if err == nil {
		err = tx.Commit()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err != nil {
		if rtx.failed {
			s.failed = append(s.failed, plan)
		}
		return err
	}
	handled := handledScriptDDL{plan: plan, skip: plan.syncSkip(), before: map[*googlesqlite.TableSpec]bool{}}
	if alter, ok := plan.(*alterTablePlan); ok && alter.sql == "" && alter.changed {
		// A standalone ALTER TABLE that changes only the metadata
		// changes no table of the catalog, so it has nothing to skip.
		// In a script, an earlier statement may have created the table,
		// which the sync after the job would then write again.
		handled.skip = alter.skips
	}
	if before != nil && before.Table != nil {
		for _, spec := range append(append([]*googlesqlite.TableSpec{}, before.Table.Added...), before.Table.Updated...) {
			handled.before[spec] = true
		}
	}
	if cat != nil && cat.Table != nil {
		handled.specs = append(append(handled.specs, cat.Table.Added...), cat.Table.Updated...)
	}
	s.handled = append(s.handled, handled)
	return nil
}

// rewritePlan returns the plan that keeps the catalog sync of the job away
// from the tables of the handled statements, or nil. cat is the catalog
// change of the job.
func (s *scriptDDL) rewritePlan(cat *googlesqlite.ChangedCatalog) ddlRewritePlan {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.handled) == 0 {
		return nil
	}
	return &scriptRewrites{handled: append([]handledScriptDDL{}, s.handled...), cat: cat}
}

// repairFailed repairs googlesqlite's in-memory catalog after a handled
// statement failed. It must run after the job transaction ended.
func (s *scriptDDL) repairFailed(ctx context.Context) {
	if s == nil {
		return
	}
	s.mu.Lock()
	failed := s.failed
	s.failed = nil
	s.mu.Unlock()
	for _, plan := range failed {
		plan.repairCatalog(ctx, s.server)
	}
}

// scriptRewrites is the ddlRewritePlan of the handled statements of a script.
// Their metadata is already written, in the job transaction.
type scriptRewrites struct {
	handled []handledScriptDDL
	cat     *googlesqlite.ChangedCatalog
}

func (r *scriptRewrites) syncSkip() skipTableFunc {
	own := map[*googlesqlite.TableSpec]bool{}
	for _, h := range r.handled {
		for _, spec := range h.specs {
			own[spec] = true
		}
	}
	// replaced reports whether a statement of the script that is not
	// handled changed the table after the handled statement h.
	replaced := func(h handledScriptDDL, ref *bigqueryv2.TableReference) bool {
		if r.cat == nil || r.cat.Table == nil {
			return false
		}
		for _, specs := range [][]*googlesqlite.TableSpec{r.cat.Table.Added, r.cat.Table.Updated} {
			for _, spec := range specs {
				if findTableSpec([]*googlesqlite.TableSpec{spec}, ref) != nil && !own[spec] && !h.before[spec] {
					return true
				}
			}
		}
		return false
	}
	return func(projectID, datasetID, tableID string) bool {
		// The last handled statement on the table decides.
		for i := len(r.handled) - 1; i >= 0; i-- {
			h := r.handled[i]
			if h.skip == nil || !h.skip(projectID, datasetID, tableID) {
				continue
			}
			return !replaced(h, &bigqueryv2.TableReference{ProjectId: projectID, DatasetId: datasetID, TableId: tableID})
		}
		return false
	}
}

func (r *scriptRewrites) applyMetadataAfterCommit(context.Context, *Server, *googlesqlite.ChangedCatalog) error {
	return nil
}
