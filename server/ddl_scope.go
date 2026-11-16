package server

import (
	"context"
	"database/sql"

	"github.com/goccy/bigquery-emulator/internal/connection"
	"github.com/goccy/bigquery-emulator/internal/metadata"
)

// A standalone CREATE TABLE ... LIKE or ALTER TABLE is planned before the job
// transaction starts, and its metadata is written after the job committed,
// each on a connection of its own. Inside a multi-statement script, the
// statement runs in a googlesqlite statement handler (script_ddl.go), in the
// middle of the job transaction: another connection could not read what the
// script changed, and would fail on the tables the transaction locks. The
// helpers below therefore use the connection of the script when ctx carries
// one (withScriptConn), and a connection of their own otherwise.

type scriptConnKey struct{}

// withScriptConn makes the DDL helpers run on conn, the connection of the
// script a statement handler runs in.
func withScriptConn(ctx context.Context, conn *sql.Conn) context.Context {
	return context.WithValue(ctx, scriptConnKey{}, conn)
}

func scriptConnFromContext(ctx context.Context) *sql.Conn {
	conn, _ := ctx.Value(scriptConnKey{}).(*sql.Conn)
	return conn
}

// beginDDLTx starts a transaction for the read-only queries of planning or
// for writing metadata. Inside a script it is a savepoint of the job
// transaction; otherwise it is a transaction of its own.
func beginDDLTx(ctx context.Context, server *Server, projectID, datasetID string) (*connection.Tx, error) {
	if conn := scriptConnFromContext(ctx); conn != nil {
		c := &connection.Conn{ProjectID: projectID, DatasetID: datasetID, Conn: conn}
		return c.BeginKeepingConn(ctx)
	}
	conn, err := server.connMgr.Connection(ctx, projectID, datasetID)
	if err != nil {
		return nil, err
	}
	return conn.Begin(ctx)
}

// findProjectForDDL returns the metadata of a project, as the script sees it
// inside a script.
func findProjectForDDL(ctx context.Context, server *Server, id string) (*metadata.Project, error) {
	if scriptConnFromContext(ctx) == nil {
		return server.metaRepo.FindProject(ctx, id)
	}
	tx, err := beginDDLTx(ctx, server, id, "")
	if err != nil {
		return nil, err
	}
	defer tx.RollbackIfNotCommitted()
	return server.metaRepo.FindProjectWithConn(ctx, tx.Tx(), id)
}
