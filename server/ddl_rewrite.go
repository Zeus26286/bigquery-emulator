package server

import (
	"context"

	"github.com/goccy/googlesqlite"
)

// ddlRewritePlan is a DDL statement the emulator runs itself instead of
// leaving it to googlesqlite: ALTER TABLE or CREATE TABLE ... LIKE.
type ddlRewritePlan interface {
	// syncSkip returns the tables whose metadata syncCatalog must leave to
	// applyMetadataAfterCommit, or nil.
	syncSkip() skipTableFunc
	// applyMetadataAfterCommit updates the table metadata after the job
	// committed. cat is the catalog change of the job.
	applyMetadataAfterCommit(ctx context.Context, server *Server, cat *googlesqlite.ChangedCatalog) error
}

func (p *alterTablePlan) syncSkip() skipTableFunc {
	if p.sql == "" {
		return nil
	}
	return p.skips
}

func (p *alterTablePlan) applyMetadataAfterCommit(ctx context.Context, server *Server, cat *googlesqlite.ChangedCatalog) error {
	if !p.changed {
		return nil
	}
	return p.applyMetadata(ctx, server, cat)
}

// rewritePlan returns the plan of the query, or nil. It never returns a nil
// pointer in a non-nil interface.
func rewritePlan(alterPlan *alterTablePlan, likePlan *createLikePlan) ddlRewritePlan {
	switch {
	case alterPlan != nil:
		return alterPlan
	case likePlan != nil:
		return likePlan
	}
	return nil
}
