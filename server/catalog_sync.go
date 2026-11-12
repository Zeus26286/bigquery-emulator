package server

import (
	"context"
	"fmt"

	"github.com/goccy/go-googlesql"
	"github.com/goccy/googlesqlite"
	"go.uber.org/zap"
	bigqueryv2 "google.golang.org/api/bigquery/v2"

	"github.com/goccy/bigquery-emulator/internal/logger"
	"github.com/goccy/bigquery-emulator/internal/metadata"
	"github.com/goccy/bigquery-emulator/types"
)

// catalogSyncResult tells afterCatalogSync what googlesqlite's analyzer
// catalog needs after syncCatalog.
type catalogSyncResult struct {
	// needsRefresh is set when a table or view was replaced.
	needsRefresh bool
	// repairTables are the existing tables a CREATE TABLE IF NOT EXISTS
	// left alone. googlesqlite recorded the statement's columns for them
	// anyway, so their specs are restored from these.
	repairTables []*bigqueryv2.Table
}

// skipTableFunc reports whether syncCatalog must leave the metadata of a
// table alone because the caller maintains it.
type skipTableFunc func(projectID, datasetID, tableID string) bool

// syncCatalog makes the emulator's table metadata reflect the DDL a query
// committed. It is idempotent: re-adding a table replaces it, dropping an
// unknown table is a no-op, and temp tables are ignored. That way it also
// repairs metadata that drifted earlier, e.g. after a script that failed
// before its catalog changes could be read.
//
// ddl is the analysis of the query's text, which adds what the statements
// say beyond the column names and types (partitioning, options, ...); it may
// be nil.
func syncCatalog(ctx context.Context, server *Server, cat *googlesqlite.ChangedCatalog, skip skipTableFunc, ddl *ddlInfo) (*catalogSyncResult, error) {
	result := &catalogSyncResult{}
	if cat == nil || cat.Table == nil || !cat.Table.Changed() {
		return result, nil
	}
	filter := func(specs []*googlesqlite.TableSpec) []*googlesqlite.TableSpec {
		var filtered []*googlesqlite.TableSpec
		for _, spec := range specs {
			// Temp tables live only for the script that created them and
			// belong to no dataset. Their cleanup is googlesqlite's job
			// (see goccy/googlesqlite#92 and goccy/googlesqlite#96).
			if spec.IsTemp {
				continue
			}
			if len(spec.NamePath) != 3 {
				logger.Logger(ctx).Debug("skip table with unexpected name path", zap.Strings("namePath", spec.NamePath))
				continue
			}
			if skip != nil && skip(spec.NamePath[0], spec.NamePath[1], spec.NamePath[2]) {
				continue
			}
			filtered = append(filtered, spec)
		}
		return filtered
	}
	deleted := filter(cat.Table.Deleted)
	added := filter(append(append([]*googlesqlite.TableSpec{}, cat.Table.Added...), cat.Table.Updated...))
	if len(deleted) == 0 && len(added) == 0 {
		return result, nil
	}

	// Load every project before writing anything: FindProject reads the
	// metadata through another connection, which can be blocked by the
	// uncommitted writes of the transaction below.
	projects := map[string]*metadata.Project{}
	var firstProjectID string
	for _, spec := range append(append([]*googlesqlite.TableSpec{}, deleted...), added...) {
		projectID := spec.NamePath[0]
		if _, exists := projects[projectID]; exists {
			continue
		}
		project, err := server.metaRepo.FindProject(ctx, projectID)
		if err != nil {
			return nil, err
		}
		projects[projectID] = project
		if firstProjectID == "" {
			firstProjectID = projectID
		}
	}
	findDataset := func(spec *googlesqlite.TableSpec) (*metadata.Project, *metadata.Dataset) {
		project := projects[spec.NamePath[0]]
		if project == nil {
			return nil, nil
		}
		return project, project.Dataset(spec.NamePath[1])
	}

	conn, err := server.connMgr.Connection(ctx, firstProjectID, "")
	if err != nil {
		return nil, err
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.RollbackIfNotCommitted()

	// Deletions first, so that a name they free can be taken by an
	// addition. googlesqlite already folds a DROP and a CREATE of the same
	// name within one script into a single change.
	for _, spec := range deleted {
		_, dataset := findDataset(spec)
		if dataset == nil {
			continue
		}
		tableID := spec.NamePath[2]
		if dataset.Table(tableID) == nil {
			continue
		}
		if err := dataset.DeleteTable(ctx, tx.Tx(), tableID); err != nil {
			return nil, err
		}
	}
	for _, spec := range added {
		project, dataset := findDataset(spec)
		if dataset == nil {
			logger.Logger(ctx).Warn("skip table of unknown dataset", zap.Strings("namePath", spec.NamePath))
			continue
		}
		tableID := spec.NamePath[2]
		if existing := dataset.Table(tableID); existing != nil {
			if spec.CreateMode == googlesql.ResolvedCreateStatementEnums_CreateModeCreateIfNotExists {
				// CREATE ... IF NOT EXISTS on an existing table changes
				// nothing, so its metadata stays.
				if !existing.IsView() {
					content, err := existing.Content()
					if err != nil {
						return nil, err
					}
					result.repairTables = append(result.repairTables, content)
				}
				continue
			}
			if err := dataset.DeleteTable(ctx, tx.Tx(), tableID); err != nil {
				return nil, err
			}
		}
		fields := make([]*bigqueryv2.TableFieldSchema, 0, len(spec.Columns))
		for _, column := range spec.Columns {
			field := types.TableFieldSchemaFromColumnType(column.Name, column.Type)
			// googlesqlite enforces NOT NULL, so the column is REQUIRED
			// even when the query's text could not be analyzed.
			if column.IsNotNull && field.Mode != "REPEATED" {
				field.Mode = "REQUIRED"
			}
			fields = append(fields, field)
		}
		table := &bigqueryv2.Table{
			TableReference: &bigqueryv2.TableReference{
				ProjectId: project.ID,
				DatasetId: dataset.ID,
				TableId:   tableID,
			},
			Schema: &bigqueryv2.TableSchema{Fields: fields},
		}
		// A view created by a CREATE VIEW DDL statement must be recorded as a
		// view so it is typed correctly and dropped with DROP VIEW.
		if spec.IsView {
			table.View = &bigqueryv2.ViewDefinition{Query: spec.Query}
		}
		if create := ddl.lookup(spec.NamePath, spec.IsView); create != nil {
			create.apply(ctx, table)
		}
		if !spec.IsView && table.TableConstraints == nil && len(spec.PrimaryKey) != 0 {
			table.TableConstraints = &bigqueryv2.TableConstraints{
				PrimaryKey: &bigqueryv2.TableConstraintsPrimaryKey{Columns: spec.PrimaryKey},
			}
		}
		if _, err := createTableMetadata(ctx, tx, server, project, dataset, table); err != nil {
			return nil, err
		}
		if spec.CreateMode == googlesql.ResolvedCreateStatementEnums_CreateModeCreateOrReplace {
			result.needsRefresh = true
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return result, nil
}

// afterCatalogSync brings googlesqlite's in-memory analyzer catalog in line
// with the tables after syncCatalog. Failures are logged and do not fail the
// request: the query itself already succeeded or failed on its own.
func afterCatalogSync(ctx context.Context, server *Server, projectID, datasetID string, res *catalogSyncResult) {
	if res == nil || (!res.needsRefresh && len(res.repairTables) == 0) {
		return
	}
	if err := refreshAnalyzerCatalog(ctx, server, projectID, datasetID, res); err != nil {
		logger.Logger(ctx).Warn("failed to refresh the analyzer catalog", zap.Error(err))
	}
}

func refreshAnalyzerCatalog(ctx context.Context, server *Server, projectID, datasetID string, res *catalogSyncResult) error {
	conn, err := server.connMgr.Connection(ctx, projectID, datasetID)
	if err != nil {
		return err
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.RollbackIfNotCommitted()
	// Restore the specs before the refresh, which would otherwise make the
	// analyzer adopt the columns of the CREATE TABLE IF NOT EXISTS.
	for _, table := range res.repairTables {
		if table.ExternalDataConfiguration != nil || table.Schema == nil || len(table.Schema.Fields) == 0 {
			continue
		}
		if err := server.contentRepo.RestoreTableSpec(ctx, tx, table); err != nil {
			return fmt.Errorf("table %s: %w", table.Id, err)
		}
	}
	if err := server.contentRepo.RefreshCatalog(ctx, tx); err != nil {
		return err
	}
	return tx.Commit()
}
