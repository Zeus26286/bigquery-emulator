package server

import (
	"context"
	"fmt"
	"strings"

	"github.com/goccy/go-json"
	"github.com/goccy/googlesqlite"
	"go.uber.org/zap"
	bigqueryv2 "google.golang.org/api/bigquery/v2"

	"github.com/goccy/bigquery-emulator/internal/connection"
	"github.com/goccy/bigquery-emulator/internal/contentdata"
	"github.com/goccy/bigquery-emulator/internal/logger"
	internaltypes "github.com/goccy/bigquery-emulator/internal/types"
	"github.com/goccy/bigquery-emulator/types"
)

// googlesqlite rejects CREATE TABLE ... LIKE. A standalone CREATE TABLE ...
// LIKE statement is therefore run here, the way ALTER TABLE is
// (alter_table.go): it is planned from the table metadata before the job
// transaction starts, its steps run inside the job transaction, and the table
// metadata is written from the plan once the job committed.
//
// The new table copies the source table's metadata: its schema, with the
// modes, descriptions, defaults and type parameters of every field, its
// partitioning, clustering, table options and table constraints. The
// PARTITION BY, CLUSTER BY and OPTIONS clauses of the statement override
// them. The SQLite table is created with the top-level columns, their NOT
// NULL and DEFAULT constraints only, which is what googlesqlite can enforce;
// the rest lives in the metadata. With an AS query, its columns must match
// the columns of the source table by name, and its rows are inserted into the
// new table.
//
// CREATE TABLE ... LIKE inside a multi-statement script is rejected
// (analyzeDDL). Statements are serialized with ALTER TABLE (Server.alterMu).
//
// TODO(googlesqlite): goccy/googlesqlite#74 (PR #75) adds CREATE TABLE LIKE.
// When upgrading to a version that has it, make sure that LIKE still reaches
// this layer first, since googlesqlite would not copy the metadata.

// likeTmpTablePrefix starts the name of the table the rows of an AS query go
// through.
const likeTmpTablePrefix = "__bqemu_like_"

// createLikePlan is a standalone CREATE TABLE ... LIKE statement rewritten
// into steps and a metadata update.
type createLikePlan struct {
	like *createLikeDDL
	// noop is set for CREATE TABLE IF NOT EXISTS on an existing table.
	noop bool
	// table is the metadata of the new table.
	table *bigqueryv2.Table
	// columns are the column definitions of the new SQLite table.
	columns []contentdata.ColumnDefinition
	// srcColumns are the top-level columns of the source table, which the
	// columns of an AS query must match.
	srcColumns []string
	// replace is set for CREATE OR REPLACE on an existing table, and oldDst
	// is then that table's metadata.
	replace bool
	oldDst  *bigqueryv2.Table
	// oldDefaults are the DEFAULT expressions of the replaced table's
	// metadata, by lower-cased column name, that can be given back to its
	// columns if the statement fails (repairCatalog).
	oldDefaults map[string]string
	// tmpRef is the table the rows of an AS query go through, or nil.
	tmpRef *bigqueryv2.TableReference
	// What the steps changed in googlesqlite's catalog, which repairCatalog
	// undoes when they are rolled back: oldDstSpec is the spec of the
	// replaced table the DROP reported, dstAdded and tmpAdded are set when
	// the new table and the temporary table were created.
	oldDstSpec *googlesqlite.TableSpec
	dstAdded   bool
	tmpAdded   bool
}

// planCreateTableLike plans a CREATE TABLE ... LIKE statement. It reads the
// metadata and evaluates option values and defaults through other
// connections, so it must run before the job transaction starts. projectID
// and datasetID are the query's defaults, and params its parameters.
func planCreateTableLike(ctx context.Context, server *Server, projectID, datasetID string, like *createLikeDDL, params []*bigqueryv2.QueryParameter) (*createLikePlan, error) {
	srcRef, dstRef := like.srcRef, like.ref
	srcName := fmt.Sprintf("%s:%s.%s", srcRef.ProjectId, srcRef.DatasetId, srcRef.TableId)
	dstName := fmt.Sprintf("%s:%s.%s", dstRef.ProjectId, dstRef.DatasetId, dstRef.TableId)

	srcProject, err := server.metaRepo.FindProject(ctx, srcRef.ProjectId)
	if err != nil {
		return nil, err
	}
	if srcProject == nil || srcProject.Dataset(srcRef.DatasetId) == nil {
		return nil, errNotFound(fmt.Sprintf("Not found: Dataset %s:%s", srcRef.ProjectId, srcRef.DatasetId))
	}
	srcTable := srcProject.Dataset(srcRef.DatasetId).Table(srcRef.TableId)
	if srcTable == nil {
		return nil, errNotFound(fmt.Sprintf("Not found: Table %s", srcName))
	}
	if srcTable.IsView() {
		return nil, fmt.Errorf("CREATE TABLE LIKE is not supported by the emulator for view %s", srcName)
	}
	src, err := srcTable.Content()
	if err != nil {
		return nil, err
	}
	if src.ExternalDataConfiguration != nil || src.View != nil || src.MaterializedView != nil || src.Schema == nil || len(src.Schema.Fields) == 0 {
		return nil, fmt.Errorf("CREATE TABLE LIKE is not supported by the emulator for table %s", srcName)
	}
	for _, field := range src.Schema.Fields {
		if strings.ContainsAny(field.Name, "`\\") {
			return nil, fmt.Errorf("invalid column name %q", field.Name)
		}
	}

	plan := &createLikePlan{like: like}
	// checkDatasets checked the dataset of the new table.
	dstProject := srcProject
	if dstRef.ProjectId != srcRef.ProjectId {
		if dstProject, err = server.metaRepo.FindProject(ctx, dstRef.ProjectId); err != nil {
			return nil, err
		}
	}
	if dstProject == nil || dstProject.Dataset(dstRef.DatasetId) == nil {
		return nil, errNotFound(fmt.Sprintf("Not found: Dataset %s:%s", dstRef.ProjectId, dstRef.DatasetId))
	}
	if existing := dstProject.Dataset(dstRef.DatasetId).Table(dstRef.TableId); existing != nil {
		switch {
		case like.ifNotExists:
			plan.noop = true
			return plan, nil
		case !like.orReplace:
			return nil, errDuplicate(fmt.Sprintf("Already Exists: Table %s", dstName))
		case existing.IsView():
			return nil, fmt.Errorf("%s is a view, not a table", dstName)
		}
		oldDst, err := existing.Content()
		if err != nil {
			return nil, err
		}
		if oldDst.ExternalDataConfiguration != nil {
			return nil, fmt.Errorf("CREATE OR REPLACE TABLE LIKE is not supported by the emulator for the external table %s", dstName)
		}
		plan.replace = true
		plan.oldDst = oldDst
		plan.oldDefaults = checkedDefaults(ctx, server, dstRef, oldDst)
	}

	// Evaluate the option values before the job transaction starts. An
	// option that cannot be evaluated is left out with a warning, as for
	// CREATE TABLE.
	for _, option := range like.options {
		if !option.assign || !option.needsEval() {
			continue
		}
		if err := evalOptionExpr(ctx, server, projectID, datasetID, params, option); err != nil {
			logger.Logger(ctx).Warn("table option is not recorded in the table metadata",
				zap.String("table", dstRef.TableId), zap.String("option", option.name), zap.Error(err))
		}
	}
	table, err := buildLikeTable(ctx, src, like)
	if err != nil {
		return nil, err
	}
	columns, err := likeColumnDefinitions(table.Schema)
	if err != nil {
		return nil, err
	}
	// The defaultValueExpression of the metadata can come from the
	// tables.insert and tables.patch input: an invalid one is dropped with a
	// warning, from the new table and from its metadata.
	for i := range columns {
		if columns[i].Default == "" {
			continue
		}
		if err := checkDefaultExpr(ctx, server, dstRef.ProjectId, dstRef.DatasetId, columns[i].Name, columns[i].Type, columns[i].Default); err != nil {
			logger.Logger(ctx).Warn("invalid defaultValueExpression of the source table is not copied by CREATE TABLE LIKE",
				zap.String("table", dstRef.TableId), zap.String("column", columns[i].Name), zap.Error(err))
			columns[i].Default = ""
			table.Schema.Fields[i].DefaultValueExpression = ""
		}
	}
	plan.table = table
	plan.columns = columns
	for _, field := range table.Schema.Fields {
		plan.srcColumns = append(plan.srcColumns, field.Name)
	}
	if like.queryText != "" {
		// A short fixed prefix and a random suffix keep the name bounded
		// whatever the length of the table name.
		plan.tmpRef = &bigqueryv2.TableReference{ProjectId: dstRef.ProjectId, DatasetId: dstRef.DatasetId, TableId: likeTmpTablePrefix + randomID()}
	}
	return plan, nil
}

// likeTableFields are the fields of a table's metadata a new table copies.
// The others (identity, times, statistics, view, snapshot, ...) are the
// source table's own; createTableMetadata sets the new table's.
func likeTableFields(src *bigqueryv2.Table) *bigqueryv2.Table {
	return &bigqueryv2.Table{
		Schema:                  src.Schema,
		TimePartitioning:        src.TimePartitioning,
		RangePartitioning:       src.RangePartitioning,
		Clustering:              src.Clustering,
		RequirePartitionFilter:  src.RequirePartitionFilter,
		Description:             src.Description,
		FriendlyName:            src.FriendlyName,
		Labels:                  src.Labels,
		ExpirationTime:          src.ExpirationTime,
		EncryptionConfiguration: src.EncryptionConfiguration,
		DefaultCollation:        src.DefaultCollation,
		DefaultRoundingMode:     src.DefaultRoundingMode,
		MaxStaleness:            src.MaxStaleness,
		TableConstraints:        src.TableConstraints,
	}
}

// buildLikeTable returns the metadata of the table a CREATE TABLE ... LIKE
// creates: a copy of the source table's, with the clauses of the statement
// applied. The option values must have been evaluated.
func buildLikeTable(ctx context.Context, src *bigqueryv2.Table, like *createLikeDDL) (*bigqueryv2.Table, error) {
	encoded, err := json.Marshal(likeTableFields(src))
	if err != nil {
		return nil, err
	}
	table := &bigqueryv2.Table{}
	if err := json.Unmarshal(encoded, table); err != nil {
		return nil, err
	}
	table.TableReference = &bigqueryv2.TableReference{
		ProjectId: like.ref.ProjectId,
		DatasetId: like.ref.DatasetId,
		TableId:   like.ref.TableId,
	}
	var fields []*bigqueryv2.TableFieldSchema
	if table.Schema != nil {
		fields = table.Schema.Fields
	}
	findField := func(name string) *bigqueryv2.TableFieldSchema {
		for _, field := range fields {
			if strings.EqualFold(field.Name, name) {
				return field
			}
		}
		return nil
	}

	if like.partitionUnrecognized {
		return nil, fmt.Errorf("CREATE TABLE LIKE: this PARTITION BY expression is not supported by the emulator")
	}
	if p := like.partition; p != nil {
		column := ""
		if p.column != "" {
			field := findField(p.column)
			if field == nil {
				return nil, fmt.Errorf("Unrecognized name: %s", p.column)
			}
			if !partitionColumnTypeOK(p, field) {
				return nil, fmt.Errorf("PARTITION BY expression does not apply to column %s of type %s", field.Name, field.Type)
			}
			column = field.Name
		}
		var expirationMs int64
		if table.TimePartitioning != nil {
			expirationMs = table.TimePartitioning.ExpirationMs
		}
		table.TimePartitioning, table.RangePartitioning = nil, nil
		if p.kind == "range" {
			table.RangePartitioning = &bigqueryv2.RangePartitioning{
				Field: column,
				Range: &bigqueryv2.RangePartitioningRange{Start: p.start, End: p.end, Interval: p.interval},
			}
		} else {
			table.TimePartitioning = &bigqueryv2.TimePartitioning{Type: p.unit, Field: column, ExpirationMs: expirationMs}
		}
	}

	if like.clusteringInvalid {
		return nil, fmt.Errorf("CREATE TABLE LIKE: CLUSTER BY supports only column names in the emulator")
	}
	if len(like.clustering) != 0 {
		if len(like.clustering) > 4 {
			return nil, fmt.Errorf("Too many clustering fields: %d clustering fields specified, exceeding the limit of 4", len(like.clustering))
		}
		columns := make([]string, 0, len(like.clustering))
		for _, name := range like.clustering {
			field := findField(name)
			if field == nil {
				return nil, fmt.Errorf("The field specified for clustering cannot be found in the schema. Invalid field: %s", name)
			}
			columns = append(columns, field.Name)
		}
		table.Clustering = &bigqueryv2.Clustering{Fields: columns}
	}

	// The options apply after the partitioning, so that
	// partition_expiration_days sets the expiration of the final one.
	for _, option := range like.options {
		if !option.assign || !tableOptionNames[option.name] {
			logger.Logger(ctx).Warn("table option is not recorded in the table metadata",
				zap.String("table", like.ref.TableId), zap.String("option", option.name))
			continue
		}
		if !option.isLiteral {
			logger.Logger(ctx).Warn("table option is not a literal and is not recorded in the table metadata",
				zap.String("table", like.ref.TableId), zap.String("option", option.name))
			continue
		}
		if err := applyTableOption(table, option); err != nil {
			return nil, fmt.Errorf("CREATE TABLE LIKE: %w", err)
		}
	}
	return table, nil
}

// likeColumnDefinitions returns the definitions of the top-level columns of
// the new table. A REQUIRED column is NOT NULL; the modes of nested fields
// live only in the metadata, since googlesqlite does not enforce them.
func likeColumnDefinitions(schema *bigqueryv2.TableSchema) ([]contentdata.ColumnDefinition, error) {
	if schema == nil || len(schema.Fields) == 0 {
		return nil, fmt.Errorf("CREATE TABLE LIKE: the source table has no columns")
	}
	defs := make([]contentdata.ColumnDefinition, 0, len(schema.Fields))
	for _, field := range schema.Fields {
		typ, ok := contentdata.FieldDDLType(field)
		if !ok {
			return nil, fmt.Errorf("CREATE TABLE LIKE is not supported by the emulator for column %s of type %s", field.Name, field.Type)
		}
		defs = append(defs, contentdata.ColumnDefinition{
			Name:    field.Name,
			Type:    typ,
			NotNull: field.Mode == "REQUIRED",
			Default: field.DefaultValueExpression,
		})
	}
	return defs, nil
}

// skips reports whether syncCatalog must leave a table to applyMetadata.
func (p *createLikePlan) skips(projectID, datasetID, tableID string) bool {
	for _, ref := range []*bigqueryv2.TableReference{p.like.ref, p.tmpRef} {
		if ref != nil && ref.ProjectId == projectID && ref.DatasetId == datasetID && ref.TableId == tableID {
			return true
		}
	}
	return false
}

// queryColumns checks, inside tx, that the columns of the AS query match the
// columns of the source table, and returns them. Each column of the source
// table must be in the query once, in any order.
func (p *createLikePlan) queryColumns(ctx context.Context, server *Server, tx *connection.Tx, projectID, datasetID string, params []*bigqueryv2.QueryParameter) ([]string, error) {
	// The closing parenthesis is on a line of its own, so that a trailing
	// line comment in the query cannot comment it out.
	response, err := server.contentRepo.Query(ctx, tx, projectID, datasetID, fmt.Sprintf("SELECT * FROM (\n%s\n) LIMIT 0", p.like.queryText), params)
	if err != nil {
		return nil, err
	}
	var columns []string
	for _, field := range response.Schema.Fields {
		columns = append(columns, field.Name)
	}
	matched := len(columns) == len(p.srcColumns)
	used := make([]bool, len(p.srcColumns))
	for i := 0; matched && i < len(columns); i++ {
		found := false
		for j, name := range p.srcColumns {
			if !used[j] && strings.EqualFold(name, columns[i]) {
				used[j], found = true, true
				break
			}
		}
		matched = found
	}
	if !matched {
		srcRef := p.like.srcRef
		return nil, fmt.Errorf("Columns of the query (%s) do not match the columns of table %s:%s.%s (%s) referenced by LIKE",
			strings.Join(columns, ", "), srcRef.ProjectId, srcRef.DatasetId, srcRef.TableId, strings.Join(p.srcColumns, ", "))
	}
	return columns, nil
}

// dryRun checks, inside tx, what run would check before changing anything.
func (p *createLikePlan) dryRun(ctx context.Context, server *Server, tx *connection.Tx, projectID, datasetID string, params []*bigqueryv2.QueryParameter) (*internaltypes.QueryResponse, error) {
	if !p.noop && p.like.queryText != "" {
		if _, err := p.queryColumns(ctx, server, tx, projectID, datasetID, params); err != nil {
			return nil, err
		}
	}
	return emptyDDLResponse(), nil
}

// run runs the steps of the plan inside tx: the rows of an AS query are
// stored in the temporary table, the replaced table is dropped, the new table
// is created, and the rows are inserted into it. The rows are stored before
// the drop, so that the query may read the replaced table.
//
// When a step fails after another changed googlesqlite's catalog, the
// in-memory catalog keeps the change when the transaction is rolled back
// (goccy/googlesqlite#104); the caller must then call repairCatalog.
func (p *createLikePlan) run(ctx context.Context, server *Server, tx *connection.Tx, projectID, datasetID string, params []*bigqueryv2.QueryParameter) (*internaltypes.QueryResponse, error) {
	if p.noop {
		return emptyDDLResponse(), nil
	}
	dst := p.like.ref
	var cat *googlesqlite.ChangedCatalog
	// step runs one statement and records what it changed in the catalog,
	// before its error is checked.
	step := func(sql string, params []*bigqueryv2.QueryParameter, record func(*googlesqlite.ChangedTable)) error {
		response, err := server.contentRepo.Query(ctx, tx, projectID, datasetID, sql, params)
		if response != nil && response.ChangedCatalog != nil {
			if changed := response.ChangedCatalog.Table; changed != nil {
				record(changed)
			}
			cat = mergeChangedCatalogs(cat, response.ChangedCatalog)
		}
		return err
	}
	added := func(changed *googlesqlite.ChangedTable, ref *bigqueryv2.TableReference) bool {
		return findTableSpec(append(append([]*googlesqlite.TableSpec{}, changed.Added...), changed.Updated...), ref) != nil
	}

	var queryColumns []string
	if p.tmpRef != nil {
		var err error
		queryColumns, err = p.queryColumns(ctx, server, tx, projectID, datasetID, params)
		if err != nil {
			return nil, err
		}
		sql, err := server.contentRepo.CreateTableAsQuerySQL(p.tmpRef, p.like.queryText)
		if err != nil {
			return nil, err
		}
		if err := step(sql, params, func(changed *googlesqlite.ChangedTable) {
			p.tmpAdded = p.tmpAdded || added(changed, p.tmpRef)
		}); err != nil {
			return nil, err
		}
	}
	if p.replace {
		path, err := server.contentRepo.TablePath(dst.ProjectId, dst.DatasetId, dst.TableId)
		if err != nil {
			return nil, err
		}
		if err := step(fmt.Sprintf("DROP TABLE `%s`", path), nil, func(changed *googlesqlite.ChangedTable) {
			if spec := findTableSpec(changed.Deleted, dst); spec != nil {
				p.oldDstSpec = spec
			}
		}); err != nil {
			return nil, err
		}
	}
	sql, err := server.contentRepo.CreateTableSQL(dst, p.columns)
	if err != nil {
		return nil, err
	}
	if err := step(sql, nil, func(changed *googlesqlite.ChangedTable) {
		p.dstAdded = p.dstAdded || added(changed, dst)
	}); err != nil {
		return nil, err
	}
	if p.tmpRef != nil {
		// The query's columns get the spelling of the source table's.
		dstColumns := make([]string, 0, len(queryColumns))
		for _, column := range queryColumns {
			for _, name := range p.srcColumns {
				if strings.EqualFold(name, column) {
					dstColumns = append(dstColumns, name)
					break
				}
			}
		}
		sql, err := server.contentRepo.InsertFromTableSQL(p.tmpRef, dst, queryColumns, dstColumns)
		if err != nil {
			return nil, err
		}
		if err := step(sql, nil, func(*googlesqlite.ChangedTable) {}); err != nil {
			return nil, err
		}
	}
	// An empty schema keeps jobs.insert from creating a result table.
	return &internaltypes.QueryResponse{
		Schema:         &bigqueryv2.TableSchema{},
		JobComplete:    true,
		ChangedCatalog: cat,
	}, nil
}

// repairCatalog brings googlesqlite's in-memory catalog back in line with
// the tables after the steps of the plan were rolled back (see
// alterTablePlan.repairCatalog). Failures are logged: the statement already
// failed with its own error.
//
// TODO(googlesqlite): roll back the in-memory catalog with the transaction
// (goccy/googlesqlite#104). Remove this once it does.
func (p *createLikePlan) repairCatalog(ctx context.Context, server *Server) {
	if p == nil || p.oldDstSpec == nil && !p.dstAdded && !p.tmpAdded {
		return
	}
	if err := p.restoreCatalog(ctx, server); err != nil {
		logger.Logger(ctx).Error("failed to repair the analyzer catalog after a failed CREATE TABLE LIKE",
			zap.String("table", p.like.ref.TableId), zap.Error(err))
	}
}

func (p *createLikePlan) restoreCatalog(ctx context.Context, server *Server) error {
	dst := p.like.ref
	conn, err := server.connMgr.Connection(ctx, dst.ProjectId, dst.DatasetId)
	if err != nil {
		return err
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.RollbackIfNotCommitted()
	// Forget the tables the rolled-back steps created. Their SQLite tables
	// are gone, so this only drops their specs. The replaced table is never
	// dropped: its SQLite table is back.
	var created []*bigqueryv2.TableReference
	if p.tmpAdded {
		created = append(created, p.tmpRef)
	}
	if p.dstAdded && !p.replace {
		created = append(created, dst)
	}
	for _, ref := range created {
		path, err := server.contentRepo.TablePath(ref.ProjectId, ref.DatasetId, ref.TableId)
		if err != nil {
			return err
		}
		if _, err := server.contentRepo.Query(ctx, tx, ref.ProjectId, ref.DatasetId, fmt.Sprintf("DROP TABLE IF EXISTS `%s`", path), nil); err != nil {
			return err
		}
	}
	switch {
	case p.oldDstSpec != nil:
		// Register the replaced table again, with the constraints
		// googlesqlite reported for it.
		defs, ok := specColumnDefinitions(ctx, p.oldDstSpec, p.oldDefaults)
		if !ok || len(defs) == 0 {
			return fmt.Errorf("the emulator cannot express the columns of the table")
		}
		sql, err := server.contentRepo.RestoreTableSpecSQL(dst, defs)
		if err != nil {
			return err
		}
		if _, err := server.contentRepo.Query(ctx, tx, dst.ProjectId, dst.DatasetId, sql, nil); err != nil {
			return err
		}
	case p.replace && p.dstAdded:
		// The new table cannot be created before the replaced one is
		// dropped.
		logger.Logger(ctx).Error("CREATE OR REPLACE TABLE LIKE created the table without dropping the replaced one",
			zap.String("table", dst.TableId))
	}
	if err := server.contentRepo.RefreshCatalog(ctx, tx); err != nil {
		return err
	}
	return tx.Commit()
}

// checkedDefaults returns the DEFAULT expressions of the metadata of table
// that can be the DEFAULT of their columns, by lower-cased column name. An
// invalid one is left out with a warning (it can come from the tables.insert
// and tables.patch input). It runs read-only queries on transactions of its
// own, so it must run before the job transaction starts.
func checkedDefaults(ctx context.Context, server *Server, ref *bigqueryv2.TableReference, table *bigqueryv2.Table) map[string]string {
	defaults := map[string]string{}
	if table.Schema == nil {
		return defaults
	}
	for _, field := range table.Schema.Fields {
		if field.DefaultValueExpression == "" {
			continue
		}
		typ, ok := contentdata.FieldDDLType(field)
		if !ok {
			continue
		}
		if err := checkDefaultExpr(ctx, server, ref.ProjectId, ref.DatasetId, field.Name, typ, field.DefaultValueExpression); err != nil {
			logger.Logger(ctx).Warn("invalid defaultValueExpression in the table metadata cannot be restored if CREATE OR REPLACE TABLE LIKE fails",
				zap.String("table", ref.TableId), zap.String("column", field.Name), zap.Error(err))
			continue
		}
		defaults[strings.ToLower(field.Name)] = field.DefaultValueExpression
	}
	return defaults
}

// specColumnDefinitions returns the definitions of the columns of a table
// spec googlesqlite reported, with its NOT NULL constraints. A column keeps
// its DEFAULT only if googlesqlite applied one; its expression comes from
// defaults (see checkedDefaults), since googlesqlite stores only its
// internal form. It reports false when a type cannot be expressed.
//
// TODO: share with alterTablePlan.columnDefinitions.
func specColumnDefinitions(ctx context.Context, spec *googlesqlite.TableSpec, defaults map[string]string) ([]contentdata.ColumnDefinition, bool) {
	defs := make([]contentdata.ColumnDefinition, 0, len(spec.Columns))
	for _, column := range spec.Columns {
		typ, ok := contentdata.FieldDDLType(types.TableFieldSchemaFromColumnType(column.Name, column.Type))
		if !ok {
			return nil, false
		}
		def := contentdata.ColumnDefinition{Name: column.Name, Type: typ, NotNull: column.IsNotNull}
		if column.DefaultExpr != "" {
			if text, ok := defaults[strings.ToLower(column.Name)]; ok {
				def.Default = text
			} else {
				logger.Logger(ctx).Warn("the DEFAULT of a column is not restored: the table metadata has no valid expression for it",
					zap.String("column", column.Name))
			}
		}
		defs = append(defs, def)
	}
	return defs, true
}

// applyMetadata writes the metadata of the new table after the job of the
// plan committed.
func (p *createLikePlan) applyMetadata(ctx context.Context, server *Server) error {
	if p.noop {
		return nil
	}
	dst := p.like.ref
	project, err := server.metaRepo.FindProject(ctx, dst.ProjectId)
	if err != nil {
		return err
	}
	if project == nil || project.Dataset(dst.DatasetId) == nil {
		return fmt.Errorf("dataset of table %s:%s.%s is not found", dst.ProjectId, dst.DatasetId, dst.TableId)
	}
	dataset := project.Dataset(dst.DatasetId)
	conn, err := server.connMgr.Connection(ctx, dst.ProjectId, dst.DatasetId)
	if err != nil {
		return err
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.RollbackIfNotCommitted()
	if dataset.Table(dst.TableId) != nil {
		if err := dataset.DeleteTable(ctx, tx.Tx(), dst.TableId); err != nil {
			return err
		}
	}
	if _, err := createTableMetadata(ctx, tx, server, project, dataset, p.table); err != nil {
		return err
	}
	return tx.Commit()
}

func (p *createLikePlan) syncSkip() skipTableFunc {
	if p.noop {
		return nil
	}
	return p.skips
}

func (p *createLikePlan) applyMetadataAfterCommit(ctx context.Context, server *Server, _ *googlesqlite.ChangedCatalog) error {
	return p.applyMetadata(ctx, server)
}
