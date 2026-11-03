package server

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/goccy/go-googlesql"
	"github.com/goccy/go-json"
	"github.com/goccy/googlesqlite"
	bigqueryv2 "google.golang.org/api/bigquery/v2"

	"github.com/goccy/bigquery-emulator/internal/connection"
	"github.com/goccy/bigquery-emulator/internal/contentdata"
	"github.com/goccy/bigquery-emulator/internal/metadata"
	"github.com/goccy/bigquery-emulator/types"
)

// googlesqlite runs ALTER TABLE as a no-op: neither the SQLite table nor any
// catalog changes. A standalone ALTER TABLE that only adds, drops or renames
// columns, or renames the table, is therefore rewritten here into a script
// that rebuilds the table by copying its rows, which googlesqlite runs
// faithfully. The emulator's table metadata is then updated from the plan, so
// that column descriptions and modes survive the rebuild.
//
// ALTER TABLE inside a multi-statement script and any other action are left
// to googlesqlite. ALTER TABLE statements are serialized (Server.alterMu)
// because the plan is built from the metadata before the rebuild runs, and
// checkUnchanged guards the rebuild against any other concurrent change.
//
// TODO(googlesqlite): implement ALTER TABLE ADD/DROP/RENAME COLUMN and
// RENAME TO, and report it through ChangedCatalog.Table.Updated (no upstream
// issue yet; googlesqlite runs ALTER TABLE as a no-op). Once it does, this
// file can be removed.

// alterTablePlan is a standalone ALTER TABLE statement rewritten into a
// table rebuild.
type alterTablePlan struct {
	ref *bigqueryv2.TableReference
	// newRef is the new name of the table for RENAME TO, and nil otherwise.
	newRef *bigqueryv2.TableReference
	// tmpRef is the table the rows go through for a column change.
	tmpRef *bigqueryv2.TableReference
	// sourceColumns are the columns of the table when the plan was made.
	// The rebuild runs only if the table still has them (checkUnchanged).
	sourceColumns []string
	// columns is the final column list for a column change.
	columns []*alterColumn
	// sql is the rewritten script. An empty sql means that there is
	// nothing to do and the statement succeeds without running anything.
	sql string
}

type alterColumn struct {
	name string
	// expr computes the column from the table before the change.
	expr string
	// oldField is the column's schema before the change, or nil for an
	// added column.
	oldField *bigqueryv2.TableFieldSchema
	// annotation is what the ADD COLUMN statement says about an added
	// column beyond its type.
	annotation *columnAnnotation
}

// columnAnnotation holds the OPTIONS(description=...) and the NOT NULL of a
// column, and of its nested fields, as written in ADD COLUMN.
type columnAnnotation struct {
	description string
	required    bool
	// fields are the annotations of the nested fields, by lower-cased name.
	fields map[string]*columnAnnotation
}

// apply sets the annotations on the schema googlesqlite resolved for the
// column. A top-level NOT NULL is rejected before, so required is applied
// only to nested fields.
func (a *columnAnnotation) apply(field *bigqueryv2.TableFieldSchema, nested bool) {
	if a == nil {
		return
	}
	if a.description != "" {
		field.Description = a.description
	}
	if nested && a.required && field.Mode != "REPEATED" {
		field.Mode = "REQUIRED"
	}
	for _, sub := range field.Fields {
		a.fields[strings.ToLower(sub.Name)].apply(sub, true)
	}
}

// alterAction is one action of an ALTER TABLE statement.
type alterAction struct {
	kind alterActionKind
	// name is the column for column actions.
	name string
	// newName is the new column name for RENAME COLUMN.
	newName string
	// typeText is the column type for ADD COLUMN, rebuilt from the
	// statement without its NOT NULL and OPTIONS parts.
	typeText   string
	annotation *columnAnnotation
	notNull    bool
	ifExists   bool // IF EXISTS, or IF NOT EXISTS for ADD COLUMN
	// newPath is the new table name for RENAME TO.
	newPath []string
}

type alterActionKind int

const (
	alterAddColumn alterActionKind = iota
	alterDropColumn
	alterRenameColumn
	alterRenameTo
)

// parsedAlterTable is the part of an ALTER TABLE statement the plan needs.
type parsedAlterTable struct {
	path     []string
	ifExists bool
	actions  []*alterAction
}

// skips reports whether syncCatalog must leave a table to applyMetadata.
func (p *alterTablePlan) skips(projectID, datasetID, tableID string) bool {
	for _, ref := range []*bigqueryv2.TableReference{p.ref, p.newRef, p.tmpRef} {
		if ref != nil && ref.ProjectId == projectID && ref.DatasetId == datasetID && ref.TableId == tableID {
			return true
		}
	}
	return false
}

// alterTmpTablePrefix starts the name of the table a column change copies
// the rows through.
const alterTmpTablePrefix = "__bqemu_alter_"

// alterBeforeRebuildHook, when set by a test, runs between planning an
// ALTER TABLE and starting the job transaction that rebuilds the table.
var alterBeforeRebuildHook struct {
	mu sync.Mutex
	fn func(plan *alterTablePlan)
}

func runAlterBeforeRebuildHook(plan *alterTablePlan) {
	if plan == nil {
		return
	}
	alterBeforeRebuildHook.mu.Lock()
	hook := alterBeforeRebuildHook.fn
	alterBeforeRebuildHook.mu.Unlock()
	if hook != nil {
		hook(plan)
	}
}

func setAlterBeforeRebuildHook(fn func(plan *alterTablePlan)) {
	alterBeforeRebuildHook.mu.Lock()
	defer alterBeforeRebuildHook.mu.Unlock()
	alterBeforeRebuildHook.fn = fn
}

// errAlterConcurrentModification is returned when the table changed between
// planning an ALTER TABLE and running its rebuild.
var errAlterConcurrentModification = errors.New("was modified concurrently while ALTER TABLE was running; retry the statement")

// checkUnchanged verifies, inside the job transaction, that the table still
// has the columns the plan was made from. alterMu serializes ALTER TABLE
// statements, but any other DDL or API call (CREATE OR REPLACE, DROP,
// tables.update, ...) can change the table between planning and the
// rebuild. Reading the table here also holds the read lock that keeps such
// changes out until the rebuild commits.
func (p *alterTablePlan) checkUnchanged(ctx context.Context, server *Server, tx *connection.Tx) error {
	if p.sql == "" {
		return nil
	}
	tableName := fmt.Sprintf("%s:%s.%s", p.ref.ProjectId, p.ref.DatasetId, p.ref.TableId)
	columns, err := server.contentRepo.TableColumnNames(ctx, tx, p.ref)
	if err != nil {
		return fmt.Errorf("table %s %w: %s", tableName, errAlterConcurrentModification, err)
	}
	same := len(columns) == len(p.sourceColumns)
	for i := 0; same && i < len(columns); i++ {
		same = strings.EqualFold(columns[i], p.sourceColumns[i])
	}
	if !same {
		return fmt.Errorf("table %s %w: planned for columns %v, found %v", tableName, errAlterConcurrentModification, p.sourceColumns, columns)
	}
	return nil
}

// planAlterTable returns (nil, nil) when query is not a standalone ALTER TABLE
// that this layer handles; the caller then runs the query unchanged. An
// error is the statement's error, to be reported as the query's error.
func planAlterTable(ctx context.Context, server *Server, defaultProjectID, defaultDatasetID, query string) (*alterTablePlan, error) {
	if !looksLikeAlter(query) {
		return nil, nil
	}
	stmt, ok, err := parseAlterTable(query)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	var path []string
	for _, part := range stmt.path {
		path = append(path, strings.Split(part, ".")...)
	}
	ref := &bigqueryv2.TableReference{ProjectId: defaultProjectID, DatasetId: defaultDatasetID}
	switch len(path) {
	case 1:
		ref.TableId = path[0]
	case 2:
		ref.DatasetId, ref.TableId = path[0], path[1]
	case 3:
		ref.ProjectId, ref.DatasetId, ref.TableId = path[0], path[1], path[2]
	default:
		return nil, nil
	}
	if ref.DatasetId == "" {
		return nil, nil
	}
	tableName := fmt.Sprintf("%s:%s.%s", ref.ProjectId, ref.DatasetId, ref.TableId)

	project, err := server.metaRepo.FindProject(ctx, ref.ProjectId)
	if err != nil {
		return nil, err
	}
	var content *bigqueryv2.Table
	if project != nil {
		if dataset := project.Dataset(ref.DatasetId); dataset != nil {
			if table := dataset.Table(ref.TableId); table != nil {
				if table.IsView() {
					return nil, fmt.Errorf("%s is a view, not a table", tableName)
				}
				content, err = table.Content()
				if err != nil {
					return nil, err
				}
			}
		}
	}
	if content == nil {
		if stmt.ifExists {
			return &alterTablePlan{ref: ref}, nil
		}
		return nil, fmt.Errorf("Not found: Table %s", tableName)
	}
	if content.ExternalDataConfiguration != nil || content.Schema == nil {
		return nil, nil
	}

	plan := &alterTablePlan{ref: ref}
	for _, field := range content.Schema.Fields {
		plan.sourceColumns = append(plan.sourceColumns, field.Name)
	}
	if stmt.actions[0].kind == alterRenameTo {
		if len(stmt.actions) != 1 {
			return nil, fmt.Errorf("ALTER TABLE RENAME TO cannot be combined with other actions")
		}
		return planRenameTable(server, project, plan, content, stmt.actions[0].newPath)
	}

	isPartitioningColumn := func(name string) bool {
		if tp := content.TimePartitioning; tp != nil && strings.EqualFold(tp.Field, name) {
			return true
		}
		if rp := content.RangePartitioning; rp != nil && strings.EqualFold(rp.Field, name) {
			return true
		}
		return false
	}
	isClusteringColumn := func(name string) bool {
		if content.Clustering == nil {
			return false
		}
		for _, field := range content.Clustering.Fields {
			if strings.EqualFold(field, name) {
				return true
			}
		}
		return false
	}
	// BigQuery rejects dropping a partitioning or clustering column. A
	// rename is rejected too, rather than rewriting the partitioning and
	// clustering specs.
	checkKeyColumn := func(verb, name string) error {
		if isPartitioningColumn(name) {
			return fmt.Errorf("Cannot %s partitioning column %s of table %s", verb, name, tableName)
		}
		if isClusteringColumn(name) {
			return fmt.Errorf("Cannot %s clustering column %s of table %s", verb, name, tableName)
		}
		return nil
	}

	columns := make([]*alterColumn, 0, len(content.Schema.Fields))
	for _, field := range content.Schema.Fields {
		columns = append(columns, &alterColumn{name: field.Name, expr: quoteIdent(field.Name), oldField: field})
	}
	find := func(name string) int {
		for i, column := range columns {
			if strings.EqualFold(column.name, name) {
				return i
			}
		}
		return -1
	}
	changed := false
	for _, action := range stmt.actions {
		switch action.kind {
		case alterAddColumn:
			if find(action.name) >= 0 {
				if action.ifExists {
					continue
				}
				return nil, fmt.Errorf("Column already exists: %s", action.name)
			}
			if action.notNull {
				return nil, fmt.Errorf("Cannot add a REQUIRED column %s to table %s", action.name, tableName)
			}
			columns = append(columns, &alterColumn{
				name:       action.name,
				expr:       fmt.Sprintf("CAST(NULL AS %s)", action.typeText),
				annotation: action.annotation,
			})
		case alterDropColumn:
			i := find(action.name)
			if i < 0 {
				if action.ifExists {
					continue
				}
				return nil, fmt.Errorf("Column not found: %s", action.name)
			}
			if columns[i].oldField != nil {
				if err := checkKeyColumn("drop", columns[i].oldField.Name); err != nil {
					return nil, err
				}
			}
			if len(columns) == 1 {
				return nil, fmt.Errorf("Cannot drop the only column %s of table %s", action.name, tableName)
			}
			columns = append(columns[:i], columns[i+1:]...)
		case alterRenameColumn:
			i := find(action.name)
			if i < 0 {
				if action.ifExists {
					continue
				}
				return nil, fmt.Errorf("Column not found: %s", action.name)
			}
			if j := find(action.newName); j >= 0 && j != i {
				return nil, fmt.Errorf("Column already exists: %s", action.newName)
			}
			if columns[i].oldField != nil {
				if err := checkKeyColumn("rename", columns[i].oldField.Name); err != nil {
					return nil, err
				}
			}
			columns[i].name = action.newName
		case alterRenameTo:
			return nil, fmt.Errorf("ALTER TABLE RENAME TO cannot be combined with other actions")
		}
		changed = true
	}
	if !changed {
		return plan, nil
	}
	for _, field := range content.Schema.Fields {
		if strings.ContainsAny(field.Name, "`\\") {
			return nil, fmt.Errorf("invalid column name %q", field.Name)
		}
	}
	projection := make([]contentdata.ProjectedColumn, 0, len(columns))
	for _, column := range columns {
		projection = append(projection, contentdata.ProjectedColumn{Expr: column.expr, Name: column.name})
	}
	// A short fixed prefix and a random suffix keep the name bounded
	// whatever the length of the table name.
	tmpTableID := alterTmpTablePrefix + randomID()
	sql, err := server.contentRepo.RebuildTableSQL(ref, ref, tmpTableID, projection)
	if err != nil {
		return nil, err
	}
	plan.tmpRef = &bigqueryv2.TableReference{ProjectId: ref.ProjectId, DatasetId: ref.DatasetId, TableId: tmpTableID}
	plan.columns = columns
	plan.sql = sql
	return plan, nil
}

func planRenameTable(server *Server, project *metadata.Project, plan *alterTablePlan, content *bigqueryv2.Table, newPath []string) (*alterTablePlan, error) {
	ref := plan.ref
	var path []string
	for _, part := range newPath {
		path = append(path, strings.Split(part, ".")...)
	}
	newRef := &bigqueryv2.TableReference{ProjectId: ref.ProjectId, DatasetId: ref.DatasetId}
	switch len(path) {
	case 1:
		newRef.TableId = path[0]
	case 2:
		newRef.DatasetId, newRef.TableId = path[0], path[1]
	case 3:
		newRef.ProjectId, newRef.DatasetId, newRef.TableId = path[0], path[1], path[2]
	default:
		return nil, fmt.Errorf("invalid table name %s", strings.Join(newPath, "."))
	}
	if newRef.ProjectId != ref.ProjectId || newRef.DatasetId != ref.DatasetId {
		return nil, fmt.Errorf("ALTER TABLE RENAME TO cannot move a table to another dataset")
	}
	if project.Dataset(ref.DatasetId).Table(newRef.TableId) != nil {
		return nil, fmt.Errorf("Already Exists: Table %s:%s.%s", newRef.ProjectId, newRef.DatasetId, newRef.TableId)
	}
	projection := make([]contentdata.ProjectedColumn, 0, len(content.Schema.Fields))
	for _, field := range content.Schema.Fields {
		projection = append(projection, contentdata.ProjectedColumn{Expr: quoteIdent(field.Name), Name: field.Name})
	}
	sql, err := server.contentRepo.RebuildTableSQL(ref, newRef, "", projection)
	if err != nil {
		return nil, err
	}
	plan.newRef = newRef
	plan.sql = sql
	return plan, nil
}

// applyMetadata updates the table metadata after the rewritten script of the
// plan committed. cat is the catalog change of that script.
func (p *alterTablePlan) applyMetadata(ctx context.Context, server *Server, cat *googlesqlite.ChangedCatalog) error {
	if p.sql == "" {
		return nil
	}
	tableName := fmt.Sprintf("%s:%s.%s", p.ref.ProjectId, p.ref.DatasetId, p.ref.TableId)
	project, err := server.metaRepo.FindProject(ctx, p.ref.ProjectId)
	if err != nil {
		return err
	}
	if project == nil || project.Dataset(p.ref.DatasetId) == nil {
		return fmt.Errorf("dataset of table %s is not found", tableName)
	}
	dataset := project.Dataset(p.ref.DatasetId)
	table := dataset.Table(p.ref.TableId)
	if table == nil {
		return fmt.Errorf("metadata of table %s is not found", tableName)
	}
	content, err := table.Content()
	if err != nil {
		return err
	}
	var fields []*bigqueryv2.TableFieldSchema
	if p.newRef == nil {
		fields, err = p.newFields(cat)
		if err != nil {
			return fmt.Errorf("table %s: %w", tableName, err)
		}
	}

	conn, err := server.connMgr.Connection(ctx, p.ref.ProjectId, p.ref.DatasetId)
	if err != nil {
		return err
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.RollbackIfNotCommitted()
	if p.newRef != nil {
		content.TableReference = &bigqueryv2.TableReference{
			ProjectId: p.newRef.ProjectId,
			DatasetId: p.newRef.DatasetId,
			TableId:   p.newRef.TableId,
		}
		if err := dataset.DeleteTable(ctx, tx.Tx(), p.ref.TableId); err != nil {
			return err
		}
		creationTime := content.CreationTime
		if _, err := createTableMetadata(ctx, tx, server, project, dataset, content); err != nil {
			return err
		}
		// The table keeps its creation time; createTableMetadata sets it
		// to now.
		encoded, err := json.Marshal(&bigqueryv2.Table{CreationTime: creationTime})
		if err != nil {
			return err
		}
		var patch map[string]interface{}
		if err := json.Unmarshal(encoded, &patch); err != nil {
			return err
		}
		if len(patch) != 0 {
			if err := dataset.Table(p.newRef.TableId).Patch(ctx, tx.Tx(), patch); err != nil {
				return err
			}
		}
	} else {
		encoded, err := json.Marshal(&bigqueryv2.Table{
			Schema:           &bigqueryv2.TableSchema{Fields: fields},
			LastModifiedTime: uint64(time.Now().Unix()),
		})
		if err != nil {
			return err
		}
		var patch map[string]interface{}
		if err := json.Unmarshal(encoded, &patch); err != nil {
			return err
		}
		if err := table.Patch(ctx, tx.Tx(), patch); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// newFields returns the schema of the table after a column change. Kept and
// renamed columns keep their schema (description, mode, policy tags); added
// columns take the type googlesqlite resolved for them.
func (p *alterTablePlan) newFields(cat *googlesqlite.ChangedCatalog) ([]*bigqueryv2.TableFieldSchema, error) {
	var spec *googlesqlite.TableSpec
	if cat != nil && cat.Table != nil {
		for _, s := range append(append([]*googlesqlite.TableSpec{}, cat.Table.Added...), cat.Table.Updated...) {
			if len(s.NamePath) == 3 && p.skips(s.NamePath[0], s.NamePath[1], s.NamePath[2]) {
				spec = s
			}
		}
	}
	fields := make([]*bigqueryv2.TableFieldSchema, 0, len(p.columns))
	for _, column := range p.columns {
		if column.oldField != nil {
			field, err := copyFieldSchema(column.oldField)
			if err != nil {
				return nil, err
			}
			field.Name = column.name
			fields = append(fields, field)
			continue
		}
		var added *googlesqlite.ColumnSpec
		if spec != nil {
			for _, c := range spec.Columns {
				if strings.EqualFold(c.Name, column.name) {
					added = c
					break
				}
			}
		}
		if added == nil {
			return nil, fmt.Errorf("type of added column %s is unknown", column.name)
		}
		field := types.TableFieldSchemaFromColumnType(column.name, added.Type)
		column.annotation.apply(field, false)
		fields = append(fields, field)
	}
	return fields, nil
}

func copyFieldSchema(field *bigqueryv2.TableFieldSchema) (*bigqueryv2.TableFieldSchema, error) {
	encoded, err := json.Marshal(field)
	if err != nil {
		return nil, err
	}
	var copied bigqueryv2.TableFieldSchema
	if err := json.Unmarshal(encoded, &copied); err != nil {
		return nil, err
	}
	return &copied, nil
}

// quoteIdent backtick-quotes a column name taken from the table metadata.
// The names are checked for a backtick or a backslash before they are used:
// by planAlterTable for column changes, and by RebuildTableSQL (which gets
// the same names as output columns) for RENAME TO.
func quoteIdent(name string) string {
	return "`" + name + "`"
}

// looksLikeAlter reports whether query starts with ALTER TABLE, after any
// leading whitespace and comments. The keywords are matched
// case-insensitively and may be separated by any whitespace.
func looksLikeAlter(query string) bool {
	rest := skipSpaceAndComments(query)
	if !hasKeywordPrefix(rest, "ALTER") {
		return false
	}
	rest = rest[len("ALTER"):]
	if trimmed := strings.TrimLeft(rest, " \t\r\n\f"); len(trimmed) != len(rest) {
		return hasKeywordPrefix(trimmed, "TABLE")
	}
	return false
}

// hasKeywordPrefix reports whether s starts with keyword, case-insensitively,
// not followed by another identifier character.
func hasKeywordPrefix(s, keyword string) bool {
	if len(s) < len(keyword) || !strings.EqualFold(s[:len(keyword)], keyword) {
		return false
	}
	if len(s) == len(keyword) {
		return true
	}
	c := s[len(keyword)]
	return !(c == '_' || c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z')
}

// skipSpaceAndComments returns query without its leading whitespace and
// comments.
func skipSpaceAndComments(query string) string {
	rest := query
	for {
		rest = strings.TrimLeft(rest, " \t\r\n\f")
		switch {
		case strings.HasPrefix(rest, "--"), strings.HasPrefix(rest, "#"):
			i := strings.IndexByte(rest, '\n')
			if i < 0 {
				return ""
			}
			rest = rest[i+1:]
		case strings.HasPrefix(rest, "/*"):
			i := strings.Index(rest[2:], "*/")
			if i < 0 {
				return ""
			}
			rest = rest[2+i+2:]
		default:
			return rest
		}
	}
}

// parseAlterTable parses query as a single ALTER TABLE statement. It reports
// false when query is anything else, or uses an action that is left to
// googlesqlite. An error is returned for a supported action that uses a
// feature the rebuild cannot honour.
func parseAlterTable(query string) (*parsedAlterTable, bool, error) {
	if err := googlesql.Init(); err != nil {
		return nil, false, nil
	}
	languageOptions, err := googlesql.NewLanguageOptions()
	if err != nil {
		return nil, false, nil
	}
	// Parse DEFAULT, so that it is reported as unsupported instead of
	// leaving the statement to googlesqlite's no-op.
	if err := languageOptions.EnableLanguageFeature(googlesql.LanguageFeatureFeatureColumnDefaultValue); err != nil {
		return nil, false, nil
	}
	opts, err := googlesql.NewParserOptions2(nil, nil, languageOptions)
	if err != nil {
		return nil, false, nil
	}
	out, err := googlesql.ParseStatement(query, opts)
	if err != nil {
		return nil, false, nil
	}
	node, err := out.Statement()
	if err != nil {
		return nil, false, nil
	}
	stmt, ok := node.(*googlesql.ASTAlterTableStatement)
	if !ok {
		return nil, false, nil
	}
	target, err := stmt.GetDdlTarget()
	if err != nil || target == nil {
		return nil, false, nil
	}
	path, err := target.ToIdentifierVector()
	if err != nil {
		return nil, false, nil
	}
	ifExists, err := stmt.IsIfExists()
	if err != nil {
		return nil, false, nil
	}
	list, err := stmt.ActionList()
	if err != nil || list == nil {
		return nil, false, nil
	}
	n, err := list.NumChildren()
	if err != nil || n == 0 {
		return nil, false, nil
	}
	parsed := &parsedAlterTable{path: path, ifExists: ifExists}
	for i := int32(0); i < n; i++ {
		node, err := list.Actions(i)
		if err != nil {
			return nil, false, nil
		}
		action, ok, err := parseAlterAction(query, node)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			return nil, false, nil
		}
		parsed.actions = append(parsed.actions, action)
	}
	return parsed, true, nil
}

func parseAlterAction(query string, node googlesql.ASTAlterActionNode) (*alterAction, bool, error) {
	switch node := node.(type) {
	case *googlesql.ASTAddColumnAction:
		// ADD COLUMN ... FIRST/AFTER and FILL USING are not BigQuery.
		if position, err := node.ColumnPosition(); err != nil || position != nil {
			return nil, false, nil
		}
		if fill, err := node.FillExpression(); err != nil || fill != nil {
			return nil, false, nil
		}
		ifNotExists, err := node.IsIfNotExists()
		if err != nil {
			return nil, false, nil
		}
		def, err := node.ColumnDefinition()
		if err != nil || def == nil {
			return nil, false, nil
		}
		name, ok := identifierString(def.Name())
		if !ok {
			return nil, false, nil
		}
		schema, err := def.Schema()
		if err != nil || schema == nil {
			return nil, false, nil
		}
		typeText, annotation, err := columnType(query, name, schema)
		if err != nil {
			return nil, false, err
		}
		return &alterAction{
			kind:       alterAddColumn,
			name:       name,
			typeText:   typeText,
			annotation: annotation,
			notNull:    annotation.required,
			ifExists:   ifNotExists,
		}, true, nil
	case *googlesql.ASTDropColumnAction:
		name, ok := identifierString(node.ColumnName())
		if !ok {
			return nil, false, nil
		}
		ifExists, err := node.IsIfExists()
		if err != nil {
			return nil, false, nil
		}
		return &alterAction{kind: alterDropColumn, name: name, ifExists: ifExists}, true, nil
	case *googlesql.ASTRenameColumnAction:
		name, ok := identifierString(node.ColumnName())
		if !ok {
			return nil, false, nil
		}
		newName, ok := identifierString(node.NewColumnName())
		if !ok {
			return nil, false, nil
		}
		ifExists, err := node.IsIfExists()
		if err != nil {
			return nil, false, nil
		}
		return &alterAction{kind: alterRenameColumn, name: name, newName: newName, ifExists: ifExists}, true, nil
	case *googlesql.ASTRenameToClause:
		newName, err := node.NewName()
		if err != nil || newName == nil {
			return nil, false, nil
		}
		path, err := newName.ToIdentifierVector()
		if err != nil {
			return nil, false, nil
		}
		return &alterAction{kind: alterRenameTo, newPath: path}, true, nil
	}
	return nil, false, nil
}

func identifierString(ident *googlesql.ASTIdentifier, err error) (string, bool) {
	if err != nil || ident == nil {
		return "", false
	}
	name, err := ident.GetAsString()
	if err != nil || name == "" {
		return "", false
	}
	return name, true
}

// columnType rebuilds the type of an ADD COLUMN schema from its type nodes,
// leaving out the NOT NULL and OPTIONS parts at every nesting level, and
// returns those parts as an annotation. column names the column in errors.
func columnType(query, column string, schema googlesql.ASTColumnSchemaNode) (string, *columnAnnotation, error) {
	unsupported := func(feature string) error {
		return fmt.Errorf("ALTER TABLE ADD COLUMN %s: %s is not supported by the emulator", column, feature)
	}
	if def, err := schema.DefaultExpression(); err != nil || def != nil {
		return "", nil, unsupported("DEFAULT")
	}
	if generated, err := schema.GeneratedColumnInfo(); err != nil || generated != nil {
		return "", nil, unsupported("a generated column")
	}
	if collate, err := schema.Collate(); err != nil || collate != nil {
		return "", nil, unsupported("COLLATE")
	}
	annotation := &columnAnnotation{fields: map[string]*columnAnnotation{}}
	notNull, err := schema.ContainsAttribute(googlesql.ASTNodeKindAstNotNullColumnAttribute)
	if err != nil {
		return "", nil, unsupported("this column definition")
	}
	annotation.required = notNull
	description, err := optionsDescription(schema)
	if err != nil {
		return "", nil, unsupported("this OPTIONS list")
	}
	annotation.description = description

	var typeText string
	switch schema := schema.(type) {
	case *googlesql.ASTSimpleColumnSchema:
		typeName, err := schema.TypeName()
		if err != nil || typeName == nil {
			return "", nil, unsupported("this column type")
		}
		start, end, ok := nodeOffsets(typeName)
		if !ok {
			return "", nil, unsupported("this column type")
		}
		// Type parameters, e.g. STRING(10) or NUMERIC(10, 2), follow the
		// type name up to the closing parenthesis.
		if params, err := schema.TypeParameters(); err == nil && params != nil {
			_, paramsEnd, ok := nodeOffsets(params)
			if !ok {
				return "", nil, unsupported("this column type")
			}
			closing := strings.IndexByte(query[paramsEnd:], ')')
			if closing < 0 {
				return "", nil, unsupported("this column type")
			}
			end = paramsEnd + closing + 1
		}
		if start < 0 || end > len(query) || start >= end {
			return "", nil, unsupported("this column type")
		}
		typeText = strings.TrimSpace(query[start:end])
	case *googlesql.ASTArrayColumnSchema:
		element, err := schema.ElementSchema()
		if err != nil || element == nil {
			return "", nil, unsupported("this column type")
		}
		elementType, elementAnnotation, err := columnType(query, column, element)
		if err != nil {
			return "", nil, err
		}
		// The element's nested fields are the array column's fields.
		annotation.fields = elementAnnotation.fields
		typeText = fmt.Sprintf("ARRAY<%s>", elementType)
	case *googlesql.ASTStructColumnSchema:
		n, err := schema.NumChildren()
		if err != nil {
			return "", nil, unsupported("this column type")
		}
		var fields []string
		for i := int32(0); i < n; i++ {
			child, err := schema.Child(i)
			if err != nil {
				return "", nil, unsupported("this column type")
			}
			field, ok := child.(*googlesql.ASTStructColumnField)
			if !ok {
				continue
			}
			name, ok := identifierString(field.Name())
			if !ok {
				return "", nil, unsupported("an unnamed STRUCT field")
			}
			if strings.ContainsAny(name, "`\\") {
				return "", nil, fmt.Errorf("invalid column name %q", name)
			}
			fieldSchema, err := field.Schema()
			if err != nil || fieldSchema == nil {
				return "", nil, unsupported("this column type")
			}
			fieldType, fieldAnnotation, err := columnType(query, column+"."+name, fieldSchema)
			if err != nil {
				return "", nil, err
			}
			annotation.fields[strings.ToLower(name)] = fieldAnnotation
			fields = append(fields, fmt.Sprintf("`%s` %s", name, fieldType))
		}
		if len(fields) == 0 {
			return "", nil, unsupported("an empty STRUCT")
		}
		typeText = fmt.Sprintf("STRUCT<%s>", strings.Join(fields, ", "))
	default:
		return "", nil, unsupported("this column type")
	}
	return typeText, annotation, nil
}

// optionsDescription returns the description option of a column schema.
func optionsDescription(schema googlesql.ASTColumnSchemaNode) (string, error) {
	options, err := schema.OptionsList()
	if err != nil {
		return "", err
	}
	if options == nil {
		return "", nil
	}
	n, err := options.NumChildren()
	if err != nil {
		return "", err
	}
	for i := int32(0); i < n; i++ {
		entry, err := options.OptionsEntries(i)
		if err != nil || entry == nil {
			return "", fmt.Errorf("invalid OPTIONS entry")
		}
		name, ok := identifierString(entry.Name())
		if !ok || !strings.EqualFold(name, "description") {
			continue
		}
		value, err := entry.Value()
		if err != nil {
			return "", err
		}
		literal, ok := value.(*googlesql.ASTStringLiteral)
		if !ok {
			return "", fmt.Errorf("description must be a string literal")
		}
		return literal.StringValue()
	}
	return "", nil
}

func nodeOffsets(node googlesql.ASTNode) (int, int, bool) {
	location, err := node.GetParseLocationRange()
	if err != nil || location == nil {
		return 0, 0, false
	}
	start, err := location.Start()
	if err != nil || start == nil {
		return 0, 0, false
	}
	end, err := location.End()
	if err != nil || end == nil {
		return 0, 0, false
	}
	startOffset, err := start.GetByteOffset()
	if err != nil {
		return 0, 0, false
	}
	endOffset, err := end.GetByteOffset()
	if err != nil {
		return 0, 0, false
	}
	return int(startOffset), int(endOffset), true
}
