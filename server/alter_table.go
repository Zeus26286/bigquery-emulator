package server

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/goccy/go-googlesql"
	"github.com/goccy/go-json"
	"github.com/goccy/googlesqlite"
	"go.uber.org/zap"
	bigqueryv2 "google.golang.org/api/bigquery/v2"

	"github.com/goccy/bigquery-emulator/internal/connection"
	"github.com/goccy/bigquery-emulator/internal/contentdata"
	"github.com/goccy/bigquery-emulator/internal/logger"
	"github.com/goccy/bigquery-emulator/internal/metadata"
	internaltypes "github.com/goccy/bigquery-emulator/internal/types"
	"github.com/goccy/bigquery-emulator/types"
)

// googlesqlite runs ALTER TABLE as a no-op: neither the SQLite table nor any
// catalog changes. Every standalone ALTER TABLE statement is therefore
// handled here: either all its actions are supported, or the statement fails
// with an explicit error.
//
// An action that changes the columns (ADD/DROP/RENAME COLUMN, ALTER COLUMN
// SET DATA TYPE, DROP NOT NULL, SET/DROP DEFAULT) or RENAME TO rebuilds the
// table by copying its rows, which googlesqlite runs faithfully. The rebuild
// keeps the NOT NULL and DEFAULT constraints googlesqlite applies to the
// table. Actions that change only the metadata (SET OPTIONS, ALTER COLUMN SET
// OPTIONS, ADD/DROP PRIMARY KEY) run nothing. In both cases the emulator's
// table metadata is then updated from the plan, so that column descriptions
// and modes survive the rebuild.
//
// ALTER TABLE inside a multi-statement script is rejected (analyzeDDL).
// ALTER TABLE statements are serialized (Server.alterMu) because the plan is
// built from the metadata before the rebuild runs, and checkUnchanged guards
// the rebuild against any other concurrent change.
//
// TODO(googlesqlite): implement ALTER TABLE and report it through
// ChangedCatalog.Table.Updated (no upstream issue yet; googlesqlite runs
// ALTER TABLE as a no-op). Once it does, this file can be removed.

// alterTablePlan is a standalone ALTER TABLE statement rewritten into a
// table rebuild and a metadata update.
type alterTablePlan struct {
	ref *bigqueryv2.TableReference
	// newRef is the new name of the table for RENAME TO, and nil otherwise.
	newRef *bigqueryv2.TableReference
	// tmpRef is the table the rows go through during a rebuild.
	tmpRef *bigqueryv2.TableReference
	// sourceColumns are the columns of the table when the plan was made.
	// The rebuild runs only if the table still has them (checkUnchanged).
	sourceColumns []string
	// columns is the final column list.
	columns []*alterColumn
	// options are the table options set by SET OPTIONS, evaluated, in
	// order.
	options []*optionValue
	// primaryKey is the primary key after the statement, when primaryKeySet.
	primaryKey    []string
	primaryKeySet bool
	// changed is set when the statement changes the table metadata.
	changed bool
	// sql is the first step of the rebuild (contentdata.RebuildTableSQL).
	// An empty sql means that no rebuild is needed: the statement succeeds
	// without running anything, and only the metadata changes.
	sql string
	// ranFirstStep is set once the first step of the rebuild ran, and
	// srcSpec is the spec googlesqlite reported for the dropped table. A
	// rolled-back rebuild needs repairCatalog from then on.
	ranFirstStep bool
	srcSpec      *googlesqlite.TableSpec
}

type alterColumn struct {
	name string
	// expr computes the column from the table before the change.
	expr string
	// oldField is the column's schema before the change, or nil for an
	// added column.
	oldField *bigqueryv2.TableFieldSchema
	// field is the column's schema after the change for a kept column.
	field *bigqueryv2.TableFieldSchema
	// annotation is what the ADD COLUMN statement says about an added
	// column beyond its type.
	annotation *columnAnnotation
	// newType is the type set by ALTER COLUMN SET DATA TYPE, or nil.
	newType *columnTypeName
	// dropNotNull is set by ALTER COLUMN DROP NOT NULL.
	dropNotNull bool
	// defaultSet is set by ALTER COLUMN SET DEFAULT (defaultText is the
	// expression) and DROP DEFAULT (defaultText is empty).
	defaultSet  bool
	defaultText string
	// keepDefault is set when the defaultValueExpression of oldField was
	// checked by checkDefaults and can be given to the rebuilt table.
	keepDefault bool
}

// columnAnnotation holds the OPTIONS(description=...), the NOT NULL and the
// DEFAULT of a column, and of its nested fields, as written in ADD COLUMN or
// CREATE TABLE.
type columnAnnotation struct {
	description string
	required    bool
	// defaultText is the source text of the DEFAULT expression.
	defaultText string
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

// columnTypeName is a simple column type: a type name and its parameters,
// e.g. NUMERIC(10, 2).
type columnTypeName struct {
	// name is the canonical GoogleSQL name, e.g. INT64 for INTEGER.
	name   string
	params []int64
}

func (t *columnTypeName) String() string {
	if len(t.params) == 0 {
		return t.name
	}
	params := make([]string, 0, len(t.params))
	for _, p := range t.params {
		params = append(params, strconv.FormatInt(p, 10))
	}
	return fmt.Sprintf("%s(%s)", t.name, strings.Join(params, ", "))
}

// canonicalTypeName returns the GoogleSQL name of a simple type name, as
// written in DDL or in the table metadata.
func canonicalTypeName(name string) string {
	switch name = strings.ToUpper(name); name {
	case "INTEGER", "INT", "SMALLINT", "BIGINT", "TINYINT", "BYTEINT":
		return "INT64"
	case "FLOAT":
		return "FLOAT64"
	case "BOOLEAN":
		return "BOOL"
	case "DECIMAL":
		return "NUMERIC"
	case "BIGDECIMAL":
		return "BIGNUMERIC"
	}
	return name
}

// fieldTypeName returns the simple type of a column in the table metadata,
// or nil for an ARRAY or STRUCT column.
func fieldTypeName(field *bigqueryv2.TableFieldSchema) *columnTypeName {
	if field.Mode == "REPEATED" || len(field.Fields) != 0 {
		return nil
	}
	t := &columnTypeName{name: canonicalTypeName(field.Type)}
	switch t.name {
	case "STRING", "BYTES":
		if field.MaxLength != 0 {
			t.params = []int64{field.MaxLength}
		}
	case "NUMERIC", "BIGNUMERIC":
		if field.Precision != 0 {
			t.params = []int64{field.Precision, field.Scale}
		}
	case "RECORD", "STRUCT":
		return nil
	}
	return t
}

// setDataTypeAllowed reports whether ALTER COLUMN SET DATA TYPE may change a
// column of type from to type to, and whether the types are the same. The
// allowed changes are BigQuery's: INT64 to NUMERIC, BIGNUMERIC or FLOAT64,
// NUMERIC to BIGNUMERIC or FLOAT64, and relaxing or removing the parameters
// of STRING, BYTES, NUMERIC and BIGNUMERIC.
func setDataTypeAllowed(from, to *columnTypeName) (allowed, same bool) {
	if from.name != to.name {
		if len(to.params) != 0 {
			return false, false
		}
		switch from.name {
		case "INT64":
			return to.name == "NUMERIC" || to.name == "BIGNUMERIC" || to.name == "FLOAT64", false
		case "NUMERIC":
			return to.name == "BIGNUMERIC" || to.name == "FLOAT64", false
		}
		return false, false
	}
	if len(from.params) == len(to.params) {
		same = true
		for i := range from.params {
			same = same && from.params[i] == to.params[i]
		}
		if same {
			return true, true
		}
	}
	switch {
	case len(to.params) == 0:
		// Removing the parameters widens the type.
		return true, false
	case len(from.params) == 0:
		return false, false
	}
	switch from.name {
	case "STRING", "BYTES":
		return len(to.params) == 1 && to.params[0] >= from.params[0], false
	case "NUMERIC", "BIGNUMERIC":
		fromScale, toScale := int64(0), int64(0)
		if len(from.params) > 1 {
			fromScale = from.params[1]
		}
		if len(to.params) > 1 {
			toScale = to.params[1]
		}
		return toScale >= fromScale && to.params[0]-toScale >= from.params[0]-fromScale, false
	}
	return false, false
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
	ifExists   bool // IF EXISTS, or IF NOT EXISTS for ADD COLUMN and ADD PRIMARY KEY
	// newPath is the new table name for RENAME TO.
	newPath []string
	// options are the entries of SET OPTIONS and ALTER COLUMN SET OPTIONS.
	options []*optionValue
	// newType is the type of ALTER COLUMN SET DATA TYPE.
	newType *columnTypeName
	// defaultText is the expression of ALTER COLUMN SET DEFAULT.
	defaultText string
	// columns are the columns of ADD PRIMARY KEY.
	columns []string
}

type alterActionKind int

const (
	alterAddColumn alterActionKind = iota
	alterDropColumn
	alterRenameColumn
	alterRenameTo
	alterSetTableOptions
	alterSetColumnOptions
	alterDropNotNull
	alterSetDataType
	alterSetDefault
	alterDropDefault
	alterAddPrimaryKey
	alterDropPrimaryKey
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

// dstRef is the table the rebuild creates.
func (p *alterTablePlan) dstRef() *bigqueryv2.TableReference {
	if p.newRef != nil {
		return p.newRef
	}
	return p.ref
}

// alterTmpTablePrefix starts the name of the table a rebuild copies the rows
// through.
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
var errAlterConcurrentModification = errors.New("was modified concurrently while ALTER TABLE was running, or its columns no longer match its metadata; retry the statement, and recreate the table if it keeps failing")

// checkUnchanged verifies, inside the job transaction, that the table still
// has the columns the plan was made from. alterMu serializes ALTER TABLE
// statements, but any other DDL or API call (CREATE OR REPLACE, DROP,
// tables.update, ...) can change the table between planning and the
// rebuild. Reading the table here also holds the read lock that keeps such
// changes out until the rebuild commits.
//
// Only column names and their order are compared: a change that keeps the
// names but not the types (CREATE OR REPLACE ... AS SELECT 'x' AS a over an
// INT64 a) is not detected. REST requests are serialized by
// sequentialAccessMiddleware, so only callers outside it can cause one.
func (p *alterTablePlan) checkUnchanged(ctx context.Context, server *Server, tx *connection.Tx) error {
	if p.sql == "" {
		return nil
	}
	tableName := fmt.Sprintf("%s:%s.%s", p.ref.ProjectId, p.ref.DatasetId, p.ref.TableId)
	columns, err := server.contentRepo.TableColumnNames(ctx, tx, p.ref)
	if err != nil {
		// The error of googlesqlite may name the table by its internal name.
		return fmt.Errorf("table %s %w: %s", tableName, errAlterConcurrentModification, translateQueryError(err, p.ref.ProjectId, nil))
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
// params are the query parameters, which SET OPTIONS values may use.
func planAlterTable(ctx context.Context, server *Server, defaultProjectID, defaultDatasetID, query string, params []*bigqueryv2.QueryParameter) (*alterTablePlan, error) {
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
		return nil, fmt.Errorf("ALTER TABLE is not supported by the emulator for table %s", tableName)
	}
	for _, field := range content.Schema.Fields {
		if strings.ContainsAny(field.Name, "`\\") {
			return nil, fmt.Errorf("invalid column name %q", field.Name)
		}
	}

	plan := &alterTablePlan{ref: ref}
	for _, field := range content.Schema.Fields {
		plan.sourceColumns = append(plan.sourceColumns, field.Name)
	}
	if stmt.actions[0].kind == alterRenameTo {
		if len(stmt.actions) != 1 {
			return nil, fmt.Errorf("ALTER TABLE RENAME TO cannot be combined with other actions")
		}
		return planRenameTable(ctx, server, project, plan, content, stmt.actions[0].newPath, defaultProjectID, defaultDatasetID)
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
		copied, err := copyFieldSchema(field)
		if err != nil {
			return nil, err
		}
		columns = append(columns, &alterColumn{name: field.Name, expr: quoteIdent(field.Name), oldField: field, field: copied})
	}
	find := func(name string) int {
		for i, column := range columns {
			if strings.EqualFold(column.name, name) {
				return i
			}
		}
		return -1
	}
	// findKept finds the column of a column action other than ADD COLUMN.
	// It returns nil when an IF EXISTS action has nothing to do.
	findKept := func(action *alterAction) (*alterColumn, error) {
		i := find(action.name)
		if i < 0 {
			if action.ifExists {
				return nil, nil
			}
			return nil, fmt.Errorf("Column not found: %s", action.name)
		}
		if columns[i].oldField == nil {
			return nil, fmt.Errorf("ALTER TABLE cannot alter column %s, which the same statement adds", action.name)
		}
		return columns[i], nil
	}
	var primaryKey []string
	if content.TableConstraints != nil && content.TableConstraints.PrimaryKey != nil {
		primaryKey = append(primaryKey, content.TableConstraints.PrimaryKey.Columns...)
	}
	primaryKeyIndex := func(name string) int {
		for i, column := range primaryKey {
			if strings.EqualFold(column, name) {
				return i
			}
		}
		return -1
	}
	rebuild := false
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
			rebuild = true
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
			if primaryKeyIndex(columns[i].name) >= 0 {
				return nil, fmt.Errorf("Cannot drop column %s of table %s because it is part of the primary key", action.name, tableName)
			}
			if len(columns) == 1 {
				return nil, fmt.Errorf("Cannot drop the only column %s of table %s", action.name, tableName)
			}
			columns = append(columns[:i], columns[i+1:]...)
			rebuild = true
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
			if k := primaryKeyIndex(columns[i].name); k >= 0 {
				primaryKey[k] = action.newName
				plan.primaryKeySet = true
			}
			columns[i].name = action.newName
			if columns[i].field != nil {
				columns[i].field.Name = action.newName
			}
			rebuild = true
		case alterRenameTo:
			return nil, fmt.Errorf("ALTER TABLE RENAME TO cannot be combined with other actions")
		case alterSetTableOptions:
			for _, option := range action.options {
				if err := checkAlterOption("ALTER TABLE SET OPTIONS", option, tableOptionNames); err != nil {
					return nil, err
				}
				if option.needsEval() {
					if err := evalOptionExpr(ctx, server, defaultProjectID, defaultDatasetID, params, option); err != nil {
						return nil, fmt.Errorf("ALTER TABLE SET OPTIONS: invalid value for option %s: %w", option.name, err)
					}
				}
				if option.name == "partition_expiration_days" && option.value != nil && content.TimePartitioning == nil {
					return nil, fmt.Errorf("ALTER TABLE SET OPTIONS: partition_expiration_days can only be set on a time-partitioned table; table %s is not time-partitioned", tableName)
				}
				// Validate the value now; the options are applied
				// again to the metadata after the job committed.
				if err := applyTableOption(&bigqueryv2.Table{TimePartitioning: &bigqueryv2.TimePartitioning{}}, option); err != nil {
					return nil, fmt.Errorf("ALTER TABLE SET OPTIONS: %w", err)
				}
				plan.options = append(plan.options, option)
			}
		case alterSetColumnOptions:
			column, err := findKept(action)
			if err != nil {
				return nil, err
			}
			for _, option := range action.options {
				if err := checkAlterOption("ALTER TABLE ALTER COLUMN SET OPTIONS", option, map[string]bool{"description": true}); err != nil {
					return nil, err
				}
				if option.needsEval() {
					if err := evalOptionExpr(ctx, server, defaultProjectID, defaultDatasetID, params, option); err != nil {
						return nil, fmt.Errorf("ALTER TABLE ALTER COLUMN SET OPTIONS: invalid value for option %s: %w", option.name, err)
					}
				}
				var description string
				if option.value != nil {
					s, ok := option.value.(string)
					if !ok {
						return nil, fmt.Errorf("ALTER TABLE ALTER COLUMN SET OPTIONS: description must be a STRING")
					}
					description = s
				}
				if column != nil {
					column.field.Description = description
				}
			}
		case alterDropNotNull:
			column, err := findKept(action)
			if err != nil {
				return nil, err
			}
			if column == nil {
				continue
			}
			if column.field.Mode == "REQUIRED" {
				column.field.Mode = "NULLABLE"
			}
			// The SQLite table may have a NOT NULL constraint even
			// when the metadata does not say REQUIRED, so the table is
			// always rebuilt.
			column.dropNotNull = true
			rebuild = true
		case alterSetDataType:
			column, err := findKept(action)
			if err != nil {
				return nil, err
			}
			if column == nil {
				continue
			}
			oldType := fieldTypeName(column.field)
			allowed, same := false, false
			if oldType != nil {
				allowed, same = setDataTypeAllowed(oldType, action.newType)
			}
			if !allowed {
				from := column.field.Type
				if oldType != nil {
					from = oldType.String()
				}
				return nil, fmt.Errorf("ALTER TABLE ALTER COLUMN SET DATA TYPE requires that the existing column type (%s) is assignable to the new type (%s)", from, action.newType)
			}
			if same {
				continue
			}
			if err := checkKeyColumn("change the type of", column.oldField.Name); err != nil {
				return nil, err
			}
			column.newType = action.newType
			column.expr = fmt.Sprintf("CAST(%s AS %s)", quoteIdent(column.oldField.Name), action.newType.name)
			rebuild = true
		case alterSetDefault, alterDropDefault:
			column, err := findKept(action)
			if err != nil {
				return nil, err
			}
			if column == nil {
				continue
			}
			column.defaultSet = true
			column.defaultText = action.defaultText
			column.field.DefaultValueExpression = action.defaultText
			rebuild = true
		case alterAddPrimaryKey:
			if len(primaryKey) != 0 {
				if action.ifExists {
					continue
				}
				return nil, fmt.Errorf("Table %s already has a primary key", tableName)
			}
			var keyColumns []string
			for _, name := range action.columns {
				i := find(name)
				if i < 0 {
					return nil, fmt.Errorf("Column not found: %s", name)
				}
				keyColumns = append(keyColumns, columns[i].name)
			}
			primaryKey = keyColumns
			plan.primaryKeySet = true
		case alterDropPrimaryKey:
			if len(primaryKey) == 0 {
				if action.ifExists {
					continue
				}
				return nil, fmt.Errorf("Table %s does not have a primary key", tableName)
			}
			primaryKey = nil
			plan.primaryKeySet = true
		}
		plan.changed = true
	}
	plan.columns = columns
	plan.primaryKey = primaryKey
	if !rebuild {
		return plan, nil
	}
	if err := plan.checkDefaults(ctx, server, defaultProjectID, defaultDatasetID); err != nil {
		return nil, err
	}
	projection := make([]contentdata.ProjectedColumn, 0, len(columns))
	for _, column := range columns {
		projection = append(projection, contentdata.ProjectedColumn{Expr: column.expr, Name: column.name})
	}
	if err := plan.setRebuildSQL(server, projection); err != nil {
		return nil, err
	}
	return plan, nil
}

// checkAlterOption rejects an OPTIONS entry ALTER TABLE does not support.
func checkAlterOption(statement string, option *optionValue, supported map[string]bool) error {
	if !supported[option.name] {
		return fmt.Errorf("%s: option %s is not supported by the emulator", statement, option.name)
	}
	if !option.assign {
		return fmt.Errorf("%s: only = is supported by the emulator for option %s", statement, option.name)
	}
	if option.name == "labels" && !option.isLiteral {
		return fmt.Errorf("%s: labels must be an array of (STRING, STRING) literals in the emulator", statement)
	}
	return nil
}

// setRebuildSQL sets the first step of the rebuild, which copies the rows
// through a temporary table.
func (p *alterTablePlan) setRebuildSQL(server *Server, projection []contentdata.ProjectedColumn) error {
	// A short fixed prefix and a random suffix keep the name bounded
	// whatever the length of the table name.
	tmpRef := &bigqueryv2.TableReference{ProjectId: p.ref.ProjectId, DatasetId: p.ref.DatasetId, TableId: alterTmpTablePrefix + randomID()}
	sql, err := server.contentRepo.RebuildTableSQL(p.ref, tmpRef, projection)
	if err != nil {
		return err
	}
	p.tmpRef = tmpRef
	p.sql = sql
	return nil
}

func planRenameTable(ctx context.Context, server *Server, project *metadata.Project, plan *alterTablePlan, content *bigqueryv2.Table, newPath []string, projectID, datasetID string) (*alterTablePlan, error) {
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
		plan.columns = append(plan.columns, &alterColumn{name: field.Name, expr: quoteIdent(field.Name), oldField: field, field: field})
	}
	plan.newRef = newRef
	if err := plan.checkDefaults(ctx, server, projectID, datasetID); err != nil {
		return nil, err
	}
	if err := plan.setRebuildSQL(server, projection); err != nil {
		return nil, err
	}
	plan.changed = true
	return plan, nil
}

// alterFinishHook, when set by a test, may rewrite the second step of a
// rebuild, e.g. to make it fail after the first step ran.
var alterFinishHook struct {
	mu sync.Mutex
	fn func(sql string) string
}

func setAlterFinishHook(fn func(sql string) string) {
	alterFinishHook.mu.Lock()
	defer alterFinishHook.mu.Unlock()
	alterFinishHook.fn = fn
}

func runAlterFinishHook(sql string) string {
	alterFinishHook.mu.Lock()
	hook := alterFinishHook.fn
	alterFinishHook.mu.Unlock()
	if hook == nil {
		return sql
	}
	return hook(sql)
}

// run runs the rebuild of the plan inside tx. The first step copies the rows
// into the temporary table and drops the table. googlesqlite reports the
// dropped table's spec, with the NOT NULL and DEFAULT constraints it applies,
// from which the second step creates the final table with the same
// constraints and copies the rows back.
//
// Everything that can be checked is checked while planning, since a failure
// after the first step leaves googlesqlite's in-memory catalog without the
// table when the transaction is rolled back (goccy/googlesqlite#104); the
// caller must then call repairCatalog.
func (p *alterTablePlan) run(ctx context.Context, server *Server, tx *connection.Tx, projectID, datasetID string, params []*bigqueryv2.QueryParameter) (*internaltypes.QueryResponse, error) {
	p.ranFirstStep = true
	first, err := server.contentRepo.Query(ctx, tx, projectID, datasetID, p.sql, params)
	if first != nil && first.ChangedCatalog != nil && first.ChangedCatalog.Table != nil {
		p.srcSpec = findTableSpec(first.ChangedCatalog.Table.Deleted, p.ref)
	}
	if err != nil {
		return first, err
	}
	finish, err := p.finishSQL(ctx, server, first.ChangedCatalog)
	if err != nil {
		return nil, err
	}
	second, err := server.contentRepo.Query(ctx, tx, projectID, datasetID, runAlterFinishHook(finish), params)
	if err != nil {
		return second, err
	}
	second.ChangedCatalog = mergeChangedCatalogs(first.ChangedCatalog, second.ChangedCatalog)
	return second, nil
}

// finishSQL returns the second step of the rebuild. cat is the catalog change
// of the first step.
//
// A column keeps the NOT NULL googlesqlite enforces on it, unless the
// statement drops it, so that a table never becomes stricter than it was:
// a REQUIRED column of a table created by tables.insert has no NOT NULL in
// SQLite and may hold NULLs. Likewise a column keeps its DEFAULT only if
// googlesqlite applies one; its expression is the defaultValueExpression of
// the metadata, since googlesqlite stores only its internal form.
func (p *alterTablePlan) finishSQL(ctx context.Context, server *Server, cat *googlesqlite.ChangedCatalog) (string, error) {
	var tmp *googlesqlite.TableSpec
	if cat != nil && cat.Table != nil {
		tmp = findTableSpec(append(append([]*googlesqlite.TableSpec{}, cat.Table.Added...), cat.Table.Updated...), p.tmpRef)
	}
	if tmp == nil || len(tmp.Columns) != len(p.columns) {
		return "", fmt.Errorf("columns of the rebuilt table %s are unknown", p.ref.TableId)
	}
	columnTypes := make([]*googlesqlite.ColumnType, 0, len(tmp.Columns))
	for _, c := range tmp.Columns {
		columnTypes = append(columnTypes, c.Type)
	}
	defs, ok := p.columnDefinitions(ctx, columnTypes, p.srcSpec, true)
	if !ok {
		// CREATE TABLE ... AS SELECT copies any column type, but drops the
		// constraints.
		logger.Logger(ctx).Warn("ALTER TABLE drops the NOT NULL and DEFAULT constraints of a table: the emulator cannot express the type of one of its columns",
			zap.String("table", p.ref.TableId))
		defs = nil
	}
	return server.contentRepo.FinishRebuildTableSQL(p.tmpRef, p.dstRef(), defs)
}

// columnDefinitions returns the definitions of the columns of the plan, of
// the given types, with the constraints src reports. after selects the
// columns after the statement; otherwise they are the columns of the table
// before it, for repairCatalog. It reports false when a type cannot be
// expressed.
func (p *alterTablePlan) columnDefinitions(ctx context.Context, columnTypes []*googlesqlite.ColumnType, src *googlesqlite.TableSpec, after bool) ([]contentdata.ColumnDefinition, bool) {
	defs := make([]contentdata.ColumnDefinition, 0, len(columnTypes))
	for i, columnType := range columnTypes {
		var column *alterColumn
		var name string
		if after {
			column = p.columns[i]
			name = column.name
		} else {
			name = src.Columns[i].Name
			for _, c := range p.columns {
				if c.oldField != nil && strings.EqualFold(c.oldField.Name, name) {
					column = c
					break
				}
			}
		}
		def := contentdata.ColumnDefinition{Name: name}
		typ, ok := contentdata.FieldDDLType(types.TableFieldSchemaFromColumnType(name, columnType))
		if !ok {
			return nil, false
		}
		def.Type = typ
		var srcColumn *googlesqlite.ColumnSpec
		if column != nil && column.oldField != nil && src != nil {
			for _, c := range src.Columns {
				if strings.EqualFold(c.Name, column.oldField.Name) {
					srcColumn = c
					break
				}
			}
		}
		if srcColumn != nil && srcColumn.IsNotNull && (!after || !column.dropNotNull) {
			def.NotNull = true
		}
		switch {
		case after && column.defaultSet:
			def.Default = column.defaultText
		case srcColumn != nil && srcColumn.DefaultExpr != "":
			if column.keepDefault {
				def.Default = column.oldField.DefaultValueExpression
			} else if after {
				logger.Logger(ctx).Warn("the DEFAULT of a column is dropped by ALTER TABLE: the table metadata has no valid expression for it",
					zap.String("table", p.ref.TableId), zap.String("column", name))
			}
		}
		defs = append(defs, def)
	}
	return defs, true
}

// checkDefaults checks, before anything runs, the DEFAULT expressions the
// rebuild may give to the columns: a SET DEFAULT of the statement, which
// fails the statement if it is invalid, and the defaultValueExpression of
// the metadata, which is dropped with a warning if it is invalid (it can
// come from the tables.insert and tables.patch input).
func (p *alterTablePlan) checkDefaults(ctx context.Context, server *Server, projectID, datasetID string) error {
	for _, column := range p.columns {
		if column.oldField == nil {
			continue
		}
		var typ string
		if column.newType != nil {
			typ = column.newType.name
		} else if t, ok := contentdata.FieldDDLType(column.field); ok {
			typ = t
		}
		if column.defaultSet {
			if column.defaultText == "" {
				continue
			}
			if typ == "" {
				return fmt.Errorf("ALTER TABLE ALTER COLUMN SET DEFAULT is not supported by the emulator for column %s of type %s", column.name, column.field.Type)
			}
			if err := checkDefaultExpr(ctx, server, projectID, datasetID, column.name, typ, column.defaultText); err != nil {
				return err
			}
			continue
		}
		text := column.oldField.DefaultValueExpression
		if text == "" || typ == "" {
			continue
		}
		if err := checkDefaultExpr(ctx, server, projectID, datasetID, column.name, typ, text); err != nil {
			logger.Logger(ctx).Warn("invalid defaultValueExpression in the table metadata",
				zap.String("table", p.ref.TableId), zap.String("column", column.name), zap.Error(err))
			continue
		}
		column.keepDefault = true
	}
	return nil
}

// checkDefaultExpr checks that text is an expression that can be the
// DEFAULT of a column of type typ: an expression whose type can be assigned
// to the column. It runs read-only queries on a transaction of its own.
func checkDefaultExpr(ctx context.Context, server *Server, projectID, datasetID, column, typ, text string) error {
	if err := validateDefaultExpression(text); err != nil {
		return fmt.Errorf("Invalid default value expression for column %s: %w", column, err)
	}
	conn, err := server.connMgr.Connection(ctx, projectID, datasetID)
	if err != nil {
		return err
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.RollbackIfNotCommitted()
	// The closing parenthesis is on a line of its own, so that a trailing
	// line comment cannot comment it out.
	response, err := server.contentRepo.Query(ctx, tx, projectID, datasetID, fmt.Sprintf("SELECT (%s\n) AS v", text), nil)
	if err != nil {
		return fmt.Errorf("Invalid default value expression for column %s: %w", column, translateQueryError(err, projectID, nil))
	}
	exprType := "unknown"
	if len(response.Schema.Fields) == 1 {
		if t, ok := contentdata.FieldDDLType(response.Schema.Fields[0]); ok {
			exprType = t
		}
	}
	notAssignable := fmt.Errorf("Invalid default value expression for column %s: an expression of type %s cannot be assigned to a column of type %s", column, exprType, typ)
	// The elements of an array literal take their common supertype; the
	// expression is assignable when that type is the column's type.
	// Literals are coerced, e.g. '2024-01-01' to a DATE column.
	response, err = server.contentRepo.Query(ctx, tx, projectID, datasetID, fmt.Sprintf("SELECT [CAST(NULL AS %s), (%s\n)] AS v", typ, text), nil)
	if err != nil || len(response.Schema.Fields) != 1 {
		return notAssignable
	}
	element := *response.Schema.Fields[0]
	element.Mode = ""
	if got, ok := contentdata.FieldDDLType(&element); !ok || got != typ {
		return notAssignable
	}
	return nil
}

// repairCatalog brings googlesqlite's in-memory catalog back in line with
// the tables after a rebuild whose first step ran was rolled back: the
// rollback restores SQLite and the stored specs, but not the in-memory
// catalog, which still misses the table and knows the temporary table
// (goccy/googlesqlite#104). Failures are logged: the statement already
// failed with its own error.
//
// TODO(googlesqlite): roll back the in-memory catalog with the transaction
// (goccy/googlesqlite#104). Remove this once it does.
func (p *alterTablePlan) repairCatalog(ctx context.Context, server *Server) {
	if p == nil || !p.ranFirstStep {
		return
	}
	if err := p.restoreCatalog(ctx, server); err != nil {
		logger.Logger(ctx).Error("failed to repair the analyzer catalog after a failed ALTER TABLE",
			zap.String("table", p.ref.TableId), zap.Error(err))
	}
}

func (p *alterTablePlan) restoreCatalog(ctx context.Context, server *Server) error {
	project, err := server.metaRepo.FindProject(ctx, p.ref.ProjectId)
	if err != nil {
		return err
	}
	if project == nil || project.Dataset(p.ref.DatasetId) == nil || project.Dataset(p.ref.DatasetId).Table(p.ref.TableId) == nil {
		return fmt.Errorf("metadata of the table is not found")
	}
	content, err := project.Dataset(p.ref.DatasetId).Table(p.ref.TableId).Content()
	if err != nil {
		return err
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
	// Forget the tables the rolled-back rebuild created. Their SQLite
	// tables are gone, so this only drops their specs.
	for _, ref := range []*bigqueryv2.TableReference{p.tmpRef, p.newRef} {
		if ref == nil {
			continue
		}
		path, err := server.contentRepo.TablePath(ref.ProjectId, ref.DatasetId, ref.TableId)
		if err != nil {
			return err
		}
		if _, err := server.contentRepo.Query(ctx, tx, ref.ProjectId, ref.DatasetId, fmt.Sprintf("DROP TABLE IF EXISTS `%s`", path), nil); err != nil {
			return err
		}
	}
	// Register the table again, with its constraints when googlesqlite
	// reported them.
	restored := false
	if src := p.srcSpec; src != nil {
		columnTypes := make([]*googlesqlite.ColumnType, 0, len(src.Columns))
		for _, c := range src.Columns {
			columnTypes = append(columnTypes, c.Type)
		}
		if defs, ok := p.columnDefinitions(ctx, columnTypes, src, false); ok && len(defs) != 0 {
			sql, err := server.contentRepo.RestoreTableSpecSQL(p.ref, defs)
			if err != nil {
				return err
			}
			if _, err := server.contentRepo.Query(ctx, tx, p.ref.ProjectId, p.ref.DatasetId, sql, nil); err != nil {
				return err
			}
			restored = true
		}
	}
	if !restored {
		if err := server.contentRepo.RestoreTableSpec(ctx, tx, content); err != nil {
			return err
		}
	}
	if err := server.contentRepo.RefreshCatalog(ctx, tx); err != nil {
		return err
	}
	return tx.Commit()
}

// validateDefaultExpression checks that text is a single expression before
// it is spliced into a CREATE TABLE statement. defaultValueExpression can
// come from the tables.insert and tables.patch input.
func validateDefaultExpression(text string) error {
	opts, err := newDDLParserOptions()
	if err != nil {
		return err
	}
	out, err := googlesql.ParseExpression(text, opts)
	if err != nil {
		return err
	}
	if out == nil {
		return fmt.Errorf("not an expression")
	}
	expr, err := out.Expression()
	if err != nil || expr == nil {
		return fmt.Errorf("not an expression")
	}
	if containsSubquery(expr) {
		return fmt.Errorf("a subquery is not allowed in a default value expression")
	}
	return nil
}

func containsSubquery(node googlesql.ASTNode) bool {
	switch node.(type) {
	case *googlesql.ASTExpressionSubquery, *googlesql.ASTQuery:
		return true
	}
	for _, child := range childNodes(node) {
		if containsSubquery(child) {
			return true
		}
	}
	return false
}

func findTableSpec(specs []*googlesqlite.TableSpec, ref *bigqueryv2.TableReference) *googlesqlite.TableSpec {
	var found *googlesqlite.TableSpec
	for _, spec := range specs {
		if len(spec.NamePath) == 3 && spec.NamePath[0] == ref.ProjectId && spec.NamePath[1] == ref.DatasetId && spec.NamePath[2] == ref.TableId {
			found = spec
		}
	}
	return found
}

// mergeChangedCatalogs returns the catalog change of a followed by b, folding
// a table dropped and created again into one change as googlesqlite does
// within a script.
func mergeChangedCatalogs(a, b *googlesqlite.ChangedCatalog) *googlesqlite.ChangedCatalog {
	if a == nil || a.Table == nil {
		return b
	}
	if b == nil || b.Table == nil {
		return a
	}
	merged := &googlesqlite.ChangedCatalog{
		Table: &googlesqlite.ChangedTable{
			Added:   append([]*googlesqlite.TableSpec{}, a.Table.Added...),
			Updated: append(append([]*googlesqlite.TableSpec{}, a.Table.Updated...), b.Table.Updated...),
			Deleted: append([]*googlesqlite.TableSpec{}, a.Table.Deleted...),
		},
		Function: &googlesqlite.ChangedFunction{},
	}
	for _, f := range []*googlesqlite.ChangedFunction{a.Function, b.Function} {
		if f != nil {
			merged.Function.Added = append(merged.Function.Added, f.Added...)
			merged.Function.Deleted = append(merged.Function.Deleted, f.Deleted...)
		}
	}
	without := func(specs []*googlesqlite.TableSpec, name string) []*googlesqlite.TableSpec {
		kept := specs[:0]
		for _, spec := range specs {
			if spec.TableName() != name {
				kept = append(kept, spec)
			}
		}
		return kept
	}
	for _, spec := range b.Table.Deleted {
		merged.Table.Added = without(merged.Table.Added, spec.TableName())
		merged.Table.Deleted = append(merged.Table.Deleted, spec)
	}
	for _, spec := range b.Table.Added {
		merged.Table.Deleted = without(merged.Table.Deleted, spec.TableName())
		merged.Table.Added = append(merged.Table.Added, spec)
	}
	return merged
}

// applyMetadata updates the table metadata after the job of the plan
// committed. cat is the catalog change of the rebuild, if any.
func (p *alterTablePlan) applyMetadata(ctx context.Context, server *Server, cat *googlesqlite.ChangedCatalog) error {
	if !p.changed {
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
		return tx.Commit()
	}
	fields, err := p.newFields(cat)
	if err != nil {
		return fmt.Errorf("table %s: %w", tableName, err)
	}
	content.Schema = &bigqueryv2.TableSchema{Fields: fields}
	for _, option := range p.options {
		if err := applyTableOption(content, option); err != nil {
			return fmt.Errorf("table %s: %w", tableName, err)
		}
	}
	if p.primaryKeySet {
		if content.TableConstraints == nil {
			content.TableConstraints = &bigqueryv2.TableConstraints{}
		}
		content.TableConstraints.PrimaryKey = nil
		if len(p.primaryKey) != 0 {
			content.TableConstraints.PrimaryKey = &bigqueryv2.TableConstraintsPrimaryKey{Columns: p.primaryKey}
		}
		if content.TableConstraints.PrimaryKey == nil && len(content.TableConstraints.ForeignKeys) == 0 {
			content.TableConstraints = nil
		}
	}
	content.LastModifiedTime = uint64(time.Now().Unix())
	// Replace rather than patch, so that the options set to NULL really
	// disappear from the metadata.
	encoded, err := json.Marshal(content)
	if err != nil {
		return err
	}
	var replacement map[string]interface{}
	if err := json.Unmarshal(encoded, &replacement); err != nil {
		return err
	}
	if err := table.Replace(ctx, tx.Tx(), replacement); err != nil {
		return err
	}
	return tx.Commit()
}

// newFields returns the schema of the table after the statement. Kept and
// renamed columns keep their schema (description, mode, policy tags) with
// the changes of the statement; added and retyped columns take the type
// googlesqlite resolved for them.
func (p *alterTablePlan) newFields(cat *googlesqlite.ChangedCatalog) ([]*bigqueryv2.TableFieldSchema, error) {
	var spec *googlesqlite.TableSpec
	if p.sql != "" && cat != nil && cat.Table != nil {
		spec = findTableSpec(append(append([]*googlesqlite.TableSpec{}, cat.Table.Added...), cat.Table.Updated...), p.dstRef())
	}
	fields := make([]*bigqueryv2.TableFieldSchema, 0, len(p.columns))
	for _, column := range p.columns {
		if column.field != nil && column.newType == nil {
			fields = append(fields, column.field)
			continue
		}
		var resolved *googlesqlite.ColumnSpec
		if spec != nil {
			for _, c := range spec.Columns {
				if strings.EqualFold(c.Name, column.name) {
					resolved = c
					break
				}
			}
		}
		if resolved == nil {
			return nil, fmt.Errorf("type of column %s is unknown", column.name)
		}
		field := types.TableFieldSchemaFromColumnType(column.name, resolved.Type)
		if column.field != nil {
			// ALTER COLUMN SET DATA TYPE keeps everything but the type.
			retyped := column.field
			retyped.Type = field.Type
			retyped.MaxLength, retyped.Precision, retyped.Scale = 0, 0, 0
			switch params := column.newType.params; {
			case len(params) == 1 && (column.newType.name == "STRING" || column.newType.name == "BYTES"):
				retyped.MaxLength = params[0]
			case len(params) >= 1 && (column.newType.name == "NUMERIC" || column.newType.name == "BIGNUMERIC"):
				retyped.Precision = params[0]
				if len(params) == 2 {
					retyped.Scale = params[1]
				}
			}
			fields = append(fields, retyped)
			continue
		}
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
// false when query is anything else or cannot be parsed, which leaves it to
// googlesqlite. An error is returned for an action the emulator does not
// support, or that uses a feature the emulator cannot honour.
func parseAlterTable(query string) (*parsedAlterTable, bool, error) {
	opts, err := newDDLParserOptions()
	if err != nil {
		return nil, false, nil
	}
	out, err := googlesql.ParseStatement(query, opts)
	if err != nil || out == nil {
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

// parseAlterAction parses one action. It reports false when the action is
// shaped unexpectedly, and returns an error for an action the emulator does
// not support.
func parseAlterAction(query string, node googlesql.ASTAlterActionNode) (*alterAction, bool, error) {
	unsupported := func() (*alterAction, bool, error) {
		keyword, err := node.GetSQLForAlterAction()
		if err != nil || keyword == "" {
			keyword = "action"
		}
		return nil, false, fmt.Errorf("ALTER TABLE %s is not supported by the emulator", keyword)
	}
	switch node := node.(type) {
	case *googlesql.ASTAddColumnAction:
		// ADD COLUMN ... FIRST/AFTER and FILL USING are not BigQuery.
		if position, err := node.ColumnPosition(); err != nil || position != nil {
			return nil, false, fmt.Errorf("ALTER TABLE ADD COLUMN: a column position is not supported by the emulator")
		}
		if fill, err := node.FillExpression(); err != nil || fill != nil {
			return nil, false, fmt.Errorf("ALTER TABLE ADD COLUMN: FILL USING is not supported by the emulator")
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
	case *googlesql.ASTSetOptionsAction:
		list, err := node.OptionsList()
		if err != nil || list == nil {
			return nil, false, nil
		}
		options, err := parseOptions(query, list)
		if err != nil {
			return nil, false, nil
		}
		return &alterAction{kind: alterSetTableOptions, options: options}, true, nil
	case *googlesql.ASTAlterColumnOptionsAction:
		name, ok := identifierString(node.ColumnName())
		if !ok {
			return nil, false, nil
		}
		ifExists, err := node.IsIfExists()
		if err != nil {
			return nil, false, nil
		}
		list, err := node.OptionsList()
		if err != nil || list == nil {
			return nil, false, nil
		}
		options, err := parseOptions(query, list)
		if err != nil {
			return nil, false, nil
		}
		return &alterAction{kind: alterSetColumnOptions, name: name, ifExists: ifExists, options: options}, true, nil
	case *googlesql.ASTAlterColumnDropNotNullAction:
		name, ok := identifierString(node.ColumnName())
		if !ok {
			return nil, false, nil
		}
		ifExists, err := node.IsIfExists()
		if err != nil {
			return nil, false, nil
		}
		return &alterAction{kind: alterDropNotNull, name: name, ifExists: ifExists}, true, nil
	case *googlesql.ASTAlterColumnTypeAction:
		name, ok := identifierString(node.ColumnName())
		if !ok {
			return nil, false, nil
		}
		ifExists, err := node.IsIfExists()
		if err != nil {
			return nil, false, nil
		}
		if collate, err := node.Collate(); err != nil || collate != nil {
			return nil, false, fmt.Errorf("ALTER TABLE ALTER COLUMN SET DATA TYPE: COLLATE is not supported by the emulator")
		}
		schema, err := node.Schema()
		if err != nil || schema == nil {
			return nil, false, nil
		}
		newType, err := simpleColumnType(schema)
		if err != nil {
			return nil, false, err
		}
		return &alterAction{kind: alterSetDataType, name: name, ifExists: ifExists, newType: newType}, true, nil
	case *googlesql.ASTAlterColumnSetDefaultAction:
		name, ok := identifierString(node.ColumnName())
		if !ok {
			return nil, false, nil
		}
		ifExists, err := node.IsIfExists()
		if err != nil {
			return nil, false, nil
		}
		def, err := node.DefaultExpression()
		if err != nil || def == nil {
			return nil, false, nil
		}
		text, ok := sourceText(query, def)
		if !ok {
			return nil, false, nil
		}
		return &alterAction{kind: alterSetDefault, name: name, ifExists: ifExists, defaultText: text}, true, nil
	case *googlesql.ASTAlterColumnDropDefaultAction:
		name, ok := identifierString(node.ColumnName())
		if !ok {
			return nil, false, nil
		}
		ifExists, err := node.IsIfExists()
		if err != nil {
			return nil, false, nil
		}
		return &alterAction{kind: alterDropDefault, name: name, ifExists: ifExists}, true, nil
	case *googlesql.ASTAddConstraintAction:
		constraint, err := node.Constraint()
		if err != nil || constraint == nil {
			return nil, false, nil
		}
		key, ok := constraint.(*googlesql.ASTPrimaryKey)
		if !ok {
			return unsupported()
		}
		if name, err := key.ConstraintName(); err != nil || name != nil {
			return nil, false, fmt.Errorf("ALTER TABLE ADD CONSTRAINT: a named PRIMARY KEY is not supported by the emulator")
		}
		enforced, err := key.Enforced()
		if err != nil {
			return nil, false, nil
		}
		if enforced {
			return nil, false, fmt.Errorf("PRIMARY KEY must be NOT ENFORCED")
		}
		columns, ok := primaryKeyColumns(key)
		if !ok {
			return nil, false, nil
		}
		ifNotExists, err := node.IsIfNotExists()
		if err != nil {
			return nil, false, nil
		}
		return &alterAction{kind: alterAddPrimaryKey, columns: columns, ifExists: ifNotExists}, true, nil
	case *googlesql.ASTDropPrimaryKeyAction:
		ifExists, err := node.IsIfExists()
		if err != nil {
			return nil, false, nil
		}
		return &alterAction{kind: alterDropPrimaryKey, ifExists: ifExists}, true, nil
	}
	return unsupported()
}

// simpleColumnType returns the type of an ALTER COLUMN SET DATA TYPE
// schema, which must be a simple type without attributes.
func simpleColumnType(schema googlesql.ASTColumnSchemaNode) (*columnTypeName, error) {
	unsupported := func(feature string) error {
		return fmt.Errorf("ALTER TABLE ALTER COLUMN SET DATA TYPE: %s is not supported by the emulator", feature)
	}
	simple, ok := schema.(*googlesql.ASTSimpleColumnSchema)
	if !ok {
		return nil, unsupported("this type")
	}
	if def, err := schema.DefaultExpression(); err != nil || def != nil {
		return nil, unsupported("DEFAULT")
	}
	if collate, err := schema.Collate(); err != nil || collate != nil {
		return nil, unsupported("COLLATE")
	}
	if options, err := schema.OptionsList(); err != nil || options != nil {
		return nil, unsupported("OPTIONS")
	}
	if attributes, err := schema.Attributes(); err != nil || attributes != nil {
		return nil, unsupported("a column attribute")
	}
	typeName, err := simple.TypeName()
	if err != nil || typeName == nil {
		return nil, unsupported("this type")
	}
	name, ok := singleName(typeName)
	if !ok {
		return nil, unsupported("this type")
	}
	t := &columnTypeName{name: canonicalTypeName(name)}
	if params, err := simple.TypeParameters(); err == nil && params != nil {
		for _, param := range childNodes(params) {
			v, ok := decodeOptionLiteral(param)
			if !ok {
				return nil, unsupported("this type parameter")
			}
			i, ok := v.(int64)
			if !ok {
				return nil, unsupported("this type parameter")
			}
			t.params = append(t.params, i)
		}
	}
	return t, nil
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
