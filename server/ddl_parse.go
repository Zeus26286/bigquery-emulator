package server

import (
	"context"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/goccy/go-googlesql"
	"go.uber.org/zap"
	bigqueryv2 "google.golang.org/api/bigquery/v2"

	"github.com/goccy/bigquery-emulator/internal/logger"
	"github.com/goccy/bigquery-emulator/internal/metadata"
)

// googlesqlite runs CREATE TABLE faithfully but reports only the names and
// types of the columns it created (ChangedCatalog): PARTITION BY, CLUSTER BY,
// column OPTIONS and, for CREATE TABLE ... AS SELECT, the table OPTIONS never
// reach the emulator. The query text is therefore parsed here as well, and
// what it says beyond the columns is applied to the table metadata by
// syncCatalog.
//
// The parse is advisory: when it fails, or a node is shaped unexpectedly,
// nothing is extracted and the query runs exactly as before; googlesqlite
// reports any syntax error. Only the checks of analyzeDDL and checkDatasets
// reject a query, and only on input they positively recognise.
//
// TODO(googlesqlite): carry PARTITION BY, CLUSTER BY, column OPTIONS and the
// OPTIONS of CREATE TABLE ... AS SELECT in TableSpec (no upstream issue yet).

// ddlInfo is what the emulator learns from a query's text before running it.
type ddlInfo struct {
	// statements is the number of top-level statements of the script.
	statements int
	// creates are the CREATE TABLE and CREATE VIEW statements of the
	// script that create a dataset table, in textual order.
	creates []*createTableDDL
	// createsSchema is set when the script contains CREATE SCHEMA.
	createsSchema bool
}

// createTableDDL is what a CREATE TABLE or CREATE VIEW statement says about
// the table beyond its column names and types.
type createTableDDL struct {
	ref    *bigqueryv2.TableReference
	isView bool
	// columns are the annotations of the columns, by lower-cased name.
	columns   map[string]*columnAnnotation
	partition *partitionSpec
	// partitionIgnored is set for a PARTITION BY expression that is not
	// recognised; googlesqlite accepted it, so it only causes a warning.
	partitionIgnored bool
	clustering       []string
	options          []*optionValue
	primaryKey       []string
}

// optionValue is one entry of an OPTIONS list.
type optionValue struct {
	// name is lower-cased.
	name string
	// value is the decoded value when isLiteral is set: a string, int64,
	// float64, bool, map[string]string for labels, or nil for NULL.
	value     any
	isLiteral bool
	// exprText is the source text of the value, evaluated by
	// evalOptionExprs when it is not a literal.
	exprText string
	// assign is false for += and -=.
	assign bool
}

// partitionSpec is a PARTITION BY clause.
type partitionSpec struct {
	// kind is "time" or "range".
	kind string
	// unit is DAY, HOUR, MONTH or YEAR for time partitioning.
	unit string
	// column is the partitioning column; empty for ingestion time.
	column string
	// function is the upper-cased function applied to the column, e.g.
	// DATE or TIMESTAMP_TRUNC; empty for the column itself.
	function             string
	start, end, interval int64
}

// partitionColumnTypes are the column types each time partitioning
// function applies to.
var partitionColumnTypes = map[string]string{
	"":                "DATE",
	"DATE":            "TIMESTAMP DATETIME",
	"TIMESTAMP_TRUNC": "TIMESTAMP",
	"DATETIME_TRUNC":  "DATETIME",
	"DATE_TRUNC":      "DATE",
}

var ddlKeywordPattern = regexp.MustCompile(`(?i)\b(CREATE|ALTER)\b`)

// errAlterInScript rejects ALTER TABLE inside a multi-statement script.
// Splitting the script into statements run one by one would break script
// variables, temp tables, control flow and the result of the last statement,
// and a rebuild planned before the script ran cannot see what the statements
// before the ALTER TABLE did.
//
// TODO(googlesqlite): implement ALTER TABLE (no upstream issue yet; it runs
// as a no-op). Scripts can then be left to googlesqlite.
var errAlterInScript = fmt.Errorf("ALTER TABLE inside a multi-statement script is not supported by the emulator; run each ALTER TABLE as a separate query")

// ddlParserOptions are the parser options of the emulator's own parse of
// DDL statements. They are built once: every object allocated in the
// googlesql wasm module is freed only by a finalizer, and building them for
// each query grows the module's memory.
var ddlParserOptions struct {
	once sync.Once
	opts *googlesql.ParserOptions
	err  error
}

// newDDLParserOptions returns the parser options for the emulator's own
// parse of DDL statements. Without an arena, each parse allocates its tree
// in an arena of its own, so the options can be shared.
func newDDLParserOptions() (*googlesql.ParserOptions, error) {
	ddlParserOptions.once.Do(func() {
		ddlParserOptions.opts, ddlParserOptions.err = buildDDLParserOptions()
	})
	return ddlParserOptions.opts, ddlParserOptions.err
}

func buildDDLParserOptions() (*googlesql.ParserOptions, error) {
	if err := googlesql.Init(); err != nil {
		return nil, err
	}
	languageOptions, err := googlesql.NewLanguageOptions()
	if err != nil {
		return nil, err
	}
	if err := languageOptions.EnableMaximumLanguageFeatures(); err != nil {
		return nil, err
	}
	// Parse DEFAULT, so that it is recognised instead of being a syntax
	// error that leaves the statement to googlesqlite.
	if err := languageOptions.EnableLanguageFeature(googlesql.LanguageFeatureFeatureColumnDefaultValue); err != nil {
		return nil, err
	}
	return googlesql.NewParserOptions2(nil, nil, languageOptions)
}

// analyzeDDL parses query and returns its CREATE TABLE and CREATE VIEW
// statements. It returns (nil, nil) when query has no DDL to look at or
// cannot be parsed. An error is the query's error: an unqualified table name
// without a default dataset, or ALTER TABLE inside a multi-statement script.
func analyzeDDL(query, defaultProjectID, defaultDatasetID string) (*ddlInfo, error) {
	if !ddlKeywordPattern.MatchString(query) {
		return nil, nil
	}
	opts, err := newDDLParserOptions()
	if err != nil {
		return nil, nil
	}
	out, err := googlesql.ParseScript(query, opts, nil)
	if err != nil || out == nil {
		return nil, nil
	}
	script, err := out.Script()
	if err != nil || script == nil {
		return nil, nil
	}
	list, err := script.StatementListNode()
	if err != nil || list == nil {
		return nil, nil
	}
	n, err := list.NumChildren()
	if err != nil {
		return nil, nil
	}
	a := &ddlAnalyzer{
		query:            query,
		defaultProjectID: defaultProjectID,
		defaultDatasetID: defaultDatasetID,
		info:             &ddlInfo{statements: int(n)},
	}
	for i := int32(0); i < n; i++ {
		child, err := list.Child(i)
		if err != nil {
			return nil, nil
		}
		if err := a.walk(child, true); err != nil {
			return nil, err
		}
	}
	return a.info, nil
}

type ddlAnalyzer struct {
	query            string
	defaultProjectID string
	defaultDatasetID string
	info             *ddlInfo
}

func (a *ddlAnalyzer) walk(node googlesql.ASTNode, topLevel bool) error {
	switch node := node.(type) {
	case *googlesql.ASTQuery:
		// Queries contain no DDL.
		return nil
	case *googlesql.ASTCreateProcedureStatement, *googlesql.ASTCreateFunctionStatement, *googlesql.ASTCreateTableFunctionStatement:
		// The body of a routine does not run when it is created.
		return nil
	case *googlesql.ASTCreateSchemaStatement:
		a.info.createsSchema = true
		return nil
	case *googlesql.ASTAlterTableStatement:
		if !topLevel || a.info.statements != 1 {
			return errAlterInScript
		}
		return nil
	case *googlesql.ASTCreateTableStatement:
		return a.createTable(node)
	case *googlesql.ASTCreateViewStatement:
		return a.createView(node)
	}
	n, err := node.NumChildren()
	if err != nil {
		return nil
	}
	for i := int32(0); i < n; i++ {
		child, err := node.Child(i)
		if err != nil || child == nil {
			continue
		}
		if err := a.walk(child, false); err != nil {
			return err
		}
	}
	return nil
}

// tableRef resolves the name of a non-TEMP CREATE statement. It returns nil
// for a name it does not resolve, and an error for an unqualified name
// without a default dataset.
func (a *ddlAnalyzer) tableRef(name *googlesql.ASTPathExpression) (*bigqueryv2.TableReference, error) {
	if name == nil {
		return nil, nil
	}
	parts, err := name.ToIdentifierVector()
	if err != nil {
		return nil, nil
	}
	var path []string
	for _, part := range parts {
		path = append(path, strings.Split(part, ".")...)
	}
	ref := &bigqueryv2.TableReference{ProjectId: a.defaultProjectID, DatasetId: a.defaultDatasetID}
	switch len(path) {
	case 1:
		if a.defaultDatasetID == "" {
			return nil, fmt.Errorf("Table \"%s\" must be qualified with a dataset (e.g. dataset.table).", path[0])
		}
		ref.TableId = path[0]
	case 2:
		ref.DatasetId, ref.TableId = path[0], path[1]
	case 3:
		ref.ProjectId, ref.DatasetId, ref.TableId = path[0], path[1], path[2]
	default:
		return nil, nil
	}
	if ref.ProjectId == "" || ref.DatasetId == "" || ref.TableId == "" {
		return nil, nil
	}
	return ref, nil
}

func (a *ddlAnalyzer) createView(node *googlesql.ASTCreateViewStatement) error {
	if temp, err := node.IsTemp(); err != nil || temp {
		return nil
	}
	name, err := node.Name()
	if err != nil {
		return nil
	}
	ref, err := a.tableRef(name)
	if err != nil || ref == nil {
		return err
	}
	create := &createTableDDL{ref: ref, isView: true}
	if options, err := node.OptionsList(); err == nil && options != nil {
		create.options = a.options(options)
	}
	a.info.creates = append(a.info.creates, create)
	return nil
}

func (a *ddlAnalyzer) createTable(node *googlesql.ASTCreateTableStatement) error {
	if temp, err := node.IsTemp(); err != nil || temp {
		return nil
	}
	name, err := node.Name()
	if err != nil {
		return nil
	}
	ref, err := a.tableRef(name)
	if err != nil || ref == nil {
		return err
	}
	// googlesqlite rejects LIKE, COPY and CLONE.
	if like, err := node.LikeTableName(); err != nil || like != nil {
		return nil
	}
	if copySource, err := node.CopyDataSource(); err != nil || copySource != nil {
		return nil
	}
	if cloneSource, err := node.CloneDataSource(); err != nil || cloneSource != nil {
		return nil
	}
	create := &createTableDDL{ref: ref, columns: map[string]*columnAnnotation{}}
	if elements, err := node.TableElementList(); err == nil && elements != nil {
		a.tableElements(create, elements)
	}
	if partitionBy, err := node.PartitionBy(); err == nil && partitionBy != nil {
		if spec, ok := parsePartitionBy(partitionBy); ok {
			create.partition = spec
		} else {
			create.partitionIgnored = true
		}
	}
	if clusterBy, err := node.ClusterBy(); err == nil && clusterBy != nil {
		create.clustering = parseClusterBy(clusterBy)
	}
	if options, err := node.OptionsList(); err == nil && options != nil {
		create.options = a.options(options)
	}
	a.info.creates = append(a.info.creates, create)
	return nil
}

func (a *ddlAnalyzer) tableElements(create *createTableDDL, elements *googlesql.ASTTableElementList) {
	n, err := elements.NumChildren()
	if err != nil {
		return
	}
	for i := int32(0); i < n; i++ {
		child, err := elements.Child(i)
		if err != nil || child == nil {
			continue
		}
		switch element := child.(type) {
		case *googlesql.ASTColumnDefinition:
			name, ok := identifierString(element.Name())
			if !ok {
				continue
			}
			schema, err := element.Schema()
			if err != nil || schema == nil {
				continue
			}
			annotation := &columnAnnotation{fields: map[string]*columnAnnotation{}}
			if description, err := optionsDescription(schema); err == nil {
				annotation.description = description
			}
			if _, isArray := schema.(*googlesql.ASTArrayColumnSchema); !isArray {
				if notNull, err := schema.ContainsAttribute(googlesql.ASTNodeKindAstNotNullColumnAttribute); err == nil {
					annotation.required = notNull
				}
			}
			if def, err := schema.DefaultExpression(); err == nil && def != nil {
				if text, ok := sourceText(a.query, def); ok {
					annotation.defaultText = text
				}
			}
			if primaryKey, err := schema.ContainsAttribute(googlesql.ASTNodeKindAstPrimaryKeyColumnAttribute); err == nil && primaryKey {
				create.primaryKey = append(create.primaryKey, name)
			}
			create.columns[strings.ToLower(name)] = annotation
		case *googlesql.ASTPrimaryKey:
			if columns, ok := primaryKeyColumns(element); ok {
				create.primaryKey = columns
			}
		}
	}
}

// primaryKeyColumns returns the columns of a PRIMARY KEY constraint.
func primaryKeyColumns(key *googlesql.ASTPrimaryKey) ([]string, bool) {
	list, err := key.ElementList()
	if err != nil || list == nil {
		return nil, false
	}
	n, err := list.NumChildren()
	if err != nil {
		return nil, false
	}
	var columns []string
	for i := int32(0); i < n; i++ {
		element, err := list.Elements(i)
		if err != nil || element == nil {
			return nil, false
		}
		name, ok := identifierString(element.Column())
		if !ok {
			return nil, false
		}
		columns = append(columns, name)
	}
	return columns, len(columns) != 0
}

func (a *ddlAnalyzer) options(list *googlesql.ASTOptionsList) []*optionValue {
	options, err := parseOptions(a.query, list)
	if err != nil {
		return nil
	}
	return options
}

// parseOptions decodes an OPTIONS list. Values that are not literals keep
// their source text.
func parseOptions(query string, list *googlesql.ASTOptionsList) ([]*optionValue, error) {
	n, err := list.NumChildren()
	if err != nil {
		return nil, err
	}
	var options []*optionValue
	for i := int32(0); i < n; i++ {
		entry, err := list.OptionsEntries(i)
		if err != nil || entry == nil {
			return nil, fmt.Errorf("invalid OPTIONS entry")
		}
		name, ok := identifierString(entry.Name())
		if !ok {
			return nil, fmt.Errorf("invalid OPTIONS entry")
		}
		option := &optionValue{name: strings.ToLower(name), assign: true}
		if op, err := entry.AssignmentOp(); err == nil && op != googlesql.ASTOptionsEntryEnums_AssignmentOpAssign && op != googlesql.ASTOptionsEntryEnums_AssignmentOpNotSet {
			option.assign = false
		}
		value, err := entry.Value()
		if err != nil || value == nil {
			return nil, fmt.Errorf("invalid OPTIONS entry %s", name)
		}
		if decoded, ok := decodeOptionLiteral(value); ok {
			option.value = decoded
			option.isLiteral = true
		} else {
			text, ok := sourceText(query, value)
			if !ok {
				return nil, fmt.Errorf("invalid OPTIONS entry %s", name)
			}
			option.exprText = text
		}
		options = append(options, option)
	}
	return options, nil
}

// decodeOptionLiteral decodes a literal OPTIONS value, including the
// ARRAY<STRUCT<STRING, STRING>> of labels.
func decodeOptionLiteral(node googlesql.ASTNode) (any, bool) {
	switch node := node.(type) {
	case *googlesql.ASTStringLiteral:
		s, err := node.StringValue()
		return s, err == nil
	case *googlesql.ASTIntLiteral:
		image, err := node.Image()
		if err != nil {
			return nil, false
		}
		v, err := strconv.ParseInt(image, 0, 64)
		return v, err == nil
	case *googlesql.ASTFloatLiteral:
		image, err := node.Image()
		if err != nil {
			return nil, false
		}
		v, err := strconv.ParseFloat(image, 64)
		return v, err == nil
	case *googlesql.ASTBooleanLiteral:
		v, err := node.Value()
		return v, err == nil
	case *googlesql.ASTNullLiteral:
		return nil, true
	case *googlesql.ASTUnaryExpression:
		op, err := node.Op()
		if err != nil || op != googlesql.ASTUnaryExpressionEnums_OpMinus {
			return nil, false
		}
		operand, err := node.Operand()
		if err != nil || operand == nil {
			return nil, false
		}
		v, ok := decodeOptionLiteral(operand)
		if !ok {
			return nil, false
		}
		switch v := v.(type) {
		case int64:
			return -v, true
		case float64:
			return -v, true
		}
		return nil, false
	case *googlesql.ASTArrayConstructor:
		return decodeLabels(node)
	}
	return nil, false
}

// decodeLabels decodes [('k', 'v'), STRUCT('k2' AS a, 'v2' AS b), ...].
func decodeLabels(node *googlesql.ASTArrayConstructor) (any, bool) {
	n, err := node.NumChildren()
	if err != nil {
		return nil, false
	}
	labels := map[string]string{}
	for i := int32(0); i < n; i++ {
		child, err := node.Child(i)
		if err != nil || child == nil {
			return nil, false
		}
		var exprs []googlesql.ASTNode
		switch element := child.(type) {
		case *googlesql.ASTArrayType:
			continue
		case *googlesql.ASTStructConstructorWithParens:
			exprs = childNodes(element)
		case *googlesql.ASTStructConstructorWithKeyword:
			for _, field := range childNodes(element) {
				arg, ok := field.(*googlesql.ASTStructConstructorArg)
				if !ok {
					continue
				}
				expr, err := arg.Expression()
				if err != nil || expr == nil {
					return nil, false
				}
				exprs = append(exprs, expr)
			}
		default:
			return nil, false
		}
		if len(exprs) != 2 {
			return nil, false
		}
		key, ok := exprs[0].(*googlesql.ASTStringLiteral)
		if !ok {
			return nil, false
		}
		value, ok := exprs[1].(*googlesql.ASTStringLiteral)
		if !ok {
			return nil, false
		}
		k, err := key.StringValue()
		if err != nil {
			return nil, false
		}
		v, err := value.StringValue()
		if err != nil {
			return nil, false
		}
		labels[k] = v
	}
	return labels, true
}

func childNodes(node googlesql.ASTNode) []googlesql.ASTNode {
	n, err := node.NumChildren()
	if err != nil {
		return nil
	}
	children := make([]googlesql.ASTNode, 0, n)
	for i := int32(0); i < n; i++ {
		child, err := node.Child(i)
		if err != nil || child == nil {
			return nil
		}
		children = append(children, child)
	}
	return children
}

// sourceText returns the source text of node.
func sourceText(query string, node googlesql.ASTNode) (string, bool) {
	start, end, ok := nodeOffsets(node)
	if !ok || start < 0 || end > len(query) || start >= end {
		return "", false
	}
	return strings.TrimSpace(query[start:end]), true
}

// singleName returns the name of a path expression with one identifier.
func singleName(node googlesql.ASTNode) (string, bool) {
	path, ok := node.(*googlesql.ASTPathExpression)
	if !ok {
		return "", false
	}
	n, err := path.NumNames()
	if err != nil || n != 1 {
		return "", false
	}
	return identifierString(path.Name(0))
}

// functionCall returns the upper-cased name and the arguments of a plain
// function call.
func functionCall(node googlesql.ASTNode) (string, []googlesql.ASTNode, bool) {
	call, ok := node.(*googlesql.ASTFunctionCall)
	if !ok {
		return "", nil, false
	}
	if modifiers, err := call.HasModifiers(false); err != nil || modifiers {
		return "", nil, false
	}
	function, err := call.Function()
	if err != nil || function == nil {
		return "", nil, false
	}
	name, ok := singleName(function)
	if !ok {
		return "", nil, false
	}
	children := childNodes(call)
	if len(children) == 0 {
		return "", nil, false
	}
	// The first child is the function name.
	return strings.ToUpper(name), children[1:], true
}

// parsePartitionBy recognises the PARTITION BY expressions of BigQuery.
func parsePartitionBy(node *googlesql.ASTPartitionBy) (*partitionSpec, bool) {
	var exprs []googlesql.ASTNode
	for _, child := range childNodes(node) {
		if _, isHint := child.(*googlesql.ASTHint); isHint {
			continue
		}
		exprs = append(exprs, child)
	}
	if len(exprs) != 1 {
		return nil, false
	}
	// timeColumn returns the column of a time-unit partitioning, or "" for
	// ingestion time (_PARTITIONTIME or _PARTITIONDATE as allowed).
	timeColumn := func(node googlesql.ASTNode, ingestion string) (string, bool) {
		name, ok := singleName(node)
		if !ok {
			return "", false
		}
		if strings.EqualFold(name, ingestion) {
			return "", true
		}
		if strings.HasPrefix(strings.ToUpper(name), "_PARTITION") {
			return "", false
		}
		return name, true
	}
	expr := exprs[0]
	if column, ok := timeColumn(expr, "_PARTITIONDATE"); ok {
		return &partitionSpec{kind: "time", unit: "DAY", column: column}, true
	}
	name, args, ok := functionCall(expr)
	if !ok {
		return nil, false
	}
	switch name {
	case "DATE":
		if len(args) != 1 {
			return nil, false
		}
		column, ok := timeColumn(args[0], "_PARTITIONTIME")
		if !ok {
			return nil, false
		}
		return &partitionSpec{kind: "time", unit: "DAY", column: column, function: name}, true
	case "TIMESTAMP_TRUNC", "DATETIME_TRUNC", "DATE_TRUNC":
		if len(args) != 2 {
			return nil, false
		}
		// The ingestion-time pseudo column each function applies to.
		ingestion := map[string]string{"TIMESTAMP_TRUNC": "_PARTITIONTIME", "DATE_TRUNC": "_PARTITIONDATE"}[name]
		column, ok := timeColumn(args[0], ingestion)
		if !ok || column == "" && ingestion == "" {
			return nil, false
		}
		unit, ok := singleName(args[1])
		if !ok {
			return nil, false
		}
		unit = strings.ToUpper(unit)
		switch unit {
		case "HOUR", "DAY":
			// DATE_TRUNC partitions only by MONTH or YEAR.
			if name == "DATE_TRUNC" {
				return nil, false
			}
		case "MONTH", "YEAR":
		default:
			return nil, false
		}
		return &partitionSpec{kind: "time", unit: unit, column: column, function: name}, true
	case "RANGE_BUCKET":
		if len(args) != 2 {
			return nil, false
		}
		column, ok := singleName(args[0])
		if !ok {
			return nil, false
		}
		generate, bounds, ok := functionCall(args[1])
		if !ok || generate != "GENERATE_ARRAY" || len(bounds) < 2 || len(bounds) > 3 {
			return nil, false
		}
		values := make([]int64, 0, 3)
		for _, bound := range bounds {
			v, ok := decodeOptionLiteral(bound)
			if !ok {
				return nil, false
			}
			i, ok := v.(int64)
			if !ok {
				return nil, false
			}
			values = append(values, i)
		}
		spec := &partitionSpec{kind: "range", column: column, start: values[0], end: values[1], interval: 1}
		if len(values) == 3 {
			spec.interval = values[2]
		}
		return spec, true
	}
	return nil, false
}

// partitionColumnTypeOK reports whether the partitioning applies to the
// type of its column, e.g. TIMESTAMP_TRUNC to a TIMESTAMP column.
func partitionColumnTypeOK(p *partitionSpec, field *bigqueryv2.TableFieldSchema) bool {
	if field.Mode == "REPEATED" {
		return false
	}
	typ := strings.ToUpper(field.Type)
	if p.kind == "range" {
		return typ == "INTEGER" || typ == "INT64"
	}
	for _, allowed := range strings.Fields(partitionColumnTypes[p.function]) {
		if typ == allowed {
			return true
		}
	}
	return false
}

// parseClusterBy returns the clustering columns, or nil when one of them is
// not a plain column name.
func parseClusterBy(node *googlesql.ASTClusterBy) []string {
	var columns []string
	for _, child := range childNodes(node) {
		name, ok := singleName(child)
		if !ok {
			return nil
		}
		columns = append(columns, name)
	}
	return columns
}

// lookup returns the last CREATE statement of the script for a table
// googlesqlite reports as created, or nil.
func (d *ddlInfo) lookup(namePath []string, isView bool) *createTableDDL {
	if d == nil || len(namePath) != 3 {
		return nil
	}
	for i := len(d.creates) - 1; i >= 0; i-- {
		c := d.creates[i]
		if c.isView == isView && c.ref.ProjectId == namePath[0] && c.ref.DatasetId == namePath[1] && c.ref.TableId == namePath[2] {
			return c
		}
	}
	return nil
}

// checkDatasets rejects a CREATE statement for a table of a dataset that does
// not exist. googlesqlite would create the table anyway, outside any dataset.
// It reads the metadata through another connection, so it must run before
// the job transaction starts.
func (d *ddlInfo) checkDatasets(ctx context.Context, server *Server) error {
	if d == nil || d.createsSchema {
		return nil
	}
	projects := map[string]*metadata.Project{}
	for _, c := range d.creates {
		project, found := projects[c.ref.ProjectId]
		if !found {
			var err error
			project, err = server.metaRepo.FindProject(ctx, c.ref.ProjectId)
			if err != nil {
				project = nil
			}
			projects[c.ref.ProjectId] = project
		}
		if project == nil || project.Dataset(c.ref.DatasetId) == nil {
			return errNotFound(fmt.Sprintf("Not found: Dataset %s:%s", c.ref.ProjectId, c.ref.DatasetId))
		}
	}
	return nil
}

// evalOptions evaluates the option values that are not literals. An option
// that cannot be evaluated is left out with a warning: the CREATE statement
// already succeeded.
func (c *createTableDDL) evalOptions(ctx context.Context, server *Server, projectID, datasetID string, params []*bigqueryv2.QueryParameter) {
	for _, option := range c.options {
		if !option.assign || !option.needsEval() {
			continue
		}
		if err := evalOptionExpr(ctx, server, projectID, datasetID, params, option); err != nil {
			logger.Logger(ctx).Warn("table option is not recorded in the table metadata",
				zap.String("table", c.ref.TableId), zap.String("option", option.name), zap.Error(err))
		}
	}
}

// apply sets what the statement says beyond column names and types on the
// table metadata googlesqlite's catalog change produced.
func (c *createTableDDL) apply(ctx context.Context, table *bigqueryv2.Table) {
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
	warn := func(msg, column string) {
		logger.Logger(ctx).Warn(msg, zap.String("table", c.ref.TableId), zap.String("column", column))
	}
	for _, field := range fields {
		annotation := c.columns[strings.ToLower(field.Name)]
		if annotation == nil {
			continue
		}
		if annotation.description != "" {
			field.Description = annotation.description
		}
		if annotation.required && field.Mode != "REPEATED" {
			field.Mode = "REQUIRED"
		}
		if annotation.defaultText != "" {
			field.DefaultValueExpression = annotation.defaultText
		}
	}
	if c.partitionIgnored {
		logger.Logger(ctx).Warn("PARTITION BY expression is not recognised and is not recorded in the table metadata", zap.String("table", c.ref.TableId))
	}
	if !c.isView {
		if p := c.partition; p != nil {
			column := ""
			if p.column != "" {
				field := findField(p.column)
				switch {
				case field == nil:
					warn("partitioning column is not in the schema", p.column)
					p = nil
				case !partitionColumnTypeOK(p, field):
					warn("PARTITION BY expression does not apply to the type of the partitioning column and is not recorded in the table metadata", p.column)
					p = nil
				default:
					column = field.Name
				}
			}
			switch {
			case p == nil:
			case p.kind == "range":
				table.RangePartitioning = &bigqueryv2.RangePartitioning{
					Field: column,
					Range: &bigqueryv2.RangePartitioningRange{Start: p.start, End: p.end, Interval: p.interval},
				}
			default:
				table.TimePartitioning = &bigqueryv2.TimePartitioning{Type: p.unit, Field: column}
			}
		}
		if len(c.clustering) != 0 {
			var columns []string
			for _, name := range c.clustering {
				if field := findField(name); field != nil {
					columns = append(columns, field.Name)
				} else {
					warn("clustering column is not in the schema", name)
				}
			}
			if len(columns) != 0 {
				table.Clustering = &bigqueryv2.Clustering{Fields: columns}
			}
		}
		if len(c.primaryKey) != 0 {
			var columns []string
			for _, name := range c.primaryKey {
				if field := findField(name); field != nil {
					columns = append(columns, field.Name)
				} else {
					warn("primary key column is not in the schema", name)
				}
			}
			if len(columns) != 0 {
				table.TableConstraints = &bigqueryv2.TableConstraints{
					PrimaryKey: &bigqueryv2.TableConstraintsPrimaryKey{Columns: columns},
				}
			}
		}
	}
	for _, option := range c.options {
		if !option.assign || !tableOptionNames[option.name] || c.isView && !viewOptionNames[option.name] {
			continue
		}
		if !option.isLiteral {
			logger.Logger(ctx).Warn("table option is not a literal and is not recorded in the table metadata",
				zap.String("table", c.ref.TableId), zap.String("option", option.name))
			continue
		}
		if err := applyTableOption(table, option); err != nil {
			logger.Logger(ctx).Warn("table option is not recorded in the table metadata",
				zap.String("table", c.ref.TableId), zap.String("option", option.name), zap.Error(err))
		}
	}
}

// tableOptionNames are the table options recorded in the table metadata.
var tableOptionNames = map[string]bool{
	"description":               true,
	"friendly_name":             true,
	"labels":                    true,
	"expiration_timestamp":      true,
	"require_partition_filter":  true,
	"partition_expiration_days": true,
}

// viewOptionNames are the view options recorded in the view metadata.
var viewOptionNames = map[string]bool{
	"description":          true,
	"friendly_name":        true,
	"labels":               true,
	"expiration_timestamp": true,
}

// needsEval reports whether the option's value must be evaluated by
// evalOptionExpr before it can be applied.
func (o *optionValue) needsEval() bool {
	if !tableOptionNames[o.name] || o.name == "labels" {
		return false
	}
	if !o.isLiteral {
		return true
	}
	// expiration_timestamp is a TIMESTAMP; a string literal is coerced.
	_, isString := o.value.(string)
	return o.name == "expiration_timestamp" && isString
}

// evalOptionExpr evaluates the value of an option that is not a literal,
// e.g. TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY), with googlesqlite.
// It runs one SELECT on its own transaction, which is rolled back, so it must
// not run while another transaction holds the database.
func evalOptionExpr(ctx context.Context, server *Server, projectID, datasetID string, params []*bigqueryv2.QueryParameter, option *optionValue) error {
	text := option.exprText
	if option.isLiteral {
		s, _ := option.value.(string)
		text = quoteStringLiteral(s)
	}
	var expr string
	switch option.name {
	case "expiration_timestamp":
		expr = fmt.Sprintf("UNIX_MILLIS(CAST((%s) AS TIMESTAMP))", text)
	case "description", "friendly_name":
		expr = fmt.Sprintf("CAST((%s) AS STRING)", text)
	case "partition_expiration_days":
		expr = fmt.Sprintf("CAST((%s) AS FLOAT64)", text)
	case "require_partition_filter":
		expr = fmt.Sprintf("CAST((%s) AS BOOL)", text)
	default:
		return fmt.Errorf("option %s cannot be evaluated", option.name)
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
	response, err := server.contentRepo.Query(ctx, tx, projectID, datasetID, "SELECT "+expr, params)
	if err != nil {
		return err
	}
	if len(response.Rows) != 1 || len(response.Rows[0].F) != 1 {
		return fmt.Errorf("unexpected result of %s", expr)
	}
	v := response.Rows[0].F[0].V
	if v == nil {
		option.value, option.isLiteral = nil, true
		return nil
	}
	s := fmt.Sprint(v)
	switch option.name {
	case "expiration_timestamp":
		ms, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return err
		}
		option.value = ms
	case "description", "friendly_name":
		option.value = s
	case "partition_expiration_days":
		days, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return err
		}
		option.value = days
	case "require_partition_filter":
		b, err := strconv.ParseBool(s)
		if err != nil {
			return err
		}
		option.value = b
	}
	option.isLiteral = true
	return nil
}

// quoteStringLiteral returns s as a GoogleSQL string literal.
func quoteStringLiteral(s string) string {
	return `"` + strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`, "\r", `\r`).Replace(s) + `"`
}

// applyTableOption sets a decoded table option on the table metadata. NULL
// clears the option. expiration_timestamp must have been evaluated into
// milliseconds since the epoch.
func applyTableOption(table *bigqueryv2.Table, option *optionValue) error {
	value := option.value
	switch option.name {
	case "description", "friendly_name":
		var s string
		if value != nil {
			v, ok := value.(string)
			if !ok {
				return fmt.Errorf("%s must be a STRING", option.name)
			}
			s = v
		}
		if option.name == "description" {
			table.Description = s
		} else {
			table.FriendlyName = s
		}
	case "labels":
		if value == nil {
			table.Labels = nil
			return nil
		}
		labels, ok := value.(map[string]string)
		if !ok {
			return fmt.Errorf("labels must be an array of (STRING, STRING) literals in the emulator")
		}
		if len(labels) == 0 {
			table.Labels = nil
		} else {
			table.Labels = labels
		}
	case "expiration_timestamp":
		if value == nil {
			table.ExpirationTime = 0
			return nil
		}
		ms, ok := value.(int64)
		if !ok {
			return fmt.Errorf("expiration_timestamp must be a TIMESTAMP")
		}
		table.ExpirationTime = ms
	case "require_partition_filter":
		if value == nil {
			table.RequirePartitionFilter = false
			return nil
		}
		b, ok := value.(bool)
		if !ok {
			return fmt.Errorf("require_partition_filter must be a BOOL")
		}
		table.RequirePartitionFilter = b
	case "partition_expiration_days":
		var days float64
		switch v := value.(type) {
		case nil:
		case int64:
			days = float64(v)
		case float64:
			days = v
		default:
			return fmt.Errorf("partition_expiration_days must be a FLOAT64")
		}
		if days < 0 {
			return fmt.Errorf("partition_expiration_days must not be negative")
		}
		// Only time-partitioned tables have a partition expiration.
		if table.TimePartitioning != nil {
			table.TimePartitioning.ExpirationMs = int64(math.Round(days * 86400000))
		}
	default:
		return fmt.Errorf("option %s is not supported by the emulator", option.name)
	}
	return nil
}
