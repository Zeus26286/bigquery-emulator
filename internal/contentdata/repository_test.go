package contentdata

import (
	"strings"
	"testing"

	bigqueryv2 "google.golang.org/api/bigquery/v2"
)

func TestValidateIdent(t *testing.T) {
	valid := []string{
		"my_table",
		"Dataset123",
		"project-id",
		"name with spaces",
		"日本語テーブル",
		"",
	}
	for _, id := range valid {
		if err := validateIdent("table", id); err != nil {
			t.Errorf("validateIdent(%q) returned error, want nil: %v", id, err)
		}
	}

	invalid := []string{
		"tbl`",
		"`; DROP TABLE x; --",
		"tbl\\",
		"a\\`; DROP TABLE victim; --",
	}
	for _, id := range invalid {
		if err := validateIdent("table", id); err == nil {
			t.Errorf("validateIdent(%q) returned nil, want error", id)
		}
	}
}

func TestTablePathRejectsInjection(t *testing.T) {
	r := NewRepository()

	got, err := r.tablePath("proj", "ds", "tbl")
	if err != nil {
		t.Fatalf("tablePath returned unexpected error: %v", err)
	}
	if want := "proj.ds.tbl"; got != want {
		t.Errorf("tablePath = %q, want %q", got, want)
	}

	// The payload below escaped the backtick-quoted identifier and dropped a
	// table when escaping alone was relied upon; it must now be rejected
	// before any SQL is built.
	cases := []struct{ project, dataset, table string }{
		{"a`; DROP TABLE victim; --", "ds", "tbl"},
		{"proj", "d`s", "tbl"},
		{"proj", "ds", "a\\`; DROP TABLE victim; --"},
	}
	for _, c := range cases {
		if _, err := r.tablePath(c.project, c.dataset, c.table); err == nil {
			t.Errorf("tablePath(%q, %q, %q) returned nil, want error", c.project, c.dataset, c.table)
		}
	}
}

func TestRoutinePathRejectsInjection(t *testing.T) {
	r := NewRepository()

	got, err := r.routinePath("proj", "ds", "fn")
	if err != nil {
		t.Fatalf("routinePath returned unexpected error: %v", err)
	}
	if want := "proj.ds.fn"; got != want {
		t.Errorf("routinePath = %q, want %q", got, want)
	}

	if _, err := r.routinePath("proj", "ds", "fn`; DROP TABLE victim; --"); err == nil {
		t.Error("routinePath with a backtick in the routine ID returned nil, want error")
	}
}

func TestEscapeIdent(t *testing.T) {
	if got, want := escapeIdent("plain"), "plain"; got != want {
		t.Errorf("escapeIdent(plain) = %q, want %q", got, want)
	}
	if got := escapeIdent("a`b"); !strings.Contains(got, "``") {
		t.Errorf("escapeIdent(a`b) = %q, want a doubled backtick", got)
	}
}

func TestRebuildTableSQL(t *testing.T) {
	r := NewRepository()
	src := &bigqueryv2.TableReference{ProjectId: "p", DatasetId: "ds", TableId: "t"}
	tmp := &bigqueryv2.TableReference{ProjectId: "p", DatasetId: "ds", TableId: "tmp"}
	dst := &bigqueryv2.TableReference{ProjectId: "p", DatasetId: "ds", TableId: "t2"}

	got, err := r.RebuildTableSQL(src, tmp, []ProjectedColumn{
		{Expr: "`a`", Name: "a"},
		{Expr: "CAST(NULL AS STRING)", Name: "b"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if want := "CREATE TABLE `p.ds.tmp` AS SELECT `a` AS `a`, CAST(NULL AS STRING) AS `b` FROM `p.ds.t`; DROP TABLE `p.ds.t`"; got != want {
		t.Errorf("RebuildTableSQL =\n%s\nwant\n%s", got, want)
	}

	got, err = r.FinishRebuildTableSQL(tmp, dst, []ColumnDefinition{
		{Name: "a", Type: "INT64", NotNull: true},
		{Name: "b", Type: "STRING", Default: "'q'"},
		{Name: "s", Type: "STRUCT<`x y` INT64>", NotNull: true, Default: "STRUCT(1)"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if want := "CREATE TABLE `p.ds.t2` (`a` INT64 NOT NULL, `b` STRING DEFAULT ('q'\n), `s` STRUCT<`x y` INT64> DEFAULT (STRUCT(1)\n) NOT NULL); " +
		"INSERT INTO `p.ds.t2` (`a`, `b`, `s`) SELECT `a`, `b`, `s` FROM `p.ds.tmp`; DROP TABLE `p.ds.tmp`"; got != want {
		t.Errorf("FinishRebuildTableSQL =\n%s\nwant\n%s", got, want)
	}

	got, err = r.FinishRebuildTableSQL(tmp, dst, nil)
	if err != nil {
		t.Fatal(err)
	}
	if want := "CREATE TABLE `p.ds.t2` AS SELECT * FROM `p.ds.tmp`; DROP TABLE `p.ds.tmp`"; got != want {
		t.Errorf("FinishRebuildTableSQL without definitions =\n%s\nwant\n%s", got, want)
	}

	// A trailing line comment in a DEFAULT ends at the newline.
	got, err = r.RestoreTableSpecSQL(src, []ColumnDefinition{{Name: "a", Type: "INT64", Default: "1 --", NotNull: true}, {Name: "b", Type: "STRING"}})
	if err != nil {
		t.Fatal(err)
	}
	if want := "CREATE TABLE IF NOT EXISTS `p.ds.t` (`a` INT64 DEFAULT (1 --\n) NOT NULL, `b` STRING)"; got != want {
		t.Errorf("RestoreTableSpecSQL =\n%s\nwant\n%s", got, want)
	}

	if _, err := r.RebuildTableSQL(src, tmp, []ProjectedColumn{{Expr: "1", Name: "a`b"}}); err == nil {
		t.Error("RebuildTableSQL accepted a column name with a backtick")
	}
	if _, err := r.FinishRebuildTableSQL(tmp, dst, []ColumnDefinition{{Name: "a\\b", Type: "INT64"}}); err == nil {
		t.Error("FinishRebuildTableSQL accepted a column name with a backslash")
	}
	if _, err := r.FinishRebuildTableSQL(tmp, dst, []ColumnDefinition{{Name: "a"}}); err == nil {
		t.Error("FinishRebuildTableSQL accepted a column without a type")
	}
}

func TestCreateTableLikeSQL(t *testing.T) {
	r := NewRepository()
	dst := &bigqueryv2.TableReference{ProjectId: "p", DatasetId: "ds", TableId: "t"}
	tmp := &bigqueryv2.TableReference{ProjectId: "p", DatasetId: "ds", TableId: "tmp"}

	got, err := r.CreateTableSQL(dst, []ColumnDefinition{{Name: "a", Type: "INT64", NotNull: true}, {Name: "b", Type: "STRING", Default: "'x'"}})
	if err != nil {
		t.Fatal(err)
	}
	if want := "CREATE TABLE `p.ds.t` (`a` INT64 NOT NULL, `b` STRING DEFAULT ('x'\n))"; got != want {
		t.Errorf("CreateTableSQL =\n%s\nwant\n%s", got, want)
	}
	if _, err := r.CreateTableSQL(dst, nil); err == nil {
		t.Error("CreateTableSQL accepted a table without columns")
	}

	// A trailing line comment in the query ends at the newline.
	got, err = r.CreateTableAsQuerySQL(tmp, "SELECT 1 AS a -- c")
	if err != nil {
		t.Fatal(err)
	}
	if want := "CREATE TABLE `p.ds.tmp` AS SELECT * FROM (\nSELECT 1 AS a -- c\n)"; got != want {
		t.Errorf("CreateTableAsQuerySQL =\n%s\nwant\n%s", got, want)
	}

	got, err = r.InsertFromTableSQL(tmp, dst, []string{"B", "a"}, []string{"b", "a"})
	if err != nil {
		t.Fatal(err)
	}
	if want := "INSERT INTO `p.ds.t` (`b`, `a`) SELECT `B`, `a` FROM `p.ds.tmp`; DROP TABLE `p.ds.tmp`"; got != want {
		t.Errorf("InsertFromTableSQL =\n%s\nwant\n%s", got, want)
	}
	if _, err := r.InsertFromTableSQL(tmp, dst, []string{"a`b"}, []string{"a"}); err == nil {
		t.Error("InsertFromTableSQL accepted a column name with a backtick")
	}
	if _, err := r.InsertFromTableSQL(tmp, dst, []string{"a"}, []string{"a", "b"}); err == nil {
		t.Error("InsertFromTableSQL accepted column lists of different lengths")
	}
}

func TestFieldDDLType(t *testing.T) {
	for _, c := range []struct {
		field *bigqueryv2.TableFieldSchema
		want  string
	}{
		{&bigqueryv2.TableFieldSchema{Type: "INTEGER"}, "INT64"},
		{&bigqueryv2.TableFieldSchema{Type: "FLOAT"}, "FLOAT64"},
		{&bigqueryv2.TableFieldSchema{Type: "BOOLEAN"}, "BOOL"},
		{&bigqueryv2.TableFieldSchema{Type: "BIGNUMERIC"}, "BIGNUMERIC"},
		{&bigqueryv2.TableFieldSchema{Type: "json"}, "JSON"},
		{&bigqueryv2.TableFieldSchema{Type: "STRING", Mode: "REPEATED"}, "ARRAY<STRING>"},
		{&bigqueryv2.TableFieldSchema{Type: "RECORD", Mode: "REPEATED", Fields: []*bigqueryv2.TableFieldSchema{
			{Name: "select", Type: "INTEGER"},
			{Name: "s", Type: "RECORD", Fields: []*bigqueryv2.TableFieldSchema{{Name: "t", Type: "TIMESTAMP", Mode: "REPEATED"}}},
		}}, "ARRAY<STRUCT<`select` INT64, `s` STRUCT<`t` ARRAY<TIMESTAMP>>>>"},
	} {
		got, ok := FieldDDLType(c.field)
		if !ok || got != c.want {
			t.Errorf("FieldDDLType(%+v) = %q, %v; want %q", c.field, got, ok, c.want)
		}
	}
	for _, field := range []*bigqueryv2.TableFieldSchema{
		{Type: "RANGE"},
		{Type: "RECORD"},
		{Type: "RECORD", Fields: []*bigqueryv2.TableFieldSchema{{Name: "a`", Type: "INTEGER"}}},
	} {
		if got, ok := FieldDDLType(field); ok {
			t.Errorf("FieldDDLType(%+v) = %q, want not ok", field, got)
		}
	}
}
