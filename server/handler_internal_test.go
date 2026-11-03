package server

import (
	"context"
	"testing"

	"github.com/goccy/googlesqlite"
	bigqueryv2 "google.golang.org/api/bigquery/v2"

	"github.com/goccy/bigquery-emulator/types"
)

func TestQueryProjectAndDataset(t *testing.T) {
	tests := []struct {
		name              string
		defaultDataset    *bigqueryv2.DatasetReference
		fallbackProjectID string
		wantProjectID     string
		wantDatasetID     string
	}{
		{
			name:              "nil default dataset",
			fallbackProjectID: "request-project",
			wantProjectID:     "request-project",
		},
		{
			name: "default dataset without project uses fallback project",
			defaultDataset: &bigqueryv2.DatasetReference{
				DatasetId: "dataset",
			},
			fallbackProjectID: "request-project",
			wantProjectID:     "request-project",
			wantDatasetID:     "dataset",
		},
		{
			name: "default dataset project overrides fallback project",
			defaultDataset: &bigqueryv2.DatasetReference{
				ProjectId: "default-project",
				DatasetId: "dataset",
			},
			fallbackProjectID: "request-project",
			wantProjectID:     "default-project",
			wantDatasetID:     "dataset",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotProjectID, gotDatasetID := queryProjectAndDataset(tt.defaultDataset, tt.fallbackProjectID)
			if gotProjectID != tt.wantProjectID {
				t.Fatalf("projectID = %q; want %q", gotProjectID, tt.wantProjectID)
			}
			if gotDatasetID != tt.wantDatasetID {
				t.Fatalf("datasetID = %q; want %q", gotDatasetID, tt.wantDatasetID)
			}
		})
	}
}

// TestSyncCatalogIgnoresTempAndUnknownTables covers catalog changes that have
// no table metadata to update: temp tables (whose name path has no dataset),
// tables of a missing dataset, and drops of tables the metadata never knew,
// e.g. one created by a script that failed before its catalog changes could
// be read. Deleting the metadata of the last one used to panic.
func TestSyncCatalogIgnoresTempAndUnknownTables(t *testing.T) {
	ctx := context.Background()
	s, err := New(TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.Load(StructSource(types.NewProject("test", types.NewDataset("ds")))); err != nil {
		t.Fatal(err)
	}
	res, err := syncCatalog(ctx, s, &googlesqlite.ChangedCatalog{
		Table: &googlesqlite.ChangedTable{
			Added: []*googlesqlite.TableSpec{
				{IsTemp: true, NamePath: []string{"test", "tmp"}},
			},
			Deleted: []*googlesqlite.TableSpec{
				{IsTemp: true, NamePath: []string{"test", "tmp"}},
				{NamePath: []string{"test", "dataset_missing", "t"}},
				{NamePath: []string{"test", "ds", "never_created"}},
				{NamePath: []string{"project_missing", "ds", "t"}},
			},
		},
		Function: &googlesqlite.ChangedFunction{},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.needsRefresh || len(res.repairTables) != 0 {
		t.Fatalf("unexpected sync result %+v", res)
	}
	if _, err := syncCatalog(ctx, s, nil, nil); err != nil {
		t.Fatalf("nil catalog: %v", err)
	}
}
