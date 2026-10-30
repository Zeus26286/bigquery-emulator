# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A BigQuery-compatible server (REST on :9050, gRPC Storage API on :9060) for local testing. It can run as a standalone binary/Docker image or in-process from Go tests via `server.New`. SQL execution is delegated to [googlesqlite](https://github.com/goccy/googlesqlite) (pure Go, registered as the `googlesqlite` `database/sql` driver), so the build needs no cgo. Feature coverage is tracked in `docs/feature-support.md`; update it when adding or changing supported BigQuery features.

## Commands

```sh
make emulator/build                      # static binary ./bigquery-emulator (CGO_ENABLED=0)
go test -v -count=1 $(go list ./... | grep -v '/test/e2e')   # unit tests, as CI runs them
go test -count=1 -run 'TestQuery' ./server/                  # single test (regex on name)
go test -count=1 -run 'TestQuery/subtest_name' ./server/     # single subtest
make test/e2e                            # multi-client conformance suite (needs Docker, ~30m timeout)
make docker/build                        # requires docker buildx
./bigquery-emulator --project=test --data-from-yaml=./server/testdata/data.yaml   # run locally
```

CI runs no linter (no golangci config); the unit-test job runs on ubuntu and macOS.

## Architecture

**Request path (REST).** `server/server.go` builds a gorilla/mux router from the `handlers` table in `server/handler_gen.go`, mounting each route both at `/projects/...` and `/bigquery/v2/projects/...`. `handler_gen.go` is **generated** by `internal/cmd/generator` (a separate Go module) from `internal/cmd/generator/resources/bigquery-api.json` — don't hand-edit it; add new endpoints there and rerun the generator (`cd internal/cmd/generator && go run .`). The handler structs themselves (`datasetsGetHandler`, `jobsInsertHandler`, …) live in `server/handler.go`.

**Middleware chain** (`server/middleware.go`): `sequentialAccessMiddleware` takes a global mutex, so **all REST requests are serialized**. The `with*Middleware` layers resolve `{projectId}`, `{datasetId}`, `{tableId}`, `{jobId}`, etc. from path params into metadata objects stored in the request context; handlers read them via `projectFromContext(ctx)`, `datasetFromContext(ctx)`, … (`server/context.go`). `methodOverrideMiddleware` wraps the router so `X-HTTP-Method-Override` tunneling hits the right method route.

**Handler pattern.** Each handler's `ServeHTTP` decodes the request (bigquery/v2 API types from `google.golang.org/api/bigquery/v2`), then calls a typed `Handle(ctx, *xxxRequest)` which does the work; responses go through `encodeResponse` / `errorResponse` with BigQuery-shaped errors from `server/error.go`.

**Storage layers** — one SQLite DB (memory, temp file, or `--database` file) holds both:
- `internal/metadata` — the emulator's own catalog (projects, datasets, tables, jobs, models, routines) as serialized rows.
- `internal/contentdata` — actual table data, created/queried as GoogleSQL tables via googlesqlite. Identifiers are backtick-escaped/validated here.

Both go through `internal/connection.Manager`: get a `Conn` for (project, dataset), `Begin` a `Tx`, then switch it into `MetadataRepoMode()` (empty name path) or `ContentRepoMode()` (name path = project[/dataset], so unqualified table names resolve) before using the respective repository. Always `defer tx.RollbackIfNotCommitted()` and `Commit()`.

**Types.** `types/` is the public package (table/project YAML schema used by `YAMLSource`/`StructSource`, Arrow/Avro encoders for the Storage Read API, proto descriptors for Storage Write). `internal/types` holds query-response shaping; `internal/zsqltypes` maps between googlesqlite values and BigQuery types.

**gRPC Storage API.** `server/storage_handler.go` implements BigQuery Storage Read (Arrow/Avro) and Write (dynamic protobuf) over plaintext gRPC.

**GCS load/extract jobs** use `cloud.google.com/go/storage`; set `STORAGE_EMULATOR_HOST` to point at a GCS emulator (tests use fake-gcs-server).

**Go test embedding.** `server.New(server.TempStorage)` + `Load(server.YAMLSource(...))` + `srv.TestServer()` gives an httptest server plus gRPC dial helpers; `server/server_test.go` is the main example and the bulk of the unit tests.

## e2e conformance suite (`test/e2e`)

Builds one Docker image containing Python/Ruby/PHP/Node.js/Java clients and `bq`, starts a fresh in-process emulator per language, and runs the shared corpus `test/e2e/cases/cases.json` (a Go subtest per language × query). Skips automatically without Docker. To add a case, add a JSON entry with `expected` values derived from BigQuery docs (not from a client's output); per-language runners are data-driven. Known divergences are listed in `knownIssues` in `e2e_test.go` — remove an entry once its fix lands.
