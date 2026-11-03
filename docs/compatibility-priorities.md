# BigQuery compatibility priorities

The 10 most urgent gaps between the emulator and real BigQuery, ranked by user
impact: silently wrong data first, then crashes and broken common workflows,
then missing features.

Sources: the open issues and PRs on
[goccy/bigquery-emulator](https://github.com/goccy/bigquery-emulator/issues),
checked against a build of `main` at `cb8555b` on 2026-10-30. "Reproduced"
means the bug was confirmed against that build. Every issue and PR number
links to goccy/bigquery-emulator, not to this fork.

**Status legend:** ✅ fixed on a branch of this fork, not merged into `main`
yet (see the item) · ⬜ open

| # | Status | Problem | Evidence | Issues / PRs |
| --- | --- | --- | --- | --- |
| 1 | ✅ | `tabledata.list` returns another dataset's table of the same name | Reproduced | [#532](https://github.com/goccy/bigquery-emulator/issues/532) |
| 2 | ✅ | BYTES values are base64-encoded twice | Reproduced | [#498](https://github.com/goccy/bigquery-emulator/issues/498) |
| 3 | ✅ | DDL doesn't keep the table catalog in sync (`CREATE OR REPLACE`, `ALTER TABLE`, failed scripts) | Reproduced | [#481](https://github.com/goccy/bigquery-emulator/issues/481), [#523](https://github.com/goccy/bigquery-emulator/issues/523); PRs [#519](https://github.com/goccy/bigquery-emulator/issues/519), [#495](https://github.com/goccy/bigquery-emulator/issues/495), [#522](https://github.com/goccy/bigquery-emulator/issues/522) |
| 4 | ⬜ | Invalid queries are reported as `jobInternalError` instead of `invalidQuery` | Reproduced | [#316](https://github.com/goccy/bigquery-emulator/issues/316); PR [#521](https://github.com/goccy/bigquery-emulator/issues/521) |
| 5 | ⬜ | The declared type of a query parameter is ignored | Reproduced (NUMERIC) | [#511](https://github.com/goccy/bigquery-emulator/issues/511), [#509](https://github.com/goccy/bigquery-emulator/issues/509); PRs [#516](https://github.com/goccy/bigquery-emulator/issues/516), [#525](https://github.com/goccy/bigquery-emulator/issues/525), [#526](https://github.com/goccy/bigquery-emulator/issues/526) |
| 6 | ⬜ | Default time zone is America/Los_Angeles instead of UTC | Reproduced | [#500](https://github.com/goccy/bigquery-emulator/issues/500) |
| 7 | ⬜ | Result values are formatted differently from BigQuery | Reproduced | [#482](https://github.com/goccy/bigquery-emulator/issues/482); PRs [#492](https://github.com/goccy/bigquery-emulator/issues/492), [#520](https://github.com/goccy/bigquery-emulator/issues/520) |
| 8 | ⬜ | The server gets slower (and reportedly larger) the longer it runs | Slowdown reproduced | [#512](https://github.com/goccy/bigquery-emulator/issues/512), [#313](https://github.com/goccy/bigquery-emulator/issues/313), [#294](https://github.com/goccy/bigquery-emulator/issues/294) |
| 9 | ⬜ | Storage Read API doesn't behave like BigQuery's | Not reproduced; from issue reports | [#493](https://github.com/goccy/bigquery-emulator/issues/493), [#507](https://github.com/goccy/bigquery-emulator/issues/507), [#399](https://github.com/goccy/bigquery-emulator/issues/399), [#527](https://github.com/goccy/bigquery-emulator/issues/527); PRs [#494](https://github.com/goccy/bigquery-emulator/issues/494), [#496](https://github.com/goccy/bigquery-emulator/issues/496), [#508](https://github.com/goccy/bigquery-emulator/issues/508) |
| 10 | ⬜ | `QUALIFY` is broken | Reproduced | [#502](https://github.com/goccy/bigquery-emulator/issues/502) |

## Details

### 1. ✅ `tabledata.list` returns another dataset's table — [#532](https://github.com/goccy/bigquery-emulator/issues/532)

With `ds1.t` and `ds2.t` both present, listing `ds2.t` returned `ds1.t`'s rows.
Silently wrong data.

**Fixed on branch [`fix/issue-532-tabledata-list-dataset`](https://github.com/Zeus26286/bigquery-emulator/tree/fix/issue-532-tabledata-list-dataset) (410ba58).** Reads
looked tables up by bare name, which the engine resolved to the first dataset
holding that name. The same bug also affected **extract jobs** and the
**Storage Read API**; all three now use the full `project.dataset.table` path.
Regression tests: `TestIssue532SameTableNameInTwoDatasets`,
`TestIssue532StorageReadSameTableNameInTwoDatasets`.

### 2. ✅ BYTES values are base64-encoded twice — [#498](https://github.com/goccy/bigquery-emulator/issues/498)

Inserting `AAECAw…` stored and returned `QUFFQ0F3…`. Silent corruption of every
BYTES value written through `insertAll`.

**Fixed on branch [`fix/issue-498-bytes-double-base64`](https://github.com/Zeus26286/bigquery-emulator/tree/fix/issue-498-bytes-double-base64) (8d35ac4).** The base64
text was stored as the bytes themselves. It is now decoded for top-level and
STRUCT-nested BYTES, which also fixes query results written to a destination
table (they were encoded three times). `ARRAY<BYTES>` was already correct.
A non-base64 value now rejects only its own row with reason `invalid`, as in
BigQuery, instead of failing the request with a 500.
Regression test: `TestIssue498BytesRoundTrip`.

Follow-up: JSON/CSV load jobs store data by a different route and were not
checked for the same bug.

### 3. ✅ DDL doesn't keep the table catalog in sync — [#481](https://github.com/goccy/bigquery-emulator/issues/481), [#523](https://github.com/goccy/bigquery-emulator/issues/523)

- `CREATE OR REPLACE TABLE` fails with "duplicate", and afterwards `SELECT *`
  on that table breaks.
- `ALTER TABLE ADD COLUMN` succeeds, but the new column is "Unrecognized".
- A script that fails partway (`CREATE …; SELECT ERROR('boom')`) crashes the
  server with a 500 nil-pointer panic.

Everyday SQL in dbt and migrations; likely one root cause. Open PRs: [#519](https://github.com/goccy/bigquery-emulator/issues/519),
[#495](https://github.com/goccy/bigquery-emulator/issues/495), [#522](https://github.com/goccy/bigquery-emulator/issues/522).

**Fixed on branch [`fix/ddl-catalog-sync`](https://github.com/Zeus26286/bigquery-emulator/tree/fix/ddl-catalog-sync), in
[Zeus26286/bigquery-emulator#1](https://github.com/Zeus26286/bigquery-emulator/pull/1).**
Three catalogs drifted apart: SQLite, googlesqlite's in-memory analyzer
catalog, and the emulator's metadata, which was synced only after a successful
query. The sync is now idempotent and runs in one transaction. A re-added
table replaces the old one, `IF NOT EXISTS` keeps the existing table, and the
analyzer is rebuilt afterwards. `ALTER TABLE` ADD/DROP/RENAME COLUMN, RENAME
TO and `IF [NOT] EXISTS` are rewritten into a rebuild that keeps the rows,
descriptions, modes, partitioning and clustering. A script that fails partway
now commits what ran and syncs the catalog on both jobs.insert and jobs.query,
as in BigQuery; this changes jobs.query, which used to roll back. Regression
tests: `TestCreateOrReplaceTableSyncsCatalog`,
`TestCreateTableIfNotExistsKeepsExistingTable`, `TestAlterTableSyncsCatalog`,
`TestAlterTableEdgeCases`, `TestFailedScriptSyncsCatalog`.

Still needing googlesqlite fixes:
- `ALTER` inside a multi-statement script is a no-op.
- A failure before a script's last statement leaves metadata stale until the
  next DDL on that table.
- After `CREATE OR REPLACE`, later statements in the same script see the old
  columns.

### 4. ⬜ Invalid queries reported as `jobInternalError` — [#316](https://github.com/goccy/bigquery-emulator/issues/316)

Even `SELEC 1` returns `jobInternalError`. Clients treat internal errors as
retryable, so a bad query keeps retrying instead of failing fast. Open PR: [#521](https://github.com/goccy/bigquery-emulator/issues/521).

### 5. ⬜ Query parameter types ignored — [#511](https://github.com/goccy/bigquery-emulator/issues/511), [#509](https://github.com/goccy/bigquery-emulator/issues/509)

A `NUMERIC` parameter is analysed as `STRING`, and an `ArrayQueryParameter`
makes the query hang. Open PRs: [#516](https://github.com/goccy/bigquery-emulator/issues/516), [#525](https://github.com/goccy/bigquery-emulator/issues/525), [#526](https://github.com/goccy/bigquery-emulator/issues/526).

### 6. ⬜ Default time zone is America/Los_Angeles, not UTC — [#500](https://github.com/goccy/bigquery-emulator/issues/500)

`CAST(TIMESTAMP '2024-01-05 09:30:00 UTC' AS STRING)` returns
`2024-01-05 01:30:00-08`; BigQuery returns `2024-01-05 09:30:00+00`. Affects
every timestamp-to-string conversion and `FORMAT_*`/`EXTRACT` without an
explicit time zone. No PR.

### 7. ⬜ Result values formatted differently — [#482](https://github.com/goccy/bigquery-emulator/issues/482)

- A TIMESTAMP inside a STRUCT comes back as `"2024-01-05 09:30:00+00"`; the Go
  client fails with `ParseInt`.
- Unnamed result columns are called `$col1` instead of `f0_`.

Open PRs: [#492](https://github.com/goccy/bigquery-emulator/issues/492), [#520](https://github.com/goccy/bigquery-emulator/issues/520).

### 8. ⬜ The server gets slower the longer it runs — [#512](https://github.com/goccy/bigquery-emulator/issues/512), [#313](https://github.com/goccy/bigquery-emulator/issues/313), [#294](https://github.com/goccy/bigquery-emulator/issues/294)

`SELECT 1` averaged ~21 ms over the first 100 queries and ~32 ms over the next
100. Memory reportedly grows on every `DROP` and is never released (catalog is
rebuilt and the old one kept). Hurts long CI runs and long-lived containers.
No PR.

### 9. ⬜ Storage Read API doesn't behave like BigQuery's — [#493](https://github.com/goccy/bigquery-emulator/issues/493), [#507](https://github.com/goccy/bigquery-emulator/issues/507), [#399](https://github.com/goccy/bigquery-emulator/issues/399), [#527](https://github.com/goccy/bigquery-emulator/issues/527)

Whole Arrow IPC streams are sent where single messages are expected, the read
offset is ignored, and ARRAY columns and anonymous result tables fail. Used by
`to_dataframe()`, Spark and the Go client's fast reads. Not reproduced here.
Open PRs: [#494](https://github.com/goccy/bigquery-emulator/issues/494), [#496](https://github.com/goccy/bigquery-emulator/issues/496), [#508](https://github.com/goccy/bigquery-emulator/issues/508).

### 10. ⬜ `QUALIFY` is broken — [#502](https://github.com/goccy/bigquery-emulator/issues/502)

`SELECT x FROM UNNEST([1,2,3]) x QUALIFY x = MAX(x) OVER ()` fails with
`misuse of aliased window function`. Worked before the googlesqlite switch;
likely needs a fix in googlesqlite. No PR.

## Next tier

- Copy jobs not implemented ([#353](https://github.com/goccy/bigquery-emulator/issues/353); PR [#386](https://github.com/goccy/bigquery-emulator/issues/386)).
- `routines.get` / `list` not implemented.
- Loading newline-delimited JSON with REPEATED fields fails ([#510](https://github.com/goccy/bigquery-emulator/issues/510)).
- DELETE returns 200 instead of 204 ([#513](https://github.com/goccy/bigquery-emulator/issues/513)).
- `INFORMATION_SCHEMA.JOBS` doesn't exist.
- Partitioning pseudo-columns like `_PARTITIONTIME` unsupported ([#317](https://github.com/goccy/bigquery-emulator/issues/317)).

## Checked and not needing work

- [#514](https://github.com/goccy/bigquery-emulator/issues/514): duplicate dataset create already returns 409 on `main`.
- [#470](https://github.com/goccy/bigquery-emulator/issues/470): rows from `insertAll` are already visible to queries on `main`.
- [#487](https://github.com/goccy/bigquery-emulator/issues/487): not a bug; real BigQuery also rejects ARRAY columns in `SELECT DISTINCT`.
