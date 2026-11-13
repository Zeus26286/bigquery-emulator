# BigQuery feature support matrix

This document tracks, feature by feature, what [`bigquery-emulator`](https://github.com/goccy/bigquery-emulator) supports.

BigQuery is a large product, so this matrix is organized to be **MECE** (mutually
exclusive, collectively exhaustive): every BigQuery feature area appears in
exactly one section, and the sections together cover the whole product surface.
The top-level grouping follows the structure of the official
[BigQuery documentation](https://docs.cloud.google.com/bigquery/docs) and the
[BigQuery REST API reference](https://docs.cloud.google.com/bigquery/docs/reference/rest).

If a feature you need is missing or wrong here, please open an
[Issue](https://github.com/goccy/bigquery-emulator/issues).

**Legend**

| Mark | Meaning |
| --- | --- |
| ✅ | Supported |
| 🟡 | Partially supported — see the note |
| ❌ | Not supported yet |

*Last reviewed: 2026-05-18, against `googlesqlite` v0.1.0.*

---

## 1. REST API

The emulator implements the [BigQuery API v2](https://docs.cloud.google.com/bigquery/docs/reference/rest)
REST surface. The tables below list every documented resource and method.

### 1.1 `datasets`

| Method | Status | Notes |
| --- | --- | --- |
| `get` | ✅ | |
| `insert` | ✅ | |
| `list` | ✅ | |
| `patch` | ✅ | |
| `update` | ✅ | |
| `delete` | ✅ | |
| `undelete` | ❌ | Dataset time travel / undelete is not implemented. |

### 1.2 `tables`

| Method | Status | Notes |
| --- | --- | --- |
| `get` | ✅ | |
| `insert` | ✅ | |
| `list` | ✅ | |
| `patch` | ✅ | |
| `update` | ❌ | Use `patch` instead. |
| `delete` | ✅ | |
| `getIamPolicy` | ❌ | IAM policy management is not implemented. |
| `setIamPolicy` | ❌ | IAM policy management is not implemented. |
| `testIamPermissions` | ❌ | IAM policy management is not implemented. |

### 1.3 `tabledata`

| Method | Status | Notes |
| --- | --- | --- |
| `insertAll` | ✅ | Streaming inserts. Unknown fields are reported as errors. |
| `list` | ✅ | |

### 1.4 `jobs`

| Method | Status | Notes |
| --- | --- | --- |
| `query` | ✅ | **Behaviour change:** when a multi-statement query fails, the statements that ran before the failing one now keep their effects, as with `jobs.insert` and in BigQuery; previously `jobs.query` rolled everything back. See [scripting](#6-query-language-googlesql). |
| `getQueryResults` | ✅ | Honors `maxResults` paging. |
| `get` | ✅ | |
| `list` | ✅ | |
| `insert` | 🟡 | Query, load and extract configurations only — see [section 2](#2-jobs). |
| `cancel` | ✅ | |
| `delete` | ✅ | |

### 1.5 `models`

| Method | Status | Notes |
| --- | --- | --- |
| `get` | 🟡 | Metadata only; the emulator does not train or store models. |
| `list` | 🟡 | Metadata only. |
| `patch` | 🟡 | Metadata only. |
| `delete` | ✅ | |

There is no `models.insert` in the BigQuery API — models are created with
`CREATE MODEL`, which is not supported (see [section 8](#8-bigquery-ml)).

### 1.6 `routines`

| Method | Status | Notes |
| --- | --- | --- |
| `insert` | ✅ | Create SQL functions, procedures and table functions. |
| `get` | ❌ | |
| `list` | ❌ | |
| `update` | ❌ | |
| `delete` | ❌ | |
| `getIamPolicy` | ❌ | |
| `setIamPolicy` | ❌ | |
| `testIamPermissions` | ❌ | |

### 1.7 `projects`

| Method | Status | Notes |
| --- | --- | --- |
| `list` | ✅ | |
| `getServiceAccount` | ✅ | Returns a placeholder service account. |

### 1.8 `rowAccessPolicies`

| Method | Status | Notes |
| --- | --- | --- |
| `list` | ❌ | Row-level security is not implemented. |
| `get` | ❌ | |
| `insert` | ❌ | |
| `update` | ❌ | |
| `delete` | ❌ | |
| `batchDelete` | ❌ | |
| `getIamPolicy` | ❌ | |
| `testIamPermissions` | ❌ | |

---

## 2. Jobs

[Job types](https://docs.cloud.google.com/bigquery/docs/jobs-overview) accepted by `jobs.insert`.

| Job type | Status | Notes |
| --- | --- | --- |
| Query | ✅ | See [section 6](#6-query-language-googlesql). Exposes the anonymous results table as `destinationTable`. |
| Load | ✅ | See [section 4](#4-data-ingestion). |
| Extract | ✅ | See [section 5](#5-data-export). |
| Copy | ❌ | Table copy jobs are not implemented. |

---

## 3. Storage and table types

| Feature | Status | Notes |
| --- | --- | --- |
| Standard tables | ✅ | |
| Logical views | ✅ | Created via DDL or `tables.insert`; view schemas are hydrated. |
| Materialized views | 🟡 | Via `tables.insert` only, served as an ordinary view; `CREATE MATERIALIZED VIEW` DDL is rejected by googlesqlite. |
| External tables | ❌ | The table type exists but is not implemented. |
| Table snapshots | ❌ | |
| Table clones | ❌ | |
| Partitioned tables | 🟡 | Partitioning metadata from `tables.insert`, load jobs and DDL `PARTITION BY` (time-unit column, ingestion time, `RANGE_BUCKET`) is stored; partition pruning, `_PARTITIONTIME` semantics and `require_partition_filter` are not enforced. |
| Clustered tables | 🟡 | From `tables.insert` and DDL `CLUSTER BY`; clustering does not affect execution. |
| Storage backend | ✅ | Embedded SQLite — in-memory or a persisted file (see the README). |

---

## 4. Data ingestion

| Feature | Status | Notes |
| --- | --- | --- |
| Streaming inserts (`tabledata.insertAll`) | ✅ | |
| Storage Write API | ✅ | gRPC; see [section 9](#9-bigquery-storage-api). |
| Load from Google Cloud Storage | ✅ | Set `STORAGE_EMULATOR_HOST` to point at a GCS emulator. |
| Load from a local file (multipart / resumable upload) | ✅ | |
| Load format: CSV | ✅ | Schema autodetect is supported. |
| Load format: JSON (newline-delimited) | ✅ | |
| Load format: Parquet | ✅ | |
| Load format: Avro | ❌ | |
| Load format: ORC | ❌ | |
| Write disposition: `WRITE_APPEND` | ✅ | Default. |
| Write disposition: `WRITE_TRUNCATE` | ✅ | |
| Write disposition: `WRITE_EMPTY` | ✅ | |
| BigQuery Data Transfer Service | ❌ | |

---

## 5. Data export

| Feature | Status | Notes |
| --- | --- | --- |
| Extract to Google Cloud Storage | ✅ | |
| Extract format: CSV | ✅ | |
| Extract format: JSON (newline-delimited) | ✅ | |
| Extract format: Avro | ❌ | |
| Extract format: Parquet | ❌ | |
| Storage Read API | ✅ | gRPC; see [section 9](#9-bigquery-storage-api). |

---

## 6. Query language (GoogleSQL)

Query execution is delegated to [`googlesqlite`](https://github.com/goccy/googlesqlite),
which parses and analyzes [GoogleSQL](https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/introduction)
and runs it against the embedded SQLite database. The table below records what
the **emulator** wires up; the authoritative, per-function and per-type matrix
lives in the [`googlesqlite` status](https://github.com/goccy/googlesqlite#status).

| Feature | Status | Notes |
| --- | --- | --- |
| `SELECT` / query statements | ✅ | |
| DDL (`CREATE` / `ALTER` / `DROP` for tables, views, functions) | ✅ | DDL keeps the table metadata in sync, including `CREATE OR REPLACE` and `CREATE ... IF NOT EXISTS` (which leaves an existing table and its metadata alone). `CREATE TABLE` (also `... AS SELECT` and inside scripts) records `PARTITION BY` (a `PARTITION BY` that does not apply to the column type, e.g. `DATE_TRUNC(d, HOUR)`, is not recorded), `CLUSTER BY`, the table `OPTIONS` `description`, `friendly_name`, `labels`, `expiration_timestamp`, `partition_expiration_days` and `require_partition_filter`, column `OPTIONS(description=...)`, `NOT NULL` (mode `REQUIRED`), `DEFAULT` (`defaultValueExpression`) and `PRIMARY KEY ... NOT ENFORCED` in the table metadata; `CREATE VIEW` records `description`, `friendly_name`, `labels` and `expiration_timestamp`. Errors as in BigQuery: `CREATE` on an existing table fails with `Already Exists: Table p:d.t` (409, `duplicate`), an unqualified table name without a default dataset is rejected (400), and a table in a missing dataset fails with `Not found: Dataset p:d` (404, `notFound`), both before anything runs, and a `NULL` in a `NOT NULL` column fails with `Required field c cannot be null`. A script that fails partway keeps the effects of the statements before the failure, on both `jobs.insert` and `jobs.query`, as BigQuery does. A standalone `ALTER TABLE` supports `ADD COLUMN`, `DROP COLUMN` and `RENAME COLUMN` (each with `IF [NOT] EXISTS`), `RENAME TO`, `SET OPTIONS` (the table options above; `NULL` clears one), `ALTER COLUMN SET OPTIONS(description=...)`, `ALTER COLUMN DROP NOT NULL`, `ALTER COLUMN SET DATA TYPE` (widening only: `INT64` to `NUMERIC`/`BIGNUMERIC`/`FLOAT64`, `NUMERIC` to `BIGNUMERIC`/`FLOAT64`, relaxed type parameters), `ALTER COLUMN SET DEFAULT` / `DROP DEFAULT` and `ADD PRIMARY KEY (...) NOT ENFORCED` / `DROP PRIMARY KEY` (metadata only). Any other action is rejected with an explicit error, and the statement then changes nothing. A column change rebuilds the table by copying its rows, so it costs O(rows), and a failed rebuild is rolled back; the rebuild keeps the `DEFAULT` and `NOT NULL` constraints googlesqlite applies. `OPTIONS(description=...)` on an added column (and its nested fields) is kept in the metadata. A partitioning or clustering column cannot be dropped, renamed or retyped, nor a primary key column dropped. Type parameters (`NUMERIC(p, s)`, `STRING(n)`) of `CREATE TABLE` columns are not kept in the metadata, so `SET DATA TYPE` treats such a column as unparameterized. A `SET DEFAULT` whose expression cannot be assigned to the column type is rejected before anything runs; an invalid `defaultValueExpression` in the metadata (e.g. from `tables.patch`) is not carried over by a rebuild and is removed from the metadata. `DEFAULT`, `COLLATE` and generated columns in `ADD COLUMN` are rejected as unsupported. A standalone `CREATE TABLE ... LIKE` (also `OR REPLACE`, `IF NOT EXISTS`, `PARTITION BY`, `CLUSTER BY`, `OPTIONS` and `AS` query, across datasets and projects) copies the source table's schema, with the modes, descriptions, defaults and type parameters of every field, its partitioning, clustering, table options and primary/foreign keys; each clause replaces what it names (a `PARTITION BY` the whole partitioning, each `OPTIONS` entry only its option). The new table enforces the top-level `NOT NULL` and `DEFAULT` constraints; nested modes and type parameters are kept in the metadata but not enforced. The columns of an `AS` query must match the source table's by name, in any order, and are inserted with the usual coercion. It fails like `CREATE` (`Already Exists` 409, missing table or dataset 404) before anything runs; a missing source table is a 404 even with `IF NOT EXISTS` on an existing table. A failure (e.g. a `NULL` in a `NOT NULL` column) changes nothing, also for `OR REPLACE`; as for `ALTER TABLE`, this includes a job that fails after the statement ran, and a `jobs.insert` with a job ID already in use is rejected with `Already Exists: Job p:id` (409) before anything runs. `OR REPLACE` over a table with a different kind of partitioning is not rejected, as for `CREATE OR REPLACE TABLE`. `CREATE TABLE ... LIKE` inside a multi-statement script, `CREATE TEMP TABLE ... LIKE`, a column list, `WITH CONNECTION`, `DEFAULT COLLATE`, a view, an external table or a table without columns as source, and a `PARTITION BY` / `CLUSTER BY` the emulator does not recognise are rejected with an explicit error. Limitations: `ALTER TABLE` inside a multi-statement script is rejected with an explicit error. Nested column `OPTIONS` / `NOT NULL` in `CREATE TABLE` and `FOREIGN KEY` are rejected by googlesqlite. A `PRIMARY KEY` from `CREATE TABLE` is enforced as `UNIQUE` by SQLite until the table's first rebuild, while `ALTER TABLE ADD PRIMARY KEY` is metadata only. Table expiration is not enforced. Non-literal `labels` are ignored by `CREATE` and rejected by `ALTER`; other unknown table options are accepted by `CREATE` and not recorded. `INFORMATION_SCHEMA.TABLE_OPTIONS` does not see `ALTER TABLE SET OPTIONS`. A `DEFAULT` that googlesqlite applies but that is missing from the metadata (tables created by an older emulator version) is dropped by a rebuild. `CREATE TABLE COPY` / `CLONE`, `CREATE SNAPSHOT TABLE`, `CREATE EXTERNAL TABLE` and `CREATE MATERIALIZED VIEW` are unsupported. After `CREATE OR REPLACE`, a later statement in the same script still sees the old columns. When a script fails in a statement before its last one, the tables it created or dropped are missing from (or left in) the metadata until the next DDL on them. |
| DML (`INSERT` / `UPDATE` / `DELETE` / `MERGE` / `TRUNCATE`) | ✅ | Subject to `googlesqlite` coverage. |
| Scripting and multi-statement queries | ✅ | Subject to `googlesqlite` coverage. When a statement fails, the statements before it keep their effects (as in BigQuery, which has no implicit script transaction); a single failing statement changes nothing. **Behaviour change:** `jobs.query` used to roll back the whole query on any failure. It now commits the effects of the statements that ran before the failing one, the same as `jobs.insert`. |
| Stored procedures | ✅ | Created via `routines.insert` or DDL. |
| Built-in functions (~570) | ✅ | `googlesqlite` v0.1.0: 523/529 GoogleSQL + 55/59 BigQuery-specific. |
| Data types (16 / 18) | ✅ | `NUMERIC` / `BIGNUMERIC` map to Arrow decimal types. |
| SQL UDFs | ✅ | |
| JavaScript UDFs | ✅ | Inline `CREATE ... LANGUAGE js` functions; persisting a JS routine via `routines.insert` is not supported. |
| Wildcard tables | ✅ | |
| Templated-argument functions | ✅ | |
| Query parameters (named and positional) | ✅ | Empty / absent numeric, temporal and string parameters are treated as typed `NULL`s. |
| Table-valued functions | ✅ | |
| `INFORMATION_SCHEMA` views | 🟡 | `googlesqlite` implements `SCHEMATA`, `TABLES`, `TABLE_OPTIONS` and `COLUMNS`; other views (e.g. `JOBS`, `VIEWS`, `ROUTINES`, `PARTITIONS`) are not implemented. |
| Time travel (`FOR SYSTEM_TIME AS OF`) | ❌ | |
| Sessions / multi-statement transactions over the REST API | ❌ | |
| BigQuery ML statements (`CREATE MODEL`, `ML.*`) | ❌ | See [section 8](#8-bigquery-ml). |

---

## 7. Security and governance

| Feature | Status | Notes |
| --- | --- | --- |
| Authentication | 🟡 | The emulator does not authenticate requests; clients connect with anonymous credentials. |
| IAM policy management | ❌ | `getIamPolicy` / `setIamPolicy` / `testIamPermissions` are not implemented. |
| Row-level security (`rowAccessPolicies`) | ❌ | |
| Column-level security / policy tags | ❌ | |
| Dynamic data masking | ❌ | |
| Authorized views / authorized routines / authorized datasets | ❌ | |
| Customer-managed encryption keys (CMEK) | ❌ | Not applicable to a local emulator. |

---

## 8. BigQuery ML

| Feature | Status | Notes |
| --- | --- | --- |
| `CREATE MODEL` and `ML.*` functions | ❌ | Model training and inference are not emulated. |
| Models REST resource | 🟡 | Metadata-only stubs — see [section 1.5](#15-models). |

---

## 9. BigQuery Storage API

The gRPC [BigQuery Storage API](https://docs.cloud.google.com/bigquery/docs/reference/storage).

| Feature | Status | Notes |
| --- | --- | --- |
| Storage Read API | ✅ | |
| Storage Write API | ✅ | |
| Apache Avro format | ✅ | |
| Apache Arrow format | ✅ | |

---

## 10. Integrations

| Feature | Status | Notes |
| --- | --- | --- |
| Google Cloud Storage (load / extract) | ✅ | Works against a GCS emulator via `STORAGE_EMULATOR_HOST`. |
| BigQuery Data Transfer Service | ❌ | |
| Federated queries / external data sources | ❌ | |
| Connections API (`Connection` resource) | ❌ | |
| BI Engine | ❌ | |
| Analytics Hub | ❌ | |
| BigQuery Omni | ❌ | |
| Dataform | ❌ | |
| Reservations / capacity management | ❌ | Not applicable to a local emulator. |

---

## 11. Emulator-specific features

Capabilities that are not part of the BigQuery product but are provided by this
emulator for local development and testing.

| Feature | Status | Notes |
| --- | --- | --- |
| In-process Go test server (`server.New` / `httptest`) | ✅ | See the README synopsis. |
| Standalone server binary / Docker image | ✅ | |
| YAML seed loader (`--data-from-yaml`) | ✅ | |
| In-memory storage | ✅ | |
| Persisted file storage | ✅ | An ordinary SQLite database file. |
| Multi-client conformance suite (`test/e2e`) | ✅ | Python, Ruby, PHP, Node.js, Java clients and the `bq` CLI. |
