# OmniKV SQL support matrix

OmniKV includes a hand-written SQL parser, a cost-aware physical planner, an
execution layer, and a separate prepared-query engine for key-oriented embedded
workloads. This page is the public contract for what the project currently
supports and what must return clear errors until it is implemented.

Status: beta. The SQL layer is suitable for experiments, demos, and regression
testing. It is not yet a complete PostgreSQL, MySQL, or SQLite replacement.

## Supported SQL statements

| Area | Supported today | Notes |
| --- | --- | --- |
| Table DDL | `CREATE TABLE`, `CREATE TABLE IF NOT EXISTS`, `DROP TABLE`, `DROP TABLE IF EXISTS`, `SHOW TABLES` | Table definitions are stored through the catalog layer. |
| Inserts | `INSERT INTO table (...) VALUES (...)` | Single-row and multi-row `VALUES` are supported by the parser and executor. |
| Reads | `SELECT *`, named columns, qualified columns, `FROM table` | Single-table reads are the most mature path. |
| Predicates | `=`, `!=`, `<>`, `>`, `<`, `>=`, `<=`, `LIKE`, `AND`, `OR`, `NOT`, `IS NULL`, `IS NOT NULL`, `IN (...)` | Predicates are represented in the AST and are regression-tested. |
| Ordering and pagination | `ORDER BY`, `ORDER BY ... DESC`, `LIMIT`, `OFFSET` | Pagination behavior is covered by integration tests. |
| Aggregates | `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `GROUP BY`, `HAVING` | Aggregate parsing and common execution paths are tested. |
| Joins | `INNER JOIN`, `LEFT JOIN`, `RIGHT JOIN` | Join planning uses hash-join plan nodes. Complex multi-join optimization is still future work. |
| Set operations | `UNION`, `UNION ALL`, `INTERSECT`, `EXCEPT` | Parsed into the SQL AST and covered by SQL feature tests. |
| Window functions | `ROW_NUMBER`, `RANK`, `DENSE_RANK` with `OVER (ORDER BY ...)` | Basic parsing and execution paths are covered by SQL feature tests. |
| Explain | `EXPLAIN`, `EXPLAIN ANALYZE` | Used to inspect plan structure and estimated costs. |
| Mutations | `UPDATE ... SET ... WHERE ...`, `DELETE FROM ... WHERE ...` | Common single-table mutation paths are covered by integration tests. |

## Keyword case

Per the SQL standard, keywords in OmniKV are case-insensitive, and whitespace
between them does not change meaning: `select ... from ... where ...`,
`SELECT ... FROM ... WHERE ...`, and `SeLeCt ... FrOm ... WhErE ...` all parse
identically. Identifiers (table and column names) keep their case as written,
matching PostgreSQL. Transaction statements over the PgWire protocol accept the
full PostgreSQL variant set in any case and spacing combination — `BEGIN`,
`BEGIN WORK`, `BEGIN TRANSACTION`, `START TRANSACTION`; `COMMIT`, `COMMIT
WORK`, `COMMIT TRANSACTION`, `END`, `END WORK`, `END TRANSACTION`; `ROLLBACK`,
`ROLLBACK WORK`, `ROLLBACK TRANSACTION`, `ABORT`, `ABORT WORK`, `ABORT
TRANSACTION` — because DBAPI drivers implicitly send lowercase `begin
transaction` when autocommit is off (issue #109). The `AND CHAIN` / `AND NO
CHAIN` suffix is part of the termination commands' grammar (COMMIT, END,
ROLLBACK, ABORT): `AND CHAIN` opens a new transaction immediately after the
commit or rollback, and is a hard error (25P01) when no transaction is open,
where the plain forms only warn. It is not valid on `BEGIN` / `START
TRANSACTION`, which reject it as a syntax error (42601) like PostgreSQL.
Client `SET` statements are accepted (and ignored) in any case.

## Prepared query support

The embedded prepared-query engine intentionally supports a smaller,
key-oriented query language than the full SQL parser. It is designed for
low-overhead application calls where stable templates and parameter binding are
more important than full SQL grammar coverage.

Supported prepared forms include:

- `SELECT * WHERE key = $1`
- `SELECT * WHERE key >= $1 AND key <= $2`
- `SELECT COUNT WHERE key >= $1 AND key <= $2`
- `INSERT key value`
- `UPDATE key value`
- `DELETE key`
- positional placeholders such as `$1`
- named placeholders such as `:user_id`

The prepared-query plan cache is explicit. `clear_cache()` invalidates all
cached prepared plans and resets cache statistics. DDL-driven automatic
invalidation is not yet part of the public contract.

## Planner contract

The optimizer turns SQL ASTs into physical plan nodes. Current single-table
access choices are:

- `PkLookup` for equality predicates on `id`
- `IndexScan` when table statistics contain a matching secondary index prefix
- `SeqScan` when no primary-key or secondary-index access path applies

The cost model uses table row-count estimates, average row size, simple
predicate selectivity, and index metadata. It is deliberately lightweight today;
future work should make selectivity estimates histogram-aware across more
predicate types and should connect `IndexScan` execution directly to the
secondary-index lookup APIs.

## Unsupported or limited SQL

Unsupported syntax must return a clear parser or optimizer error instead of
silently producing an ambiguous plan.

| Feature | Current behavior |
| --- | --- |
| `ALTER TABLE` | Unsupported; parser returns an `Unsupported: ALTER` error. |
| SQL `CREATE INDEX` / `DROP INDEX` | Use the secondary-index manager API; SQL DDL is not exposed yet. |
| Transactions through SQL text | Storage transactions exist separately; SQL transaction grammar is not exposed yet. |
| Foreign keys and constraints beyond primary-key column metadata | Not part of the current SQL contract. |
| Recursive CTEs, common table expressions, triggers, stored procedures | Not implemented. |
| Full PostgreSQL dialect compatibility | Not a current goal. OmniKV exposes a focused embedded-database SQL subset. |

## Regression policy

The SQL contract is guarded by:

- parser golden-output tests for common `SELECT`, `INSERT`, `UPDATE`, and
  `DELETE` statements
- planner tests for primary-key lookup, secondary-index scan, range/index
  choice, and sequential-scan fallback
- prepared plan-cache invalidation tests
- SQL integration tests for pagination, aggregation, joins, window functions,
  mutations, and set operations

Every new SQL feature should update this matrix and add either an execution
test or an explicit parser/planner regression test.
