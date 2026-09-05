# Protocol and result-size limits

OmniKV rejects oversized protocol frames and caps public result surfaces to keep
one client from forcing unbounded memory allocation or response amplification.

## REST API

| Surface | Limit |
| --- | --- |
| JSON request bodies | 1 MiB |
| `/scan` default rows | 1,000 |
| `/scan` maximum rows | 10,000 |

REST scan requests above the maximum return `400 Bad Request` before the storage
scan is executed.

## PostgreSQL wire protocol

| Surface | Limit |
| --- | --- |
| Message body | 4 MiB |
| Default `SELECT` rows without explicit `LIMIT` | 10,000 |
| Maximum explicit `SELECT LIMIT` | 10,000 |
| Maximum `LIMIT + OFFSET` query window | 10,000 |

PGWire frame lengths below the protocol minimum or above the maximum are
rejected before allocation. Oversized `SELECT` windows return a protocol error
instead of streaming an unbounded result.

### Connection negotiation

Before the StartupMessage, libpq-based clients (psql, JDBC, psycopg2, pg8000,
node-postgres) send an SSLRequest, and optionally a GSSENCRequest, negotiation
packet. OmniKV answers each with a single byte `'N'` (no TLS/GSS upgrade on this
listener) and then continues reading the StartupMessage on the same connection,
matching PostgreSQL's plaintext fallback. Startup messages carrying any other
protocol code are rejected with SQLSTATE `08P01` (protocol violation), and
cancel-request connections for unknown backend keys are drained and closed. At
most 8 negotiation packets are accepted before the StartupMessage; further
negotiation attempts close the connection to prevent a pre-authentication spin
loop.

The PgWire listener therefore works with default client configurations
(`sslmode=prefer` included) even though it does not offer TLS yet. Cleartext
passwords still cross the wire, so the listener ships with a fail-closed
exposure policy: in production mode (`OMNIKV_MODE=production`) the PgWire
server refuses to start unless the bind address is loopback or a private
network address (RFC 1918 / ULA). Development mode allows any bind for local
experiments. Callers can check the policy without binding via
`PgWireServer::validate_security_policy`. Until PgWire TLS support lands,
production deployments that must expose PgWire off-host should terminate TLS
in front of the listener (for example a local `stunnel`/`socat` hop).

### Extended query protocol

OmniKV implements the extended query protocol alongside the simple one, so
DBAPI drivers (psycopg2, pg8000, JDBC, Npgsql) — whose `commit()`/`rollback()`
and parameterized statements drive Parse/Bind/Execute frames — work without
configuration:

| Message | Reply | Behavior |
| --- | --- | --- |
| `Parse` ('P') | `ParseComplete` ('1') | Statement syntax is checked at Parse time — a statement no grammar accepts is a `42601` error right here, like PostgreSQL. Statement stored under its name; the unnamed statement is destroyed by any Parse. Distinct names are capped per connection (see below) |
| `Bind` ('B') | `BindComplete` ('2') | `$1..$n` parameter values resolve into a portal (text format only; binary parameter OR result format is rejected with `08P01` at Bind time); any Bind destroys the unnamed portal. Distinct portal names are capped per connection (see below) |
| `Describe` ('D') | statement: `ParameterDescription` ('t') + `RowDescription` ('T')/`NoData` ('n'); portal: `RowDescription`/`NoData` | Side-effect-free: the row shape derives from the statement text, never by executing it. `ParameterDescription` reports the parameters the statement needs — the highest `$n` its text references — echoing the type OIDs the client declared at Parse (0 = unspecified; parameters bind as text) |
| `Execute` ('E') | `DataRow` ('D') rows + `CommandComplete` ('C'), or `PortalSuspended` ('s') | Runs the portal's statement through the same execution core as the simple protocol — transaction semantics cannot drift between the two paths. Execute itself never sends `RowDescription` (PostgreSQL: "Execute doesn't cause ReadyForQuery or RowDescription to be issued") — clients Describe first. A fresh execution consumes a rate-limit permit like a simple-protocol Query; resuming a suspended portal is free. A nonzero max-rows bound streams at most that many rows and ends with `PortalSuspended`; the next Execute resumes from retained rows (the statement is never re-run) and the final round closes with `CommandComplete` |
| `Close` ('C') | `CloseComplete` ('3') | Destroys a named statement or portal; closing a statement implicitly closes the portals constructed from it; closing an unknown name is not an error |
| `Sync` ('S') | `ReadyForQuery` | Ends the pipeline; clears any pending error state |
| `Flush` ('H') | — | Drains the socket |

Named statements and portals are bounded per connection: at most 1,000
distinct prepared statement names and 1,000 distinct portal names each.
The caps count distinct names only — replacing an existing name, or the
unnamed statement/portal slots that every driver round rewrites, never
grows the session. A Parse or Bind that would exceed a cap is a `54000`
error naming the limit.

Errors inside an extended-protocol pipeline follow PostgreSQL's
skip-until-Sync rule: the ErrorResponse is sent, the transaction (if any) is
marked failed, and every subsequent Parse/Bind/Describe/Execute/Close is
skipped without execution until the next `Sync`, which answers
`ReadyForQuery` and restores normal processing. Benign warnings (COMMIT with
no open transaction, BEGIN inside one) travel as `NoticeResponse` ('N')
frames — never `ErrorResponse` — because DBAPI drivers raise on any
ErrorResponse.

Parameters bind as text **as AST data, not SQL text**: `$n` placeholders are
parsed into marker nodes and the bound bytes are substituted into the parsed
statement after parsing — a value like `x OR 1=1` compares as one plain text
value and cannot alter the statement structure. An explicitly NULL Bind
value (length -1) binds as `NULL`; a placeholder with no corresponding Bind
value is a `08P01` "no value specified for parameter $n" error — missing is
not NULL (full NULL literal support in comparisons is tracked by #111).
Parameterized statements require the SQL grammar; the legacy KV grammar
rejects them with `0A000`. Write buffering inside explicit transactions is
tracked by #121.

## QUIC binary protocol

The QUIC server currently reads at most one 64 KiB request frame per bidirectional
stream. Larger request-body and response-budget configuration should be handled
under the runtime configuration hardening track.
