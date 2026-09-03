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
passwords still cross the wire, so the listener should only be exposed on
trusted networks until PgWire TLS support lands.

## QUIC binary protocol

The QUIC server currently reads at most one 64 KiB request frame per bidirectional
stream. Larger request-body and response-budget configuration should be handled
under the runtime configuration hardening track.
