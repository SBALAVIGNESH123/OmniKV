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

## QUIC binary protocol

The QUIC server currently reads at most one 64 KiB request frame per bidirectional
stream. Larger request-body and response-budget configuration should be handled
under the runtime configuration hardening track.
