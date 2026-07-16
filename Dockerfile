FROM rust:1.96-slim AS builder

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY bindings/python/ ./bindings/python/
COPY crates/ ./crates/
COPY omni-client/ ./omni-client/

RUN cargo build --release -p omnikv-server

FROM debian:trixie-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10001 omnikv \
    && useradd --system --uid 10001 --gid omnikv --home-dir /data --shell /usr/sbin/nologin omnikv \
    && mkdir -p /data /etc/omni \
    && chown -R omnikv:omnikv /data /etc/omni

WORKDIR /data
COPY --from=builder /app/target/release/omnikv-server /usr/local/bin/omnikv-server
COPY omni.toml.example /etc/omni/omni.toml
RUN chown omnikv:omnikv /etc/omni/omni.toml

EXPOSE 8080 8443 4433 5433

ENV RUST_LOG=info
ENV OMNIKV_CONFIG=/etc/omni/omni.toml
ENV OMNI_CONFIG=/etc/omni/omni.toml

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD curl -kfsS https://127.0.0.1:8443/health || exit 1

USER omnikv
ENTRYPOINT ["omnikv-server"]
