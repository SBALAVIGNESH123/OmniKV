FROM rust:1.96-slim AS builder

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY crates/ ./crates/
COPY omni-client/ ./omni-client/

RUN cargo build --release -p omnikv-server

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system omnikv \
    && useradd --system --gid omnikv --home-dir /data --shell /usr/sbin/nologin omnikv \
    && mkdir -p /data /etc/omni \
    && chown -R omnikv:omnikv /data /etc/omni

WORKDIR /data
COPY --from=builder /app/target/release/omnikv-server /usr/local/bin/omnikv-server
COPY omni.toml.example /etc/omni/omni.toml
RUN chown omnikv:omnikv /etc/omni/omni.toml

EXPOSE 8080 8443 4433 5433

ENV RUST_LOG=info,omni_engine=debug
ENV OMNI_CONFIG=/etc/omni/omni.toml

USER omnikv
ENTRYPOINT ["omnikv-server"]
