FROM rust:1.86-slim AS builder

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src/ ./src/
COPY tests/ ./tests/
COPY omni-client/ ./omni-client/

RUN cargo build --release --bin omni_engine

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system omnikv \
    && useradd --system --gid omnikv --home-dir /data --shell /usr/sbin/nologin omnikv \
    && mkdir -p /data /etc/omni \
    && chown -R omnikv:omnikv /data /etc/omni

WORKDIR /data
COPY --from=builder /app/target/release/omni_engine /usr/local/bin/omni_engine
COPY omni.toml.example /etc/omni/omni.toml
RUN chown omnikv:omnikv /etc/omni/omni.toml

EXPOSE 8080 8443 4433 5433

ENV RUST_LOG=info,omni_engine=debug
ENV OMNI_CONFIG=/etc/omni/omni.toml

USER omnikv
ENTRYPOINT ["omni_engine"]
