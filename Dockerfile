# syntax=docker/dockerfile:1.7

FROM rust:1.89-bookworm AS rust-builder
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY benches ./benches
COPY docs/openapi ./docs/openapi
RUN cargo build --locked --release --bin multidb

FROM node:24-bookworm-slim AS studio-builder
WORKDIR /studio
COPY studio/package.json studio/package-lock.json ./
COPY sdk/typescript ../sdk/typescript
RUN cd ../sdk/typescript && npm ci && npm run build
RUN npm ci
COPY studio ./
ENV VITE_MULTIDB_API_BASE=/api
RUN npm run build

FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10001 multidb \
    && useradd --system --uid 10001 --gid multidb --home-dir /var/lib/multidb multidb \
    && mkdir -p /var/lib/multidb /usr/share/multidb/studio \
    && chown -R multidb:multidb /var/lib/multidb /usr/share/multidb

COPY --from=rust-builder /src/target/release/multidb /usr/local/bin/multidb
COPY --from=studio-builder /studio/dist/ /usr/share/multidb/studio/

ENV MULTIDB_BIND=0.0.0.0:8080 \
    MULTIDB_PG_BIND=0.0.0.0:5432 \
    MULTIDB_DB_PATH=/var/lib/multidb/multidb.redb \
    MULTIDB_PROFILE=transactional \
    MULTIDB_RUNTIME_MODE=production \
    MULTIDB_STUDIO_DIR=/usr/share/multidb/studio

USER multidb
VOLUME ["/var/lib/multidb"]
EXPOSE 5432 8080
HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=6 \
    CMD curl --fail --silent http://127.0.0.1:8080/health >/dev/null || exit 1

ENTRYPOINT ["multidb", "serve"]
