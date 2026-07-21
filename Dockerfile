# Multi-stage build: compile the release binaries once, ship them on a slim runtime.
# Produces one image that runs either role (coordinator or shard-node) via the entrypoint
# argument — the binaries are selected by `command:` in docker-compose.yml.

FROM rust:1-bookworm AS builder
RUN apt-get update && apt-get install -y --no-install-recommends protobuf-compiler && rm -rf /var/lib/apt/lists/*
WORKDIR /build
# Copy the manifests and sources needed to build the two server binaries.
COPY Cargo.toml Cargo.lock ./
COPY proto ./proto
COPY crates ./crates
RUN cargo build --release -p coordinator -p shard-node

FROM debian:bookworm-slim AS runtime
RUN useradd -m aether
COPY --from=builder /build/target/release/coordinator /usr/local/bin/coordinator
COPY --from=builder /build/target/release/shard-node /usr/local/bin/shard-node
USER aether
# No default command: docker-compose selects `coordinator` or `shard-node`. Configuration is
# entirely via AETHER_* environment variables (see docker-compose.yml).
