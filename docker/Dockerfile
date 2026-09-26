# NOTE: 1.85 is the real floor, not a preference. Cargo.lock pins dependencies
# (clap 4.6.4, clap_builder 4.6.2, getrandom 0.4.3, ...) that declare
# `edition = "2024"` and `rust-version = "1.85"`, so an older toolchain cannot
# build this lockfile at all. Keep in sync with `rust-version` in Cargo.toml.
FROM rust:1.85-slim-bookworm AS builder
# canal-proto/build.rs drives prost-build, which shells out to `protoc`;
# the slim image does not ship it, so the build fails without this.
RUN apt-get update && apt-get install -y --no-install-recommends protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY crates/ crates/
COPY proto/ proto/
RUN cargo build --release --locked -p canal-cli

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/canal-rust /usr/local/bin/canal-rust
# Docker-specific config: binds 0.0.0.0 so published ports actually work.
# Overridden by the bind mount in docker-compose.yml if you want to edit it live.
COPY docker/canal.yaml /etc/canal/canal.yaml
EXPOSE 11111 11112 9090
ENTRYPOINT ["/usr/local/bin/canal-rust"]
CMD ["server", "--config", "/etc/canal/canal.yaml"]
