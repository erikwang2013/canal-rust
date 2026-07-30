FROM rust:1.80-slim-bookworm AS builder
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY crates/ crates/
COPY proto/ proto/
RUN cargo build --release -p canal-cli

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/canal-rust /usr/local/bin/canal-rust
COPY canal.yaml /etc/canal/canal.yaml
EXPOSE 11111 8080 9090
ENTRYPOINT ["/usr/local/bin/canal-rust"]
CMD ["server", "--config", "/etc/canal/canal.yaml"]
