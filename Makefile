.PHONY: build test clippy fmt doc clean run

build:
	cargo build --release

test:
	cargo test --all

clippy:
	cargo clippy --all-targets -- -D warnings

fmt:
	cargo fmt --all

fmt-check:
	cargo fmt --check --all

doc:
	cargo doc --no-deps --open

clean:
	cargo clean

run:
	cargo run --release -- server --config canal.yaml

check: fmt-check clippy test build
