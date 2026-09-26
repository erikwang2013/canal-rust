.PHONY: build test clippy fmt fmt-check doc doc-check clean run check

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

doc-check:
	cargo doc --no-deps --document-private-items

clean:
	cargo clean

run:
	cargo run --release -- server --config canal.yaml

# Mirrors .github/workflows/ci.yml `check` job, step for step.
check: fmt-check clippy doc-check test build
