# Stub — fill in once Phase 2 (docs/plan.md) produces a patched
# durable-streams-server binary to build. Shape it after the vendored
# crate's own Dockerfile (vendor/durable-streams-0.1.2/Dockerfile once
# vendored) rather than from scratch — the patched binary has the same
# runtime dependencies as upstream, just different source.
FROM rust:1.75 AS builder
WORKDIR /build
# COPY vendor/durable-streams-0.1.2 .
# RUN cargo build --release --features tier
FROM debian:bookworm-slim
# COPY --from=builder /build/target/release/durable-streams-server /usr/local/bin/
# ENTRYPOINT ["/usr/local/bin/durable-streams-server"]
