# Pinned rather than `latest` so a rebuild cannot silently change the toolchain.
# The runtime image shares the trixie base, keeping glibc compatible.
FROM rust:1.94-trixie AS builder

WORKDIR /usr/src/app

# Build dependencies against a stub binary first so this layer stays cached
# whenever only application sources change.
COPY Cargo.toml Cargo.lock ./
RUN \
    mkdir -v src && \
    echo "fn main() {println!(\"Building dependencies...\");}" > src/main.rs && \
    cargo build --release --locked && \
    rm -Rvf src

COPY src ./src
COPY regexes.yaml ./regexes.yaml
RUN \
    touch src/main.rs && \
    cargo build --release --locked

FROM debian:trixie-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Run unprivileged. The numeric UID lets orchestrators enforce runAsNonRoot
# without having to resolve the name.
RUN useradd --system --uid 10001 --no-create-home --shell /usr/sbin/nologin appuser

COPY --from=builder /usr/src/app/target/release/urlshortener /usr/local/bin/urlshortener

USER 10001

EXPOSE 8080

# No HEALTHCHECK here on purpose: it would mean shipping curl in the runtime
# image. Point your orchestrator's liveness probe at GET /health instead.
CMD ["urlshortener"]
