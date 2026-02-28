FROM rust:latest AS builder

WORKDIR /usr/src/app
COPY Cargo.toml Cargo.lock ./
RUN \
    mkdir -v src && \
    echo "fn main() {println!(\"Building dependencies...\");}" > src/main.rs && \
    # debug build
    # cargo build && \
    cargo build --release && \
    rm -Rvf src

COPY src ./src
COPY regexes.yaml ./regexes.yaml
RUN \
    touch src/main.rs && \
    # debug build
    # cargo build
    cargo build --release

FROM debian:trixie-slim

RUN apt-get update && \
    apt-get install -y ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# debug build
# COPY --from=builder /usr/src/app/target/debug/urlshortener /usr/local/bin/urlshortener
COPY --from=builder /usr/src/app/target/release/urlshortener /usr/local/bin/urlshortener

CMD ["urlshortener"]
