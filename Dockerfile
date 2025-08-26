FROM rust:latest as builder

WORKDIR /usr/src/app
COPY Cargo.toml Cargo.lock ./
RUN \
    mkdir -v src && \
    echo "fn main() {println!(\"Building dependencies...\");}" > src/main.rs && \
    cargo build --release && \
    rm -Rvf src

COPY src ./src
COPY regexes.yaml ./regexes.yaml
RUN \
    touch src/main.rs && \
    cargo build --release

FROM debian:trixie-slim

RUN apt-get update && \
    apt-get install -y ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/src/app/target/release/urlshortener /usr/local/bin/urlshortener

ENV RUST_LOG="trace"

CMD ["urlshortener"]
