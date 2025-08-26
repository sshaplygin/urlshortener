# URL shotener with YDB

This project implements a link shortener in the language of Rust and YDB as Data storage platform.
The YDB used for store link as row oriented store and column oriented store for visits.
The operation of adding statistics is implemented via async producer and consumer under YDB topics.

## Technologies

- Rust
- YDB
- Docker
