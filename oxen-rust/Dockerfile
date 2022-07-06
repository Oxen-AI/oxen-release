FROM rust:1.62 as builder

USER root
RUN apt-get update && apt-get install --assume-yes apt-utils
RUN apt-get install -y libclang-dev
RUN cargo install cargo-build-deps

# create an empty project to install dependencies
RUN cd /usr/src && cargo new --bin oxen-server
WORKDIR /usr/src/oxen-server
COPY Cargo.toml Cargo.lock ./
COPY src/lib/Cargo.toml src/lib/Cargo.toml
COPY src/cli/Cargo.toml src/cli/Cargo.toml
COPY src/server/Cargo.toml src/server/Cargo.toml
# build just the deps for caching
RUN cargo build-deps --release

# copy the rest of the source and build the server
COPY src src
RUN cargo build --release --bin oxen-server

# Minimal image to run the binary (without Rust toolchain)
FROM debian:bullseye-slim AS runtime
WORKDIR /oxen-server
COPY --from=builder /usr/src/oxen-server/target/release/oxen-server /usr/local/bin
ENV SYNC_DIR=/var/oxen/data
EXPOSE 3000
CMD ["oxen-server", "start"]