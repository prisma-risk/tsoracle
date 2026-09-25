# Multi-stage build for the in-cluster e2e assertion driver.
#
# The driver only depends on tsoracle-client (no rocksdb), so the build stage
# needs protobuf-compiler for the proto codegen but not clang/cmake.

FROM rust:1-bookworm@sha256:93ce27a88655056a51dbdd8f5f2d7ddc071c7b0070fb288a37b5a285fc83971e AS build
WORKDIR /src
RUN apt-get update \
    && apt-get install -y --no-install-recommends protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
COPY . .
RUN cargo build --release -p kube-e2e-driver --bin kube-e2e-driver

FROM debian:bookworm-slim@sha256:3783cc01769c7b2b1b83a5c5ad96c815348e28ed7da68e2e3687004faa906251 AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --create-home driver
COPY --from=build /src/target/release/kube-e2e-driver /usr/local/bin/kube-e2e-driver
USER driver
ENTRYPOINT ["/usr/local/bin/kube-e2e-driver"]
