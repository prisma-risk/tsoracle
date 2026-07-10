# Multi-stage build for the in-cluster e2e assertion driver.
#
# The driver only depends on tsoracle-client (no rocksdb), so the build stage
# needs protobuf-compiler for the proto codegen but not clang/cmake.

FROM rust:1-bookworm@sha256:7d0723df719e7f213b69dc7c8c595985c3f4b060cfbee4f7bc0e347a86fe3b6a AS build
WORKDIR /src
RUN apt-get update \
    && apt-get install -y --no-install-recommends protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
COPY . .
RUN cargo build --release -p kube-e2e-driver --bin kube-e2e-driver

FROM debian:bookworm-slim@sha256:60eac759739651111db372c07be67863818726f754804b8707c90979bda511df AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --create-home driver
COPY --from=build /src/target/release/kube-e2e-driver /usr/local/bin/kube-e2e-driver
USER driver
ENTRYPOINT ["/usr/local/bin/kube-e2e-driver"]
