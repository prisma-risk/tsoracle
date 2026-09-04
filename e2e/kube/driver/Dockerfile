# Multi-stage build for the in-cluster e2e assertion driver.
#
# The driver only depends on tsoracle-client (no rocksdb), so the build stage
# needs protobuf-compiler for the proto codegen but not clang/cmake.

FROM rust:1-bookworm@sha256:82150a52ec202c1b14d7817e14516c392bb7f5cfebd88f1ed531cb37ebd39922 AS build
WORKDIR /src
RUN apt-get update \
    && apt-get install -y --no-install-recommends protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
COPY . .
RUN cargo build --release -p kube-e2e-driver --bin kube-e2e-driver

FROM debian:bookworm-slim@sha256:88200866dfff7ea7f5cbcb6ec7c8a701889efe6fe859fe64d6990e4b07ea4171 AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --create-home driver
COPY --from=build /src/target/release/kube-e2e-driver /usr/local/bin/kube-e2e-driver
USER driver
ENTRYPOINT ["/usr/local/bin/kube-e2e-driver"]
