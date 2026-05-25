# Multi-stage build for the kind e2e cluster node.
#
# Each pod runs the shipped `tsoracle serve openraft` subcommand. The
# entrypoint derives the node id and membership topology from the
# StatefulSet ordinal at runtime.

FROM rust:1-bookworm AS build
WORKDIR /src

# RocksDB (pulled in by the openraft toolkit) needs a C/C++ toolchain + clang
# for its bindgen step; tsoracle-proto's build script needs protoc.
RUN apt-get update \
    && apt-get install -y --no-install-recommends clang libclang-dev cmake protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

COPY . .
RUN cargo build --release -p tsoracle --bin tsoracle

FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --create-home tsoracle

COPY --from=build /src/target/release/tsoracle /usr/local/bin/tsoracle
COPY e2e/kube/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

USER tsoracle
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
