# Multi-stage build for the openraft-standalone cluster node.
#
# The stock `tsoracle serve` binary is single-node (file driver); a real
# cluster runs the `openraft-standalone` example, which takes peer topology
# on the command line. The entrypoint derives that topology from the
# StatefulSet ordinal at runtime.

FROM rust:1-bookworm AS build
WORKDIR /src

# RocksDB (pulled in by the openraft toolkit) needs a C/C++ toolchain + clang
# for its bindgen step.
RUN apt-get update \
    && apt-get install -y --no-install-recommends clang libclang-dev cmake \
    && rm -rf /var/lib/apt/lists/*

COPY . .
RUN cargo build --release -p example-openraft-standalone --bin openraft-standalone

FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --create-home tsoracle

COPY --from=build /src/target/release/openraft-standalone /usr/local/bin/openraft-standalone
COPY e2e/kube/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

USER tsoracle
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
