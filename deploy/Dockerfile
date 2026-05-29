# Production tsoracle image. Build context is the REPO ROOT (compiles the
# workspace). `ARG FEATURES` selects the build: empty => fat (all drivers,
# default features); a driver name => lean (--no-default-features --features X).
# NOTE: ARG before FROM is global and MUST be re-declared inside the build
# stage for RUN to see it.
ARG FEATURES=""

FROM rust:1-bookworm@sha256:13c186980fa33cc12759b429662a1322939dbe697484b7c33b47dd2698d28460 AS build
ARG FEATURES
WORKDIR /src
# RocksDB (openraft/paxos toolkits) needs clang+cmake for bindgen; tsoracle-proto
# build script needs protoc.
RUN apt-get update \
    && apt-get install -y --no-install-recommends clang libclang-dev cmake protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
COPY . .
RUN cargo build --release -p tsoracle --bin tsoracle ${FEATURES:+--no-default-features --features $FEATURES}

FROM debian:bookworm-slim@sha256:0104b334637a5f19aa9c983a91b54c89887c0984081f2068983107a6f6c21eeb AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --create-home tsoracle
COPY --from=build /src/target/release/tsoracle /usr/local/bin/tsoracle
COPY deploy/entrypoint.sh /usr/local/bin/entrypoint.sh
# Pre-create the default DATA_DIR (entrypoint.sh defaults to /data) owned by the
# non-root runtime user. Without this the dir never exists in the image, so a
# bare `docker run` (no volume) can't mkdir it at / as uid 10001, and a fresh
# named volume mounted at /data is seeded root-owned -> "Permission denied". With
# the dir present, Docker seeds a fresh named volume from it (carrying ownership),
# and the no-volume case has a writable dir. Kubernetes is unaffected: the chart's
# podSecurityContext.fsGroup still re-owns the PVC on top (OnRootMismatch skips the
# re-chown since this already matches).
RUN chmod +x /usr/local/bin/entrypoint.sh \
    && mkdir -p /data \
    && chown tsoracle:tsoracle /data
USER tsoracle
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
