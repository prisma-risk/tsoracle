# Production tsoracle image. Build context is the REPO ROOT (compiles the
# workspace). `ARG FEATURES` selects the build: empty => fat (all drivers,
# default features); a driver name => lean (--no-default-features --features X).
# NOTE: ARG before FROM is global and MUST be re-declared inside the build
# stage for RUN to see it.
ARG FEATURES=""

FROM rust:1-bookworm@sha256:0e2bcaef56d041a486784e54104a81aebe0da44bd03019bd70bc0401e42e4a97 AS build
ARG FEATURES
WORKDIR /src
# RocksDB (openraft/paxos toolkits) needs clang+cmake for bindgen; tsoracle-proto
# build script needs protoc.
RUN apt-get update \
    && apt-get install -y --no-install-recommends clang libclang-dev cmake protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
COPY . .
RUN cargo build --release -p tsoracle --bin tsoracle ${FEATURES:+--no-default-features --features $FEATURES}

FROM debian:bookworm-slim@sha256:88200866dfff7ea7f5cbcb6ec7c8a701889efe6fe859fe64d6990e4b07ea4171 AS runtime
# OCI image metadata. IMAGE_TITLE/IMAGE_DESCRIPTION are passed per-variant by the
# release-images workflow so each driver image (tsoracle-file, tsoracle-openraft,
# tsoracle) carries its own description; defaults cover a bare `docker build`. This
# description LABEL is the per-arch-config copy that `docker inspect` reads; the
# release workflow ALSO re-emits the same value as an index-level annotation on the
# multi-arch manifest, which is the layer GHCR's package page actually surfaces.
ARG IMAGE_TITLE="tsoracle"
ARG IMAGE_DESCRIPTION="A distributed timestamp and sequence oracle"
LABEL org.opencontainers.image.title="${IMAGE_TITLE}" \
      org.opencontainers.image.description="${IMAGE_DESCRIPTION}" \
      org.opencontainers.image.source="https://github.com/prisma-risk/tsoracle" \
      org.opencontainers.image.licenses="Apache-2.0"
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
