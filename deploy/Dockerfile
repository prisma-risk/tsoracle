# Production tsoracle image. Build context is the REPO ROOT (compiles the
# workspace). `ARG FEATURES` selects the build: empty => fat (all drivers,
# default features); a driver name => lean (--no-default-features --features X).
# NOTE: ARG before FROM is global and MUST be re-declared inside the build
# stage for RUN to see it.
ARG FEATURES=""

FROM rust:1-bookworm@sha256:6258907abe69656e41cd992e0b705cdcfabcbbe3db374f92ed2d47121282d4a1 AS build
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
RUN chmod +x /usr/local/bin/entrypoint.sh
USER tsoracle
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
