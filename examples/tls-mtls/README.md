# TLS and mTLS configuration end-to-end

A single binary that demonstrates the four common shapes of TLS configuration on [`tsoracle-server`](https://docs.rs/tsoracle-server) and [`tsoracle-client`](https://docs.rs/tsoracle-client):

1. **Plain TLS** — server presents a leaf cert, client verifies the chain.
2. **mTLS** — server *also* verifies the client's cert against its configured CA root.
3. **Custom connector** — same mTLS server, but the client uses `.channel_connector(...)` to add transport knobs (keepalive in this example) not exposed on `.tls_config(...)`.
4. **mTLS misconfiguration** — same mTLS server, client *without* a client identity. The handshake fails and the error variant is printed so readers can recognize the pattern.

All certificates are minted in memory via [`rcgen`](https://docs.rs/rcgen). Nothing is read from disk; nothing is shipped in the repo. Production callers would replace `certs::mint()` with `Identity::from_pem(std::fs::read_to_string(path)?, ...)` and load PEM material from their configured cert store.

## Run

```bash
cargo run -p example-tls-mtls
```

Each step boots a fresh `tsoracle_server::Server` on `127.0.0.1:0`, captures the OS-picked port, runs three `get_ts()` calls, and tears down before the next step. Total runtime is a few hundred milliseconds.

## What to look at in `src/main.rs`

- **`ServerBuilder::tls_config(ServerTlsConfig::new().identity(...))`** is the server-side surface for plain TLS. Adding `.client_ca_root(...)` turns it into mTLS.
- **`ClientBuilder::tls_config(ClientTlsConfig::new().ca_certificate(...).domain_name(...))`** is the client-side one-liner for the common case. Adding `.identity(...)` upgrades it to mTLS.
- **`ClientBuilder::channel_connector(|endpoint| async { ... })`** is the escape hatch for transport behaviour beyond `ClientTlsConfig` — keepalive, proxies, service-mesh integrations. Errors returned from the closure surface as `ClientError::Connector`.
- **The scheme rule** matters: bare `host:port` endpoints become `https://host:port` when `.tls_config(...)` is set; explicit `http://...` and `https://...` schemes are honored as-is. See `docs/client-api-and-usage.md` for the full table.
- **Step 4** intentionally fails. `ClientError::Transport(_)` and `ClientError::Rpc(_)` are both possible failure surfaces for an mTLS handshake error depending on whether the handshake fails eagerly during `connect()` or lazily during the first RPC.

## When this example is *not* the right shape

- **Production cert management** (file watching, ACME, hot rotation, key rotation, cert reload on SIGHUP) is out of scope. Plug your own PEM-loading code into `Identity::from_pem` and `Certificate::from_pem`.
- **Authentication policy** beyond "is the cert signed by the configured CA" is out of scope — that includes SPIFFE / SVID validation, subject DN matching, OCSP, and audit logging. Use a tonic interceptor or a service-mesh sidecar for those.
- **CLI flags for TLS on the stock `tsoracle` binary** are not currently exposed. If you need TLS today, embed the library as this example does.
- **HA / openraft.** This example uses [`FileDriver`](https://docs.rs/tsoracle-driver-file), a single-node driver. The TLS wiring is independent of consensus — swap in a `ConsensusDriver` from [`tsoracle-driver-openraft`](https://docs.rs/tsoracle-driver-openraft) and keep the `.tls_config(...)` calls exactly as written here.
