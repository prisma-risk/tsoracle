# Deployment

How to deploy tsoracle from the published container images and Helm chart. For raw server configuration (tuning `window_ahead`, monitoring, client retry behavior), see [Operations](operations.md). For embedding the server inside your own binary, see [Getting Started](getting-started.md#embedding-the-server).

## Images

Two image families are published to GHCR on every release.

**Fat image** — `ghcr.io/prisma-risk/tsoracle:<version>` — ships all three drivers (`file`, `openraft`, `paxos`) in a single image. Use it with the Helm chart and select the driver via the `driver` value; the chart's entrypoint picks the right binary at runtime. Convenient when you don't want to pin to a driver at image-pull time.

**Lean images** — `ghcr.io/prisma-risk/tsoracle-file:<version>`, `ghcr.io/prisma-risk/tsoracle-openraft:<version>`, `ghcr.io/prisma-risk/tsoracle-paxos:<version>` — each carries only one driver binary. Smaller footprint; useful when you know the driver ahead of time or want a tighter supply-chain attestation surface. Switch to a lean image by overriding `image.repository` in the Helm values (see the values table below).

All images are multi-arch: `linux/amd64` and `linux/arm64`.

## Quick start (Helm)

The chart is published as an OCI artifact. Pull and install it with `helm`:

**3-node HA with openraft (recommended default):**

```bash
helm install tso oci://ghcr.io/prisma-risk/charts/tsoracle \
  --set driver=openraft
```

This creates a 3-replica StatefulSet, a headless peer service, and a PodDisruptionBudget. Ordinal-0 bootstraps the raft cluster automatically on first start.

**Single-node with the file driver:**

```bash
helm install tso oci://ghcr.io/prisma-risk/charts/tsoracle \
  --set driver=file,replicas=1
```

The `file` driver requires `replicas=1`; the chart rejects any combination of `driver=file` with `replicas>1` at template-render time.

**3-node HA with paxos:**

```bash
helm install tso oci://ghcr.io/prisma-risk/charts/tsoracle \
  --set driver=paxos
```

**Using a lean image** (e.g. the openraft-only image, to avoid shipping unused driver binaries):

```bash
helm install tso oci://ghcr.io/prisma-risk/charts/tsoracle \
  --set driver=openraft \
  --set image.repository=ghcr.io/prisma-risk/tsoracle-openraft
```

## Values reference

| Value | Default | Meaning |
|---|---|---|
| `driver` | `openraft` | Which consensus driver to run: `file`, `openraft`, or `paxos`. |
| `replicas` | `3` | Number of replicas. Must be `1` when `driver=file`; the chart errors otherwise. Recommended odd values for HA drivers: 3 or 5. |
| `image.repository` | `ghcr.io/prisma-risk/tsoracle` | Image repository. Override with a lean image name to pull a single-driver image. |
| `image.tag` | *(chart appVersion)* | Image tag. Defaults to the chart's appVersion; override to pin a specific release. |
| `ports.client` | `50551` | gRPC port exposed to application clients. |
| `ports.peer` | `50552` | gRPC port used for inter-node replication (openraft/paxos only). |
| `tls.enabled` | `false` | Enable TLS on the client-facing gRPC port. Requires `tls.secretName` to be set. |
| `tls.secretName` | `""` | Name of a Kubernetes Secret containing `tls.crt`, `tls.key`, and `ca.crt`. |
| `tls.clientMtls` | `false` | Require client certificates on the API port (mutual TLS). Only meaningful when `tls.enabled=true`. |
| `tls.allowInsecurePeer` | `false` | HA drivers (`openraft`, `paxos`) refuse to render with `tls.enabled=false` unless this is set. Setting it injects `ALLOW_INSECURE_PEER=true` into the container env, which the entrypoint translates to `--allow-insecure-peer` on the binary — opting out the peer-listener secure-by-default guard at both render and runtime. See [Peer-port trust boundary](operations.md#peer-port-trust-boundary). |
| `server.windowAhead` | `1s` | How far ahead the allocator extends the high-water on each window extension. See [Sizing window_ahead](operations.md#sizing-window_ahead). |
| `server.failoverAdvance` | `1s` | How far past `serving_floor` the failover fence advances the high-water on leadership gain. See [Sizing failover_advance](operations.md#sizing-failover_advance). |
| `server.logLevel` | `info` | Log level passed to the server (`error`, `warn`, `info`, `debug`, `trace`). |
| `persistence.size` | `1Gi` | PVC size per pod. |
| `persistence.storageClassName` | `""` | StorageClass for PVCs. Empty string means "use the cluster default". |
| `resources` | `{}` | Standard Kubernetes resource requests/limits block applied to the tsoracle container. |

## TLS and mTLS

### Client TLS

To terminate TLS on the client-facing port, create a Secret with the server certificate and enable TLS in the chart:

```bash
kubectl create secret generic tso-tls \
  --from-file=tls.crt=server.crt \
  --from-file=tls.key=server.key \
  --from-file=ca.crt=ca.crt

helm install tso oci://ghcr.io/prisma-risk/charts/tsoracle \
  --set driver=openraft \
  --set tls.enabled=true \
  --set tls.secretName=tso-tls
```

To also require client certificates on the API (mutual TLS), add `--set tls.clientMtls=true`. Clients must then present a certificate signed by the same CA.

### Peer mTLS (HA drivers)

For `openraft` and `paxos` deployments, replication traffic between pods travels on the peer port (`ports.peer`). The chart secures this channel with the same Secret (`tls.secretName`). The peer CA is used to authenticate joining nodes: **any node holding a certificate signed by that CA can participate in the replication group**. Use a cluster-dedicated CA — one created solely for this tsoracle deployment — rather than a shared organizational CA. Rotating to a new CA requires replacing all peer certificates simultaneously; a mixed-CA cluster will not form.

Node certificates must include SANs for every pod's DNS name in the StatefulSet headless service. For a release named `tso` in the `default` namespace with the default headless service suffix `-peer`, the required SANs are:

```
tso-0.tso-peer.default.svc.cluster.local
tso-1.tso-peer.default.svc.cluster.local
tso-2.tso-peer.default.svc.cluster.local
```

Adjust ordinals for the `replicas` count. Use a wildcard SAN (`*.tso-peer.default.svc.cluster.local`) if your cert tooling supports it.

## Topologies

### Single-node (`driver=file`)

Creates a single Kubernetes **Deployment** (not a StatefulSet) backed by one PVC. No peer service; no PDB. Suitable for development, testing, or workloads that can tolerate TSO downtime during pod restarts. Not HA: a pod restart interrupts timestamp service until the new pod is ready.

### HA (`driver=openraft` or `driver=paxos`)

Creates a **StatefulSet** with `replicas` pods (default 3), a headless peer **Service** for inter-node replication, and a **PodDisruptionBudget** to preserve quorum during voluntary disruptions. Each pod gets its own PVC under the StatefulSet's `volumeClaimTemplates`.

For `openraft`, ordinal-0 bootstraps the raft cluster on first start; ordinals 1 and 2 join automatically. Replacing the StatefulSet without a data migration resets the cluster; scale carefully and back up PVCs before destructive operations.

For `paxos`, all nodes are peers from the start; the OmniPaxos leader election determines which node serves timestamps.

In both HA modes, the application client should be configured with all pod addresses (or the headless service DNS names) so it can discover the current leader via `NOT_LEADER` redirects. See [Client API and Usage](client-api-and-usage.md#leader-discovery-and-retries).
