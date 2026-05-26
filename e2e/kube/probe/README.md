# NetworkPolicy enforcement probe

Asserts that the chart's NetworkPolicy (introduced in PR #452) actually applies at runtime: the `peer` consensus port must be DENIED to any source outside the StatefulSet's labels, and the `tso` client port must be ALLOWED to everyone. Issue #486.

The chart's NetworkPolicy renders for any HA driver (`openraft` or `paxos`) when `networkPolicy.enabled` is true (the chart default). Its `from:` clause keys on `app.kubernetes.io/name=tsoracle` + `app.kubernetes.io/instance=<release>`, so a pod with a different name or in a different namespace satisfies neither — exactly the negative test we want.

`run-netpol-probe.sh` applies `job-netpol-probe.yaml` into a *separate* namespace (passed as `PROBE_NAMESPACE`) with `app.kubernetes.io/name: tsoracle-netpol-probe` labels and a small busybox-`nc` script that:

  * Probes `<TARGET_RELEASE>-0.<TARGET_RELEASE>-peer.<TARGET_NAMESPACE>.svc.cluster.local:5100` and asserts the connect times out (NetworkPolicy DENIED).
  * Probes the same FQDN on port 5051 and asserts the connect succeeds (NetworkPolicy ALLOWED).

Selective `envsubst '${TARGET_RELEASE} ${TARGET_NAMESPACE}'` substitutes only those two variables; every other `${...}` in the manifest is a shell reference that the container resolves at runtime.

## CNI requirement

kindnet's bundled `kube-network-policies` sidecar enforces NetworkPolicy as of kind v0.31.0 (the default version helm/kind-action@v1.14.0 ships). The kube-e2e workflow uses that version, so no CNI swap is needed. If you point this probe at a kind cluster older than v0.27 you may see false PASSes (the probe expects the peer port to be unreachable, and an unenforced policy actually leaves it reachable — `wrapper-ec=1` then). The current workflow's `helm/kind-action@v1.14.0` pin keeps us above that threshold.

## Run locally

Against a chart deployment in the `default` namespace:

```sh
helm install tsoracle deploy/charts/tsoracle --set driver=openraft,replicas=3,ports.client=5051,ports.peer=5100,tls.allowInsecurePeer=true --wait
TARGET_RELEASE=tsoracle TARGET_NAMESPACE=default PROBE_NAMESPACE=e2e-netpol-probe-insecure ./e2e/kube/probe/run-netpol-probe.sh
```

To verify the probe correctly catches a regression, `kubectl delete networkpolicy <release>-peer` and re-run — the probe should exit non-zero with `FAIL: peer port ... reachable`.
