# TLS-cell driver Job manifests

These manifests are the TLS-cell counterparts of `e2e/kube/driver/job-*.yaml`, used by the kube-e2e TLS cell (issue #483). The lane orchestrator (`e2e/kube/run-assertions.sh`) picks this directory by setting `JOB_DIR=$here/driver/tls` and pointing the kubectl context at the TLS-cell namespace (`e2e-tls`).

The three Jobs (`cold-start`, `soak`, `sigkill-soak`) share these properties with their plaintext siblings:

  * same `metadata.name` — the namespace separates them from the insecure cell.
  * same args except: `--endpoints` use the TLS-cell pod FQDNs (full DNS form so SNI matches a SAN on the chart's leaf cert) and a `--tls-ca` flag points at the mounted Secret's `ca.crt`.
  * an extra `tls` volume mount that exposes the same Secret the chart's pods consume (so the driver trusts the same CA the cluster's server cert chains to).

The Secret name (`tsoracle-tls-certs`) is fixed across these manifests and matches the `--set tls.secretName=` passed to `helm install` in the workflow. A `gen-certs` invocation on the runner produces the PEM trio populating that Secret before `helm install`.
