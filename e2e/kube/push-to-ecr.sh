#!/usr/bin/env bash
# Build the arm64 node + driver images and push them to ECR for the EKS e2e
# lane (the Helm chart lives under deploy/charts/tsoracle; the cluster is
# deployed via helm install pointing at the ECR image).
# Run from the tsoracle repo root — both Dockerfiles compile Rust from this
# build context.
set -euo pipefail

# Account and region are resolved from the caller's AWS context rather than
# hardcoded, so this script carries no environment-specific identifiers.
# Override either via the env if your shell default is not the target.
AWS_REGION="${AWS_REGION:-$(aws configure get region)}"
: "${AWS_REGION:?set AWS_REGION (no default region configured)}"
ACCOUNT="${ACCOUNT:-$(aws sts get-caller-identity --query Account --output text)}"
: "${ACCOUNT:?could not resolve AWS account id; set ACCOUNT}"
TAG="${TAG:-$(git rev-parse --short HEAD)}"
REG="${ACCOUNT}.dkr.ecr.${AWS_REGION}.amazonaws.com"

root="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$root"

echo "== ecr login (${REG}) =="
aws ecr get-login-password --region "$AWS_REGION" \
    | docker login --username AWS --password-stdin "$REG"

build_push() {
    local name="$1" dockerfile="$2"
    local repo="${REG}/prisma-risk/${name}"
    echo "== build ${repo} (arm64, tags: ${TAG}, latest) =="
    docker build --platform linux/arm64 -f "$dockerfile" \
        -t "${repo}:${TAG}" -t "${repo}:latest" .
    docker push "${repo}:${TAG}"
    docker push "${repo}:latest"
    echo "pushed ${repo}:${TAG}"
}

build_push tsoracle            deploy/Dockerfile
build_push tsoracle-e2e-driver e2e/kube/driver/Dockerfile

echo
echo "Pinned refs (for 'helm upgrade --set image.tag=...' / the driver sed in infra/k8s/tsoracle-e2e):"
echo "  tsoracle=${REG}/prisma-risk/tsoracle:${TAG}"
echo "  tsoracle-e2e-driver=${REG}/prisma-risk/tsoracle-e2e-driver:${TAG}"
