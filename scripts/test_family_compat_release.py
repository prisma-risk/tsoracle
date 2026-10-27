#!/usr/bin/env python3
"""Guard the file image's compatibility evidence chain without building Rust."""

from pathlib import Path
import re
import unittest

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = (ROOT / ".github/workflows/release-images.yml").read_text()
IMAGE_JOB = WORKFLOW.split("  build-image:\n", 1)[1].split("\n  manifest:\n", 1)[0]
STEPS = dict(re.findall(r"      - name: ([^\n]+)\n(.*?)(?=      - |\Z)", IMAGE_JOB, re.S))
SUBJECT = "subject: ${{ steps.image.outputs.subject }}"


class FamilyCompatReleaseTests(unittest.TestCase):
    def test_extract_uses_immutable_image_and_real_binary_version(self):
        push = STEPS["Push per-arch image and record its digest"]
        self.assertIn('^sha256:[0-9a-f]{64}$', push)
        self.assertIn('subject=ghcr.io/prisma-risk/$IMAGE@$digest', push)
        self.assertIn('docker pull "$SUBJECT"', STEPS["Pull immutable file-server image"])
        extract = STEPS["Extract file-server family compatibility"]
        for expected in (SUBJECT, "binary: tsoracle", "service: tsoracle",
                         "version: ${{ steps.family-version.outputs.version }}",
                         "asset-suffix: -${{ matrix.arch }}"):
            self.assertIn(expected, extract)
        version = STEPS["Read file-server manifest version"]
        self.assertIn('crates/tsoracle-bin/Cargo.toml', version)
        self.assertIn('version != os.environ["IMAGE_VERSION"]', version)

    def test_both_architectures_share_attestation_identity(self):
        self.assertIn("arch: [amd64, arm64]", IMAGE_JOB)
        for name in ("Extract file-server family compatibility",
                     "Attest file-server family compatibility",
                     "Verify file-server family compatibility"):
            step = STEPS[name]
            self.assertIn(SUBJECT, step)
            self.assertIn("variant: default", step)
            self.assertIn("architecture: ${{ matrix.arch }}", step)
            self.assertRegex(step, r"attest-family-compat@[a-f0-9]{40}\n")
        attest = STEPS["Attest file-server family compatibility"]
        verify = STEPS["Verify file-server family compatibility"]
        for step in (attest, verify):
            self.assertIn("manifest-path: ${{ steps.family-compat.outputs.manifest-path }}", step)
        self.assertIn("key: awskms:///alias/calcgraph-release-signing", attest)
        self.assertIn("public-key-path: release-metadata/cosign.pub", verify)

    def test_only_file_images_claim_file_receipt_support(self):
        for name, step in STEPS.items():
            if "file-server" in name or name == "Pull immutable file-server image":
                self.assertIn("if: matrix.variant == 'file'", step, name)
        manifest = (ROOT / "crates/tsoracle-bin/Cargo.toml").read_text()
        self.assertRegex(manifest, r'file\s*=.*"dep:cg-family-compat"')
        self.assertIn('version = "=11.0.0", default-features = false, optional = true', manifest)

    def test_verify_precedes_upload_of_manifest_checksum_and_bundle(self):
        names = list(STEPS)
        verify = names.index("Verify file-server family compatibility")
        self.assertLess(verify, names.index("Upload file-server family evidence"))
        self.assertLess(verify, names.index("Attach file-server family evidence to the GitHub release"))
        upload = STEPS["Attach file-server family evidence to the GitHub release"]
        for asset in ("family-compat-${{ matrix.arch }}.json",
                      "family-compat-${{ matrix.arch }}.json.sha256",
                      "tsoracle-file-${{ matrix.arch }}.family-compat.bundle.json",
                      "cosign.pub"):
            self.assertIn(asset, upload)
        self.assertIn("path: release-metadata/", STEPS["Upload file-server family evidence"])
        self.assertIn("python3 -B scripts/test_family_compat_release.py", WORKFLOW)

    def test_signing_configuration_is_required_before_publish(self):
        self.assertIn("id-token: write", IMAGE_JOB)
        gate = STEPS["Require file-server signing configuration"]
        self.assertIn("${{ vars.TSORACLE_RELEASE_ROLE_ARN }}", gate)
        self.assertIn('test -n "$RELEASE_ROLE"', gate)
        names = list(STEPS)
        self.assertLess(names.index("Require file-server signing configuration"),
                        names.index("Push per-arch image and record its digest"))
        credentials = STEPS["Configure file-server release signing credentials"]
        self.assertIn("role-to-assume: ${{ vars.TSORACLE_RELEASE_ROLE_ARN }}", credentials)


if __name__ == "__main__":
    unittest.main()
