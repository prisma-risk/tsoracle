# tsoracle.rs

The marketing and landing site for tsoracle, served at [tsoracle.rs](https://tsoracle.rs). It is a static site built with [Zola](https://www.getzola.org/) and deployed to GitHub Pages.

This is **not** the documentation. The long-form prose guide lives in [`docs/`](../docs/) and the API reference is on [docs.rs](https://docs.rs/tsoracle-server).

## Layout

- `config.toml` — Zola configuration (base URL, taxonomies, Markdown options).
- `content/` — page and post sources. Social cards are generated per post under `content/posts/`.
- `templates/` — Tera templates.
- `sass/` — stylesheets compiled by Zola.
- `static/` — assets copied verbatim into the build. The `static/og/` directory and `static/og-default.png` are generated social cards and are gitignored.

## Building locally

Social cards are rendered by the `scripts/og-image` Rust tool before Zola runs, exactly as CI does. Run it from the repository root, then build or serve from this directory:

```sh
# From the repository root: regenerate the og:image PNGs into site/static/og/
cargo run --release --manifest-path scripts/og-image/Cargo.toml

# From site/: live-reloading preview at http://127.0.0.1:1111
zola serve

# Or produce the static build into site/public/
zola build

# Validate internal links the way the PR check does
zola check --skip-external-links
```

## Deployment

`.github/workflows/pages.yml` builds and deploys to GitHub Pages on every push to `main` that touches `site/**`, `scripts/og-image/**`, or the workflow itself. `.github/workflows/pages-build-check.yml` runs the same build (plus link checking) on pull requests so breakage is caught before merge.

## Upgrading Zola

The Zola binary is downloaded from GitHub Releases and verified against a pinned SHA-256 in **both** workflows. The version and checksum must be updated together, and in both files, or the build will fail the integrity check:

- `.github/workflows/pages.yml` — the deploy workflow.
- `.github/workflows/pages-build-check.yml` — the pull-request build check.

Each pins the same two values in its `Install Zola` step:

```sh
ZOLA_VERSION="0.22.1"
ZOLA_SHA256="0ca09aa40376aaa9ddfb512ff9ad963262ef95edb0d0f2d5ec6961b6f5cf22ef"
```

To bump to a new release (for example `0.23.0`), compute the checksum of the Linux artifact the workflows download and update both files in lockstep:

```sh
ZOLA_VERSION="0.23.0"
curl -fsSL -o zola.tar.gz \
  "https://github.com/getzola/zola/releases/download/v${ZOLA_VERSION}/zola-v${ZOLA_VERSION}-x86_64-unknown-linux-gnu.tar.gz"
sha256sum zola.tar.gz
```

Paste the resulting digest into `ZOLA_SHA256` and the new version into `ZOLA_VERSION` in both workflows. Verify the upgrade still builds the site locally (`zola build`) before opening the pull request — the `pages-build-check` workflow will also exercise the new pin on CI.
