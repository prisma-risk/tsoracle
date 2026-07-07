# og-image

Generates social-card PNGs for tsoracle.rs.

Reads every `.md` file under `site/content/posts/`, takes the `title` and `date` from the TOML front-matter, and rasterizes a 1200×630 PNG into `site/static/og/<slug>.png`. Also writes a site-wide fallback to `site/static/og-default.png`.

## Running locally

Run from the repo root:

```bash
cargo run --release --manifest-path scripts/og-image/Cargo.toml
```

Generated PNGs land under `site/static/og/` and at `site/static/og-default.png`. Both are gitignored; the script is the source of truth and the CI workflow regenerates them on every deploy.

If you forget to run this before `zola serve`, social-card meta tags will point to 404s during local preview but the rest of the site renders fine.

## Updating the card

The card layout, colors, and embedded bitmap font usage live in `src/main.rs`. Edit the drawing constants or text calls there and re-run the binary; every post's PNG is regenerated. The script is not part of the main Rust workspace (it is in `[workspace] exclude` in the root `Cargo.toml`) so it does not affect `cargo build --workspace`.
