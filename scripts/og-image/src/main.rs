//! Generates social-card PNGs for tsoracle.rs.
//!
//! Reads every Markdown post under `site/content/posts/`, parses the TOML
//! front-matter for `title` + `date`, substitutes them into `template.svg`,
//! and rasterizes the result to `site/static/og/<slug>.png` (per-post) and
//! `site/static/og-default.png` (site-wide fallback). Outputs are gitignored
//! and regenerated unconditionally on every run.

use anyhow::{anyhow, Context, Result};
use resvg::tiny_skia;
use serde::Deserialize;
use std::fs;
use std::path::{Path, PathBuf};
use walkdir::WalkDir;

const TEMPLATE: &str = include_str!("../template.svg");
const POSTS_DIR: &str = "site/content/posts";
const OUT_PER_POST: &str = "site/static/og";
const OUT_DEFAULT: &str = "site/static/og-default.png";
const DEFAULT_TITLE: &str = "Strictly monotonic timestamps in Rust";

#[derive(Deserialize)]
struct FrontMatter {
    title: String,
    date: Option<toml::value::Datetime>,
}

fn main() -> Result<()> {
    let repo_root = find_repo_root()?;
    let posts_dir = repo_root.join(POSTS_DIR);
    let out_per_post = repo_root.join(OUT_PER_POST);
    let out_default = repo_root.join(OUT_DEFAULT);

    fs::create_dir_all(&out_per_post)
        .with_context(|| format!("creating {}", out_per_post.display()))?;

    render_to_png(DEFAULT_TITLE, "", &out_default)?;
    eprintln!("wrote {}", out_default.display());

    let mut generated = 0usize;
    for entry in WalkDir::new(&posts_dir).max_depth(1).min_depth(1) {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|extension| extension.to_str()) != Some("md") {
            continue;
        }
        let file_stem = match path.file_stem().and_then(|stem| stem.to_str()) {
            Some(stem) if stem != "_index" => stem,
            _ => continue,
        };
        let front_matter = parse_front_matter(path)
            .with_context(|| format!("parsing front-matter from {}", path.display()))?;
        let date_str = front_matter
            .date
            .map(|datetime| datetime.to_string())
            .unwrap_or_default();
        let title_for_image = truncate_title(&front_matter.title);
        let out_path = out_per_post.join(format!("{}.png", file_stem));
        render_to_png(&title_for_image, &date_str, &out_path)?;
        eprintln!("wrote {}", out_path.display());
        generated += 1;
    }

    eprintln!("og-image: {} post images + 1 default = {} PNGs", generated, generated + 1);
    Ok(())
}

fn find_repo_root() -> Result<PathBuf> {
    let mut current = std::env::current_dir()?;
    loop {
        if current.join("Cargo.toml").exists() && current.join("site").is_dir() {
            return Ok(current);
        }
        if !current.pop() {
            return Err(anyhow!("could not locate repo root from current directory"));
        }
    }
}

fn parse_front_matter(path: &Path) -> Result<FrontMatter> {
    let contents = fs::read_to_string(path)?;
    let trimmed = contents.trim_start();
    let body_after_open = trimmed
        .strip_prefix("+++")
        .ok_or_else(|| anyhow!("missing opening +++ delimiter"))?;
    let close_index = body_after_open
        .find("+++")
        .ok_or_else(|| anyhow!("missing closing +++ delimiter"))?;
    let toml_block = &body_after_open[..close_index];
    let front_matter: FrontMatter = toml::from_str(toml_block)?;
    Ok(front_matter)
}

fn truncate_title(title: &str) -> String {
    const MAX_CHARS: usize = 56;
    let char_count = title.chars().count();
    if char_count <= MAX_CHARS {
        return escape_xml(title);
    }
    let mut truncated: String = title.chars().take(MAX_CHARS - 1).collect();
    truncated.push('…');
    escape_xml(&truncated)
}

fn escape_xml(input: &str) -> String {
    input
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

fn render_to_png(title: &str, date: &str, out_path: &Path) -> Result<()> {
    let dateline = if date.is_empty() {
        "tsoracle.rs".to_string()
    } else {
        format!("{} · tsoracle.rs", date)
    };

    let svg = TEMPLATE
        .replace("__TITLE__", title)
        .replace("__DATELINE__", &escape_xml(&dateline));

    let mut options = usvg::Options::default();
    options.fontdb_mut().load_system_fonts();

    let tree = usvg::Tree::from_str(&svg, &options)
        .map_err(|e| anyhow!("parsing SVG: {e}"))?;
    let mut pixmap = tiny_skia::Pixmap::new(1200, 630)
        .ok_or_else(|| anyhow!("allocating pixmap"))?;
    resvg::render(&tree, tiny_skia::Transform::default(), &mut pixmap.as_mut());
    pixmap.save_png(out_path)?;
    Ok(())
}
