//
//  ░▀█▀░█▀▀░█▀█░█▀▄░█▀█░█▀▀░█░░░█▀▀
//  ░░█░░▀▀█░█░█░█▀▄░█▀█░█░░░█░░░█▀▀
//  ░░▀░░▀▀▀░▀▀▀░▀░▀░▀░▀░▀▀▀░▀▀▀░▀▀▀
//
//  tsoracle — Distributed Timestamp Oracle
//  https://www.tsoracle.rs
//
//  Copyright (c) 2026 Prisma Risk
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

//! One-shot PNG rasterizer for site/static/favicon.svg.
//!
//! Produces site/static/favicon-32.png and site/static/apple-touch-icon.png.
//! Run on demand whenever favicon.svg changes; outputs are committed.

use anyhow::{anyhow, Context, Result};
use resvg::tiny_skia;
use std::fs;
use std::path::PathBuf;

fn main() -> Result<()> {
    let repo_root = find_repo_root()?;
    let svg_path = repo_root.join("site/static/favicon.svg");
    let svg =
        fs::read_to_string(&svg_path).with_context(|| format!("reading {}", svg_path.display()))?;

    let options = usvg::Options::default();
    let tree = usvg::Tree::from_str(&svg, &options).map_err(|e| anyhow!("parsing SVG: {e}"))?;

    let view = tree.size();
    let source_w = view.width();

    for (size, name) in [(32u32, "favicon-32.png"), (180u32, "apple-touch-icon.png")] {
        let mut pixmap =
            tiny_skia::Pixmap::new(size, size).ok_or_else(|| anyhow!("allocating pixmap"))?;
        let scale = size as f32 / source_w;
        let transform = tiny_skia::Transform::from_scale(scale, scale);
        resvg::render(&tree, transform, &mut pixmap.as_mut());

        let out = repo_root.join("site/static").join(name);
        pixmap.save_png(&out)?;
        eprintln!("wrote {} ({}x{})", out.display(), size, size);
    }

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
