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

//! Generates social-card PNGs for tsoracle.rs.
//!
//! Reads every Markdown post under `site/content/posts/`, parses the TOML
//! front-matter for `title` + `date`, and rasterizes a 1200x630 PNG to
//! `site/static/og/<slug>.png` (per-post) and `site/static/og-default.png`
//! (site-wide fallback). Outputs are gitignored and regenerated
//! unconditionally on every run.
//!
//! Text is drawn with an embedded bitmap font, so the generator does not
//! depend on host fonts or a font parser.

use anyhow::{anyhow, Context, Result};
use embedded_graphics::{
    draw_target::DrawTarget,
    geometry::{OriginDimensions, Point, Size},
    mono_font::{
        ascii::{FONT_9X18, FONT_9X18_BOLD},
        MonoFont, MonoTextStyle,
    },
    pixelcolor::{Rgb888, RgbColor},
    prelude::Drawable,
    text::{Baseline, Text},
    Pixel,
};
use serde::Deserialize;
use std::convert::Infallible;
use std::fs;
use std::path::{Path, PathBuf};
use tiny_skia::{Color, Paint, Pixmap, PremultipliedColorU8, Rect, Transform};
use walkdir::WalkDir;

const POSTS_DIR: &str = "site/content/posts";
const OUT_PER_POST: &str = "site/static/og";
const OUT_DEFAULT: &str = "site/static/og-default.png";
const DEFAULT_TITLE: &str = "Strictly monotonic timestamps in Rust";
const IMAGE_WIDTH: u32 = 1200;
const IMAGE_HEIGHT: u32 = 630;
const BACKGROUND: [u8; 3] = [0x0E, 0x0E, 0x10];
const ACCENT: [u8; 3] = [0xFF, 0xB0, 0x00];
const TITLE_COLOR: [u8; 3] = [0xE8, 0xE6, 0xE3];
const MUTED: [u8; 3] = [0x8A, 0x8A, 0x86];

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

    eprintln!(
        "og-image: {} post images + 1 default = {} PNGs",
        generated,
        generated + 1
    );
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
        return title.to_string();
    }
    let mut truncated: String = title.chars().take(MAX_CHARS - 1).collect();
    truncated.push('…');
    truncated
}

fn render_to_png(title: &str, date: &str, out_path: &Path) -> Result<()> {
    let dateline = if date.is_empty() {
        "tsoracle.rs".to_string()
    } else {
        format!("{} · tsoracle.rs", date)
    };

    let mut pixmap =
        Pixmap::new(IMAGE_WIDTH, IMAGE_HEIGHT).ok_or_else(|| anyhow!("allocating pixmap"))?;
    pixmap.fill(color(BACKGROUND));

    fill_rect(&mut pixmap, 60.0, 60.0, 6.0, 510.0, ACCENT);
    draw_text(&mut pixmap, "tsoracle", &FONT_9X18_BOLD, 2, 100, 86, ACCENT);
    draw_text(
        &mut pixmap,
        title,
        &FONT_9X18_BOLD,
        2,
        100,
        284,
        TITLE_COLOR,
    );
    draw_text(&mut pixmap, &dateline, &FONT_9X18, 1, 100, 548, MUTED);

    pixmap.save_png(out_path)?;
    Ok(())
}

fn fill_rect(pixmap: &mut Pixmap, x: f32, y: f32, width: f32, height: f32, rgb: [u8; 3]) {
    let mut paint = Paint::default();
    paint.set_color(color(rgb));
    let rect = Rect::from_xywh(x, y, width, height).expect("static rectangle dimensions are valid");
    pixmap.fill_rect(rect, &paint, Transform::identity(), None);
}

fn color(rgb: [u8; 3]) -> Color {
    Color::from_rgba8(rgb[0], rgb[1], rgb[2], 255)
}

fn draw_text(
    pixmap: &mut Pixmap,
    text: &str,
    font: &'static MonoFont<'static>,
    scale: u32,
    x: i32,
    y: i32,
    rgb: [u8; 3],
) {
    let style = MonoTextStyle::new(font, rgb888(rgb));
    let mut target = ScaledPixmapTarget { pixmap, scale };
    let logical_x = x / scale as i32;
    let logical_y = y / scale as i32;
    Text::with_baseline(text, Point::new(logical_x, logical_y), style, Baseline::Top)
        .draw(&mut target)
        .expect("drawing into pixmap cannot fail");
}

fn rgb888(rgb: [u8; 3]) -> Rgb888 {
    Rgb888::new(rgb[0], rgb[1], rgb[2])
}

struct ScaledPixmapTarget<'a> {
    pixmap: &'a mut Pixmap,
    scale: u32,
}

impl DrawTarget for ScaledPixmapTarget<'_> {
    type Color = Rgb888;
    type Error = Infallible;

    fn draw_iter<I>(&mut self, pixels: I) -> std::result::Result<(), Self::Error>
    where
        I: IntoIterator<Item = Pixel<Self::Color>>,
    {
        let width = self.pixmap.width() as i32;
        let height = self.pixmap.height() as i32;
        let stride = self.pixmap.width() as usize;
        let scale = self.scale as i32;
        let output = self.pixmap.pixels_mut();

        for Pixel(point, color) in pixels {
            let color = PremultipliedColorU8::from_rgba(color.r(), color.g(), color.b(), 255)
                .expect("opaque color is valid");
            let start_x = point.x * scale;
            let start_y = point.y * scale;

            for dy in 0..scale {
                let y = start_y + dy;
                if !(0..height).contains(&y) {
                    continue;
                }

                for dx in 0..scale {
                    let x = start_x + dx;
                    if !(0..width).contains(&x) {
                        continue;
                    }

                    output[y as usize * stride + x as usize] = color;
                }
            }
        }

        Ok(())
    }
}

impl OriginDimensions for ScaledPixmapTarget<'_> {
    fn size(&self) -> Size {
        Size::new(
            self.pixmap.width() / self.scale,
            self.pixmap.height() / self.scale,
        )
    }
}
