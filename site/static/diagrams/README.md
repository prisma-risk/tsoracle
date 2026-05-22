# Diagram modules

Per-diagram-type JS modules loaded lazily by `loader.js` when a `[data-d3-init]` element scrolls into view.

## Architecture

A Tera shortcode (`templates/shortcodes/<name>.html`) renders this HTML structure:

```html
<figure class="diagram" data-d3-init="sequence-flow" data-diagram-id="diagram-1">
    <svg viewBox="0 0 800 400" role="img" aria-label="..." aria-describedby="diagram-1-caption">
        <!-- SSR static fallback rendered by the shortcode -->
    </svg>
    <script type="application/json" id="diagram-1-data">{"actors": [...], "messages": [...]}</script>
    <figcaption id="diagram-1-caption">Optional caption</figcaption>
</figure>
```

When the figure scrolls into view, `loader.js`:

1. Reads `data-d3-init` to determine which module to load.
2. Loads `/vendor/d3.min.js` (only on first diagram per page; cached after).
3. Dynamic-imports `/diagrams/<name>.js`.
4. Invokes the module's default export with the figure element.

The module reads its data from the embedded `<script type="application/json">`, parses it, and uses D3 to animate / interact with the SVG.

## Adding a new diagram type

1. Create `templates/shortcodes/<name>.html` (Tera) that renders the SSR fallback SVG + data JSON.
2. Create `<name>.js` in this directory exporting a default `init(figure)` function.
3. Use `<name>` in a post via `{{ <name>(...) }}` shortcode invocation.
4. Add CSS in `sass/main.scss` if the diagram needs visual specifics beyond the base `.diagram` rules.

## Available modules

- `sequence-flow.js` — actors (columns) + messages (arrows), animate on enter.
- `window-timeline.js` — high-water mark advancing through window flips.
- `cluster_state.js` — small raft cluster with state colours and message arrows. Foundation for future interactive raft-election demo.

## Browser support

The loader uses `IntersectionObserver` (any browser since 2017). Falls back to eager load on browsers without it. Diagram modules use ES module dynamic `import()` (any browser since 2018).
