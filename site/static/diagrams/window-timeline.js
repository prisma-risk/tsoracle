export default function init(figure) {
    const dataScript = figure.querySelector('script[type="application/json"]');
    if (!dataScript) return;

    let data;
    try {
        data = JSON.parse(dataScript.textContent);
    } catch (err) {
        return;
    }

    const svg = d3.select(figure).select('svg');
    svg.selectAll('*').remove();

    const width = 800;
    const height = 220;
    svg.attr('viewBox', `0 0 ${width} ${height}`);

    const margin = 60;
    const totalIds = data.windows[data.windows.length - 1].end + 1;
    const xForId = (id) => margin + (id / totalIds) * (width - 2 * margin);

    const windowY = 70;
    const windowHeight = 50;

    // Window rectangles
    data.windows.forEach((w, i) => {
        const x1 = xForId(w.start);
        const x2 = xForId(w.end + 1);
        const g = svg.append('g').attr('data-window-index', i);

        g.append('rect')
            .attr('class', 'diagram-window')
            .attr('x', x1 + 1)
            .attr('y', windowY)
            .attr('width', x2 - x1 - 2)
            .attr('height', windowHeight)
            .attr('rx', 2);

        g.append('text')
            .attr('class', 'diagram-window-label')
            .attr('x', (x1 + x2) / 2)
            .attr('y', windowY + 22)
            .style('font-weight', '700')
            .text(w.label);

        g.append('text')
            .attr('class', 'diagram-window-label')
            .attr('x', (x1 + x2) / 2)
            .attr('y', windowY + 40)
            .text(`[${w.start}–${w.end}]`);
    });

    // HWM marker: vertical line + label, all translated by transform
    const hwmMarkerY = windowY + windowHeight + 8;
    const hwmMarker = svg.append('g').attr('class', 'diagram-hwm-marker');
    hwmMarker.append('line')
        .attr('x1', 0).attr('x2', 0)
        .attr('y1', windowY - 6).attr('y2', hwmMarkerY + 10)
        .attr('stroke', getComputedStyle(figure).getPropertyValue('--accent').trim() || '#FFB000')
        .attr('stroke-width', 2);
    const hwmLabel = hwmMarker.append('text')
        .attr('class', 'diagram-window-label')
        .attr('x', 0)
        .attr('y', hwmMarkerY + 24)
        .attr('text-anchor', 'middle')
        .style('font-weight', '700');

    // Frame description text
    const description = svg.append('text')
        .attr('class', 'diagram-message-label')
        .attr('x', width / 2)
        .attr('y', height - 36)
        .attr('text-anchor', 'middle');

    // Hint
    const hint = svg.append('text')
        .attr('class', 'diagram-message-label')
        .attr('x', width / 2)
        .attr('y', height - 14)
        .attr('text-anchor', 'middle')
        .style('opacity', 0.6)
        .text('click to advance');

    let frameIdx = 0;

    function renderFrame(idx, animate) {
        const frame = data.frames[idx];

        svg.selectAll('g[data-window-index] rect')
            .classed('diagram-window--active', function (_, i) {
                return i === frame.active;
            });

        const newX = xForId(frame.hwm);
        const tx = `translate(${newX}, 0)`;
        if (animate) {
            hwmMarker.transition().duration(500).attr('transform', tx);
        } else {
            hwmMarker.attr('transform', tx);
        }
        hwmLabel.text(`hwm = ${frame.hwm}`);

        description.text(frame.description);
    }

    renderFrame(0, false);

    figure.style.cursor = 'pointer';
    figure.setAttribute('role', 'button');
    figure.setAttribute('tabindex', '0');

    function advance() {
        frameIdx = (frameIdx + 1) % data.frames.length;
        renderFrame(frameIdx, true);
        hint.style('opacity', 0.3);
    }

    figure.addEventListener('click', advance);
    figure.addEventListener('keydown', function (e) {
        if (e.key === 'Enter' || e.key === ' ') {
            e.preventDefault();
            advance();
        }
    });
}
