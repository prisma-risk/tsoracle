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
    const height = 400;
    svg.attr('viewBox', `0 0 ${width} ${height}`);

    const centerX = width / 2;
    const centerY = height / 2 - 20;
    const nodeRadius = 38;
    const layoutRadius = Math.min(width, height) / 2 - 90;

    const accent = getComputedStyle(figure).getPropertyValue('--accent').trim() || '#FFB000';

    // Arrowhead marker
    const markerId = 'arrow-' + Math.random().toString(36).slice(2, 8);
    svg.append('defs')
        .append('marker')
        .attr('id', markerId)
        .attr('viewBox', '0 -5 10 10')
        .attr('refX', 8)
        .attr('refY', 0)
        .attr('markerWidth', 6)
        .attr('markerHeight', 6)
        .attr('orient', 'auto')
        .append('path')
        .attr('d', 'M0,-5L10,0L0,5')
        .attr('fill', accent);

    // Node positions on a circle, first node at top
    const nodeCount = data.nodes.length;
    const nodePos = {};
    data.nodes.forEach((name, i) => {
        const angle = (2 * Math.PI * i) / nodeCount - Math.PI / 2;
        nodePos[name] = {
            x: centerX + layoutRadius * Math.cos(angle),
            y: centerY + layoutRadius * Math.sin(angle),
        };
    });

    // Static node geometry (re-rendered per frame for state)
    const nodeGroups = {};
    data.nodes.forEach(function (name) {
        const pos = nodePos[name];
        const g = svg.append('g')
            .attr('data-node', name)
            .attr('transform', `translate(${pos.x}, ${pos.y})`);

        const circle = g.append('circle')
            .attr('class', 'diagram-node')
            .attr('r', nodeRadius);

        g.append('text')
            .attr('class', 'diagram-node-label')
            .text(name);

        nodeGroups[name] = { group: g, circle: circle };
    });

    // Message layer (cleared each frame)
    const messageLayer = svg.append('g').attr('class', 'message-layer');

    // Frame description
    const description = svg.append('text')
        .attr('class', 'diagram-message-label')
        .attr('x', width / 2)
        .attr('y', height - 36)
        .attr('text-anchor', 'middle');

    const hint = svg.append('text')
        .attr('class', 'diagram-message-label')
        .attr('x', width / 2)
        .attr('y', height - 14)
        .attr('text-anchor', 'middle')
        .style('opacity', data.frames.length > 1 ? 0.6 : 0)
        .text('click to advance');

    let frameIdx = 0;

    function renderFrame(idx, animate) {
        const frame = data.frames[idx];

        // Update node states
        data.nodes.forEach(function (name) {
            const state = frame.states[name] || 'follower';
            const node = nodeGroups[name];
            const isLeader = state === 'leader';
            const isCandidate = state === 'candidate';
            const isDown = state === 'down';

            node.circle
                .classed('diagram-node--leader', isLeader)
                .attr('stroke-dasharray', isCandidate ? '4 4' : null)
                .style('opacity', isDown ? 0.3 : 1);

            node.group.select('text')
                .style('fill', isLeader ? 'var(--bg)' : null);
        });

        // Draw messages
        messageLayer.selectAll('*').remove();
        (frame.messages || []).forEach(function (msg, i) {
            const from = nodePos[msg.from];
            const to = nodePos[msg.to];
            if (!from || !to) return;

            // Shorten endpoints so the arrow lands at the circle edge, not the center
            const dx = to.x - from.x;
            const dy = to.y - from.y;
            const dist = Math.sqrt(dx * dx + dy * dy);
            const ux = dx / dist;
            const uy = dy / dist;
            const x1 = from.x + ux * nodeRadius;
            const y1 = from.y + uy * nodeRadius;
            const x2 = to.x - ux * (nodeRadius + 6);
            const y2 = to.y - uy * (nodeRadius + 6);

            const line = messageLayer.append('line')
                .attr('class', 'diagram-message-line')
                .attr('x1', x1).attr('y1', y1)
                .attr('x2', animate ? x1 : x2)
                .attr('y2', animate ? y1 : y2)
                .attr('marker-end', `url(#${markerId})`);

            if (animate) {
                line.transition()
                    .delay(i * 120)
                    .duration(400)
                    .attr('x2', x2)
                    .attr('y2', y2);
            }

            messageLayer.append('text')
                .attr('class', 'diagram-message-label')
                .attr('x', (x1 + x2) / 2)
                .attr('y', (y1 + y2) / 2 - 8)
                .attr('text-anchor', 'middle')
                .style('opacity', animate ? 0 : 1)
                .transition()
                .delay(animate ? i * 120 + 200 : 0)
                .duration(animate ? 300 : 0)
                .style('opacity', 1)
                .selection()
                .text(msg.label);
        });

        description.text(frame.description || '');
    }

    renderFrame(0, true);

    if (data.frames.length > 1) {
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
}
