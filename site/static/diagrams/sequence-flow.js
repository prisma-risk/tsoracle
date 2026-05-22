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
    const actorCount = data.actors.length;
    const columnWidth = width / actorCount;
    const headerY = 30;
    const messageY0 = 70;
    const messageSpacing = 50;
    const height = messageY0 + data.messages.length * messageSpacing + 20;

    svg.attr('viewBox', `0 0 ${width} ${height}`);

    const accent = getComputedStyle(figure).getPropertyValue('--accent').trim() || '#FFB000';

    // Arrowhead marker (unique per figure)
    const markerId = 'arrow-' + Math.random().toString(36).slice(2, 8);
    const defs = svg.append('defs');
    defs.append('marker')
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

    // Actor columns + lifelines
    const actorX = {};
    data.actors.forEach((actor, i) => {
        const x = columnWidth / 2 + i * columnWidth;
        actorX[actor] = x;

        svg.append('text')
            .attr('class', 'diagram-actor')
            .attr('x', x)
            .attr('y', headerY)
            .attr('text-anchor', 'middle')
            .text(actor);

        svg.append('line')
            .attr('class', 'diagram-lifeline')
            .attr('x1', x)
            .attr('y1', headerY + 12)
            .attr('x2', x)
            .attr('y2', height - 10);
    });

    // Messages
    data.messages.forEach((msg, i) => {
        const y = messageY0 + i * messageSpacing;
        const x1 = actorX[msg.from];
        const x2 = actorX[msg.to];
        const isReply = msg.kind === 'reply';
        const isSelf = msg.kind === 'self' || x1 === x2;
        const delay = 150 + i * 180;

        if (isSelf) {
            const path = `M ${x1 + 2} ${y} q 36 0 36 18 q 0 18 -36 18`;
            const node = svg.append('path')
                .attr('class', 'diagram-message-line')
                .attr('d', path)
                .attr('marker-end', `url(#${markerId})`)
                .style('opacity', 0);

            node.transition().delay(delay).duration(300).style('opacity', 1);

            svg.append('text')
                .attr('class', 'diagram-message-label')
                .attr('x', x1 + 42)
                .attr('y', y + 8)
                .style('opacity', 0)
                .transition()
                .delay(delay + 100)
                .duration(300)
                .style('opacity', 1)
                .selection()
                .text(msg.label);
        } else {
            const node = svg.append('line')
                .attr('class', 'diagram-message-line')
                .attr('x1', x1)
                .attr('y1', y)
                .attr('x2', x1)
                .attr('y2', y)
                .attr('marker-end', `url(#${markerId})`);

            if (isReply) node.attr('stroke-dasharray', '4 4');

            node.transition()
                .delay(delay)
                .duration(400)
                .attr('x2', x2);

            svg.append('text')
                .attr('class', 'diagram-message-label')
                .attr('x', (x1 + x2) / 2)
                .attr('y', y - 6)
                .attr('text-anchor', 'middle')
                .style('opacity', 0)
                .transition()
                .delay(delay + 200)
                .duration(300)
                .style('opacity', 1)
                .selection()
                .text(msg.label);
        }
    });
}
