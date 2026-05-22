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
    const height = 540;
    svg.attr('viewBox', `0 0 ${width} ${height}`);

    const centerX = width / 2;
    const centerY = 220;
    const nodeRadius = 36;
    const layoutRadius = 140;

    const styles = getComputedStyle(figure);
    const accent = styles.getPropertyValue('--accent').trim() || '#FFB000';
    const codeBg = styles.getPropertyValue('--code-bg').trim() || '#17171A';
    const fg = styles.getPropertyValue('--fg').trim() || '#E8E6E3';
    const fgDim = styles.getPropertyValue('--fg-dim').trim() || '#8A8A86';
    const ruleColor = styles.getPropertyValue('--rule').trim() || '#26262A';

    // Node positions
    const nodePos = {};
    data.nodes.forEach(function (name, i) {
        const angle = (2 * Math.PI * i) / data.nodes.length - Math.PI / 2;
        nodePos[name] = {
            x: centerX + layoutRadius * Math.cos(angle),
            y: centerY + layoutRadius * Math.sin(angle),
        };
    });

    // Layer order: timeout arcs (below nodes), nodes, messages (above)
    const arcLayer = svg.append('g').attr('class', 'arc-layer');
    const nodeLayer = svg.append('g').attr('class', 'node-layer');
    const messageLayer = svg.append('g').attr('class', 'message-layer');

    // Pre-create node groups; states updated each frame
    const nodeGroups = {};
    data.nodes.forEach(function (name) {
        const pos = nodePos[name];
        const g = nodeLayer.append('g').attr('transform', `translate(${pos.x}, ${pos.y})`);
        const circle = g.append('circle')
            .attr('r', nodeRadius)
            .attr('fill', codeBg)
            .attr('stroke', accent)
            .attr('stroke-width', 2)
            .style('transition', 'fill 250ms ease, stroke 250ms ease, opacity 250ms ease');
        const label = g.append('text')
            .attr('text-anchor', 'middle')
            .attr('dominant-baseline', 'middle')
            .attr('fill', fg)
            .attr('font-family', 'JetBrainsMono, ui-monospace, monospace')
            .attr('font-size', 16)
            .attr('font-weight', 700)
            .style('transition', 'fill 250ms ease')
            .text(name);
        // Per-node value badge below the circle. Empty by default; set by node_value events.
        const valueBadge = g.append('text')
            .attr('text-anchor', 'middle')
            .attr('y', nodeRadius + 22)
            .attr('fill', fgDim)
            .attr('font-family', 'JetBrainsMono, ui-monospace, monospace')
            .attr('font-size', 13)
            .style('transition', 'fill 250ms ease');
        nodeGroups[name] = { circle: circle, label: label, valueBadge: valueBadge };
    });

    // Annotation text below the cluster. Manual line-wrap via tspans, since SVG
    // doesn't wrap by default.
    const annotationText = svg.append('text')
        .attr('text-anchor', 'middle')
        .attr('fill', fg)
        .attr('font-family', 'JetBrainsMono, ui-monospace, monospace')
        .attr('font-size', 16);

    const annotationMaxWidth = 680;
    const annotationCharsPerLine = 68;

    function setAnnotationText(text) {
        annotationText.selectAll('tspan').remove();
        if (!text) return;
        const words = text.split(/\s+/);
        const lines = [];
        let currentLine = '';
        for (const word of words) {
            const testLine = currentLine ? currentLine + ' ' + word : word;
            if (testLine.length > annotationCharsPerLine && currentLine) {
                lines.push(currentLine);
                currentLine = word;
            } else {
                currentLine = testLine;
            }
        }
        if (currentLine) lines.push(currentLine);

        // Position the block so there's breathing room between the last line and
        // the progress bar (bar at y = height - 50).
        const lineHeight = 26;
        const blockHeight = lines.length * lineHeight;
        const startY = height - 90 - blockHeight + lineHeight;
        lines.forEach(function (line, i) {
            annotationText.append('tspan')
                .attr('x', width / 2)
                .attr('y', startY + i * lineHeight)
                .text(line);
        });
    }

    // Term indicator (top-right corner). Just the current term number.
    const termGroup = svg.append('g').attr('class', 'term-indicator');
    const termLabel = termGroup.append('text')
        .attr('x', width - 30).attr('y', 38)
        .attr('text-anchor', 'end')
        .attr('fill', fgDim)
        .attr('font-family', 'JetBrainsMono, ui-monospace, monospace')
        .attr('font-size', 14)
        .attr('font-weight', 700);

    // Cluster state tracking — declared up-front because applyState references them
    // before the simulation loop runs, when initializing default node states.
    let currentTerm = '—';
    let currentVotes = [];
    const nodeStateMap = {};

    // Time bar at the bottom: shows simulation progress
    const barX = 80;
    const barY = height - 50;
    const barWidth = width - 160;
    svg.append('rect')
        .attr('x', barX).attr('y', barY)
        .attr('width', barWidth).attr('height', 4)
        .attr('rx', 2)
        .attr('fill', ruleColor);
    const barFill = svg.append('rect')
        .attr('x', barX).attr('y', barY)
        .attr('width', 0).attr('height', 4)
        .attr('rx', 2)
        .attr('fill', accent);

    // Arc generator for timeout indicators
    const arcGen = d3.arc()
        .innerRadius(nodeRadius + 4)
        .outerRadius(nodeRadius + 8)
        .startAngle(0);

    // State application
    function applyState(name, state) {
        const node = nodeGroups[name];
        if (!node) return;
        nodeStateMap[name] = state;
        const isLeader = state === 'leader';
        const isCandidate = state === 'candidate';
        const isDown = state === 'down';

        node.circle
            .attr('fill', isLeader ? accent : codeBg)
            .attr('stroke', isDown ? fgDim : accent)
            .attr('stroke-dasharray', isCandidate ? '4 4' : null)
            .style('opacity', isDown ? 0.35 : 1);

        node.label
            .attr('fill', isLeader ? codeBg : fg);

        renderTermTable();
    }

    // Initialize all nodes to "follower" by default
    data.nodes.forEach(function (name) { applyState(name, 'follower'); });

    // Simulation state
    const totalDuration = data.duration || 6000;
    const events = (data.events || []).slice().sort(function (a, b) { return a.t - b.t; });
    const annotations = (data.annotations || []).slice().sort(function (a, b) { return a.t - b.t; });

    let simTime = 0;          // current simulation time in ms
    let lastFrameTime = 0;    // for delta calculation
    let playing = false;
    let rafId = null;
    let stopAtTime = null;    // when set, play loop pauses at this time (for step mode)

    // Active visuals (in-flight)
    const activeTimeouts = [];  // {node, startTime, duration, arcEl}
    const activeMessages = [];  // {from, to, startTime, duration, label, kind, particleEl, labelEl}
    let nextEventIdx = 0;

    function clearActiveVisuals() {
        activeTimeouts.forEach(function (to) { to.arcEl.remove(); });
        activeTimeouts.length = 0;
        activeMessages.forEach(function (msg) { msg.particleEl.remove(); msg.labelEl.remove(); });
        activeMessages.length = 0;
    }

    function renderTermTable() {
        termLabel.text('term: ' + currentTerm);
    }

    function reset() {
        simTime = 0;
        nextEventIdx = 0;
        clearActiveVisuals();
        data.nodes.forEach(function (name) {
            applyState(name, 'follower');
            if (nodeGroups[name].valueBadge) {
                nodeGroups[name].valueBadge.text('').style('fill', fgDim).style('font-weight', 400);
            }
        });
        currentTerm = '—';
        currentVotes = [];
        renderTermTable();
        // Apply any events at t=0 so the initial state matches the data.
        processEventsUpTo(0);
        setAnnotationText(currentAnnotation(0));
        barFill.attr('width', 0);
    }

    function processEventsUpTo(t) {
        while (nextEventIdx < events.length && events[nextEventIdx].t <= t) {
            const ev = events[nextEventIdx];
            nextEventIdx++;
            applyEvent(ev);
        }
    }

    function applyEvent(ev) {
        if (ev.type === 'state') {
            applyState(ev.node, ev.state);
        } else if (ev.type === 'timeout') {
            const pos = nodePos[ev.node];
            if (!pos) return;
            const arcEl = arcLayer.append('path')
                .attr('transform', `translate(${pos.x}, ${pos.y})`)
                .attr('fill', accent);
            activeTimeouts.push({
                node: ev.node,
                startTime: ev.t,
                duration: ev.duration || 1000,
                arcEl: arcEl,
            });
        } else if (ev.type === 'term') {
            currentTerm = ev.label || String(ev.term || '?');
            currentVotes = [];
            renderTermTable();
        } else if (ev.type === 'vote') {
            currentVotes.push({ voter: ev.voter, candidate: ev.candidate });
            renderTermTable();
        } else if (ev.type === 'node_value') {
            const node = nodeGroups[ev.node];
            if (node) {
                node.valueBadge.text(ev.value || '');
                node.valueBadge
                    .style('fill', accent)
                    .style('font-weight', 700)
                    .transition()
                    .duration(800)
                    .style('fill', fgDim)
                    .style('font-weight', 400);
            }
        } else if (ev.type === 'message') {
            const fromPos = nodePos[ev.from];
            const toPos = nodePos[ev.to];
            if (!fromPos || !toPos) return;
            const isReply = ev.kind === 'reply';
            const duration = ev.duration || 700;
            const particleEl = messageLayer.append('circle')
                .attr('r', 6)
                .attr('fill', isReply ? codeBg : accent)
                .attr('stroke', accent)
                .attr('stroke-width', 2)
                .attr('cx', fromPos.x)
                .attr('cy', fromPos.y);
            const midX = (fromPos.x + toPos.x) / 2;
            const midY = (fromPos.y + toPos.y) / 2 - 12;
            const labelEl = messageLayer.append('text')
                .attr('x', midX).attr('y', midY)
                .attr('text-anchor', 'middle')
                .attr('fill', fgDim)
                .attr('font-family', 'JetBrainsMono, ui-monospace, monospace')
                .attr('font-size', 11)
                .text(ev.label || '');
            activeMessages.push({
                from: ev.from,
                to: ev.to,
                startTime: ev.t,
                duration: duration,
                particleEl: particleEl,
                labelEl: labelEl,
            });
        }
    }

    function currentAnnotation(t) {
        let text = '';
        for (let i = 0; i < annotations.length; i++) {
            if (annotations[i].t <= t) text = annotations[i].text;
            else break;
        }
        return text;
    }

    function updateVisuals(t) {
        // Timeout arcs
        for (let i = activeTimeouts.length - 1; i >= 0; i--) {
            const to = activeTimeouts[i];
            const progress = Math.min(1, (t - to.startTime) / to.duration);
            const endAngle = progress * 2 * Math.PI;
            to.arcEl.attr('d', arcGen({ startAngle: 0, endAngle: endAngle }));
            if (progress >= 1) {
                to.arcEl.remove();
                activeTimeouts.splice(i, 1);
            }
        }

        // Message particles
        for (let i = activeMessages.length - 1; i >= 0; i--) {
            const msg = activeMessages[i];
            const progress = (t - msg.startTime) / msg.duration;
            if (progress >= 1) {
                msg.particleEl.remove();
                msg.labelEl.remove();
                activeMessages.splice(i, 1);
                continue;
            }
            const fromPos = nodePos[msg.from];
            const toPos = nodePos[msg.to];
            // Start and end slightly outside the node circle so the particle
            // doesn't overlap the node when emerging or arriving.
            const dx = toPos.x - fromPos.x;
            const dy = toPos.y - fromPos.y;
            const len = Math.sqrt(dx * dx + dy * dy);
            const ux = dx / len;
            const uy = dy / len;
            const x0 = fromPos.x + ux * nodeRadius;
            const y0 = fromPos.y + uy * nodeRadius;
            const x1 = toPos.x - ux * (nodeRadius + 2);
            const y1 = toPos.y - uy * (nodeRadius + 2);
            const cx = x0 + (x1 - x0) * progress;
            const cy = y0 + (y1 - y0) * progress;
            msg.particleEl.attr('cx', cx).attr('cy', cy);
            // Fade in over first 15%, fade out over last 15%
            const opacity = progress < 0.15 ? progress / 0.15
                : progress > 0.85 ? (1 - progress) / 0.15
                : 1;
            msg.particleEl.style('opacity', opacity);
            msg.labelEl
                .attr('x', cx)
                .attr('y', cy - 14)
                .style('opacity', opacity);
        }

        setAnnotationText(currentAnnotation(t));
        barFill.attr('width', barWidth * Math.min(1, t / totalDuration));
    }

    function showPlayBtn() {
        if (playBtn) playBtn.removeAttribute('hidden');
        if (pauseBtn) pauseBtn.setAttribute('hidden', '');
    }

    function showPauseBtn() {
        if (playBtn) playBtn.setAttribute('hidden', '');
        if (pauseBtn) pauseBtn.removeAttribute('hidden');
    }

    function tick(timestamp) {
        if (!playing) return;
        const delta = lastFrameTime === 0 ? 0 : timestamp - lastFrameTime;
        lastFrameTime = timestamp;
        simTime += delta;

        // Hit the stop watermark (step mode) — clamp and pause.
        if (stopAtTime !== null && simTime >= stopAtTime) {
            simTime = stopAtTime;
            processEventsUpTo(simTime);
            updateVisuals(simTime);
            playing = false;
            stopAtTime = null;
            showPlayBtn();
            return;
        }

        processEventsUpTo(simTime);
        updateVisuals(simTime);

        if (simTime >= totalDuration) {
            simTime = totalDuration;
            playing = false;
            showPlayBtn();
            return;
        }
        rafId = requestAnimationFrame(tick);
    }

    function play() {
        if (simTime >= totalDuration) reset();
        stopAtTime = null;
        playing = true;
        lastFrameTime = 0;
        showPauseBtn();
        rafId = requestAnimationFrame(tick);
    }

    function pause() {
        playing = false;
        if (rafId) cancelAnimationFrame(rafId);
        showPlayBtn();
    }

    function restart() {
        pause();
        reset();
    }

    function stepForward() {
        // Find the next annotation strictly after current sim time.
        let nextBeatTime = null;
        for (let i = 0; i < annotations.length; i++) {
            if (annotations[i].t > simTime + 1) { // +1ms to skip the current beat
                nextBeatTime = annotations[i].t;
                break;
            }
        }
        // If no more annotations, advance to end.
        if (nextBeatTime === null) nextBeatTime = totalDuration;

        if (simTime >= totalDuration) reset();
        stopAtTime = nextBeatTime;
        playing = true;
        lastFrameTime = 0;
        showPauseBtn();
        rafId = requestAnimationFrame(tick);
    }

    const playBtn = figure.querySelector('[data-action="play"]');
    const pauseBtn = figure.querySelector('[data-action="pause"]');
    const restartBtn = figure.querySelector('[data-action="restart"]');
    const stepBtn = figure.querySelector('[data-action="step"]');

    // Play-through is opt-in via data.allow_play_through. Default: hidden.
    // Pause is still wired because step mode uses the play loop and may want to pause mid-step.
    if (!data.allow_play_through) {
        if (playBtn) playBtn.remove();
        if (pauseBtn) pauseBtn.remove();
    }

    if (playBtn) playBtn.addEventListener('click', play);
    if (pauseBtn) pauseBtn.addEventListener('click', pause);
    if (restartBtn) restartBtn.addEventListener('click', restart);
    if (stepBtn) stepBtn.addEventListener('click', stepForward);

    reset();
}
