(function () {
    "use strict";

    const figures = document.querySelectorAll('[data-d3-init]');
    if (figures.length === 0) return;

    let d3Loading = null;

    function loadD3() {
        if (window.d3) return Promise.resolve();
        if (d3Loading) return d3Loading;
        d3Loading = new Promise(function (resolve, reject) {
            const script = document.createElement("script");
            script.src = "/vendor/d3.min.js";
            script.onload = resolve;
            script.onerror = reject;
            document.head.appendChild(script);
        });
        return d3Loading;
    }

    async function loadDiagram(figure) {
        const name = figure.dataset.d3Init;
        try {
            await loadD3();
            const module = await import("/diagrams/" + name + ".js");
            if (typeof module.default === "function") {
                module.default(figure);
            }
        } catch (err) {
            console.warn("Failed to load diagram '" + name + "':", err);
        }
    }

    if ("IntersectionObserver" in window) {
        const observer = new IntersectionObserver(
            function (entries) {
                entries.forEach(function (entry) {
                    if (!entry.isIntersecting) return;
                    observer.unobserve(entry.target);
                    loadDiagram(entry.target);
                });
            },
            { rootMargin: "200px 0px", threshold: 0 }
        );
        figures.forEach(function (fig) {
            observer.observe(fig);
        });
    } else {
        figures.forEach(loadDiagram);
    }
})();
