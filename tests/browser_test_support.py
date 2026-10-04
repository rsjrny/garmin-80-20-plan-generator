"""Wait for browser rendering after an asserted application state change."""


def wait_for_layout(page):
    page.evaluate("""async () => {
        await document.fonts.ready;
        const animations = document.getAnimations().filter(animation =>
            animation.playState === 'running' &&
            Number.isFinite(animation.effect?.getComputedTiming().endTime));
        await Promise.all(animations.map(animation => animation.finished.catch(() => {})));
        await new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve)));
    }""")
    page.wait_for_function("""() => [...document.querySelectorAll('.js-plotly-plot')].every(plot => {
        if (!plot.getBoundingClientRect().width || !plot._fullLayout) return true;
        return plot._fullLayout.width <= plot.clientWidth + 1;
    })""")

    # Responsive drawers and server-rendered content may update after resize.
    # Wait for the same bounded layout guarantee asserted by the scenarios.
    page.wait_for_function("document.documentElement.scrollWidth <= window.innerWidth")
