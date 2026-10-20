(() => {
    const systemTheme = window.matchMedia("(prefers-color-scheme: dark)");
    const pendingPlayback = new WeakMap();

    function updateVideos() {
        const scheme = document.body.getAttribute("data-md-color-scheme");
        const dark = scheme === "slate" || (!scheme && systemTheme.matches);

        document.querySelectorAll("video[data-theme-light][data-theme-dark]").forEach((video) => {
            const source = dark ? video.dataset.themeDark : video.dataset.themeLight;
            if (video.getAttribute("src") === source) return;

            const playback = pendingPlayback.get(video) ?? {
                time: video.currentTime,
                playing: !video.paused && !video.ended,
            };
            pendingPlayback.set(video, playback);

            video.onloadedmetadata = () => {
                video.currentTime = Math.min(playback.time, video.duration);
                pendingPlayback.delete(video);
                if (playback.playing) video.play().catch(() => {});
            };
            video.src = source;
            video.load();
        });
    }

    new MutationObserver(updateVideos).observe(document.body, {
        attributes: true,
        attributeFilter: ["data-md-color-scheme"],
    });
    systemTheme.addEventListener("change", updateVideos);
    document$.subscribe(updateVideos);
})();
