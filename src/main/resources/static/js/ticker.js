/**
 * Shared stock ticker — SSE-based with REST polling fallback.
 * - Sidebar index cards: Nifty 50 and Bank Nifty (always shown)
 * - Scrolling ticker: only active position symbols
 * Included from all page templates via <script src="/js/ticker.js?v=2"></script>
 */
(function() {
    var tickerEventSource = null;
    var tickerFallbackInterval = null;
    var sseRetryTimeout = null;

    // Index symbols to show in sidebar (not in scrolling ticker)
    // All symbols shown in ticker (indices + key stocks + positions)

    function initTicker() {
        if (typeof EventSource !== 'undefined') {
            connectSSE();
        } else {
            startPolling();
        }
    }

    function connectSSE() {
        if (tickerEventSource) { tickerEventSource.close(); tickerEventSource = null; }

        tickerEventSource = new EventSource('/api/market-ticker/stream');
        window.__tickerSSE = tickerEventSource;

        tickerEventSource.addEventListener('ticker', function(event) {
            try {
                var data = JSON.parse(event.data);
                if (data && data.length) {
                    renderTicker(data);
                    stopPolling();
                }
            } catch (e) {}
        });

        tickerEventSource.onerror = function() {
            tickerEventSource.close();
            tickerEventSource = null;
            window.__tickerSSE = null;
            startPolling();
            if (sseRetryTimeout) clearTimeout(sseRetryTimeout);
            sseRetryTimeout = setTimeout(connectSSE, 30000);
        };
    }

    function startPolling() {
        if (tickerFallbackInterval) return;
        loadTickerREST();
        tickerFallbackInterval = setInterval(loadTickerREST, 60000);
    }

    function stopPolling() {
        if (tickerFallbackInterval) { clearInterval(tickerFallbackInterval); tickerFallbackInterval = null; }
    }

    function loadTickerREST() {
        fetch('/api/market-ticker')
            .then(function(r) { return r.json(); })
            .then(function(data) { if (data && data.length) renderTicker(data); })
            .catch(function() {});
    }

    /** Render scrolling ticker — all symbols */
    function renderTicker(data) {
        var track = document.getElementById('tickerTrack');
        if (!track) return;
        if (!data || !data.length) { track.innerHTML = ''; return; }

        var html = '';
        for (var i = 0; i < data.length; i++) {
            var t = data[i];
            var dir = t.ch >= 0 ? 'up' : 'down';
            var arrow = t.ch >= 0 ? '\u25B2' : '\u25BC';
            var pos = t.position ? ' has-position' : '';
            html += '<div class="ticker-item' + pos + '">' +
                '<span class="ticker-symbol">' + t.symbol + '</span>' +
                '<span class="ticker-price">' + t.lp.toLocaleString('en-IN', {minimumFractionDigits:2}) + '</span>' +
                '<span class="ticker-change ' + dir + '">' + arrow + ' ' + Math.abs(t.ch).toFixed(2) + ' (' + Math.abs(t.chp).toFixed(2) + '%)</span>' +
                '</div>';
        }
        track.innerHTML = html + html;
    }

    // Clean up SSE on page unload
    window.addEventListener('beforeunload', function() {
        if (tickerEventSource) { tickerEventSource.close(); tickerEventSource = null; }
        if (sseRetryTimeout) clearTimeout(sseRetryTimeout);
        stopPolling();
    });

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', initTicker);
    } else {
        initTicker();
    }
})();
