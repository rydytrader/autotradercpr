/**
 * Shared stock ticker — SSE-based with REST polling fallback.
 * - Sidebar index cards: Nifty 50 and Bank Nifty (always shown)
 * - Scrolling ticker: only active position symbols
 * - Trade notifications: browser alerts on new/closed positions (all pages)
 * Included from all page templates via <script src="/js/ticker.js?v=3"></script>
 */
(function() {
    var tickerEventSource = null;
    var tickerFallbackInterval = null;
    var sseRetryTimeout = null;

    // ── TRADE NOTIFICATIONS (runs on every page) ─────────────────────────────
    var knownSymbols = null; // null = not yet initialized

    // Request notification permission on first user click
    function requestNotifPerm() {
        if ('Notification' in window && Notification.permission === 'default') {
            Notification.requestPermission();
        }
        document.removeEventListener('click', requestNotifPerm);
    }
    document.addEventListener('click', requestNotifPerm);

    function showTradeNotif(title, body) {
        if ('Notification' in window && Notification.permission === 'granted') {
            var n = new Notification('TraderEdge CPR - ' + title, {
                body: body,
                icon: '/favicon.ico',
                tag: 'trade-' + Date.now(),
                requireInteraction: true
            });
            setTimeout(function() { n.close(); }, 15000);
        }
    }

    function checkPositionChanges(positions) {
        if (!positions) return;
        // First call: seed with current positions (don't alert on page load)
        if (knownSymbols === null) {
            knownSymbols = new Set();
            positions.forEach(function(p) { knownSymbols.add(p.symbol); });
            return;
        }

        var currentSymbols = new Set();
        positions.forEach(function(p) { currentSymbols.add(p.symbol); });

        // New position — notify
        currentSymbols.forEach(function(sym) {
            if (!knownSymbols.has(sym)) {
                var pos = positions.find(function(p) { return p.symbol === sym; });
                var name = sym.replace('NSE:', '').replace('-EQ', '');
                var side = pos ? pos.side : '';
                var price = pos ? pos.avgPrice : 0;
                var time = new Date().toLocaleTimeString('en-IN', {hour12: false, hour: '2-digit', minute: '2-digit'});
                showTradeNotif(side + ' ' + name, 'Entry @ ' + price.toFixed(2) + ' | ' + time);
            }
        });

        // Position closed — notify with exit details
        knownSymbols.forEach(function(sym) {
            if (!currentSymbols.has(sym)) {
                var name = sym.replace('NSE:', '').replace('-EQ', '');
                fetch('/api/trades').then(function(r) { return r.json(); }).then(function(trades) {
                    var trade = trades.find(function(t) { return t.symbol === sym; });
                    if (trade) {
                        var pnl = trade.netPnl || trade.pnl || 0;
                        var pnlStr = (pnl >= 0 ? '+' : '') + pnl.toFixed(2);
                        var reason = trade.exitReason || 'CLOSED';
                        showTradeNotif(name + ' ' + reason, 'P&L: ' + pnlStr + ' | Exit @ ' + (trade.exitPrice || 0).toFixed(2));
                    } else {
                        showTradeNotif(name + ' CLOSED', 'Position closed');
                    }
                }).catch(function() { showTradeNotif(name + ' CLOSED', 'Position closed'); });
            }
        });

        knownSymbols = currentSymbols;
    }

    // Poll positions for notification checks (fallback when SSE positions event not available)
    var notifPollInterval = setInterval(function() {
        fetch('/api/positions').then(function(r) { return r.json(); }).then(function(data) {
            checkPositionChanges(data.positions || []);
        }).catch(function() {});
    }, 5000);

    // ── TICKER ───────────────────────────────────────────────────────────────

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

        // Listen for positions event (for notifications from any page)
        tickerEventSource.addEventListener('positions', function(event) {
            try {
                var data = JSON.parse(event.data);
                checkPositionChanges(data.positions || []);
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
        if (notifPollInterval) clearInterval(notifPollInterval);
    });

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', initTicker);
    } else {
        initTicker();
    }
})();
