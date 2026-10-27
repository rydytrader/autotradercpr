/**
 * Shared header strip — replaces the old scrolling ticker.
 *
 * Renders two chips into #tickerTrack (kept for DOM compat with old templates):
 *   NIFTY  24587.50  +12.35 (+0.05%)   |   P&L +₹1,240
 *
 * Data sources (real-time):
 *   NIFTY — SSE /api/market-ticker/stream, `ticker` event carries an array
 *           that includes {symbol:"NSE:NIFTY50-INDEX", lp, ch, chp}; the
 *           WebSocket-backed feed pushes on every tick.
 *   P&L   — /api/portfolio-pnl-today polled every 2 s. Aggregates
 *           liveNetPnlToday across every registered strategy.
 *
 * Trade notifications kept unchanged (browser alerts on new/closed positions).
 */
(function() {
    var tickerEventSource = null;
    var tickerFallbackInterval = null;
    var sseRetryTimeout = null;
    var pnlPollInterval = null;

    // Last-known values so partial updates (either feed alone) still render both chips.
    var lastNifty  = { lp: 0, ch: 0, chp: 0 };
    var lastSensex = { lp: 0, ch: 0, chp: 0 };
    var lastVix    = { lp: 0, ch: 0, chp: 0 };
    var lastDayPnl = 0;
    var lastRisk   = null;   // {maxLoss, usedPct, warnPct, warn} from /api/portfolio-pnl-today

    // ── TRADE NOTIFICATIONS (unchanged) ─────────────────────────────────────
    var knownSymbols = null;

    function requestNotifPerm() {
        if ('Notification' in window && Notification.permission === 'default') {
            Notification.requestPermission();
        }
        document.removeEventListener('click', requestNotifPerm);
    }
    document.addEventListener('click', requestNotifPerm);

    function showTradeNotif(title, body) {
        if ('Notification' in window && Notification.permission === 'granted') {
            var n = new Notification('TraderEdge - ' + title, {
                body: body, icon: '/favicon.ico',
                tag: 'trade-' + Date.now(), requireInteraction: true
            });
            setTimeout(function() { n.close(); }, 15000);
        }
    }

    function checkPositionChanges(positions) {
        if (!positions) return;
        if (knownSymbols === null) {
            knownSymbols = new Set();
            positions.forEach(function(p) { knownSymbols.add(p.symbol); });
            return;
        }
        var currentSymbols = new Set();
        positions.forEach(function(p) { currentSymbols.add(p.symbol); });
        currentSymbols.forEach(function(sym) {
            if (!knownSymbols.has(sym)) {
                var pos = positions.find(function(p) { return p.symbol === sym; });
                var name = sym.replace('NSE:', '').replace('-EQ', '');
                var side = pos ? pos.side : '';
                var price = pos ? pos.avgPrice : 0;
                var time = new Date().toLocaleTimeString('en-IN', {hour12: false, hour: '2-digit', minute: '2-digit'});
                showTradeNotif(side + ' ' + name, 'Entry @ ' + (price || 0).toFixed(2) + ' | ' + time);
            }
        });
        knownSymbols.forEach(function(sym) {
            if (!currentSymbols.has(sym)) {
                var name = sym.replace('NSE:', '').replace('-EQ', '');
                showTradeNotif(name + ' CLOSED', 'Position closed');
            }
        });
        knownSymbols = currentSymbols;
    }

    var notifPollInterval = setInterval(function() {
        fetch('/api/positions').then(function(r) { return r.json(); }).then(function(data) {
            checkPositionChanges(data.positions || []);
        }).catch(function() {});
    }, 5000);

    // ── HEADER STRIP ────────────────────────────────────────────────────────

    /** Prepare the #tickerTrack container — kill the old scrolling animation
     *  and neutralise the ticker-wrap edge mask so the leftmost chip isn't faded. */
    function styleTrackForStrip(track) {
        if (!track) return;
        track.style.animation = 'none';
        track.style.width = '100%';
        track.style.display = 'flex';
        track.style.justifyContent = 'flex-start';
        track.style.alignItems = 'center';
        track.style.gap = '0';
        track.style.whiteSpace = 'nowrap';
        var wrap = track.parentElement;
        if (wrap && wrap.classList && wrap.classList.contains('ticker-wrap')) {
            wrap.style.maskImage = 'none';
            wrap.style.webkitMaskImage = 'none';
        }
    }

    function fmtInr(n) {
        if (!isFinite(n) || n === 0) return '₹0';
        var abs = Math.abs(Math.round(n));
        return (n < 0 ? '−₹' : '+₹') + abs.toLocaleString('en-IN');
    }

    function chip(label, value, valueColor) {
        return '<span style="color:var(--text-muted); font-weight:700; font-size:0.66rem; letter-spacing:0.08em;">' + label + '</span>' +
               '<span style="margin-left:6px; color:' + valueColor + ';">' + value + '</span>';
    }

    // One-time keyframes for the portfolio-risk warning frame on the P&L chip.
    var riskStyleDone = false;
    function ensureRiskStyle() {
        if (riskStyleDone) return;
        riskStyleDone = true;
        var st = document.createElement('style');
        st.textContent = '@keyframes pnlRiskPulse { 0%,100% { box-shadow: 0 0 0 0 rgba(251,191,36,0.0); } 50% { box-shadow: 0 0 0 4px rgba(251,191,36,0.28); } }'
                       + ' .pnl-risk-warn { animation: pnlRiskPulse 1.4s ease-in-out infinite; }';
        document.head.appendChild(st);
    }

    function divider() {
        return '<span style="color:var(--text-muted); margin:0 18px; opacity:0.35; font-weight:400;">|</span>';
    }

    /** Format one index chip's value + change text + color from a {lp,ch,chp} record. */
    function formatIndex(rec) {
        var ltp = Number(rec.lp  || 0);
        var ch  = Number(rec.ch  || 0);
        var chp = Number(rec.chp || 0);
        var ltpText = ltp > 0
            ? ltp.toLocaleString('en-IN', {minimumFractionDigits:2, maximumFractionDigits:2})
            : '—';
        var color = ltp <= 0 ? 'var(--text-primary)'
                  : ch > 0   ? 'var(--accent-green, #34d399)'
                  : ch < 0   ? 'var(--accent-red, #f87171)'
                  : 'var(--text-primary)';
        var chgText = '';
        if (ltp > 0 && !(ch === 0 && chp === 0)) {
            var sign = ch > 0 ? '+' : '−';
            chgText = ' ' + sign + Math.abs(ch).toFixed(2) + ' (' + sign + Math.abs(chp).toFixed(2) + '%)';
        }
        return { text: ltpText + chgText, color: color };
    }

    function renderStrip() {
        var track = document.getElementById('tickerTrack');
        if (!track) return;
        styleTrackForStrip(track);

        var nifty  = formatIndex(lastNifty);
        var sensex = formatIndex(lastSensex);
        // VIX: green when rising, red when falling (operator preference).
        var vixLtp = Number(lastVix.lp || 0);
        var vixText = vixLtp > 0
            ? vixLtp.toLocaleString('en-IN', {minimumFractionDigits:2, maximumFractionDigits:2})
            : '—';
        var vixChp = Number(lastVix.chp || 0);
        if (vixLtp > 0 && vixChp !== 0) {
            var vixSign = vixChp > 0 ? '+' : '−';
            vixText += ' (' + vixSign + Math.abs(vixChp).toFixed(2) + '%)';
        }
        var vixColor = vixChp > 0 ? 'var(--accent-green, #34d399)'
                     : vixChp < 0 ? 'var(--accent-red, #f87171)'
                     : 'var(--text-primary)';

        var pnl = Number(lastDayPnl || 0);
        var pnlColor = pnl > 0 ? 'var(--accent-green, #34d399)'
                     : pnl < 0 ? 'var(--accent-red, #f87171)'
                     : 'var(--text-muted)';

        // Portfolio kill-switch proximity. From warnPct (75%) of the max daily loss the P&L
        // chip gets an amber pulsing frame + "RISK nn%" tag; at/after 100% it turns red.
        var pnlHtml = chip('P&L', fmtInr(pnl), pnlColor);
        var r = lastRisk || {};
        if (r.maxLoss > 0 && r.usedPct >= r.warnPct) {
            ensureRiskStyle();
            var killed = r.usedPct >= 100;
            var col = killed ? 'var(--accent-red, #f87171)' : 'var(--accent-amber, #fbbf24)';
            var bg  = killed ? 'rgba(248,113,113,0.14)'     : 'rgba(251,191,36,0.14)';
            var title = (killed ? 'PORTFOLIO MAX LOSS HIT — ' : 'Portfolio loss warning — ')
                + Math.round(r.usedPct) + '% of the max daily loss used ('
                + fmtInr(pnl) + ' of −₹' + Math.round(r.maxLoss).toLocaleString('en-IN') + ')';
            pnlHtml = '<span class="pnl-risk-warn" title="' + title + '" style="display:inline-flex; align-items:center; gap:8px;'
                + ' padding:2px 10px; border-radius:999px; border:1px solid ' + col + '; background:' + bg + ';">'
                + pnlHtml
                + '<span style="font-size:0.6rem; font-weight:700; letter-spacing:0.08em; color:' + col + ';">'
                + (killed ? 'KILL' : 'RISK ' + Math.round(r.usedPct) + '%') + '</span></span>';
        }

        var leadingRule = '<span style="display:inline-block; width:1px; height:22px; background:var(--border); margin-right:18px; opacity:0.7;"></span>';
        track.innerHTML = leadingRule +
            chip('NIFTY',  nifty.text,  nifty.color)  + divider() +
            chip('SENSEX', sensex.text, sensex.color) + divider() +
            chip('VIX',    vixText,     vixColor)     + divider() +
            pnlHtml;
    }

    // Extract NIFTY + SENSEX + VIX ticks from the SSE ticker array. Any subset may be in
    // one payload; we update whichever we find and re-render so the strip stays live.
    function applyTickerPayload(data) {
        if (!Array.isArray(data)) return;
        var changed = false;
        for (var i = 0; i < data.length; i++) {
            var t = data[i];
            var sym = (t && (t.symbol || t.short_name)) || '';
            if (sym === 'NSE:NIFTY50-INDEX' || sym === 'NIFTY 50' || sym === 'NIFTY50' || sym === 'Nifty 50') {
                lastNifty = { lp: Number(t.lp || 0), ch: Number(t.ch || 0), chp: Number(t.chp || 0) };
                changed = true;
            } else if (sym === 'BSE:SENSEX-INDEX' || sym === 'SENSEX' || sym === 'Sensex') {
                lastSensex = { lp: Number(t.lp || 0), ch: Number(t.ch || 0), chp: Number(t.chp || 0) };
                changed = true;
            } else if (sym === 'NSE:INDIAVIX-INDEX' || sym === 'INDIA VIX' || sym === 'INDIAVIX' || sym === 'India VIX') {
                lastVix = { lp: Number(t.lp || 0), ch: Number(t.ch || 0), chp: Number(t.chp || 0) };
                changed = true;
            }
        }
        if (changed) renderStrip();
    }

    function connectSSE() {
        if (tickerEventSource) { tickerEventSource.close(); tickerEventSource = null; }
        tickerEventSource = new EventSource('/api/market-ticker/stream');
        window.__tickerSSE = tickerEventSource;

        tickerEventSource.addEventListener('ticker', function(event) {
            try {
                var data = JSON.parse(event.data);
                applyTickerPayload(data);
                stopPolling();
            } catch (e) {}
        });
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
        fetch('/api/market-ticker').then(function(r) { return r.json(); })
            .then(function(data) { applyTickerPayload(data); }).catch(function() {});
    }

    // P&L — poll aggregate every 2 s
    function loadPnl() {
        fetch('/api/portfolio-pnl-today').then(function(r) { return r.json(); })
            .then(function(d) {
                if (d && typeof d.dayPnl !== 'undefined') {
                    lastDayPnl = Number(d.dayPnl || 0);
                    lastRisk = {
                        maxLoss: Number(d.maxLoss || 0),
                        usedPct: Number(d.lossUsedPct || 0),
                        warnPct: Number(d.warnPct || 75),
                        warn:    d.riskWarn === true
                    };
                    renderStrip();
                }
            }).catch(function() {});
    }

    function initTicker() {
        renderStrip();  // paint placeholder immediately so the slot isn't empty
        if (typeof EventSource !== 'undefined') { connectSSE(); } else { startPolling(); }
        loadPnl();
        // 500ms poll — P&L updates feel real-time. Server cost is negligible
        // (iterate strategies, sum liveNetPnlToday which reads cached LTPs).
        pnlPollInterval = setInterval(loadPnl, 500);
    }

    window.addEventListener('beforeunload', function() {
        if (tickerEventSource) { tickerEventSource.close(); tickerEventSource = null; }
        if (sseRetryTimeout) clearTimeout(sseRetryTimeout);
        if (pnlPollInterval) clearInterval(pnlPollInterval);
        stopPolling();
        if (notifPollInterval) clearInterval(notifPollInterval);
    });

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', initTicker);
    } else {
        initTicker();
    }
})();
