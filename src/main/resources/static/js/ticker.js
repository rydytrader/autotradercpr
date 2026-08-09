/**
 * Shared header strip — populates the empty space between the logo and the
 * right-side controls with a live at-a-glance market + strategy summary.
 *
 * Layout (rendered into #tickerTrack, kept for backward compat with the old
 * scrolling ticker DOM):
 *
 *   NIFTY  24587.50  +12.35 (+0.05%)  ·  VWAP 24575.20  ·  ▲ BULLISH   |
 *   OPB  WAITING  ·  OPS  1CE  ·  P&L +₹1,240
 *
 * Data sources (polled every 3 s):
 *   /api/chart/symbols          — NIFTY futures LTP / change / VWAP
 *   /api/option-scalping/state  — OPB (OPTION BUYING) FSM state + closed cycles
 *   /api/option-selling/state   — OPS (OPTION SELLING) open positions + closed cycles
 *
 * Also runs trade-open / trade-close browser notifications from a 5 s
 * /api/positions poll (unchanged from the previous ticker implementation).
 */
(function() {
    var pollInterval  = null;
    var notifInterval = null;

    // ── TRADE NOTIFICATIONS (runs on every page) ─────────────────────────────
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

    // ── HEADER STRIP ─────────────────────────────────────────────────────────

    /** Prepare the #tickerTrack container — kill the old scrolling animation
     *  and centre a single static row inside the mask-wrapped wrap. */
    function styleTrackForStrip(track) {
        if (!track) return;
        track.style.animation = 'none';
        track.style.width = '100%';
        track.style.display = 'flex';
        track.style.justifyContent = 'center';
        track.style.alignItems = 'center';
        track.style.gap = '0';
        track.style.whiteSpace = 'nowrap';
    }

    function fmtInr(n) {
        if (!isFinite(n) || n === 0) return '₹0';
        var abs = Math.abs(Math.round(n));
        return (n < 0 ? '−₹' : '+₹') + abs.toLocaleString('en-IN');
    }

    function sumRealizedPnl(state) {
        if (!state || !Array.isArray(state.todayClosedTrades)) return 0;
        var sum = 0;
        for (var i = 0; i < state.todayClosedTrades.length; i++) {
            var v = Number(state.todayClosedTrades[i].netPnl || 0);
            if (isFinite(v)) sum += v;
        }
        return sum;
    }

    function countOpenBySide(state) {
        var out = { ce: 0, pe: 0 };
        if (!state || !Array.isArray(state.openPositions)) return out;
        state.openPositions.forEach(function(p) {
            var s = String((p && (p.side || p.setup)) || '').toUpperCase();
            if (s.indexOf('CE') >= 0) out.ce++;
            else if (s.indexOf('PE') >= 0) out.pe++;
        });
        return out;
    }

    function opbSummary(state) {
        // OPTION BUYING lifecycle from OptionScalping.currentState()
        // — WAITING_FOR_TRIGGER / PENDING_ENTRY / IN_POSITION / DONE_FOR_DAY / IDLE
        if (!state) return { text: '—', color: 'var(--text-muted)' };
        var lc = String(state.lifecycle || '').toUpperCase();
        if (lc === 'IN_POSITION')  return { text: 'IN POSITION',  color: 'var(--accent-green, #34d399)' };
        if (lc === 'PENDING_ENTRY') return { text: 'PENDING',      color: 'var(--accent-yellow, #fbbf24)' };
        if (lc === 'DONE_FOR_DAY') return { text: 'DONE',          color: 'var(--text-muted)' };
        if (lc === 'WAITING_FOR_TRIGGER') return { text: 'WAITING', color: 'var(--text-secondary)' };
        if (lc === 'IDLE')         return { text: 'IDLE',          color: 'var(--text-muted)' };
        return { text: lc || '—', color: 'var(--text-secondary)' };
    }

    function opsSummary(state) {
        if (!state) return { text: '—', color: 'var(--text-muted)' };
        var counts = countOpenBySide(state);
        if (counts.ce === 0 && counts.pe === 0) {
            return { text: 'IDLE', color: 'var(--text-muted)' };
        }
        var parts = [];
        if (counts.ce > 0) parts.push(counts.ce + 'CE');
        if (counts.pe > 0) parts.push(counts.pe + 'PE');
        return { text: parts.join(' + '), color: 'var(--accent-red, #f87171)' };
    }

    function esc(s) {
        return String(s == null ? '' : s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
    }

    function chip(label, value, valueColor) {
        return '<span style="color:var(--text-muted); font-weight:700; font-size:0.66rem; letter-spacing:0.08em;">' + esc(label) + '</span>' +
               '<span style="margin-left:6px; color:' + valueColor + ';">' + esc(value) + '</span>';
    }

    function sep() {
        return '<span style="color:var(--text-muted); margin:0 12px; opacity:0.45;">·</span>';
    }

    function divider() {
        return '<span style="color:var(--text-muted); margin:0 18px; opacity:0.35; font-weight:400;">|</span>';
    }

    function buildStripHtml(sym, opb, ops) {
        var ft = (sym && sym.futuresTick) || {};
        var ltp  = Number(ft.ltp  || 0);
        var ch   = Number(ft.ch   || 0);
        var chp  = Number(ft.chp  || 0);
        var vwap = Number(ft.vwap || 0);

        // NIFTY LTP
        var ltpText = ltp > 0 ? ltp.toFixed(2) : '—';
        var ltpColor = ltp <= 0 ? 'var(--text-primary)'
                     : ch > 0  ? 'var(--accent-green, #34d399)'
                     : ch < 0  ? 'var(--accent-red, #f87171)'
                     : 'var(--text-primary)';
        var chgText = '';
        if (ltp > 0 && !(ch === 0 && chp === 0)) {
            var sign = ch > 0 ? '+' : '−';
            chgText = ' ' + sign + Math.abs(ch).toFixed(2) + ' (' + sign + Math.abs(chp).toFixed(2) + '%)';
        }

        // VWAP
        var vwapText = vwap > 0 ? vwap.toFixed(2) : '—';

        // Trend
        var trendText = '—', trendColor = 'var(--text-muted)';
        if (ltp > 0 && vwap > 0) {
            if (ltp > vwap)      { trendText = '▲ BULLISH'; trendColor = 'var(--accent-green, #34d399)'; }
            else if (ltp < vwap) { trendText = '▼ BEARISH'; trendColor = 'var(--accent-red, #f87171)'; }
            else                 { trendText = 'NEUTRAL';   trendColor = 'var(--text-muted)'; }
        }

        // Strategy states
        var opbSum = opbSummary(opb);
        var opsSum = opsSummary(ops);

        // Day P&L (realised across both strategies)
        var pnl = sumRealizedPnl(opb) + sumRealizedPnl(ops);
        var pnlColor = pnl > 0 ? 'var(--accent-green, #34d399)'
                     : pnl < 0 ? 'var(--accent-red, #f87171)'
                     : 'var(--text-muted)';

        return '' +
            chip('NIFTY', ltpText + chgText, ltpColor) +
            sep() +
            chip('VWAP',  vwapText, 'var(--text-secondary)') +
            sep() +
            '<span style="color:' + trendColor + '; font-weight:700; font-size:0.72rem; letter-spacing:0.03em;">' + esc(trendText) + '</span>' +
            divider() +
            chip('OPB', opbSum.text, opbSum.color) +
            sep() +
            chip('OPS', opsSum.text, opsSum.color) +
            sep() +
            chip('P&L', fmtInr(pnl), pnlColor);
    }

    function renderHeaderStrip() {
        var track = document.getElementById('tickerTrack');
        if (!track) return;
        styleTrackForStrip(track);

        // Fetch all three in parallel; each is resilient to failure.
        var pSym = fetch('/api/chart/symbols').then(function(r) { return r.ok ? r.json() : null; }).catch(function() { return null; });
        var pOpb = fetch('/api/option-scalping/state').then(function(r) { return r.ok ? r.json() : null; }).catch(function() { return null; });
        var pOps = fetch('/api/option-selling/state').then(function(r) { return r.ok ? r.json() : null; }).catch(function() { return null; });

        Promise.all([pSym, pOpb, pOps]).then(function(vals) {
            var track = document.getElementById('tickerTrack');
            if (!track) return;
            track.innerHTML = buildStripHtml(vals[0], vals[1], vals[2]);
        }).catch(function() {});
    }

    // ── Boot ─────────────────────────────────────────────────────────────────

    function initStrip() {
        renderHeaderStrip();
        if (pollInterval) clearInterval(pollInterval);
        pollInterval = setInterval(renderHeaderStrip, 3000);
    }

    function initNotifications() {
        if (notifInterval) return;
        notifInterval = setInterval(function() {
            fetch('/api/positions').then(function(r) { return r.json(); }).then(function(data) {
                checkPositionChanges((data && data.positions) || []);
            }).catch(function() {});
        }, 5000);
    }

    window.addEventListener('beforeunload', function() {
        if (pollInterval) clearInterval(pollInterval);
        if (notifInterval) clearInterval(notifInterval);
    });

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', function() { initStrip(); initNotifications(); });
    } else {
        initStrip();
        initNotifications();
    }
})();
