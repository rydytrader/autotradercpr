/**
 * Shared header strip — populates the empty space between the logo and the
 * right-side controls with a live at-a-glance market + strategy summary.
 *
 * Layout (rendered into #tickerTrack, kept for backward compat with the old
 * scrolling ticker DOM):
 *
 *   NIFTY  24587.50  +12.35 (+0.05%)  ·  VWAP 24575.20  ·  ▲ BULLISH   |
 *   OPB  WAITING  ·  P&L +₹1,240
 *
 * Data sources (polled every 3 s):
 *   /api/chart/symbols          — NIFTY futures LTP / change / VWAP
 *   /api/option-buying/state  — OPB (OPTION BUYING) FSM state + closed cycles
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

    /** Prepare the #tickerTrack container — kill the old scrolling animation,
     *  left-align a single static row, and neutralise the wrap's edge-fade
     *  mask so the leftmost chip isn't faded out. */
    function styleTrackForStrip(track) {
        if (!track) return;
        track.style.animation = 'none';
        track.style.width = '100%';
        track.style.display = 'flex';
        track.style.justifyContent = 'flex-start';
        track.style.alignItems = 'center';
        track.style.gap = '0';
        track.style.whiteSpace = 'nowrap';
        // Neutralise the ticker-wrap mask (used by the old scrolling ticker) so
        // the leftmost chip doesn't fade into the logo edge.
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

    function liveNetPnl(state) {
        // Prefer strategy.liveNetPnlToday() — includes closed trades + live
        // MTM on open positions (recomputed on every LTP tick server-side).
        // Falls back to summing closed-trade netPnl if the field is missing
        // (older /state payloads).
        if (!state) return 0;
        if (state.liveNetPnl != null && isFinite(Number(state.liveNetPnl))) return Number(state.liveNetPnl);
        if (!Array.isArray(state.todayClosedTrades)) return 0;
        var sum = 0;
        for (var i = 0; i < state.todayClosedTrades.length; i++) {
            var v = Number(state.todayClosedTrades[i].netPnl || 0);
            if (isFinite(v)) sum += v;
        }
        return sum;
    }

    function opbSummary(state) {
        // OPTION BUYING lifecycle from OptionBuying.currentState()
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

    function buildStripHtml(sym, opb) {
        var ltp  = Number((sym && sym.spotLtp)       || 0);
        var ch   = Number((sym && sym.spotChange)    || 0);
        var chp  = Number((sym && sym.spotChangePct) || 0);

        // NIFTY SPOT LTP + change (post-strategy-cutover: no futures/VWAP/trend
        // chips — strategy uses each option's own VWAP + ST, not spot VWAP).
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

        // Day P&L (closed trades + live MTM on open positions)
        var pnl = liveNetPnl(opb);
        var pnlColor = pnl > 0 ? 'var(--accent-green, #34d399)'
                     : pnl < 0 ? 'var(--accent-red, #f87171)'
                     : 'var(--text-muted)';

        var leadingRule = '<span style="display:inline-block; width:1px; height:22px; background:var(--border); margin-right:18px; opacity:0.7;"></span>';

        return leadingRule +
            chip('NIFTY', ltpText + chgText, ltpColor) +
            divider() +
            chip('P&L', fmtInr(pnl), pnlColor);
    }

    function renderHeaderStrip() {
        var track = document.getElementById('tickerTrack');
        if (!track) return;
        styleTrackForStrip(track);

        // Fetch both in parallel; each is resilient to failure.
        var pSym = fetch('/api/chart/symbols').then(function(r) { return r.ok ? r.json() : null; }).catch(function() { return null; });
        var pOpb = fetch('/api/option-buying/state').then(function(r) { return r.ok ? r.json() : null; }).catch(function() { return null; });

        Promise.all([pSym, pOpb]).then(function(vals) {
            var track = document.getElementById('tickerTrack');
            if (!track) return;
            track.innerHTML = buildStripHtml(vals[0], vals[1]);
        }).catch(function() {});
    }

    // ── Boot ─────────────────────────────────────────────────────────────────

    function initStrip() {
        renderHeaderStrip();
        if (pollInterval) clearInterval(pollInterval);
        // 1-s poll for live P&L feel. Both endpoints are pure in-memory
        // reads on the server — no DB or WS call — so the load is trivial.
        pollInterval = setInterval(renderHeaderStrip, 1000);
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
