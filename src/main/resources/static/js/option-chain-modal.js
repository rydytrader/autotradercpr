/**
 * NIFTY Options Chain modal — opened from the ⊞ icon in the navbar.
 *
 * Snapshot view of ATM ±10 strikes for the current weekly expiry. Server endpoint:
 * GET /api/option-chain → { spot, atmStrike, expiry, asOf, maxCeOiStrike, maxPeOiStrike, rows[] }.
 *
 * Color layers (low → high priority):
 *   1. ITM tint   — CE cells green when strike < spot; PE cells red when strike > spot.
 *   2. ATM band   — full-width gold band on the ATM row (overrides layer 1 for that row).
 *   3. Max-OI accent — cyan border + bold bracketed OI value + ■ marker on the strike with
 *      the highest CE OI / PE OI within the visible window.
 */
(function() {
    var overlayEl  = null;
    var bodyEl     = null;
    var titleSubEl = null;
    var refreshBtn = null;
    var refreshLockUntil = 0;

    function ensureBuilt() {
        if (overlayEl) return overlayEl;
        var html =
            '<div id="ocOverlay" style="display:none;position:fixed;inset:0;background:rgba(0,0,0,0.6);z-index:999;align-items:center;justify-content:center;">' +
              '<div style="background:var(--bg-card);border:1px solid var(--border);border-radius:12px;width:1080px;max-width:96vw;max-height:90vh;display:flex;flex-direction:column;box-shadow:0 16px 48px rgba(0,0,0,0.3);">' +
                '<div style="display:flex;align-items:center;justify-content:space-between;padding:16px 22px;border-bottom:1px solid var(--border);gap:12px;">' +
                  '<div style="flex:1;min-width:0;">' +
                    '<div style="display:flex;align-items:center;gap:10px;flex-wrap:wrap;">' +
                      '<div style="font-family:var(--font-mono);font-size:0.92rem;font-weight:700;color:var(--text-primary);">⊞ NIFTY OPTIONS CHAIN</div>' +
                      '<span class="oc-chip oc-chip-ce">CALLS ←</span>' +
                      '<span class="oc-chip oc-chip-pe">→ PUTS</span>' +
                    '</div>' +
                    '<div id="oc-sub" style="font-family:var(--font-mono);font-size:0.7rem;color:var(--text-muted);margin-top:4px;">—</div>' +
                  '</div>' +
                  '<button id="oc-refresh" title="Refresh" style="background:transparent;border:1px solid var(--border);color:var(--text-secondary);width:30px;height:30px;border-radius:6px;cursor:pointer;font-size:0.95rem;">⟳</button>' +
                  '<button onclick="OptionChainModal.close()" style="background:transparent;border:none;color:var(--text-muted);font-size:1.5rem;cursor:pointer;line-height:1;padding:0 4px;">&times;</button>' +
                '</div>' +
                '<div id="oc-body" style="flex:1;overflow:auto;padding:0 18px 22px;"></div>' +
              '</div>' +
            '</div>';
        var wrap = document.createElement('div');
        wrap.innerHTML = html;
        document.body.appendChild(wrap.firstChild);
        overlayEl  = document.getElementById('ocOverlay');
        bodyEl     = document.getElementById('oc-body');
        titleSubEl = document.getElementById('oc-sub');
        refreshBtn = document.getElementById('oc-refresh');

        refreshBtn.addEventListener('click', function() {
            var now = Date.now();
            if (now < refreshLockUntil) return;
            refreshLockUntil = now + 1500;
            fetchAndRender();
        });

        overlayEl.addEventListener('click', function(e) {
            if (e.target === overlayEl) close();
        });
        document.addEventListener('keydown', function(e) {
            if (e.key === 'Escape' && overlayEl && overlayEl.style.display === 'flex') close();
        });

        var style = document.createElement('style');
        style.textContent =
            '#oc-body { font-family:var(--font-mono);font-size:0.78rem; }' +
            '.oc-table { width:100%;border-collapse:collapse;table-layout:auto; }' +
            '.oc-table thead th { position:sticky;top:0;background:var(--bg-card);border-bottom:1px solid var(--border);padding:10px 6px;font-size:0.66rem;font-weight:700;letter-spacing:0.06em;color:var(--text-muted);text-transform:uppercase;text-align:left;z-index:1; }' +
            '.oc-table thead th.oc-strike-h { text-align:center;color:var(--text-secondary); }' +
            '.oc-table tbody td { padding:7px 6px;border-bottom:1px solid rgba(128,128,128,0.08);text-align:left;color:var(--text-primary);font-variant-numeric:tabular-nums; }' +
            '.oc-table thead th.oc-strike-h { padding-left:8px;padding-right:8px;white-space:nowrap; }' +
            '.oc-table tbody td.oc-strike { text-align:center;font-weight:700;color:var(--text-primary);background:rgba(128,128,128,0.05);border-left:1px solid var(--border);border-right:1px solid var(--border);padding-left:8px;padding-right:8px;white-space:nowrap; }' +
            '.oc-itm-ce { background:rgba(34,197,94,0.10); }' +
            '.oc-itm-pe { background:rgba(248,113,113,0.10); }' +
            '.oc-atm td { background:rgba(250,204,21,0.18) !important;border-top:1px solid rgba(250,204,21,0.55);border-bottom:1px solid rgba(250,204,21,0.55); }' +
            '.oc-atm .oc-strike { background:rgba(250,204,21,0.28) !important; }' +
            '.oc-table tbody td.oc-act-cell { background:transparent !important; }' +
            '.oc-table tbody tr.oc-atm td.oc-act-cell { background:transparent !important;border-top:1px solid rgba(128,128,128,0.08) !important;border-bottom:1px solid rgba(128,128,128,0.08) !important; }' +
            '.oc-table tbody td.oc-wall-ce { background:rgba(34,211,238,0.18) !important; }' +
            '.oc-table tbody td.oc-wall-pe { background:rgba(34,211,238,0.18) !important; }' +
            '.oc-wall-oi { font-weight:700; }' +
            '.oc-tag { display:inline-block;font-size:0.6rem;font-weight:700;letter-spacing:0.04em;padding:1px 5px;border-radius:3px;vertical-align:middle; }' +
            '.oc-tag-r { color:var(--accent-red, #f87171);background:rgba(248,113,113,0.15);border:1px solid rgba(248,113,113,0.45); }' +
            '.oc-tag-s { color:var(--accent-green, #34d399);background:rgba(34,197,94,0.15);border:1px solid rgba(34,197,94,0.45); }' +
            '.oc-tag-r2 { color:rgba(248,113,113,0.7);background:rgba(248,113,113,0.07);border:1px solid rgba(248,113,113,0.28); }' +
            '.oc-tag-s2 { color:rgba(52,211,153,0.7);background:rgba(34,197,94,0.07);border:1px solid rgba(34,197,94,0.28); }' +
            // ATM pill — gold, matches the row's gold band so the synthetic-futures ATM strike
            // reads as a labelled level alongside R1/R2/S1/S2 instead of just a backgrounded row.
            '.oc-tag-atm { color:rgba(250,204,21,1);background:rgba(250,204,21,0.18);border:1px solid rgba(250,204,21,0.55); }' +
            '.oc-tag-slot { display:inline-block;width:34px;text-align:center;vertical-align:middle; }' +
            '.oc-strike-num { display:inline-block;vertical-align:middle;margin:0 4px; }' +
            '.oc-act { display:inline-block;font-size:0.62rem;font-weight:700;letter-spacing:0.04em;padding:1px 5px;border-radius:3px; }' +
            '.oc-act-lb { color:var(--accent-green, #34d399);background:rgba(34,197,94,0.18);border:1px solid rgba(34,197,94,0.45); }' +
            '.oc-act-sb { color:var(--accent-red, #f87171);background:rgba(248,113,113,0.18);border:1px solid rgba(248,113,113,0.45); }' +
            '.oc-act-sc { color:rgba(52,211,153,0.85);background:rgba(34,197,94,0.07);border:1px solid rgba(34,197,94,0.22); }' +
            '.oc-act-lu { color:rgba(248,113,113,0.80);background:rgba(248,113,113,0.07);border:1px solid rgba(248,113,113,0.22); }' +
            '.oc-up   { color:var(--accent-green, #34d399); }' +
            '.oc-down { color:var(--accent-red, #f87171); }' +
            '.oc-muted { color:var(--text-muted); }' +
            '.oc-msg { padding:40px 20px;text-align:center;color:var(--text-muted);font-size:0.82rem; }' +
            '.oc-chip { font-family:var(--font-mono);font-size:0.66rem;font-weight:700;letter-spacing:0.08em;padding:3px 9px;border-radius:4px;border:1px solid transparent; }' +
            '.oc-chip-ce { color:var(--accent-green, #34d399);background:rgba(34,197,94,0.10);border-color:rgba(34,197,94,0.30); }' +
            '.oc-chip-pe { color:var(--accent-red, #f87171);background:rgba(248,113,113,0.10);border-color:rgba(248,113,113,0.30); }' +
            '#oc-refresh:hover { background:rgba(128,128,128,0.12);color:var(--text-primary); }' +
            '#oc-body::-webkit-scrollbar { width:4px;height:4px; }' +
            '#oc-body::-webkit-scrollbar-thumb { background:var(--border);border-radius:4px; }' +
            '#oc-body::-webkit-scrollbar-track { background:transparent; }';
        document.head.appendChild(style);
        return overlayEl;
    }

    function open() {
        ensureBuilt();
        overlayEl.style.display = 'flex';
        fetchAndRender();
    }

    function close() {
        if (overlayEl) overlayEl.style.display = 'none';
    }

    function fetchAndRender() {
        bodyEl.innerHTML = '<div class="oc-msg">Loading chain…</div>';
        titleSubEl.textContent = '—';
        fetch('/api/option-chain', { credentials: 'same-origin' })
            .then(function(r) {
                if (r.status === 401) return r.json().then(function(j) { throw { kind:'auth', msg:j.error || 'not_logged_in' }; });
                if (!r.ok) return r.json().catch(function(){return{};}).then(function(j) { throw { kind:'svc', msg:(j && j.error) || ('HTTP ' + r.status), detail:j && j.message }; });
                return r.json();
            })
            .then(render)
            .catch(function(err) {
                var msg, hint;
                if (err && err.kind === 'auth') { msg = 'Not logged in'; hint = 'Please log in to Fyers first.'; }
                else if (err && err.kind === 'svc') {
                    if (err.msg === 'empty_chain' || err.msg === 'empty_response') { msg = 'Chain is empty'; hint = 'No data returned for this expiry yet.'; }
                    else if (err.msg === 'spot_unresolved') { msg = 'Spot price not resolved'; hint = 'Could not read NIFTY spot from the chain.'; }
                    else if (err.msg === 'fyers_unavailable') { msg = 'Fyers temporarily unavailable'; hint = err.detail || ''; }
                    else { msg = "Couldn't fetch chain · " + err.msg; hint = err.detail || ''; }
                } else { msg = "Couldn't fetch chain"; hint = String(err && err.message || ''); }
                bodyEl.innerHTML = '<div class="oc-msg"><div style="font-size:0.92rem;color:var(--text-primary);margin-bottom:6px;">' + esc(msg) + '</div><div>' + esc(hint) + '</div></div>';
            });
    }

    function render(payload) {
        if (!payload || !Array.isArray(payload.rows) || payload.rows.length === 0) {
            bodyEl.innerHTML = '<div class="oc-msg">No chain data</div>';
            return;
        }
        var spot = numeric(payload.spot);
        titleSubEl.textContent =
            'Expiry ' + (payload.expiry || '—') +
            ' · Spot ' + (spot != null ? fmtPrice(spot) : '—') +
            ' · ATM ' + payload.atmStrike +
            ' · ' + (payload.asOf || '');

        var ceStrikes = (payload.maxCeOiStrikes || []).map(function(v) { return numeric(v) || 0; });
        var peStrikes = (payload.maxPeOiStrikes || []).map(function(v) { return numeric(v) || 0; });

        var html = '<table class="oc-table">' +
            '<thead><tr>' +
              '<th style="text-align:left;">ACT</th><th style="text-align:left;">ΔOI</th><th style="text-align:left;">OI</th><th style="text-align:left;">VOL</th><th style="text-align:left;">CHG%</th><th style="text-align:center;">LTP</th><th style="text-align:center;">DELTA</th>' +
              '<th class="oc-strike-h">STRIKE</th>' +
              '<th style="text-align:center;">DELTA</th><th style="text-align:center;">LTP</th><th style="text-align:right;">CHG%</th><th style="text-align:right;">VOL</th><th style="text-align:right;">OI</th><th style="text-align:right;">ΔOI</th><th style="text-align:right;">ACT</th>' +
            '</tr></thead><tbody>';

        // Render high strikes first (resistance up top, support down — matches a price chart).
        for (var i = payload.rows.length - 1; i >= 0; i--) {
            var row = payload.rows[i];
            var ce = row.ce || {};
            var pe = row.pe || {};
            var strike = row.strike;
            var isAtm    = !!row.isAtm;
            var isSynAtm = !!row.isSyntheticAtm;
            var ceItm  = !isAtm && strike < spot;   // CE intrinsic > 0 when strike < spot
            var peItm  = !isAtm && strike > spot;   // PE intrinsic > 0 when strike > spot
            var ceRank = ceStrikes.indexOf(strike); // 0 = R1, 1 = R2, -1 = none
            var peRank = peStrikes.indexOf(strike); // 0 = S1, 1 = S2, -1 = none
            var isMaxCe = ceRank >= 0;
            var isMaxPe = peRank >= 0;

            var ceSideCls = isMaxCe ? 'oc-wall-ce' : (ceItm ? 'oc-itm-ce' : '');
            var peSideCls = isMaxPe ? 'oc-wall-pe' : (peItm ? 'oc-itm-pe' : '');
            var ceOiCls   = ceSideCls + (isMaxCe ? ' oc-wall-oi' : '');
            var peOiCls   = peSideCls + (isMaxPe ? ' oc-wall-oi' : '');

            var rTagHtml = isMaxCe
                ? '<span class="oc-tag oc-tag-r' + (ceRank > 0 ? '2' : '') + '">R' + (ceRank + 1) + '</span>'
                : '';
            var sTagHtml = isMaxPe
                ? '<span class="oc-tag oc-tag-s' + (peRank > 0 ? '2' : '') + '">S' + (peRank + 1) + '</span>'
                : '';
            // ATM pill marks the synthetic-futures (put-call parity) ATM — the strike the
            // straddle bot actually trades. Spot-rounded ATM row keeps the gold band; this
            // pill goes on the synthetic row (often the same row, sometimes one strike off).
            var atmTagHtml = isSynAtm
                ? '<span class="oc-tag oc-tag-atm" title="Synthetic-futures ATM (put-call parity)">ATM</span>'
                : '';
            var leftSlotHtml = isSynAtm ? atmTagHtml : rTagHtml;
            var strikeCellHtml =
                '<span class="oc-tag-slot">' + leftSlotHtml + '</span>' +
                '<span class="oc-strike-num">' + String(strike) + '</span>' +
                '<span class="oc-tag-slot">' + sTagHtml + '</span>';

            var ceOiText = fmtOiVal(ce.oi);
            var peOiText = fmtOiVal(pe.oi);

            html += '<tr' + (isAtm ? ' class="oc-atm"' : '') + '>' +
              '<td class="oc-act-cell" style="text-align:left;">' + fmtActivityCell(ce.activity) + '</td>' +
              '<td class="' + ceSideCls + '" style="text-align:left;">' + fmtDeltaOi(ce.oiChange) + '</td>' +
              '<td class="' + ceOiCls   + '" style="text-align:left;">' + ceOiText + '</td>' +
              '<td class="' + ceSideCls + '" style="text-align:left;">' + fmtVol(ce.volume) + '</td>' +
              '<td class="' + ceSideCls + '" style="text-align:left;">' + fmtChg(ce.chgPct) + '</td>' +
              '<td class="' + ceSideCls + '" style="text-align:center;">' + fmtPrice(ce.ltp) + '</td>' +
              '<td class="' + ceSideCls + '" style="text-align:center;">' + fmtDelta(ce.delta) + '</td>' +
              '<td class="oc-strike">' + strikeCellHtml + '</td>' +
              '<td class="' + peSideCls + '" style="text-align:center;">' + fmtDelta(pe.delta) + '</td>' +
              '<td class="' + peSideCls + '" style="text-align:center;">' + fmtPrice(pe.ltp) + '</td>' +
              '<td class="' + peSideCls + '" style="text-align:right;">' + fmtChg(pe.chgPct) + '</td>' +
              '<td class="' + peSideCls + '" style="text-align:right;">' + fmtVol(pe.volume) + '</td>' +
              '<td class="' + peOiCls   + '" style="text-align:right;">' + peOiText + '</td>' +
              '<td class="' + peSideCls + '" style="text-align:right;">' + fmtDeltaOi(pe.oiChange) + '</td>' +
              '<td class="oc-act-cell" style="text-align:right;">' + fmtActivityCell(pe.activity) + '</td>' +
            '</tr>';
        }
        html += '</tbody></table>';
        bodyEl.innerHTML = html;
    }

    // ── Formatters ─────────────────────────────────────────────────────────────
    function fmtDelta(v) {
        // Render to 2 decimals. CE delta is positive, PE delta negative; the sign carries
        // information so we don't strip it. "—" when Fyers didn't supply greeks.
        var n = numeric(v);
        if (n == null) return '<span class="oc-muted">—</span>';
        return n.toFixed(2);
    }
    function fmtPrice(v) {
        var n = numeric(v);
        if (n == null) return '<span class="oc-muted">—</span>';
        return n.toFixed(2);
    }
    function fmtChg(chgPct) {
        var c = numeric(chgPct);
        if (c == null || c === 0) return '<span class="oc-muted">—</span>';
        var cls = c > 0 ? 'oc-up' : 'oc-down';
        var sign = c > 0 ? '+' : '−';
        return '<span class="' + cls + '">' + sign + Math.abs(c).toFixed(2) + '%</span>';
    }
    function fmtVol(v) {
        var n = numeric(v);
        if (n == null || n === 0) return '<span class="oc-muted">—</span>';
        return shortNum(n);
    }
    function fmtOiVal(v) {
        var n = numeric(v);
        if (n == null || n === 0) return '<span class="oc-muted">—</span>';
        return shortNum(n);
    }
    function fmtActivityCell(act) {
        if (!act) return '<span class="oc-muted">—</span>';
        var cls = 'oc-act-' + act.toLowerCase();
        return '<span class="oc-act ' + cls + '">' + act + '</span>';
    }
    function fmtDeltaOi(v) {
        var n = numeric(v);
        if (n == null || n === 0) return '<span class="oc-muted">—</span>';
        var s = shortNum(Math.abs(n));
        if (n > 0) return '<span class="oc-up">+' + s + '</span>';
        return '<span class="oc-down">−' + s + '</span>';
    }
    function shortNum(n) {
        var a = Math.abs(n);
        if (a >= 1e7)  return (n / 1e7).toFixed(2) + 'Cr';
        if (a >= 1e5)  return (n / 1e5).toFixed(2) + 'L';
        if (a >= 1e3)  return (n / 1e3).toFixed(2) + 'K';
        return n.toFixed(2);
    }
    function numeric(v) {
        if (v == null) return null;
        if (typeof v === 'number') return isFinite(v) ? v : null;
        var n = Number(v);
        return isFinite(n) ? n : null;
    }
    function esc(s) {
        return String(s == null ? '' : s)
            .replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')
            .replace(/"/g,'&quot;').replace(/'/g,'&#39;');
    }

    window.OptionChainModal = { open: open, close: close };
})();
