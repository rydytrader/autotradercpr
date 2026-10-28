// SIMULATE modal on the POSITIONS page — Opstra-style payoff simulator (options only).
// Loads the instance's open legs (LIVE rows), lets the operator add hypothetical BUY/SELL
// CE/PE rows (SIM rows), and renders metrics + an expiry / T+n payoff chart from
// POST /api/strategies/{id}/sim/payoff. Read-only: nothing here places orders.
//
// Expects on the page: STRATEGY_ID, csrfHeaders(), escapeHtml(), fmtRs(), showToast(), Chart (Chart.js 4).
(function () {
    var ctx = null;            // /sim/context payload (spot, lotSize, expiries, legs)
    var legs = [];             // simulator rows (LIVE + SIM), kept for the browser session
    var chains = {};           // expiry → /sim/chain rows
    var chart = null;
    var nextId = 1;
    var debounceTimer = null;
    var marginTimer = null;
    var marginSeq = 0;
    var lastResult = null;
    var params = { targetDays: 0, spotShiftPct: 0, ivShiftPts: 0 };

    var OPT = 'background:var(--bg-card);color:var(--text-primary);';
    var CELL_INPUT = 'width:100%;box-sizing:border-box;font-family:var(--font-mono);font-size:0.74rem;padding:3px 6px;background:var(--bg-primary);color:var(--text-primary);border:1px solid var(--border);border-radius:4px;';

    function api(path, opts) {
        return fetch('/api/strategies/' + encodeURIComponent(STRATEGY_ID) + path, Object.assign({ credentials: 'same-origin' }, opts || {}))
            .then(function (r) { return r.json(); });
    }
    function toast(msg, kind) { if (window.showToast) showToast(msg, kind || 'info'); }
    function fmtN(v, dp) { return (v == null || isNaN(v)) ? '—' : Number(v).toLocaleString('en-IN', { minimumFractionDigits: dp == null ? 0 : dp, maximumFractionDigits: dp == null ? 0 : dp }); }
    function fmtExpiry(iso) {
        var p = String(iso || '').split('-'); if (p.length !== 3) return iso || '';
        var mon = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'][parseInt(p[1], 10) - 1] || p[1];
        return parseInt(p[2], 10) + ' ' + mon;
    }
    function addDays(iso, n) { var d = new Date(iso + 'T00:00:00'); d.setDate(d.getDate() + n); return d.toISOString().slice(0, 10); }
    function todayIso() { var d = new Date(); return d.getFullYear() + '-' + String(d.getMonth() + 1).padStart(2, '0') + '-' + String(d.getDate()).padStart(2, '0'); }

    // ── open / close ──────────────────────────────────────────────────────────
    function open() {
        var ov = document.getElementById('simOverlay');
        ov.style.display = 'flex';
        ov.onclick = function (e) { if (e.target === ov) close(); };
        document.addEventListener('keydown', esc);
        api('/sim/context').then(function (c) {
            if (!c || c.error) { toast('Simulator context unavailable', 'error'); return; }
            ctx = c;
            document.getElementById('simTitle').textContent = 'SIMULATE · ' + (c.underlying || '');
            document.getElementById('simSub').textContent = 'spot ' + fmtN(c.spot, 2) + ' · lot ' + (c.lotSize || '—');
            if (!legs.length) loadPositions(); else { renderLegs(); recompute(); }
            (c.expiries || []).forEach(function (e) { if (!chains[e.date]) loadChain(e.date); });
        }).catch(function () { toast('Simulator context unavailable', 'error'); });
    }
    function esc(e) { if (e.key === 'Escape') close(); }
    function close() {
        document.getElementById('simOverlay').style.display = 'none';
        document.removeEventListener('keydown', esc);
    }

    // ── legs ──────────────────────────────────────────────────────────────────
    function loadPositions() {
        if (!ctx) return;
        legs = legs.filter(function (l) { return l.tag !== 'LIVE'; });
        var live = (ctx.legs || []).map(function (l) {
            return { id: nextId++, tag: 'LIVE', include: true, side: l.side, type: l.type, expiry: l.expiry, strike: Number(l.strike),
                     lots: Number(l.lots) || 1, price: Number(l.price) || 0, ltp: Number(l.ltp) || 0, iv: Number(l.iv) || 0, symbol: l.symbol, legId: l.legId,
                     // remembered so a LIVE row returns to its entry price when the contract is switched back
                     orig: { expiry: l.expiry, strike: Number(l.strike), type: l.type, price: Number(l.price) || 0, iv: Number(l.iv) || 0, symbol: l.symbol } };
        });
        legs = live.concat(legs);
        renderLegs(); recompute();
        if (!live.length) toast('No open legs — add SIM rows to simulate', 'info');
    }
    function addLeg() {
        if (!ctx) return;
        var exp = (ctx.expiries && ctx.expiries[0] && ctx.expiries[0].date) || '';
        var rows = (chains[exp] && chains[exp].rows) || [];
        var atm = chains[exp] ? Number(chains[exp].atm || 0) : 0;
        var row = rows.find(function (r) { return Number(r.strike) === atm; }) || rows[0];
        var leg = { id: nextId++, tag: 'SIM', include: true, side: 'SELL', type: 'CE', expiry: exp,
                    strike: row ? Number(row.strike) : 0, lots: 1, price: row ? Number(row.ceLtp) || 0 : 0, iv: row ? Number(row.ceIv) || 0 : 0 };
        legs.push(leg);
        renderLegs(); recompute();
        if (!exp) toast('Expiry list unavailable — try again', 'error');
        else if (!rows.length) loadChain(exp, function () { syncLegFromChain(leg); renderLegs(); recompute(); });
    }
    function clearLegs() { legs = []; renderLegs(); recompute(); }
    function removeLeg(id) { legs = legs.filter(function (l) { return l.id !== id; }); renderLegs(); recompute(); }
    function legById(id) { return legs.find(function (l) { return l.id === id; }); }

    function loadChain(expiry, done) {
        api('/sim/chain?expiry=' + encodeURIComponent(expiry)).then(function (c) {
            if (c && !c.error) chains[expiry] = c;
            if (done) done();
        }).catch(function () { if (done) done(); });
    }
    // Pull price + IV for the leg's strike / type from the chain for its expiry. A LIVE row that is
    // back on its own contract gets its entry price again, not the chain LTP.
    function syncLegFromChain(leg) {
        var o = leg.orig;
        if (o && o.expiry === leg.expiry && o.type === leg.type && Number(o.strike) === Number(leg.strike)) {
            leg.price = o.price; leg.iv = o.iv; leg.symbol = o.symbol;
            return true;
        }
        var c = chains[leg.expiry]; if (!c) return false;
        var row = (c.rows || []).find(function (r) { return Number(r.strike) === Number(leg.strike); });
        if (!row) { row = (c.rows || [])[0]; if (!row) return false; leg.strike = Number(row.strike); }
        var ce = leg.type === 'CE';
        leg.price = Number(ce ? row.ceLtp : row.peLtp) || 0;
        leg.iv = Number(ce ? row.ceIv : row.peIv) || 0;
        leg.symbol = ce ? row.ceSymbol : row.peSymbol;
        return true;
    }

    window.simSet = function (id, field, value) {
        var leg = legById(id); if (!leg) return;
        if (field === 'include') leg.include = !!value;
        else if (field === 'side') leg.side = leg.side === 'SELL' ? 'BUY' : 'SELL';
        else if (field === 'type') { leg.type = leg.type === 'CE' ? 'PE' : 'CE'; syncLegFromChain(leg); }
        else if (field === 'expiry') {
            leg.expiry = value;
            if (!chains[value]) { loadChain(value, function () { syncLegFromChain(leg); renderLegs(); recompute(); }); }
            else syncLegFromChain(leg);
        }
        else if (field === 'strike') { leg.strike = Number(value); syncLegFromChain(leg); }
        else if (field === 'lots') leg.lots = Math.max(1, parseInt(value, 10) || 1);
        else if (field === 'price') leg.price = Math.max(0, parseFloat(value) || 0);
        else if (field === 'iv') leg.iv = Math.max(0, (parseFloat(value) || 0) / 100);
        if (field === 'side' || field === 'type' || field === 'expiry' || field === 'strike' || field === 'include') renderLegs();
        recompute();
    };
    window.simRemove = removeLeg;

    function strikeOptions(leg) {
        var c = chains[leg.expiry];
        var rows = (c && c.rows) || [];
        if (!rows.length) return '<option value="' + leg.strike + '" selected style="' + OPT + '">' + (leg.strike || '—') + '</option>';
        var seen = false;
        var html = rows.map(function (r) {
            var sel = Number(r.strike) === Number(leg.strike); if (sel) seen = true;
            var px = leg.type === 'CE' ? r.ceLtp : r.peLtp;
            return '<option value="' + r.strike + '"' + (sel ? ' selected' : '') + ' style="' + OPT + '">' + r.strike + ' · ₹' + Number(px || 0).toFixed(2) + (r.isAtm ? ' · ATM' : '') + '</option>';
        }).join('');
        if (!seen && leg.strike) html = '<option value="' + leg.strike + '" selected style="' + OPT + '">' + leg.strike + '</option>' + html;
        return html;
    }
    function expiryOptions(leg) {
        var exps = (ctx && ctx.expiries) || [];
        var seen = false;
        var html = exps.map(function (e) {
            var sel = e.date === leg.expiry; if (sel) seen = true;
            return '<option value="' + escapeHtml(e.date) + '"' + (sel ? ' selected' : '') + ' style="' + OPT + '">' + escapeHtml(fmtExpiry(e.date)) + (e.monthly ? ' M' : '') + '</option>';
        }).join('');
        if (!seen && leg.expiry) html = '<option value="' + escapeHtml(leg.expiry) + '" selected style="' + OPT + '">' + escapeHtml(fmtExpiry(leg.expiry)) + '</option>' + html;
        return html;
    }
    function renderLegs() {
        var body = document.getElementById('simLegsBody');
        if (!legs.length) {
            body.innerHTML = '<tr><td colspan="9" style="text-align:center;color:var(--text-muted);padding:28px 10px;">No legs. LOAD POSITIONS or + ADD LEG.</td></tr>';
            return;
        }
        var html = '';
        legs.forEach(function (l, i) {
            var live = l.tag === 'LIVE';
            var sideCls = l.side === 'SELL' ? 'sim-sell' : 'sim-buy';
            html += '<tr style="' + (l.include ? '' : 'opacity:0.45;') + '">'
                + '<td><input type="checkbox"' + (l.include ? ' checked' : '') + ' onchange="simSet(' + l.id + ', \'include\', this.checked)"></td>'
                + '<td><span class="pill ' + (live ? 'sim-live' : 'sim-sim') + '">' + l.tag + '</span></td>'
                + '<td><button class="act-btn ' + sideCls + '" onclick="simSet(' + l.id + ', \'side\')">' + l.side + '</button></td>'
                + '<td><button class="act-btn" onclick="simSet(' + l.id + ', \'type\')">' + l.type + '</button></td>'
                + '<td><select style="' + CELL_INPUT + '" onchange="simSet(' + l.id + ', \'expiry\', this.value)">' + expiryOptions(l) + '</select></td>'
                + '<td><select style="' + CELL_INPUT + 'min-width:120px;" onchange="simSet(' + l.id + ', \'strike\', this.value)">' + strikeOptions(l) + '</select></td>'
                + '<td><input type="number" min="1" step="1" value="' + l.lots + '" style="' + CELL_INPUT + 'width:56px;text-align:right;" onchange="simSet(' + l.id + ', \'lots\', this.value)"></td>'
                + '<td><input type="number" min="0" step="0.05" value="' + Number(l.price || 0).toFixed(2) + '" title="' + (live ? 'Entry premium — edit for a what-if' : 'Chain LTP — editable') + '" style="' + CELL_INPUT + 'width:76px;text-align:right;" onchange="simSet(' + l.id + ', \'price\', this.value)"></td>'
                + '<td><button class="act-btn close-x" onclick="simRemove(' + l.id + ')" title="Remove row">✕</button></td>'
                + '</tr>';
        });
        body.innerHTML = html;
    }

    // ── compute ───────────────────────────────────────────────────────────────
    function recompute() {
        if (debounceTimer) clearTimeout(debounceTimer);
        debounceTimer = setTimeout(doCompute, 120);
    }
    function doCompute() {
        var payload = {
            legs: payloadLegs(),
            params: { targetDate: addDays(todayIso(), params.targetDays), spotShiftPct: params.spotShiftPct, ivShiftPts: params.ivShiftPts }
        };
        api('/sim/payoff', { method: 'POST', headers: csrfHeaders(), body: JSON.stringify(payload) })
            .then(function (res) {
                if (!res || res.error) { toast('Payoff failed: ' + (res && res.error || 'unknown'), 'error'); return; }
                lastResult = res;
                renderMetrics(res); renderChart(res); renderSliders(res);
                scheduleMargin();
            })
            .catch(function (e) { toast('Payoff request failed: ' + (e.message || e), 'error'); });
    }
    // ── margin (Fyers span margin for the whole basket; slower, so debounced separately) ──
    function scheduleMargin() {
        if (marginTimer) clearTimeout(marginTimer);
        marginTimer = setTimeout(fetchMargin, 500);
    }
    function fetchMargin() {
        var el = document.getElementById('simMargin'); if (!el) return;
        var withSym = legs.filter(function (l) { return l.include && l.symbol; });
        if (!withSym.length) { el.textContent = '—'; el.title = 'No contracts to margin'; return; }
        var seq = ++marginSeq;
        el.style.opacity = '0.5';
        var product = (ctx && ctx.defaultProduct) || 'INTRADAY';
        api('/sim/margin?product=' + encodeURIComponent(product), { method: 'POST', headers: csrfHeaders(), body: JSON.stringify({ legs: payloadLegs() }) })
            .then(function (m) {
                if (seq !== marginSeq) return;
                el.style.opacity = '';
                if (!m || m.error) { el.textContent = '—'; el.title = 'Margin unavailable' + (m && m.error ? ': ' + m.error : ''); return; }
                el.textContent = fmtRs(m.margin);
                el.title = 'Fyers span margin for the basket as ' + (m.product === 'MARGIN' ? 'OVERNIGHT' : 'INTRADAY') + ' (' + m.legs + ' leg(s))' + (m.available ? ' · available ' + fmtRs(m.available) : '');
            })
            .catch(function () { if (seq === marginSeq) { el.style.opacity = ''; el.textContent = '—'; el.title = 'Margin unavailable'; } });
    }
    function payloadLegs() {
        return legs.map(function (l) { return { side: l.side, type: l.type, strike: l.strike, expiry: l.expiry, lots: l.lots, price: l.price, iv: l.iv, include: l.include, tag: l.tag, symbol: l.symbol || null, legId: l.legId || null }; });
    }

    // ── execute SIM rows through the position manager ─────────────────────────
    function execute() {
        var rows = legs.filter(function (l) { return l.tag === 'SIM' && l.include && l.lots > 0; });
        if (!rows.length) { toast('No SIM rows to execute — LIVE rows are already open', 'info'); return; }
        var missing = rows.filter(function (l) { return !l.symbol; });
        if (missing.length) { toast('Pick a strike from the chain for every SIM row first', 'error'); return; }
        var d = window.lastDashboardData || {};
        var n = new Date();
        var sqp = String(d.intradaySquareoff || '15:20').split(':');
        var afterSq = n.getHours() * 60 + n.getMinutes() >= (parseInt(sqp[0], 10) * 60 + parseInt(sqp[1], 10));
        var defMargin = afterSq || String((ctx && ctx.defaultProduct) || '') === 'MARGIN';
        var defSl = ctx && Number(ctx.defaultSlPct) >= 0 ? Number(ctx.defaultSlPct) : 50;
        var hasSell = rows.some(function (l) { return l.side === 'SELL'; });
        var list = rows.map(function (l) {
            return '<div style="display:flex;gap:10px;padding:4px 0;border-top:1px solid rgba(128,128,128,0.12);">'
                + '<span class="act-btn ' + (l.side === 'SELL' ? 'sim-sell' : 'sim-buy') + '" style="pointer-events:none;">' + l.side + '</span>'
                + '<span style="font-weight:700;">' + l.type + ' ' + l.strike + '</span><span style="color:var(--text-muted);">' + escapeHtml(fmtExpiry(l.expiry)) + '</span>'
                + '<span style="margin-left:auto;">' + l.lots + ' lot' + (l.lots === 1 ? '' : 's') + ' @ ~' + Number(l.price || 0).toFixed(2) + '</span></div>';
        }).join('');
        var INPUT = 'color-scheme:dark;margin-top:6px;width:100%;font-family:var(--font-mono);font-size:0.82rem;padding:6px 8px;background:var(--bg-primary);color:var(--text-primary);border:1px solid var(--border);border-radius:6px;';
        var LABEL = 'font-family:var(--font-mono);font-size:0.7rem;letter-spacing:0.06em;color:var(--text-muted);text-transform:uppercase;';
        var body = '<div>Places these at market through the position manager. A BUY on a contract you are short exits that leg; a SELL on a contract you hold as a hedge exits it. LIVE rows are untouched.</div>'
            + '<div style="margin-top:10px;">' + list + '</div>'
            + '<div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;">'
            + '<div style="margin-top:12px;"><label style="' + LABEL + '">Order type</label><br><select id="simExecProduct" style="' + INPUT + '">'
            + '<option value="INTRADAY"' + (defMargin ? '' : ' selected') + ' style="' + OPT + '">INTRADAY</option>'
            + '<option value="MARGIN"' + (defMargin ? ' selected' : '') + ' style="' + OPT + '">OVERNIGHT</option></select></div>'
            + (hasSell ? '<div style="margin-top:12px;"><label style="' + LABEL + '">SL % of entry (0 = no SL)</label><br><input id="simExecSl" type="number" min="0" step="5" value="' + defSl + '" style="' + INPUT + '"></div>' : '')
            + '</div>';
        showConfirm({
            width: 640, title: 'Execute ' + rows.length + ' SIM row' + (rows.length === 1 ? '' : 's') + '?', bodyHtml: body, confirmLabel: 'Place orders',
            onConfirm: function () {
                var prod = document.getElementById('simExecProduct');
                var slEl = document.getElementById('simExecSl');
                var sl = slEl ? parseFloat(slEl.value) : -1;
                var q = '?product=' + encodeURIComponent(prod ? prod.value : '') + '&slPct=' + (sl >= 0 ? sl : -1);
                api('/sim/execute' + q, { method: 'POST', headers: csrfHeaders(), body: JSON.stringify({ legs: payloadLegs() }) })
                    .then(function (j) {
                        toast((j && j.message) || 'Done', j && j.success ? 'success' : 'error');
                        var results = (j && j.results) || [];
                        results.forEach(function (r) { if (!r.ok) toast(r.label + ': ' + r.code, 'error'); });
                        // Drop the SIM rows that went through; the next LOAD picks them up as LIVE.
                        var okLabels = {}; results.forEach(function (r) { if (r.ok) okLabels[r.label] = true; });
                        legs = legs.filter(function (l) {
                            if (l.tag !== 'SIM' || !l.include) return true;
                            var label = l.side + ' ' + l.lots + ' lot ' + l.type + ' ' + l.strike + ' ' + (l.expiry || '');
                            return !okLabels[label];
                        });
                        if (typeof loadDashboard === 'function') loadDashboard();
                        setTimeout(function () { api('/sim/context').then(function (c) { if (c && !c.error) { ctx = c; loadPositions(); } }); }, 1500);
                    })
                    .catch(function (e) { toast('Execute failed: ' + (e.message || e), 'error'); });
            }
        });
    }
    window.simExecute = execute;

    // ── metrics ───────────────────────────────────────────────────────────────
    function tile(id, text, cls, title) {
        var el = document.getElementById(id); if (!el) return;
        el.textContent = text; el.className = 'stat-value ' + (cls || ''); el.style.fontSize = '0.98rem';
        if (title != null) el.title = title;
    }
    function renderMetrics(res) {
        if (res.empty) {
            ['simPop', 'simMaxProfit', 'simMaxLoss', 'simBreakeven', 'simNetCredit', 'simRr', 'simExpected', 'simDelta', 'simTheta', 'simVega', 'simRange'].forEach(function (id) { tile(id, '—', '', ''); });
            document.getElementById('simExpiryNote').textContent = '';
            return;
        }
        tile('simPop', res.pop == null ? '—' : Number(res.pop).toFixed(0) + '%', res.pop >= 50 ? 'pnl-positive' : (res.pop == null ? '' : 'pnl-negative'),
             'Probability the expiry P&L is above zero (lognormal, σ ' + (res.sigmaEval ? (res.sigmaEval * 100).toFixed(1) + '%' : '—') + ')');
        tile('simMaxProfit', res.maxProfitUnlimited ? 'Unlimited' : fmtRs(res.maxProfit), 'pnl-positive', '');
        tile('simMaxLoss', res.maxLossUnlimited ? 'Unlimited' : fmtRs(res.maxLoss), 'pnl-negative', '');
        var be = (res.breakevens || []).map(function (b) { return fmtN(b, 0); });
        tile('simBreakeven', be.length ? be.join(' / ') : '—', '', 'Expiry breakeven price(s)');
        tile('simNetCredit', fmtRs(res.netCredit), res.netCredit >= 0 ? 'pnl-positive' : 'pnl-negative', res.netCredit >= 0 ? 'Net credit received' : 'Net debit paid');
        var rr = (!res.maxProfitUnlimited && !res.maxLossUnlimited && res.maxLoss < 0 && res.maxProfit > 0) ? '1 : ' + (res.maxProfit / Math.abs(res.maxLoss)).toFixed(2) : '—';
        tile('simRr', rr, '', 'Risk : reward — risk 1 to make ' + (rr === '—' ? '?' : rr.slice(4)) + ' (max loss vs max profit)');
        tile('simExpected', res.expectedPnl == null ? '—' : fmtRs(res.expectedPnl), res.expectedPnl > 0 ? 'pnl-positive' : (res.expectedPnl < 0 ? 'pnl-negative' : ''), 'Probability-weighted expiry P&L');
        var g = res.greeks || {};
        tile('simDelta', fmtN(g.delta, 1), '', 'Net delta (units) · gamma ' + (g.gamma == null ? '—' : g.gamma));
        tile('simTheta', g.theta == null ? '—' : fmtRs(g.theta), g.theta > 0 ? 'pnl-positive' : (g.theta < 0 ? 'pnl-negative' : ''), 'Net theta per day');
        tile('simVega', g.vega == null ? '—' : fmtRs(g.vega), g.vega > 0 ? 'pnl-positive' : (g.vega < 0 ? 'pnl-negative' : ''), 'Net vega per 1 % IV');
        var b = res.bands || {};
        tile('simRange', b.lo1 ? fmtN(b.lo1, 0) + ' – ' + fmtN(b.hi1, 0) : '—', '', '±1σ expected range at the nearest expiry · ±2σ ' + (b.lo2 ? fmtN(b.lo2, 0) + ' – ' + fmtN(b.hi2, 0) : '—'));
        document.getElementById('simExpiryNote').textContent = 'expiry ' + fmtExpiry(res.nearestExpiry) + ' · ' + res.daysToExpiry + 'd · T+' + params.targetDays + (params.spotShiftPct ? ' · spot ' + (params.spotShiftPct > 0 ? '+' : '') + params.spotShiftPct + '%' : '') + (params.ivShiftPts ? ' · IV ' + (params.ivShiftPts > 0 ? '+' : '') + params.ivShiftPts : '');
    }

    // ── sliders ───────────────────────────────────────────────────────────────
    function renderSliders(res) {
        var td = document.getElementById('simTargetDays');
        var max = Math.max(0, Number(res.daysToExpiry || 0));
        td.max = max; if (params.targetDays > max) { params.targetDays = max; td.value = max; }
        document.getElementById('simTargetLabel').textContent = params.targetDays === 0 ? 'today (T+0)' : fmtExpiry(addDays(todayIso(), params.targetDays)) + ' (T+' + params.targetDays + ')';
        document.getElementById('simSpotLabel').textContent = (params.spotShiftPct > 0 ? '+' : '') + params.spotShiftPct.toFixed(1) + '% → ' + fmtN(res.spotEval, 0);
        document.getElementById('simIvLabel').textContent = (params.ivShiftPts > 0 ? '+' : '') + params.ivShiftPts.toFixed(1) + ' pts' + (res.sigmaEval ? ' → ' + (res.sigmaEval * 100).toFixed(1) + '%' : '');
    }
    window.simSlider = function (which, value) {
        if (which === 'target') params.targetDays = parseInt(value, 10) || 0;
        else if (which === 'spot') params.spotShiftPct = parseFloat(value) || 0;
        else if (which === 'iv') params.ivShiftPts = parseFloat(value) || 0;
        recompute();
    };
    window.simResetSliders = function () {
        params = { targetDays: 0, spotShiftPct: 0, ivShiftPts: 0 };
        document.getElementById('simTargetDays').value = 0;
        document.getElementById('simSpotShift').value = 0;
        document.getElementById('simIvShift').value = 0;
        recompute();
    };

    // ── chart ─────────────────────────────────────────────────────────────────
    var markerPlugin = {
        id: 'simMarkers',
        afterDraw: function (c) {
            var res = c.$simResult; if (!res || res.empty) return;
            var g = c.ctx, x = c.scales.x, y = c.scales.y, area = c.chartArea;
            g.save();
            function vline(v, color, dash, label) {
                if (v == null || v < x.min || v > x.max) return;
                var px = x.getPixelForValue(v);
                g.beginPath(); g.setLineDash(dash || []); g.strokeStyle = color; g.lineWidth = 1;
                g.moveTo(px, area.top); g.lineTo(px, area.bottom); g.stroke();
                if (label) { g.setLineDash([]); g.fillStyle = color; g.font = '10px ' + (getComputedStyle(document.body).getPropertyValue('--font-mono') || 'monospace'); g.textAlign = 'center'; g.fillText(label, px, area.top + 10); }
            }
            var b = res.bands || {};
            if (b.lo2 && b.hi2) { g.fillStyle = 'rgba(125,211,252,0.05)'; g.fillRect(x.getPixelForValue(Math.max(b.lo2, x.min)), area.top, x.getPixelForValue(Math.min(b.hi2, x.max)) - x.getPixelForValue(Math.max(b.lo2, x.min)), area.bottom - area.top); }
            if (b.lo1 && b.hi1) { g.fillStyle = 'rgba(125,211,252,0.08)'; g.fillRect(x.getPixelForValue(Math.max(b.lo1, x.min)), area.top, x.getPixelForValue(Math.min(b.hi1, x.max)) - x.getPixelForValue(Math.max(b.lo1, x.min)), area.bottom - area.top); }
            vline(res.spotEval, 'rgba(125,211,252,0.9)', [4, 3], 'spot ' + fmtN(res.spotEval, 0));
            (res.breakevens || []).forEach(function (be) { vline(be, 'rgba(250,204,21,0.85)', [2, 3], 'BE ' + fmtN(be, 0)); });
            if (y.min < 0 && y.max > 0) { var py = y.getPixelForValue(0); g.setLineDash([]); g.strokeStyle = 'rgba(148,163,184,0.5)'; g.beginPath(); g.moveTo(area.left, py); g.lineTo(area.right, py); g.stroke(); }
            g.restore();
        }
    };
    function renderChart(res) {
        var canvas = document.getElementById('simChartCanvas');
        var curve = (res.curve || []);
        var expPts = curve.map(function (p) { return { x: p.s, y: p.e }; });
        var tgtPts = curve.map(function (p) { return { x: p.s, y: p.t }; });
        if (!chart) {
            chart = new Chart(canvas.getContext('2d'), {
                type: 'line',
                data: { datasets: [
                    { label: 'At expiry', data: expPts, borderColor: 'rgba(226,232,240,0.95)', borderWidth: 2, pointRadius: 0, tension: 0,
                      fill: { target: 'origin', above: 'rgba(52,211,153,0.18)', below: 'rgba(248,113,113,0.18)' } },
                    { label: 'T+n', data: tgtPts, borderColor: 'rgba(125,211,252,0.95)', borderDash: [6, 4], borderWidth: 1.5, pointRadius: 0, tension: 0.2, fill: false }
                ] },
                options: {
                    animation: false, responsive: true, maintainAspectRatio: false, parsing: false,
                    interaction: { mode: 'nearest', axis: 'x', intersect: false },
                    scales: {
                        x: { type: 'linear', ticks: { color: '#94a3b8', font: { size: 10 }, callback: function (v) { return fmtN(v, 0); } }, grid: { color: 'rgba(148,163,184,0.08)' } },
                        y: { ticks: { color: '#94a3b8', font: { size: 10 }, callback: function (v) { return fmtN(v, 0); } }, grid: { color: 'rgba(148,163,184,0.08)' } }
                    },
                    plugins: {
                        legend: { labels: { color: '#94a3b8', font: { size: 10 }, boxWidth: 18 } },
                        tooltip: { callbacks: {
                            title: function (items) { return items.length ? 'at ' + fmtN(items[0].parsed.x, 0) : ''; },
                            label: function (it) { return it.dataset.label + ': ' + fmtRs(it.parsed.y); }
                        } }
                    }
                },
                plugins: [markerPlugin]
            });
        } else {
            chart.data.datasets[0].data = expPts;
            chart.data.datasets[1].data = tgtPts;
        }
        chart.$simResult = res;
        chart.options.scales.x.min = res.gridLo; chart.options.scales.x.max = res.gridHi;
        chart.update();
    }

    window.openSimulator = open;
    window.closeSimulator = close;
    window.simLoadPositions = loadPositions;
    window.simAddLeg = addLeg;
    window.simClear = clearLegs;
})();
