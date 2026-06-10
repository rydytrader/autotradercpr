/**
 * Per-instance settings modal — opened from the ⚙ Settings button on each straddle's
 * dashboard page. Schema-driven: fetches /api/strategies/{id}/settings/schema +
 * /settings, renders the form, POSTs back on save. The same field types and styling as
 * the legacy strategy tabs in the global Settings modal.
 *
 * Usage: StrategySettings.openFor("inst-7");
 */
(function() {
    var overlayEl = null;
    var currentId = null;
    var schemaById = {};

    function ensureBuilt() {
        if (overlayEl) return overlayEl;
        var html =
            '<div id="ssOverlay" style="display:none;position:fixed;inset:0;background:rgba(0,0,0,0.6);z-index:1000;align-items:center;justify-content:center;">' +
              '<div style="background:var(--bg-card);border:1px solid var(--border);border-radius:12px;width:740px;max-width:94vw;max-height:88vh;display:flex;flex-direction:column;box-shadow:0 16px 48px rgba(0,0,0,0.3);">' +
                '<div style="display:flex;align-items:center;justify-content:space-between;padding:18px 24px;border-bottom:1px solid var(--border);">' +
                  '<div style="font-family:var(--font-mono);font-size:0.92rem;font-weight:700;color:var(--text-primary);" id="ssTitle">⚙ Settings</div>' +
                  '<button onclick="StrategySettings.close()" style="background:transparent;border:none;color:var(--text-muted);font-size:1.5rem;cursor:pointer;line-height:1;padding:0 4px;">&times;</button>' +
                '</div>' +
                '<div id="ssTabstrip" style="display:flex;border-bottom:1px solid var(--border);padding:0 24px;"></div>' +
                '<div style="flex:1;overflow-y:auto;padding:20px 24px;" id="ssBody"></div>' +
                '<div style="display:flex;justify-content:space-between;align-items:center;gap:10px;padding:16px 24px;border-top:1px solid var(--border);">' +
                  '<div id="ssBanner" style="font-family:var(--font-mono);font-size:0.78rem;flex:1;"></div>' +
                  '<div style="display:flex;gap:10px;">' +
                    '<button class="ss-btn-secondary" onclick="StrategySettings.close()" style="background:transparent;border:1px solid var(--border);color:var(--text-secondary);padding:8px 18px;border-radius:6px;font-family:var(--font-mono);font-size:0.74rem;cursor:pointer;">Close</button>' +
                    '<button class="ss-btn-primary" onclick="StrategySettings.save()" id="ssSaveBtn" style="background:rgba(52,211,153,0.12);border:1px solid rgba(52,211,153,0.4);color:var(--accent-green);padding:8px 18px;border-radius:6px;font-family:var(--font-mono);font-size:0.74rem;font-weight:700;cursor:pointer;">✓ Save</button>' +
                  '</div>' +
                '</div>' +
              '</div>' +
            '</div>';
        var wrapper = document.createElement('div');
        wrapper.innerHTML = html;
        document.body.appendChild(wrapper.firstChild);
        overlayEl = document.getElementById('ssOverlay');
        var style = document.createElement('style');
        style.textContent =
            '.ss-regular-grid { display:grid;grid-template-columns:1fr 1fr;gap:0 18px; }' +
            '.ss-field { margin-bottom:14px;font-family:var(--font-mono);font-size:0.78rem; }' +
            '.ss-field label { display:block;color:var(--text-muted);font-size:0.7rem;letter-spacing:0.06em;text-transform:uppercase;margin-bottom:6px; }' +
            '.ss-field input { width:100%;padding:8px 12px;border-radius:6px;border:1px solid var(--border);background-color:var(--bg-primary);color:var(--text-primary);font-family:var(--font-mono);font-size:0.82rem;outline:none; }' +
            // Matches the event-log dropdowns (eventLogStrategy / eventLogSeverity): bg-card
            // background with a two-gradient chevron arrow, appearance:none. Cross-browser
            // consistent — no SVG data URL, no color-scheme dependency.
            '.ss-field select { width:100%;padding:8px 24px 8px 12px;border-radius:6px;border:1px solid var(--border);background-color:var(--bg-card);color:var(--text-primary);font-family:var(--font-mono);font-size:0.82rem;outline:none;cursor:pointer;appearance:none;-webkit-appearance:none;background-image:linear-gradient(45deg, transparent 50%, var(--text-muted) 50%), linear-gradient(135deg, var(--text-muted) 50%, transparent 50%);background-position:calc(100% - 12px) 50%, calc(100% - 7px) 50%;background-size:5px 5px, 5px 5px;background-repeat:no-repeat; }' +
            '.ss-field select option { background-color:var(--bg-card);color:var(--text-primary); }' +
            '.ss-field input[type=checkbox] { width:auto; }' +
            '.ss-hint { color:var(--text-muted);font-size:0.7rem;margin-top:4px; }' +
            '.ss-section-title { font-family:var(--font-mono);font-size:0.72rem;font-weight:700;letter-spacing:0.08em;text-transform:uppercase;color:var(--text-secondary);margin:22px 0 6px;padding-top:14px;border-top:1px solid var(--border); }' +
            '.ss-day-grid { display:flex;flex-direction:column;font-family:var(--font-mono);font-size:0.78rem;margin-top:10px; }' +
            '.ss-day-header, .ss-day-row { display:grid;grid-template-columns:1fr 80px 90px 100px;gap:12px;align-items:center;padding:8px 4px;border-bottom:1px solid var(--border); }' +
            '.ss-day-header { font-size:0.66rem;color:var(--text-muted);text-transform:uppercase;letter-spacing:0.06em; }' +
            '.ss-day-name { color:var(--text-primary); }' +
            '.ss-day-row input[type=checkbox] { width:18px;height:18px;justify-self:start;margin:0; }' +
            '.ss-day-row input[type=number] { width:100%;padding:6px 10px;border-radius:6px;border:1px solid var(--border);background:var(--bg-primary);color:var(--text-primary);font-family:var(--font-mono);font-size:0.78rem;outline:none;text-align:right; }' +
            '.ss-tab { background:transparent;border:none;color:var(--text-secondary);padding:12px 18px;font-family:var(--font-mono);font-size:0.74rem;font-weight:600;letter-spacing:0.06em;cursor:pointer;border-bottom:2px solid transparent;white-space:nowrap; }' +
            '.ss-tab.active { color:var(--text-primary);border-bottom-color:var(--accent-cyan); }' +
            '.ss-tab:hover { color:var(--text-primary); }' +
            '.ss-pane { display:none; }' +
            '.ss-pane.active { display:block; }';
        document.head.appendChild(style);
        return overlayEl;
    }

    function csrf() {
        try { return (window.csrfHeaders && window.csrfHeaders()) || {}; } catch (e) { return {}; }
    }

    function openFor(strategyId) {
        if (!strategyId) return;
        ensureBuilt();
        currentId = strategyId;
        overlayEl.style.display = 'flex';
        var body = document.getElementById('ssBody');
        body.innerHTML = '<div style="color:var(--text-muted);padding:14px;">Loading…</div>';
        clearBanner();
        fetch('/api/strategies/' + encodeURIComponent(strategyId) + '/settings/schema')
            .then(function(r) { return r.json(); })
            .then(function(schema) {
                schemaById[strategyId] = schema;
                renderFields(schema);
                loadValues(strategyId, schema);
            })
            .catch(function() { body.innerHTML = '<div style="color:var(--accent-red);padding:14px;">Failed to load schema.</div>'; });
    }

    function renderFieldHtml(f) {
        var fieldId = 'ss-' + f.key;
        var input = '';
        switch (f.type) {
            case 'time':    input = '<input type="time" id="' + fieldId + '" step="60">'; break;
            case 'int':
            case 'percent': input = '<input type="number" id="' + fieldId + '" step="1" min="0">'; break;
            case 'rupees':  input = '<input type="number" id="' + fieldId + '" step="500" min="0">'; break;
            case 'double':  input = '<input type="number" id="' + fieldId + '" step="0.01">'; break;
            case 'boolean': input = '<input type="checkbox" id="' + fieldId + '">'; break;
            case 'select':
                var opts = (f.options || []).map(function(o) {
                    return '<option value="' + escapeHtml(o) + '">' + escapeHtml(o) + '</option>';
                }).join('');
                input = '<select id="' + fieldId + '">' + opts + '</select>';
                break;
            default:        input = '<input type="text" id="' + fieldId + '">';
        }
        // Wide fields span both grid columns — used for fields whose hint runs long enough
        // that a 1-column wrap reads awkwardly. The schema marks these with {wide: true}.
        var wideAttr = f.wide ? ' style="grid-column:1 / -1;"' : '';
        var s = '<div class="ss-field"' + wideAttr + '><label>' + escapeHtml(f.label || f.key) + '</label>' + input;
        if (f.hint) s += '<div class="ss-hint">' + escapeHtml(f.hint) + '</div>';
        s += '</div>';
        return s;
    }

    function renderFields(schema) {
        // Two-tab layout: Basic (entry/order placement) and Risk (per-leg SL / move-to-cost
        // / per-DTE rows). Field's tab attribute decides bucket; missing tab falls back to
        // basic so older schemas keep rendering. DTE rows always live under Risk.
        var basicFields = [];
        var riskFields  = [];
        var dteMap = {};
        var dteOrder = [];
        schema.forEach(function(f) {
            var m = /^dte\.(\d)\.(enabled|legSlPct|legSlPoints)$/.exec(f.key);
            if (m) {
                var n = m[1];
                if (!dteMap[n]) { dteMap[n] = {}; dteOrder.push(n); }
                dteMap[n][m[2]] = f;
                return;
            }
            if (f.tab === 'risk') riskFields.push(f);
            else                  basicFields.push(f);
        });

        function gridOf(fields) {
            if (!fields.length) return '';
            return '<div class="ss-regular-grid">' + fields.map(renderFieldHtml).join('') + '</div>';
        }

        var basicHtml = gridOf(basicFields)
            || '<div style="color:var(--text-muted);font-style:italic;padding:14px 0;">No basic settings.</div>';

        var riskHtml = gridOf(riskFields);
        if (dteOrder.length > 0) {
            riskHtml += '<div class="ss-section-title">Days to Expiry · enable + per-leg SL</div>';
            riskHtml += '<div class="ss-day-grid">';
            riskHtml += '<div class="ss-day-header"><span>DTE</span><span>Enable</span><span>SL %</span><span>SL Points</span></div>';
            dteOrder.forEach(function(n) {
                var pair = dteMap[n];
                var enF = pair.enabled, slF = pair.legSlPct, ptF = pair.legSlPoints;
                if (!enF || !slF) return;
                riskHtml +=
                    '<div class="ss-day-row">' +
                        '<span class="ss-day-name">' + escapeHtml(n) + ' DTE</span>' +
                        '<input type="checkbox" id="ss-' + escapeHtml(enF.key) + '">' +
                        '<input type="number" id="ss-' + escapeHtml(slF.key) + '" step="1" min="0">' +
                        (ptF
                            ? '<input type="number" id="ss-' + escapeHtml(ptF.key) + '" step="0.05" min="0" title="Direct premium points (takes precedence over SL % when > 0)">'
                            : '<span style="color:var(--text-muted);">—</span>') +
                    '</div>';
            });
            riskHtml += '</div>';
        }
        if (!riskHtml) riskHtml = '<div style="color:var(--text-muted);font-style:italic;padding:14px 0;">No risk settings.</div>';

        // Tab strip — buttons wire up in switchTab below.
        var strip = document.getElementById('ssTabstrip');
        strip.innerHTML =
            '<button class="ss-tab active" data-tab="basic">BASIC</button>' +
            '<button class="ss-tab" data-tab="risk">RISK</button>';
        strip.querySelectorAll('.ss-tab').forEach(function(b) {
            b.addEventListener('click', function() { switchTab(b.getAttribute('data-tab')); });
        });
        document.getElementById('ssBody').innerHTML =
            '<div class="ss-pane active" data-pane="basic">' + basicHtml + '</div>' +
            '<div class="ss-pane"        data-pane="risk">'  + riskHtml  + '</div>';
    }

    function switchTab(tab) {
        var strip = document.getElementById('ssTabstrip');
        strip.querySelectorAll('.ss-tab').forEach(function(b) {
            b.classList.toggle('active', b.getAttribute('data-tab') === tab);
        });
        document.getElementById('ssBody').querySelectorAll('.ss-pane').forEach(function(p) {
            p.classList.toggle('active', p.getAttribute('data-pane') === tab);
        });
    }

    function loadValues(strategyId, schema) {
        fetch('/api/strategies/' + encodeURIComponent(strategyId) + '/settings')
            .then(function(r) { return r.json(); })
            .then(function(values) {
                schema.forEach(function(f) {
                    var el = document.getElementById('ss-' + f.key);
                    if (!el) return;
                    var v = values && values[f.key] != null ? values[f.key] : f.default;
                    if (f.type === 'boolean') el.checked = (v === true || v === 'true');
                    else                      el.value = v == null ? '' : v;
                });
            })
            .catch(function() {});
    }

    function save() {
        var strategyId = currentId;
        var schema = schemaById[strategyId];
        if (!schema) return;
        var body = {};
        schema.forEach(function(f) {
            var el = document.getElementById('ss-' + f.key);
            if (!el) return;
            if (f.type === 'boolean') body[f.key] = el.checked;
            else if (f.type === 'int' || f.type === 'percent') body[f.key] = parseInt(el.value, 10) || 0;
            else if (f.type === 'rupees' || f.type === 'double') body[f.key] = parseFloat(el.value) || 0;
            else body[f.key] = (el.value || '').trim();
        });
        var btn = document.getElementById('ssSaveBtn');
        if (btn) { btn.disabled = true; btn.textContent = 'Saving…'; }
        clearBanner();
        fetch('/api/strategies/' + encodeURIComponent(strategyId) + '/settings', {
            method: 'POST',
            headers: Object.assign({ 'Content-Type': 'application/json' }, csrf()),
            body: JSON.stringify(body)
        }).then(function(r) { return r.json(); }).then(function(d) {
            if (d && d.success) showBanner('✓ Settings saved', 'success');
            else                showBanner('✗ ' + ((d && d.message) || 'Save failed'), 'error');
        }).catch(function(err) { showBanner('✗ Save failed: ' + (err.message || err), 'error'); })
          .finally(function() { if (btn) { btn.disabled = false; btn.textContent = '✓ Save'; } });
    }

    function close_() {
        if (overlayEl) overlayEl.style.display = 'none';
    }

    function showBanner(msg, kind) {
        var el = document.getElementById('ssBanner');
        if (!el) return;
        el.textContent = msg;
        el.style.color = kind === 'success' ? 'var(--accent-green)'
                       : kind === 'error'   ? 'var(--accent-red, #f87171)'
                       : 'var(--text-secondary)';
        clearTimeout(showBanner._t);
        showBanner._t = setTimeout(clearBanner, 4000);
    }
    function clearBanner() {
        var el = document.getElementById('ssBanner');
        if (el) el.textContent = '';
    }
    function escapeHtml(s) {
        if (s == null) return '';
        return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
    }

    window.StrategySettings = {
        openFor: openFor,
        close: close_,
        save: save
    };
})();
