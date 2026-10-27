/**
 * Settings modal — opened from the gear icon in the navbar.
 *
 * Tabs: POSITIONS (instances, one per underlying) · PORTFOLIO RISK · CHARGES · DATA · USERS.
 * Per-instance trading settings (entry time, lots, SL%) no longer live here — they're
 * opened from each instance's dashboard page via the ⚙ Settings button.
 */
(function() {
    var modalEl = null;
    var activeTab = null;
    var strategiesList = [];     // [{id, displayName, shortCode, navIcon, currentState}]
    var straddles      = [];     // [{id, name, description, shortCode, navIcon, enabled, currentState}]
    var strangles      = [];     // [{id, name, description, shortCode, navIcon, enabled, currentState}]

    function ensureBuilt() {
        if (modalEl) return modalEl;
        var html =
            '<div id="settingsModalOverlay" style="display:none;position:fixed;inset:0;background:rgba(0,0,0,0.6);z-index:999;align-items:center;justify-content:center;">' +
              '<div style="background:var(--bg-card);border:1px solid var(--border);border-radius:12px;width:720px;max-width:94vw;max-height:88vh;display:flex;flex-direction:column;box-shadow:0 16px 48px rgba(0,0,0,0.3);">' +
                // Header
                '<div style="display:flex;align-items:center;justify-content:space-between;padding:18px 24px;border-bottom:1px solid var(--border);">' +
                  '<div style="font-family:var(--font-mono);font-size:0.92rem;font-weight:700;color:var(--text-primary);">⚙ Settings</div>' +
                  '<button onclick="SettingsModal.close()" style="background:transparent;border:none;color:var(--text-muted);font-size:1.5rem;cursor:pointer;line-height:1;padding:0 4px;">&times;</button>' +
                '</div>' +
                // Tab strip — populated dynamically on first open
                '<div id="sm-tabstrip" style="display:flex;border-bottom:1px solid var(--border);padding:0 24px;overflow-x:auto;"></div>' +
                // Body (each tab pane scrolls independently)
                '<div class="sm-body" id="sm-body" style="flex:1;overflow-y:auto;padding:20px 24px;">' +
                  // Positions tab — one instance per underlying
                  '<div class="sm-pane" data-pane="positions" style="display:none;">' +
                    '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:14px;gap:12px;">' +
                      '<div class="sm-hint" style="margin:0;">Each row is a positions page with its own book, P&L and calendar history. The underlying is what tells them apart.</div>' +
                      '<div style="display:flex;align-items:center;gap:8px;white-space:nowrap;">' +
                        '<select id="sm-positions-new-underlying" style="background:var(--bg-card);border:1px solid var(--border);color:var(--text-primary);padding:6px 8px;border-radius:6px;font-family:var(--font-mono);font-size:0.76rem;font-weight:700;outline:none;">' +
                          '<option value="NIFTY" style="background:var(--bg-card);color:var(--text-primary);">NIFTY</option>' +
                          '<option value="SENSEX" style="background:var(--bg-card);color:var(--text-primary);">SENSEX</option>' +
                        '</select>' +
                        '<button class="sm-btn-primary" onclick="SettingsModal.addInstance()">+ Add</button>' +
                      '</div>' +
                    '</div>' +
                    '<div id="sm-positions-list" style="font-family:var(--font-mono);font-size:0.78rem;">Loading…</div>' +
                  '</div>' +
                  // Portfolio Risk tab (static)
                  '<div class="sm-pane" data-pane="portfolio-risk" style="display:none;">' +
                    '<div class="sm-field"><label>Initial Capital (₹)</label><input type="number" id="sm-startingCapital" step="1000" min="0"><div class="sm-hint">Baseline used by the Home analytics page (capital growth %, equity curve, return %). Default ₹10,00,000.</div></div>' +
                    '<div class="sm-field"><label>Portfolio Max Daily Risk (%)</label><input type="number" id="sm-portfolioMaxRiskPct" step="0.1" min="0"><div class="sm-hint">Global kill switch trigger. When aggregate net day P&L across all strategies drops below this % of Initial Capital, every strategy is flattened. 0 disables.</div></div>' +
                    '<div class="sm-field"><label>Intraday Squareoff Time (HH:mm)</label><input type="time" id="sm-intradaySquareOffTime"><div class="sm-hint">Positions pages close every INTRADAY (MIS) leg at this time. Positional (NRML) legs are carried to expiry. Default 15:20.</div></div>' +
                    '<div class="sm-field"><label>Max Portfolio Risk (₹)</label><div class="sm-readonly" id="sm-portfolioMaxRiskRupees">—</div><div class="sm-hint">Auto-calculated from Initial Capital × Daily Risk %. This is the sole rupee loss threshold across the whole book — when aggregate (consumed risk + open MTM − charges) across every enabled strategy breaches this, all positions are flattened.</div></div>' +
                  '</div>' +
                  '<div class="sm-pane" data-pane="charges" style="display:none;">' +
                    '<div class="sm-field"><label>Brokerage per Order (₹)</label><input type="number" id="sm-brokeragePerOrder" step="1" min="0"><div class="sm-hint">Flat per-order brokerage. Drives charge estimates on every dashboard + session row.</div></div>' +
                    '<div class="sm-field"><label>STT Rate (%)</label><input type="number" id="sm-sttRate" step="0.0001" min="0"></div>' +
                    '<div class="sm-field"><label>NSE Exchange Transaction Charge (%)</label><input type="number" id="sm-exchangeRate" step="0.0001" min="0"><div class="sm-hint">Applied when the strategy trades NIFTY (NSE F&amp;O).</div></div>' +
                    '<div class="sm-field"><label>BSE Exchange Transaction Charge (%)</label><input type="number" id="sm-bseExchangeRate" step="0.0001" min="0"><div class="sm-hint">Applied when the strategy trades SENSEX (BSE F&amp;O).</div></div>' +
                    '<div class="sm-field"><label>GST Rate (%)</label><input type="number" id="sm-gstRate" step="0.01" min="0"></div>' +
                    '<div class="sm-field"><label>SEBI Turnover Fee (₹ per crore)</label><input type="number" id="sm-sebiRate" step="0.1" min="0"></div>' +
                    '<div class="sm-field"><label>Stamp Duty Rate (%)</label><input type="number" id="sm-stampDutyRate" step="0.0001" min="0"></div>' +
                  '</div>' +
                  // Data tab (static) — danger zone
                  '<div class="sm-pane" data-pane="data" style="display:none;">' +
                    '<div class="sm-hint" style="margin-bottom:14px;">Persisted trade history across <b>all</b> instances. Clearing wipes the analytics, calendar and home-page history. Today\'s running P&amp;L in memory is not touched and rolls over normally.</div>' +
                    '<div id="sm-data-stats" style="font-family:var(--font-mono);font-size:0.78rem;margin-bottom:18px;color:var(--text-primary);">Loading…</div>' +
                    '<div style="padding:14px;border:1px solid rgba(248,113,113,0.45);border-radius:8px;background:rgba(248,113,113,0.06);">' +
                      '<div style="font-family:var(--font-mono);font-size:0.84rem;font-weight:700;margin-bottom:8px;color:var(--accent-red, #f87171);">Clear all trade history</div>' +
                      '<div class="sm-hint" style="margin-bottom:10px;">Deletes every row in strategy_trades and strategy_sessions. Cannot be undone. Refused while any instance has an open leg. Type <b>DELETE</b> to enable the button.</div>' +
                      '<div style="display:flex;gap:8px;align-items:center;">' +
                        '<input type="text" id="sm-data-confirm" placeholder="DELETE" autocomplete="off" style="max-width:160px;" oninput="SettingsModal.onClearConfirmInput()">' +
                        '<button class="sm-btn-primary" id="sm-data-clear-btn" disabled style="background:var(--accent-red, #f87171);border-color:var(--accent-red, #f87171);" onclick="SettingsModal.clearTradeData()">Clear trade history</button>' +
                      '</div>' +
                    '</div>' +
                  '</div>' +
                  // Users tab (static, admin only)
                  '<div class="sm-pane" data-pane="users" style="display:none;">' +
                    '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:14px;">' +
                      '<div class="sm-hint" style="margin:0;">App users with login access. Trader = full admin. Observer = read-only.</div>' +
                      '<button class="sm-btn-primary" onclick="SettingsModal.showUserForm()">+ Add User</button>' +
                    '</div>' +
                    '<div id="sm-users-list" style="font-family:var(--font-mono);font-size:0.78rem;">Loading…</div>' +
                    '<div id="sm-user-form" style="display:none;margin-top:18px;padding:14px;border:1px solid var(--border);border-radius:8px;background:var(--bg-primary);">' +
                      '<div style="font-family:var(--font-mono);font-size:0.84rem;font-weight:700;margin-bottom:10px;color:var(--text-primary);" id="sm-user-form-title">Add User</div>' +
                      '<input type="hidden" id="sm-user-id">' +
                      '<div class="sm-field"><label>Email</label><input type="email" id="sm-user-email"></div>' +
                      '<div class="sm-field"><label>First Name</label><input type="text" id="sm-user-fname"></div>' +
                      '<div class="sm-field"><label>Last Name</label><input type="text" id="sm-user-lname"></div>' +
                      '<div class="sm-field"><label>Role</label><select id="sm-user-role"><option value="ROLE_ADMIN">Trader (admin)</option><option value="ROLE_VIEWER">Observer (read-only)</option></select></div>' +
                      '<div class="sm-field"><label>Password <span style="font-size:0.66rem;color:var(--text-muted);">(blank to keep)</span></label><input type="password" id="sm-user-password"></div>' +
                      '<div style="display:flex;justify-content:flex-end;gap:8px;margin-top:12px;">' +
                        '<button class="sm-btn-secondary" onclick="SettingsModal.cancelUserForm()">Cancel</button>' +
                        '<button class="sm-btn-primary" onclick="SettingsModal.saveUser()">Save User</button>' +
                      '</div>' +
                    '</div>' +
                  '</div>' +
                '</div>' +
                // Footer
                '<div style="display:flex;justify-content:space-between;align-items:center;gap:10px;padding:16px 24px;border-top:1px solid var(--border);">' +
                  '<div id="sm-banner" style="font-family:var(--font-mono);font-size:0.78rem;flex:1;"></div>' +
                  '<div style="display:flex;gap:10px;">' +
                    '<button class="sm-btn-secondary" onclick="SettingsModal.close()">Close</button>' +
                    '<button class="sm-btn-primary" onclick="SettingsModal.save()" id="sm-save-btn">✓ Save Settings</button>' +
                  '</div>' +
                '</div>' +
              '</div>' +
            '</div>';
        var wrapper = document.createElement('div');
        wrapper.innerHTML = html;
        document.body.appendChild(wrapper.firstChild);
        modalEl = document.getElementById('settingsModalOverlay');

        // Inline styles for tab buttons + fields (kept here so the modal is self-contained)
        var style = document.createElement('style');
        style.textContent =
            '.sm-tab { background:transparent;border:none;color:var(--text-secondary);padding:14px 18px;font-family:var(--font-mono);font-size:0.78rem;font-weight:600;cursor:pointer;border-bottom:2px solid transparent;white-space:nowrap; }' +
            '.sm-tab.active { color:var(--text-primary);border-bottom-color:var(--accent-cyan); }' +
            '.sm-tab:hover { color:var(--text-primary); }' +
            '.sm-field { margin-bottom:14px;font-family:var(--font-mono);font-size:0.78rem; }' +
            '.sm-field label { display:block;color:var(--text-muted);font-size:0.7rem;letter-spacing:0.06em;text-transform:uppercase;margin-bottom:6px; }' +
            '.sm-field input, .sm-field select { width:100%;padding:8px 12px;border-radius:6px;border:1px solid var(--border);background:var(--bg-primary);color:var(--text-primary);font-family:var(--font-mono);font-size:0.82rem;outline:none; }' +
            '.sm-field input[type=checkbox] { width:auto; }' +
            '.sm-readonly { width:100%;padding:8px 12px;border-radius:6px;border:1px dashed var(--border);background:var(--bg-card);color:var(--accent-cyan);font-family:var(--font-mono);font-size:0.82rem;font-weight:700;letter-spacing:0.04em; }' +
            '.sm-section-title { font-family:var(--font-mono);font-size:0.72rem;font-weight:700;letter-spacing:0.08em;text-transform:uppercase;color:var(--text-secondary);margin:22px 0 6px;padding-top:14px;border-top:1px solid var(--border); }' +
            '.sm-hint { color:var(--text-muted);font-size:0.7rem;margin-top:4px; }' +
            '.sm-btn-primary { background:rgba(52,211,153,0.12);border:1px solid rgba(52,211,153,0.4);color:var(--accent-green);padding:8px 18px;border-radius:6px;font-family:var(--font-mono);font-size:0.74rem;font-weight:700;cursor:pointer; }' +
            '.sm-btn-secondary { background:transparent;border:1px solid var(--border);color:var(--text-secondary);padding:8px 18px;border-radius:6px;font-family:var(--font-mono);font-size:0.74rem;cursor:pointer; }' +
            '.sm-user-row { display:flex;justify-content:space-between;align-items:center;padding:10px 12px;border:1px solid var(--border);border-radius:6px;margin-bottom:8px;background:var(--bg-primary); }' +
            '.sm-user-row button { background:transparent;border:1px solid var(--border);color:var(--text-muted);padding:3px 10px;border-radius:4px;font-family:var(--font-mono);font-size:0.66rem;cursor:pointer;margin-left:6px; }' +
            '.sm-body::-webkit-scrollbar { width: 4px; }' +
            '.sm-body::-webkit-scrollbar-thumb { background: var(--border); border-radius: 4px; }' +
            '.sm-body::-webkit-scrollbar-track { background: transparent; }' +
            '.sm-body { scrollbar-width: thin; scrollbar-color: var(--border) transparent; }';
        document.head.appendChild(style);

        return modalEl;
    }

    // ── Tab strip ─────────────────────────────────────────────────────────────
    function buildTabs() {
        var strip = document.getElementById('sm-tabstrip');
        if (!strip) return;
        var html = '';
        html += '<button class="sm-tab" data-tab="positions">POSITIONS</button>';
        html += '<button class="sm-tab" data-tab="portfolio-risk">PORTFOLIO RISK</button>';
        html += '<button class="sm-tab" data-tab="charges">CHARGES</button>';
        html += '<button class="sm-tab" data-tab="data">DATA</button>';
        html += '<button class="sm-tab" data-tab="users">USERS</button>';
        strip.innerHTML = html;
        strip.querySelectorAll('.sm-tab').forEach(function(b) {
            b.addEventListener('click', function() { switchTab(b.getAttribute('data-tab')); });
        });
    }

    function switchTab(tab) {
        activeTab = tab;
        var strip = document.getElementById('sm-tabstrip');
        if (strip) strip.querySelectorAll('.sm-tab').forEach(function(b) {
            b.classList.toggle('active', b.getAttribute('data-tab') === tab);
        });
        modalEl.querySelectorAll('.sm-pane').forEach(function(p) { p.style.display = 'none'; });
        if (tab === 'positions') {
            var pp0 = modalEl.querySelector('[data-pane="positions"]'); if (pp0) pp0.style.display = '';
            loadPositions();
        } else if (tab === 'portfolio-risk') {
            var pp = modalEl.querySelector('[data-pane="portfolio-risk"]'); if (pp) pp.style.display = '';
            loadPortfolioRiskValues();
        } else if (tab === 'charges') {
            var pane = modalEl.querySelector('[data-pane="charges"]'); if (pane) pane.style.display = '';
        } else if (tab === 'data') {
            var dp = modalEl.querySelector('[data-pane="data"]'); if (dp) dp.style.display = '';
            loadDataStats();
        } else if (tab === 'users') {
            var p2 = modalEl.querySelector('[data-pane="users"]'); if (p2) p2.style.display = '';
            loadUsers();
        }
    }

    // ── Save ─────────────────────────────────────────────────────────────────
    function saveSettings() {
        if (activeTab === 'portfolio-risk') return savePortfolioRiskTab();
        if (activeTab === 'charges')        return saveChargesTab();
        if (activeTab === 'positions')      { showBanner('Use the row controls — changes apply immediately.', 'info'); return; }
        if (activeTab === 'users')          { showBanner('Use the row buttons to manage users.', 'info'); return; }
        if (activeTab === 'data')           { showBanner('Nothing to save here — use the Clear button.', 'info'); return; }
        showBanner('No save action for this tab.', 'info');
    }

    // ── Data tab — trade-history maintenance ─────────────────────────────────
    function loadDataStats() {
        var el = document.getElementById('sm-data-stats');
        if (!el) return;
        el.textContent = 'Loading…';
        fetch('/api/trade-data/stats').then(function(r) { return r.json(); }).then(function(d) {
            var range = (d.firstDate && d.lastDate) ? (d.firstDate + ' → ' + d.lastDate) : '—';
            var open  = (d.openLegs && d.openLegs.length) ? d.openLegs.join(', ') : 'none';
            el.innerHTML =
                '<div>Trade rows: <b>' + (d.tradeRows || 0) + '</b></div>' +
                '<div>Session rows: <b>' + (d.sessionRows || 0) + '</b></div>' +
                '<div>Date range: <b>' + range + '</b></div>' +
                '<div>Open legs: <b style="color:' + (open === 'none' ? 'var(--accent-green)' : 'var(--accent-red, #f87171)') + ';">' + open + '</b></div>';
        }).catch(function() { el.textContent = 'Failed to load stats.'; });
        var inp = document.getElementById('sm-data-confirm'); if (inp) inp.value = '';
        var btn = document.getElementById('sm-data-clear-btn'); if (btn) btn.disabled = true;
    }

    function onClearConfirmInput() {
        var inp = document.getElementById('sm-data-confirm');
        var btn = document.getElementById('sm-data-clear-btn');
        if (inp && btn) btn.disabled = (inp.value.trim() !== 'DELETE');
    }

    function clearTradeData() {
        var inp = document.getElementById('sm-data-confirm');
        if (!inp || inp.value.trim() !== 'DELETE') { showBanner('Type DELETE to confirm.', 'error'); return; }
        window.showConfirm({
            title: 'Clear ALL trade history?',
            body: 'Deletes every strategy_trades and strategy_sessions row for every instance and resets today\'s in-memory counters.\n\nThis cannot be undone.',
            confirmLabel: 'Delete everything',
            onConfirm: doClearTradeData
        });
    }
    function doClearTradeData() {
        var btn = document.getElementById('sm-data-clear-btn');
        if (btn) { btn.disabled = true; btn.textContent = 'Clearing…'; }
        fetch('/api/trade-data/clear', {
            method: 'POST',
            headers: Object.assign({ 'Content-Type': 'application/json' }, csrfHeaders()),
            body: JSON.stringify({ confirm: 'DELETE' })
        }).then(function(r) { return r.json(); }).then(function(d) {
            showBanner(d.message || (d.success ? 'Cleared.' : 'Failed.'), d.success ? 'success' : 'error');
        }).catch(function(e) {
            showBanner('Clear failed: ' + (e.message || e), 'error');
        }).finally(function() {
            if (btn) btn.textContent = 'Clear trade history';
            loadDataStats();
        });
    }

    // ── Positions pages (one instance per underlying) ────────────────────────
    // Name / description / short code are no longer edited: the underlying is the only
    // thing that tells instances apart. Both legacy endpoints are read so STRADDLE-typed
    // rows still appear; new rows are created through /api/strangles.
    var instances = [];
    function apiBase(s) { return s && s.type === 'STRADDLE' ? '/api/straddles' : '/api/strangles'; }
    function findInstance(id) { return instances.find(function(s) { return s.id === id; }); }

    function loadPositions() {
        var list = document.getElementById('sm-positions-list');
        if (!list) return;
        list.textContent = 'Loading…';
        Promise.all([
            fetch('/api/strangles').then(function(r) { return r.json(); }).catch(function() { return []; }),
            fetch('/api/straddles').then(function(r) { return r.json(); }).catch(function() { return []; })
        ]).then(function(res) {
            var a = Array.isArray(res[0]) ? res[0].map(function(s) { s.type = 'STRANGLE'; return s; }) : [];
            var b = Array.isArray(res[1]) ? res[1].map(function(s) { s.type = 'STRADDLE'; return s; }) : [];
            instances = a.concat(b);
            renderPositionsList();
        }).catch(function() { list.textContent = 'Failed to load positions pages.'; });
    }

    function renderPositionsList() {
        var list = document.getElementById('sm-positions-list');
        if (!list) return;
        if (!instances.length) {
            list.innerHTML = '<div style="color:var(--text-muted);font-style:italic;padding:14px 0;">No positions pages yet. Pick an underlying and click + Add.</div>';
            return;
        }
        var selStyle = 'background:var(--bg-card);border:1px solid var(--border);color:var(--text-primary);padding:4px 8px;border-radius:6px;font-family:var(--font-mono);font-size:0.76rem;font-weight:700;outline:none;';
        list.innerHTML = instances.map(function(s) {
            var enabled = s.enabled === true;
            var state = escapeHtml(s.currentState || '—');
            var u = String(s.underlying || 'NIFTY').toUpperCase();
            var opts = ['NIFTY', 'SENSEX'].map(function(x) {
                return '<option value="' + x + '"' + (x === u ? ' selected' : '') + ' style="background:var(--bg-card);color:var(--text-primary);">' + x + '</option>';
            }).join('');
            var stripStyle = enabled
                ? 'border-left:4px solid var(--accent-green);padding-left:9px;'
                : 'border-left:4px solid var(--accent-red, #f87171);padding-left:9px;';
            var openLegs = /^OPEN/.test(s.currentState || '');
            return '<div class="sm-user-row" style="' + stripStyle + '">' +
                '<div style="flex:1;display:flex;align-items:center;gap:12px;flex-wrap:wrap;">' +
                    '<select style="' + selStyle + '"' + (openLegs ? ' disabled title="Close all legs before changing the underlying"' : ' title="Underlying for this positions page"') +
                        ' onchange="SettingsModal.setInstanceUnderlying(\'' + escapeHtml(s.id) + '\', this)">' + opts + '</select>' +
                    '<span style="color:var(--text-primary);">POSITIONS</span>' +
                    '<label style="display:inline-flex;align-items:center;gap:6px;color:var(--text-muted);font-size:0.66rem;">LOTS ' +
                        '<input type="number" min="1" max="100" step="1" value="' + (Number(s.defaultLots) > 0 ? Number(s.defaultLots) : 1) + '" title="Default lots in the open dialogs" style="' + selStyle + 'width:64px;" ' +
                        'onchange="SettingsModal.setInstanceDefaults(\'' + escapeHtml(s.id) + '\', { defaultLots: this.value })"></label>' +
                    '<select title="Default order type in the open dialogs" style="' + selStyle + '" onchange="SettingsModal.setInstanceDefaults(\'' + escapeHtml(s.id) + '\', { defaultProduct: this.value })">' +
                        '<option value="INTRADAY"' + (String(s.defaultProduct || 'INTRADAY') === 'INTRADAY' ? ' selected' : '') + ' style="background:var(--bg-card);color:var(--text-primary);">INTRADAY</option>' +
                        '<option value="MARGIN"' + (String(s.defaultProduct) === 'MARGIN' ? ' selected' : '') + ' style="background:var(--bg-card);color:var(--text-primary);">OVERNIGHT</option></select>' +
                    '<label style="display:inline-flex;align-items:center;gap:6px;color:var(--text-muted);font-size:0.66rem;">SL % ' +
                        '<input type="number" min="0" max="500" step="5" value="' + (Number(s.defaultSlPct) >= 0 ? Number(s.defaultSlPct) : 50) + '" title="Default SL % of entry in the open dialogs (0 = no SL)" style="' + selStyle + 'width:70px;" ' +
                        'onchange="SettingsModal.setInstanceDefaults(\'' + escapeHtml(s.id) + '\', { defaultSlPct: this.value })"></label>' +
                    '<span style="color:var(--text-muted);font-size:0.66rem;">state ' + state + ' · ' + (enabled ? 'ENABLED' : 'DISABLED') + '</span>' +
                '</div>' +
                '<div>' +
                    '<button onclick="SettingsModal.toggleInstance(\'' + escapeHtml(s.id) + '\', ' + (!enabled) + ')">' + (enabled ? 'Disable' : 'Enable') + '</button>' +
                    '<button onclick="SettingsModal.deleteInstance(\'' + escapeHtml(s.id) + '\')"' + (openLegs ? ' disabled title="Close all legs first"' : '') + '>Delete</button>' +
                '</div>' +
            '</div>';
        }).join('');
    }

    function addInstance() {
        var sel = document.getElementById('sm-positions-new-underlying');
        var u = sel ? sel.value : 'NIFTY';
        fetch('/api/strangles', {
            method: 'POST',
            headers: Object.assign({ 'Content-Type': 'application/json' }, csrfHeaders()),
            body: JSON.stringify({ name: u + ' Positions', shortCode: u, description: '', underlying: u })
        }).then(function(r) { return r.json().then(function(d) { return { ok: r.ok, body: d }; }); })
          .then(function(res) {
            if (!res.ok) { showBanner('✗ ' + ((res.body && res.body.error) || 'Create failed'), 'error'); return; }
            showBanner('✓ ' + u + ' positions page created', 'success');
            loadPositions();
            refreshSidebar();
        }).catch(function(err) { showBanner('✗ Create failed: ' + (err.message || err), 'error'); });
    }

    function setInstanceUnderlying(id, sel) {
        var s = findInstance(id);
        if (!s) return;
        var u = sel.value;
        fetch(apiBase(s) + '/' + encodeURIComponent(id), {
            method: 'PUT',
            headers: Object.assign({ 'Content-Type': 'application/json' }, csrfHeaders()),
            body: JSON.stringify({ underlying: u })
        }).then(function(r) { return r.json().then(function(d) { return { ok: r.ok, body: d }; }); })
          .then(function(res) {
            if (!res.ok || (res.body && res.body.underlying && res.body.underlying !== u)) {
                showBanner('✗ ' + ((res.body && res.body.error) || 'Underlying not changed — close all legs first'), 'error');
                loadPositions();
                return;
            }
            showBanner('✓ Underlying set to ' + u, 'success');
            loadPositions();
            refreshSidebar();
        }).catch(function(err) { showBanner('✗ Update failed: ' + (err.message || err), 'error'); loadPositions(); });
    }

    function setInstanceDefaults(id, patch) {
        var s = findInstance(id);
        if (!s) return;
        fetch(apiBase(s) + '/' + encodeURIComponent(id), {
            method: 'PUT',
            headers: Object.assign({ 'Content-Type': 'application/json' }, csrfHeaders()),
            body: JSON.stringify(patch)
        }).then(function(r) { return r.json().then(function(d) { return { ok: r.ok, body: d }; }); })
          .then(function(res) {
            if (!res.ok) { showBanner('✗ ' + ((res.body && res.body.error) || 'Update failed'), 'error'); loadPositions(); return; }
            showBanner('✓ Defaults saved', 'success');
            loadPositions();
        }).catch(function(err) { showBanner('✗ Update failed: ' + (err.message || err), 'error'); loadPositions(); });
    }

    function toggleInstance(id, enable) {
        var s = findInstance(id);
        if (!s) return;
        fetch(apiBase(s) + '/' + encodeURIComponent(id) + (enable ? '/enable' : '/disable'), { method: 'POST', headers: csrfHeaders() })
            .then(function(r) { return r.json().then(function(d) { return { ok: r.ok, status: r.status, body: d }; }); })
            .then(function(res) {
                if (!res.ok) {
                    var msg = (res.body && res.body.error) || 'Toggle failed';
                    if (res.status === 409 && res.body && res.body.currentState) msg += ' (state=' + res.body.currentState + ')';
                    showBanner('✗ ' + msg, 'error');
                    return;
                }
                showBanner('✓ Positions page ' + (enable ? 'enabled' : 'disabled'), 'success');
                loadPositions();
                refreshSidebar();
            }).catch(function(err) { showBanner('✗ Toggle failed: ' + (err.message || err), 'error'); });
    }

    function deleteInstance(id) {
        var s = findInstance(id);
        if (!s) return;
        var go = function() {
            fetch(apiBase(s) + '/' + encodeURIComponent(id), { method: 'DELETE', headers: csrfHeaders() })
                .then(function(r) { return r.json().then(function(d) { return { ok: r.ok, body: d }; }); })
                .then(function(res) {
                    if (!res.ok) { showBanner('✗ ' + ((res.body && res.body.error) || 'Delete failed'), 'error'); return; }
                    showBanner('✓ Positions page removed', 'success');
                    loadPositions();
                    refreshSidebar(id);
                }).catch(function(err) { showBanner('✗ Delete failed: ' + (err.message || err), 'error'); });
        };
        if (typeof window.showConfirm === 'function') {
            window.showConfirm({ title: 'Remove the ' + (s.underlying || '') + ' positions page?', body: 'History stays in the database; the page disappears from the sidebar.', confirmLabel: 'Remove', onConfirm: go });
        } else if (confirm('Remove this positions page?')) go();
    }

    // ── Sidebar live refresh ─────────────────────────────────────────────────
    // After any straddle/strangle CRUD op, re-fetch /api/strategies and rebuild the strategy
    // chips in the left sidebar — preserving the HOME button + MAIN label above the strategy
    // groups, and re-rendering both STRADDLES and STRANGLES sections with their headers.
    function refreshSidebar(deletedId) {
        fetch('/api/strategies')
            .then(function(r) { return r.json(); })
            .then(function(arr) {
                var aside = document.querySelector('.strategy-nav-aside');
                if (!aside) return;
                // Anchor on the HOME nav link so we preserve everything above it (MAIN label
                // + HOME button). Wipe everything AFTER the HOME button, then rebuild the
                // grouped strategy sections.
                var homeLink = aside.querySelector('[data-nav-key="home"]');
                if (!homeLink) return;
                while (homeLink.nextSibling) aside.removeChild(homeLink.nextSibling);

                var list = (Array.isArray(arr) ? arr : []).filter(function(s) { return s && s.enabled !== false; });
                var groups = {}, order = [];
                list.forEach(function(s) { var u = String(s.underlying || 'OTHER').toUpperCase(); if (!groups[u]) { groups[u] = []; order.push(u); } groups[u].push(s); });
                var rank = { NIFTY: 0, SENSEX: 1 };
                order.sort(function(x, y) { return (rank[x] == null ? 9 : rank[x]) - (rank[y] == null ? 9 : rank[y]); });
                var pathnow = window.location.pathname;

                var chipStyle = 'display:flex;align-items:center;justify-content:center;width:88px;height:36px;'
                    + 'padding:0 12px;border-radius:8px;text-decoration:none;font-family:var(--font-mono);'
                    + 'font-weight:700;font-size:0.78rem;letter-spacing:0.10em;color:var(--text-secondary);'
                    + 'border:1px solid transparent;background:transparent;cursor:pointer;';
                var dividerStyle = 'width:64px;height:1px;background:var(--border);margin:4px 0;';
                var labelStyle = 'font-family:var(--font-mono);font-size:0.62rem;letter-spacing:0.10em;'
                    + 'color:var(--text-muted);text-transform:uppercase;text-align:center;'
                    + 'width:72px;margin:6px 0 2px;';

                function appendDivider() {
                    var d = document.createElement('div');
                    d.style.cssText = dividerStyle;
                    aside.appendChild(d);
                }
                function appendLabel(text, topMargin) {
                    var l = document.createElement('div');
                    l.style.cssText = labelStyle.replace('margin:6px 0 2px;', 'margin:' + topMargin + ' 0 2px;');
                    l.textContent = text;
                    aside.appendChild(l);
                }
                function appendChips(group) {
                    group.forEach(function(s, idx) {
                        var a = document.createElement('a');
                        a.href = '/strategies/' + s.id;
                        a.className = 'nav-icon';
                        var label = s.displayName || s.id;
                        var underlying = s.underlying || '';
                        a.title = s.description
                            ? (label + ' (' + underlying + ') — ' + s.description)
                            : (underlying ? (label + ' — ' + underlying) : label);
                        a.setAttribute('data-nav-key', s.id);
                        // Two-line chip: bold short code on top, small muted underlying below.
                        var primary = document.createElement('span');
                        primary.textContent = String(s.underlying || 'OTHER').toUpperCase();
                        primary.style.cssText = 'display:block;';
                        a.appendChild(primary);
                        a.style.cssText = chipStyle;
                        if (pathnow === ('/strategies/' + s.id)) a.classList.add('active');
                        aside.appendChild(a);
                        if (idx < group.length - 1) appendDivider();
                    });
                }

                appendDivider();
                if (order.length) appendLabel('POSITIONS', '6px');
                order.forEach(function(u, gi) {
                    if (gi > 0) appendDivider();
                    appendChips(groups[u]);
                });

                if (deletedId && pathnow === ('/strategies/' + deletedId)) {
                    window.location.href = '/home';
                }
            })
            .catch(function() {});
    }

    function savePortfolioRiskTab() {
        var body = {
            startingCapital:     parseFloat(document.getElementById('sm-startingCapital').value) || 0,
            portfolioMaxRiskPct: parseFloat(document.getElementById('sm-portfolioMaxRiskPct').value) || 0,
            intradaySquareOffTime: (document.getElementById('sm-intradaySquareOffTime') || {}).value || '15:20'
        };
        postSettings('/api/settings/risk', body);
    }

    function saveChargesTab() {
        var body = {
            brokeragePerOrder: parseFloat(document.getElementById('sm-brokeragePerOrder').value) || 0,
            sttRate:           parseFloat(document.getElementById('sm-sttRate').value) || 0,
            exchangeRate:      parseFloat(document.getElementById('sm-exchangeRate').value) || 0,
            bseExchangeRate:   parseFloat(document.getElementById('sm-bseExchangeRate').value) || 0,
            gstRate:           parseFloat(document.getElementById('sm-gstRate').value) || 0,
            sebiRate:          parseFloat(document.getElementById('sm-sebiRate').value) || 0,
            stampDutyRate:     parseFloat(document.getElementById('sm-stampDutyRate').value) || 0
        };
        postSettings('/api/settings/risk', body);
    }

    function postSettings(url, body) {
        var btn = document.getElementById('sm-save-btn');
        if (btn) { btn.disabled = true; btn.textContent = 'Saving…'; }
        clearBanner();
        fetch(url, {
            method: 'POST',
            headers: Object.assign({ 'Content-Type': 'application/json' }, csrfHeaders()),
            body: JSON.stringify(body)
        }).then(function(r) { return r.json(); }).then(function(d) {
            // Accept either {success: true} (new strategy endpoints) or {ok: true} (legacy /api/settings/risk)
            var ok = d && (d.success === true || d.ok === true);
            if (ok) showBanner('✓ ' + (d.message || 'Settings saved'), 'success');
            else    showBanner('✗ ' + ((d && d.message) || 'Save failed'), 'error');
        }).catch(function(err) { showBanner('✗ Save failed: ' + (err.message || err), 'error'); })
          .finally(function() { if (btn) { btn.disabled = false; btn.textContent = '✓ Save Settings'; } });
    }

    function showBanner(msg, kind) {
        var el = document.getElementById('sm-banner');
        if (!el) return;
        el.textContent = msg;
        el.style.color = kind === 'success' ? 'var(--accent-green)'
                       : kind === 'error'   ? 'var(--accent-red, #f87171)'
                       : 'var(--text-secondary)';
        clearTimeout(showBanner._t);
        showBanner._t = setTimeout(clearBanner, 4000);
    }
    function clearBanner() {
        var el = document.getElementById('sm-banner');
        if (el) el.textContent = '';
    }

    // ── Portfolio Risk values ────────────────────────────────────────────────
    function loadPortfolioRiskValues() {
        // Re-fetch /api/strategies first so the enabled flag (driving the green/red stripe
        // on each allocation row) reflects any toggles the operator just made in the
        // Straddles tab without closing + reopening the modal.
        fetch('/api/settings/risk').then(function(r) { return r.json(); }).then(function(d) {
            if (!d) return;
            var capInput = document.getElementById('sm-startingCapital');
            var pctInput = document.getElementById('sm-portfolioMaxRiskPct');
            if (capInput) capInput.value = d.startingCapital != null ? d.startingCapital : 1000000;
            if (pctInput) pctInput.value = d.portfolioMaxRiskPct != null ? d.portfolioMaxRiskPct : 0;
            var sqInput = document.getElementById('sm-intradaySquareOffTime');
            if (sqInput) sqInput.value = d.intradaySquareOffTime || '15:20';
            updatePortfolioRiskHint(d.startingCapital || 0, d.portfolioMaxRiskPct || 0);
            if (capInput) capInput.oninput = function() {
                updatePortfolioRiskHint(parseFloat(capInput.value) || 0, parseFloat(pctInput && pctInput.value) || 0);
            };
            if (pctInput) pctInput.oninput = function() {
                updatePortfolioRiskHint(parseFloat(capInput && capInput.value) || 0, parseFloat(pctInput.value) || 0);
            };
        }).catch(function() {});
    }

    function updatePortfolioRiskHint(capital, pct) {
        var display = document.getElementById('sm-portfolioMaxRiskRupees');
        if (!display) return;
        if (pct <= 0 || capital <= 0) {
            display.textContent = '— (disabled)';
            display.style.color = 'var(--text-muted)';
            return;
        }
        var rs = capital * pct / 100;
        display.textContent = '₹' + Math.round(rs).toLocaleString('en-IN');
        display.style.color = 'var(--accent-cyan)';
    }

    // ── Charges values — still come from the legacy /api/settings/risk ───────
    function loadChargesValues() {
        fetch('/api/settings/risk').then(function(r) { return r.json(); }).then(function(d) {
            if (!d) return;
            document.getElementById('sm-brokeragePerOrder').value = d.brokeragePerOrder != null ? d.brokeragePerOrder : 0;
            document.getElementById('sm-sttRate').value           = d.sttRate != null ? d.sttRate : 0;
            document.getElementById('sm-exchangeRate').value      = d.exchangeRate != null ? d.exchangeRate : 0;
            document.getElementById('sm-bseExchangeRate').value   = d.bseExchangeRate != null ? d.bseExchangeRate : 0;
            document.getElementById('sm-gstRate').value           = d.gstRate != null ? d.gstRate : 0;
            document.getElementById('sm-sebiRate').value          = d.sebiRate != null ? d.sebiRate : 0;
            document.getElementById('sm-stampDutyRate').value     = d.stampDutyRate != null ? d.stampDutyRate : 0;
        }).catch(function() {});
    }

    function openModal() {
        ensureBuilt();
        modalEl.style.display = 'flex';
        // Build tabs on first open; cache the strategy list so the Portfolio Risk allocation
        // rows can render without an extra round trip.
        if (!modalEl.dataset.tabsBuilt) {
            buildTabs();
            loadChargesValues();
            modalEl.dataset.tabsBuilt = '1';
            fetch('/api/strategies').then(function(r) { return r.json(); }).then(function(arr) {
                strategiesList = Array.isArray(arr) ? arr : [];
            }).catch(function() {});
            switchTab('positions');
        } else {
            // Re-open: refresh values for the active tab.
            if (activeTab === 'positions')           loadPositions();
            else if (activeTab === 'portfolio-risk') loadPortfolioRiskValues();
            else if (activeTab === 'charges')        loadChargesValues();
            else                                     switchTab('positions');
        }
    }

    function closeModal() {
        if (modalEl) modalEl.style.display = 'none';
    }

    // ── Users CRUD (unchanged behaviour) ─────────────────────────────────────
    function loadUsers() {
        var list = document.getElementById('sm-users-list');
        if (!list) return;
        list.textContent = 'Loading…';
        fetch('/api/users').then(function(r) { return r.json(); }).then(function(users) {
            if (!users || users.length === 0) {
                list.innerHTML = '<div style="color:var(--text-muted);font-style:italic;padding:14px 0;">No users yet.</div>';
                return;
            }
            list.innerHTML = users.map(function(u) {
                var fullName = ((u.firstName || '') + ' ' + (u.lastName || '')).trim() || '—';
                var roleLabel = u.role === 'ROLE_ADMIN' ? 'Trader' : 'Observer';
                return '<div class="sm-user-row">' +
                    '<div>' +
                        '<div style="color:var(--text-primary);">' + escapeHtml(u.email) + '</div>' +
                        '<div style="color:var(--text-muted);font-size:0.7rem;margin-top:2px;">' + escapeHtml(fullName) + ' · ' + roleLabel + '</div>' +
                    '</div>' +
                    '<div>' +
                        '<button onclick="SettingsModal.editUser(' + u.id + ', ' + JSON.stringify(u).replace(/'/g, '&#39;').replace(/"/g, '&quot;') + ')">Edit</button>' +
                        '<button onclick="SettingsModal.deleteUser(' + u.id + ', \'' + escapeHtml(u.email) + '\')">Delete</button>' +
                    '</div>' +
                '</div>';
            }).join('');
        }).catch(function() { list.textContent = 'Failed to load users.'; });
    }
    function showUserForm(u) {
        document.getElementById('sm-user-form').style.display = '';
        document.getElementById('sm-user-form-title').textContent = u ? 'Edit User' : 'Add User';
        document.getElementById('sm-user-id').value      = u ? u.id : '';
        document.getElementById('sm-user-email').value   = u ? u.email : '';
        document.getElementById('sm-user-email').disabled = !!u;
        document.getElementById('sm-user-fname').value   = u ? (u.firstName || '') : '';
        document.getElementById('sm-user-lname').value   = u ? (u.lastName || '') : '';
        document.getElementById('sm-user-role').value    = u && u.role ? u.role : 'ROLE_VIEWER';
        document.getElementById('sm-user-password').value = '';
    }
    function cancelUserForm() {
        document.getElementById('sm-user-form').style.display = 'none';
    }
    function saveUser() {
        var id = document.getElementById('sm-user-id').value;
        var payload = {
            email:     document.getElementById('sm-user-email').value,
            firstName: document.getElementById('sm-user-fname').value,
            lastName:  document.getElementById('sm-user-lname').value,
            role:      document.getElementById('sm-user-role').value,
            password:  document.getElementById('sm-user-password').value
        };
        var url = id ? ('/api/users/' + id + '/update') : '/api/users';
        fetch(url, {
            method: 'POST',
            headers: Object.assign({ 'Content-Type': 'application/json' }, csrfHeaders()),
            body: JSON.stringify(payload)
        }).then(function(r) { return r.json(); }).then(function(d) {
            if (d && d.ok) {
                showBanner('✓ ' + (id ? 'User updated' : 'User added'), 'success');
                cancelUserForm();
                loadUsers();
            } else {
                showBanner('✗ ' + ((d && d.error) || 'Save failed'), 'error');
            }
        }).catch(function(err) { showBanner('✗ Save failed: ' + (err.message || err), 'error'); });
    }
    function deleteUser(id, email) {
        window.showConfirm({
            title: 'Delete user?',
            body: 'Delete user "' + email + '"? This cannot be undone.',
            confirmLabel: 'Delete',
            onConfirm: function() { doDeleteUser(id); }
        });
    }
    function doDeleteUser(id) {
        fetch('/api/users/' + id + '/delete', {
            method: 'POST',
            headers: csrfHeaders()
        }).then(function(r) { return r.json(); }).then(function(d) {
            if (d && d.ok) {
                showBanner('✓ User deleted', 'success');
                loadUsers();
            } else {
                showBanner('✗ ' + ((d && d.error) || 'Delete failed'), 'error');
            }
        }).catch(function(err) { showBanner('✗ Delete failed: ' + (err.message || err), 'error'); });
    }

    function escapeHtml(s) {
        if (s == null) return '';
        return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
    }

    window.SettingsModal = {
        open: openModal,
        close: closeModal,
        save: saveSettings,
        showUserForm: function(u) { showUserForm(u); },
        cancelUserForm: cancelUserForm,
        saveUser: saveUser,
        editUser: function(id, u) { showUserForm(u); },
        deleteUser: deleteUser,
        addInstance: addInstance,
        setInstanceUnderlying: setInstanceUnderlying,
        setInstanceDefaults: setInstanceDefaults,
        toggleInstance: toggleInstance,
        deleteInstance: deleteInstance,
        onClearConfirmInput: onClearConfirmInput,
        clearTradeData: clearTradeData
    };
})();
