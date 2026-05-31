/**
 * Settings modal — opened from the gear icon in the navbar.
 *
 * Tabs: STRADDLES (CRUD for short-straddle instances) · PORTFOLIO RISK · CHARGES · USERS.
 * Per-instance trading settings (entry time, lots, SL%) no longer live here — they're
 * opened from each instance's dashboard page via the ⚙ Settings button.
 */
(function() {
    var modalEl = null;
    var activeTab = null;
    var strategiesList = [];     // [{id, displayName, shortCode, navIcon, currentState}]
    var straddles      = [];     // [{id, name, description, shortCode, navIcon, enabled, currentState}]

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
                  // Straddles tab — CRUD for short-straddle instances
                  '<div class="sm-pane" data-pane="straddles" style="display:none;">' +
                    '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:14px;">' +
                      '<div class="sm-hint" style="margin:0;">Each row is an independent short straddle with its own settings, state, sidebar entry and dashboard. Open the per-instance page for trading config.</div>' +
                      '<button class="sm-btn-primary" onclick="SettingsModal.showStraddleForm()">+ Add Straddle</button>' +
                    '</div>' +
                    '<div id="sm-straddles-list" style="font-family:var(--font-mono);font-size:0.78rem;">Loading…</div>' +
                    '<div id="sm-straddle-form" style="display:none;margin-top:18px;padding:14px;border:1px solid var(--border);border-radius:8px;background:var(--bg-primary);">' +
                      '<div style="font-family:var(--font-mono);font-size:0.84rem;font-weight:700;margin-bottom:10px;color:var(--text-primary);" id="sm-straddle-form-title">Add Straddle</div>' +
                      '<input type="hidden" id="sm-straddle-id">' +
                      '<div class="sm-field"><label>Name</label><input type="text" id="sm-straddle-name" maxlength="80" placeholder="e.g. 9:20 Default"></div>' +
                      '<div class="sm-field"><label>Description</label><input type="text" id="sm-straddle-description" maxlength="240" placeholder="e.g. standard 1-lot, 50% leg SL"></div>' +
                      '<div class="sm-field"><label>Short Code <span style="font-size:0.66rem;color:var(--text-muted);">(used as sidebar label — must be unique, ≤24 chars)</span></label><input type="text" id="sm-straddle-shortCode" maxlength="24" placeholder="e.g. 9:20"></div>' +
                      '<div style="display:flex;justify-content:flex-end;gap:8px;margin-top:12px;">' +
                        '<button class="sm-btn-secondary" onclick="SettingsModal.cancelStraddleForm()">Cancel</button>' +
                        '<button class="sm-btn-primary" onclick="SettingsModal.saveStraddle()">Save</button>' +
                      '</div>' +
                    '</div>' +
                  '</div>' +
                  // Portfolio Risk tab (static)
                  '<div class="sm-pane" data-pane="portfolio-risk" style="display:none;">' +
                    '<div class="sm-field"><label>Initial Capital (₹)</label><input type="number" id="sm-startingCapital" step="1000" min="0"><div class="sm-hint">Baseline used by the Home analytics page (capital growth %, equity curve, return %). Default ₹10,00,000.</div></div>' +
                    '<div class="sm-field"><label>Portfolio Max Daily Risk (%)</label><input type="number" id="sm-portfolioMaxRiskPct" step="0.1" min="0"><div class="sm-hint">Global kill switch trigger. When aggregate net day P&L across all strategies drops below this % of Initial Capital, every strategy is flattened. 0 disables.</div></div>' +
                    '<div class="sm-field"><label>Max Portfolio Risk (₹)</label><div class="sm-readonly" id="sm-portfolioMaxRiskRupees">—</div><div class="sm-hint">Auto-calculated from Initial Capital × Daily Risk %. This is the rupee loss threshold that triggers the global kill switch.</div></div>' +
                    '<div class="sm-section-title">Risk Allocation per Strategy</div>' +
                    '<div class="sm-hint" style="margin-bottom:10px;">Splits the Max Portfolio Risk across strategies. Each strategy\'s individual kill switch fires at its own slice. Defaults to 50% per strategy.</div>' +
                    '<div id="sm-allocations-list" style="font-family:var(--font-mono);font-size:0.78rem;"></div>' +
                    '<div class="sm-alloc-total"><span>Total allocation</span><span id="sm-allocations-total">—</span></div>' +
                  '</div>' +
                  '<div class="sm-pane" data-pane="charges" style="display:none;">' +
                    '<div class="sm-field"><label>Brokerage per Order (₹)</label><input type="number" id="sm-brokeragePerOrder" step="1" min="0"><div class="sm-hint">Flat per-order brokerage. Drives charge estimates on every dashboard + session row.</div></div>' +
                    '<div class="sm-field"><label>STT Rate (%)</label><input type="number" id="sm-sttRate" step="0.0001" min="0"></div>' +
                    '<div class="sm-field"><label>Exchange Rate (%)</label><input type="number" id="sm-exchangeRate" step="0.0001" min="0"></div>' +
                    '<div class="sm-field"><label>GST Rate (%)</label><input type="number" id="sm-gstRate" step="0.01" min="0"></div>' +
                    '<div class="sm-field"><label>SEBI Rate (%)</label><input type="number" id="sm-sebiRate" step="0.0001" min="0"></div>' +
                    '<div class="sm-field"><label>Stamp Duty Rate (%)</label><input type="number" id="sm-stampDutyRate" step="0.0001" min="0"></div>' +
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
            '.sm-alloc-row { display:grid;grid-template-columns:1fr 110px 130px;gap:12px;align-items:center;padding:8px 0;border-bottom:1px solid var(--border); }' +
            '.sm-alloc-name { color:var(--text-primary);font-size:0.78rem; }' +
            '.sm-alloc-pct { padding:6px 10px;border-radius:6px;border:1px solid var(--border);background:var(--bg-primary);color:var(--text-primary);font-family:var(--font-mono);font-size:0.78rem;outline:none;text-align:right; }' +
            '.sm-alloc-rupees { color:var(--accent-cyan);font-family:var(--font-mono);font-size:0.78rem;font-weight:700;text-align:right; }' +
            '.sm-alloc-total { display:flex;justify-content:space-between;padding:10px 0 0;font-family:var(--font-mono);font-size:0.78rem;font-weight:700; }' +
            '.sm-alloc-total .ok { color:var(--accent-green); } .sm-alloc-total .bad { color:var(--accent-red, #f87171); }' +
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
        html += '<button class="sm-tab" data-tab="straddles">STRADDLES</button>';
        html += '<button class="sm-tab" data-tab="portfolio-risk">PORTFOLIO RISK</button>';
        html += '<button class="sm-tab" data-tab="charges">CHARGES</button>';
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
        if (tab === 'straddles') {
            var sp = modalEl.querySelector('[data-pane="straddles"]'); if (sp) sp.style.display = '';
            loadStraddles();
        } else if (tab === 'portfolio-risk') {
            var pp = modalEl.querySelector('[data-pane="portfolio-risk"]'); if (pp) pp.style.display = '';
            loadPortfolioRiskValues();
        } else if (tab === 'charges') {
            var pane = modalEl.querySelector('[data-pane="charges"]'); if (pane) pane.style.display = '';
        } else if (tab === 'users') {
            var p2 = modalEl.querySelector('[data-pane="users"]'); if (p2) p2.style.display = '';
            loadUsers();
        }
    }

    // ── Save ─────────────────────────────────────────────────────────────────
    function saveSettings() {
        if (activeTab === 'portfolio-risk') return savePortfolioRiskTab();
        if (activeTab === 'charges')        return saveChargesTab();
        if (activeTab === 'straddles')      { showBanner('Use the row buttons (✎ ✕ enable/disable) — no batch save.', 'info'); return; }
        if (activeTab === 'users')          { showBanner('Use the row buttons to manage users.', 'info'); return; }
        showBanner('No save action for this tab.', 'info');
    }

    // ── Straddles CRUD ───────────────────────────────────────────────────────
    function loadStraddles() {
        var list = document.getElementById('sm-straddles-list');
        if (!list) return;
        list.textContent = 'Loading…';
        fetch('/api/straddles').then(function(r) { return r.json(); }).then(function(arr) {
            straddles = Array.isArray(arr) ? arr : [];
            renderStraddlesList();
        }).catch(function() { list.textContent = 'Failed to load straddles.'; });
    }

    function renderStraddlesList() {
        var list = document.getElementById('sm-straddles-list');
        if (!list) return;
        if (!straddles || straddles.length === 0) {
            list.innerHTML = '<div style="color:var(--text-muted);font-style:italic;padding:14px 0;">No straddles yet. Click + Add Straddle to create your first one.</div>';
            return;
        }
        list.innerHTML = straddles.map(function(s) {
            var enabled = s.enabled === true;
            var name = escapeHtml(s.name || s.shortCode || s.id);
            var desc = escapeHtml(s.description || '');
            var sc   = escapeHtml(s.shortCode || '');
            var state = escapeHtml(s.currentState || '—');
            var rowJson = JSON.stringify(s).replace(/'/g, '&#39;').replace(/"/g, '&quot;');
            // 4px left strip: green = enabled, red = disabled. Padding-left shaved by 3px
            // (default 12px → 9px) to compensate for the extra 3px of border beyond the row's
            // default 1px so content alignment is identical across rows.
            var stripStyle = enabled
                ? 'border-left:4px solid var(--accent-green);padding-left:9px;'
                : 'border-left:4px solid var(--accent-red, #f87171);padding-left:9px;';
            return '<div class="sm-user-row" style="' + stripStyle + '">' +
                '<div style="flex:1;">' +
                    '<div style="color:var(--text-primary);">' + sc + ' · ' + name + '</div>' +
                    (desc ? '<div style="color:var(--text-muted);font-size:0.7rem;margin-top:2px;">' + desc + '</div>' : '') +
                    '<div style="color:var(--text-muted);font-size:0.66rem;margin-top:2px;">state ' + state + ' · ' + (enabled ? 'ENABLED' : 'DISABLED') + '</div>' +
                '</div>' +
                '<div>' +
                    '<button onclick="SettingsModal.toggleStraddle(\'' + escapeHtml(s.id) + '\', ' + (!enabled) + ')">' + (enabled ? 'Disable' : 'Enable') + '</button>' +
                    '<button onclick="SettingsModal.editStraddleForm(' + rowJson + ')">Edit</button>' +
                '</div>' +
            '</div>';
        }).join('');
    }

    function showStraddleForm(s) {
        document.getElementById('sm-straddle-form').style.display = '';
        document.getElementById('sm-straddle-form-title').textContent = s ? 'Edit Straddle' : 'Add Straddle';
        document.getElementById('sm-straddle-id').value          = s ? s.id : '';
        document.getElementById('sm-straddle-name').value        = s ? (s.name || '') : '';
        document.getElementById('sm-straddle-description').value = s ? (s.description || '') : '';
        document.getElementById('sm-straddle-shortCode').value   = s ? (s.shortCode || '') : '';
    }

    function cancelStraddleForm() {
        document.getElementById('sm-straddle-form').style.display = 'none';
    }

    function saveStraddle() {
        var id = document.getElementById('sm-straddle-id').value;
        var body = {
            name:        document.getElementById('sm-straddle-name').value.trim(),
            description: document.getElementById('sm-straddle-description').value.trim(),
            shortCode:   document.getElementById('sm-straddle-shortCode').value.trim()
        };
        if (!body.name)      { showBanner('✗ Name is required', 'error'); return; }
        if (!body.shortCode) { showBanner('✗ Short code is required', 'error'); return; }
        var url    = id ? ('/api/straddles/' + encodeURIComponent(id)) : '/api/straddles';
        var method = id ? 'PUT' : 'POST';
        fetch(url, {
            method: method,
            headers: Object.assign({ 'Content-Type': 'application/json' }, csrfHeaders()),
            body: JSON.stringify(body)
        }).then(function(r) { return r.json().then(function(d) { return { ok: r.ok, body: d }; }); })
          .then(function(res) {
            if (!res.ok) { showBanner('✗ ' + ((res.body && res.body.error) || 'Save failed'), 'error'); return; }
            showBanner('✓ Straddle ' + (id ? 'updated' : 'created'), 'success');
            cancelStraddleForm();
            loadStraddles();
            refreshSidebar();
        }).catch(function(err) { showBanner('✗ Save failed: ' + (err.message || err), 'error'); });
    }

    function toggleStraddle(id, enable) {
        var url = '/api/straddles/' + encodeURIComponent(id) + (enable ? '/enable' : '/disable');
        fetch(url, { method: 'POST', headers: csrfHeaders() })
            .then(function(r) { return r.json().then(function(d) { return { ok: r.ok, status: r.status, body: d }; }); })
            .then(function(res) {
                if (!res.ok) {
                    var msg = (res.body && res.body.error) || 'Toggle failed';
                    if (res.status === 409 && res.body && res.body.currentState) {
                        msg += ' (state=' + res.body.currentState + ')';
                    }
                    showBanner('✗ ' + msg, 'error');
                    return;
                }
                showBanner('✓ Straddle ' + (enable ? 'enabled' : 'disabled'), 'success');
                loadStraddles();
                refreshSidebar();
            }).catch(function(err) { showBanner('✗ Toggle failed: ' + (err.message || err), 'error'); });
    }

    // ── Sidebar live refresh ─────────────────────────────────────────────────
    // After any straddle CRUD op, re-fetch /api/strategies and rebuild the strategy chips in
    // the left sidebar so the operator doesn't have to refresh the page. If `deletedId` is
    // provided and the current page is that strategy's dashboard, redirect to /home so they
    // don't end up on an orphaned page.
    function refreshSidebar(deletedId) {
        fetch('/api/strategies')
            .then(function(r) { return r.json(); })
            .then(function(arr) {
                var aside = document.querySelector('.strategy-nav-aside');
                if (!aside) return;
                // First <div> child of the aside is the HOME-to-strategies divider. Everything
                // after it is the strategy chip stack — wipe + rebuild.
                var firstDiv = aside.querySelector('div');
                if (!firstDiv) return;
                while (firstDiv.nextSibling) aside.removeChild(firstDiv.nextSibling);
                // Sidebar shows only enabled instances. Disabled ones live in the Straddles tab.
                var list = (Array.isArray(arr) ? arr : []).filter(function(s) { return s && s.enabled !== false; });
                var pathnow = window.location.pathname;
                var chipStyle = 'display:flex;align-items:center;justify-content:center;'
                    + 'width:72px;height:40px;border-radius:8px;text-decoration:none;'
                    + 'font-family:var(--font-mono);font-weight:700;font-size:0.86rem;'
                    + 'color:var(--text-secondary);border:1px solid transparent;'
                    + 'background:transparent;cursor:pointer;letter-spacing:0.06em;'
                    + 'white-space:nowrap;overflow:hidden;text-overflow:ellipsis;padding:0 6px;';
                var dividerStyle = 'width:64px;height:1px;background:var(--border);margin:4px 0;';
                list.forEach(function(s, idx) {
                    var a = document.createElement('a');
                    a.href = '/strategies/' + s.id;
                    a.className = 'nav-icon';
                    a.title = s.displayName || s.id;
                    a.setAttribute('data-nav-key', s.id);
                    a.textContent = s.navIcon || s.shortCode || s.id;
                    a.style.cssText = chipStyle;
                    if (pathnow === ('/strategies/' + s.id)) a.classList.add('active');
                    aside.appendChild(a);
                    if (idx < list.length - 1) {
                        var d = document.createElement('div');
                        d.style.cssText = dividerStyle;
                        aside.appendChild(d);
                    }
                });
                // If the operator just deleted the strategy they're currently viewing, send
                // them to the analytics home so they don't end up polling /api/strategies/X
                // for a 404'd id.
                if (deletedId && pathnow === ('/strategies/' + deletedId)) {
                    window.location.href = '/home';
                }
            })
            .catch(function() {});
    }

    function savePortfolioRiskTab() {
        var body = {
            startingCapital:     parseFloat(document.getElementById('sm-startingCapital').value) || 0,
            portfolioMaxRiskPct: parseFloat(document.getElementById('sm-portfolioMaxRiskPct').value) || 0
        };
        // Collect per-strategy allocation %s — keyed by strategy id.
        var allocs = {};
        document.querySelectorAll('.sm-alloc-row .sm-alloc-pct').forEach(function(inp) {
            var sid = inp.getAttribute('data-strategy');
            if (sid) allocs[sid] = parseFloat(inp.value) || 0;
        });
        if (Object.keys(allocs).length > 0) body.strategyAllocations = allocs;
        postSettings('/api/settings/risk', body);
    }

    function saveChargesTab() {
        var body = {
            brokeragePerOrder: parseFloat(document.getElementById('sm-brokeragePerOrder').value) || 0,
            sttRate:           parseFloat(document.getElementById('sm-sttRate').value) || 0,
            exchangeRate:      parseFloat(document.getElementById('sm-exchangeRate').value) || 0,
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
        var refreshList = fetch('/api/strategies')
            .then(function(r) { return r.json(); })
            .then(function(arr) { strategiesList = Array.isArray(arr) ? arr : strategiesList; })
            .catch(function() {});
        var settingsP = fetch('/api/settings/risk').then(function(r) { return r.json(); });
        Promise.all([refreshList, settingsP]).then(function(arr) {
            var d = arr[1];
            if (!d) return;
            var capInput = document.getElementById('sm-startingCapital');
            var pctInput = document.getElementById('sm-portfolioMaxRiskPct');
            if (capInput) capInput.value = d.startingCapital != null ? d.startingCapital : 1000000;
            if (pctInput) pctInput.value = d.portfolioMaxRiskPct != null ? d.portfolioMaxRiskPct : 0;
            updatePortfolioRiskHint(d.startingCapital || 0, d.portfolioMaxRiskPct || 0);
            if (capInput) capInput.oninput = function() {
                updatePortfolioRiskHint(parseFloat(capInput.value) || 0, parseFloat(pctInput && pctInput.value) || 0);
                refreshAllocationRupees();
            };
            if (pctInput) pctInput.oninput = function() {
                updatePortfolioRiskHint(parseFloat(capInput && capInput.value) || 0, parseFloat(pctInput.value) || 0);
                refreshAllocationRupees();
            };
            renderAllocations(d.strategyAllocations || {});
        }).catch(function() {});
    }

    function renderAllocations(currentAllocs) {
        var host = document.getElementById('sm-allocations-list');
        if (!host) return;
        if (!strategiesList || strategiesList.length === 0) {
            host.innerHTML = '<div class="sm-hint">No strategies registered.</div>';
            return;
        }
        var html = '';
        strategiesList.forEach(function(s) {
            var pct = currentAllocs[s.id] != null ? currentAllocs[s.id] : 50;
            // 4px left strip mirroring the Straddles tab: green = enabled, red = disabled.
            // 6px padding-left so content sits clear of the border.
            var enabled = s.enabled !== false;
            var stripStyle = 'border-left:4px solid ' + (enabled
                ? 'var(--accent-green)'
                : 'var(--accent-red, #f87171)') + ';padding-left:6px;';
            html += '<div class="sm-alloc-row" style="' + stripStyle + '">'
                + '<span class="sm-alloc-name">' + escapeHtml(s.displayName || s.id) + '</span>'
                + '<input type="number" class="sm-alloc-pct" data-strategy="' + escapeHtml(s.id) + '" step="1" min="0" max="100" value="' + pct + '">'
                + '<span class="sm-alloc-rupees" data-strategy="' + escapeHtml(s.id) + '">—</span>'
                + '</div>';
        });
        host.innerHTML = html;
        // Live recompute on % input changes.
        host.querySelectorAll('.sm-alloc-pct').forEach(function(inp) {
            inp.addEventListener('input', refreshAllocationRupees);
        });
        refreshAllocationRupees();
    }

    function refreshAllocationRupees() {
        var capInp = document.getElementById('sm-startingCapital');
        var pctInp = document.getElementById('sm-portfolioMaxRiskPct');
        var cap = capInp ? parseFloat(capInp.value) || 0 : 0;
        var pct = pctInp ? parseFloat(pctInp.value) || 0 : 0;
        var portfolioMax = cap * pct / 100;
        var totalPct = 0;
        document.querySelectorAll('.sm-alloc-row').forEach(function(row) {
            var pctInput = row.querySelector('.sm-alloc-pct');
            var rsSpan   = row.querySelector('.sm-alloc-rupees');
            var allocPct = parseFloat(pctInput.value) || 0;
            totalPct += allocPct;
            if (rsSpan) {
                if (portfolioMax > 0) {
                    var rs = Math.round(portfolioMax * allocPct / 100);
                    rsSpan.textContent = '₹' + rs.toLocaleString('en-IN');
                    rsSpan.style.color = 'var(--accent-cyan)';
                } else {
                    rsSpan.textContent = '— (disabled)';
                    rsSpan.style.color = 'var(--text-muted)';
                }
            }
        });
        var totalEl = document.getElementById('sm-allocations-total');
        if (totalEl) {
            totalEl.textContent = totalPct.toFixed(0) + '%';
            totalEl.className = (Math.abs(totalPct - 100) < 0.01) ? 'ok' : 'bad';
        }
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
            switchTab('straddles');
        } else {
            // Re-open: refresh values for the active tab.
            if (activeTab === 'straddles')           loadStraddles();
            else if (activeTab === 'portfolio-risk') loadPortfolioRiskValues();
            else if (activeTab === 'charges')        loadChargesValues();
            else                                     switchTab('straddles');
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
        if (!confirm('Delete user "' + email + '"? This cannot be undone.')) return;
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
        showStraddleForm: function() { showStraddleForm(); },
        editStraddleForm: function(s) { showStraddleForm(s); },
        cancelStraddleForm: cancelStraddleForm,
        saveStraddle: saveStraddle,
        toggleStraddle: toggleStraddle
    };
})();
