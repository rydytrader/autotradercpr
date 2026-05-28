/**
 * Settings modal — opened from the gear icon in the navbar.
 *
 * Schema-driven multi-strategy version. On open it fetches /api/strategies, builds one
 * tab per registered strategy from the displayName, and renders each form dynamically from
 * the strategy's getSettingsSchema() output. Adding a new strategy = no JS changes here.
 *
 * Static tabs: CHARGES (global rates) + USERS (admin only). Strategy tabs use the new
 * /api/strategies/{id}/settings endpoints; CHARGES keeps the legacy /api/settings/risk.
 */
(function() {
    var modalEl = null;
    var activeTab = null;
    var strategiesList = [];     // [{id, displayName, navIcon, currentState}]
    var strategySchemas = {};    // {strategyId: [{key, type, default, label, hint}, ...]}
    var strategyValues  = {};    // {strategyId: {key: value}}

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
                  // Charges tab (static)
                  '<div class="sm-pane" data-pane="charges" style="display:none;">' +
                    '<div class="sm-field"><label>Starting Capital (₹)</label><input type="number" id="sm-startingCapital" step="1000" min="0"><div class="sm-hint">Baseline used by the Home analytics page (capital growth %, equity curve). Default ₹10,00,000.</div></div>' +
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
        // Strategy tabs first
        strategiesList.forEach(function(s) {
            html += '<button class="sm-tab" data-tab="strategy:' + s.id + '">' + escapeHtml((s.displayName || s.id).toUpperCase()) + '</button>';
        });
        // Static tabs
        html += '<button class="sm-tab" data-tab="charges">CHARGES</button>';
        html += '<button class="sm-tab" data-tab="users">USERS</button>';
        strip.innerHTML = html;
        // Wire clicks
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
        // Hide every pane
        modalEl.querySelectorAll('.sm-pane').forEach(function(p) { p.style.display = 'none'; });
        if (tab && tab.indexOf('strategy:') === 0) {
            var sid = tab.substring('strategy:'.length);
            renderStrategyPane(sid);
        } else if (tab === 'charges') {
            var pane = modalEl.querySelector('[data-pane="charges"]');
            if (pane) pane.style.display = '';
        } else if (tab === 'users') {
            var p2 = modalEl.querySelector('[data-pane="users"]');
            if (p2) p2.style.display = '';
            loadUsers();
        }
    }

    function renderStrategyPane(strategyId) {
        var body = document.getElementById('sm-body');
        var paneId = 'sm-pane-strategy-' + strategyId;
        var pane = document.getElementById(paneId);
        if (!pane) {
            // First activation — build the pane from schema, then load values
            pane = document.createElement('div');
            pane.className = 'sm-pane';
            pane.setAttribute('data-pane', 'strategy:' + strategyId);
            pane.id = paneId;
            pane.innerHTML = '<div style="color:var(--text-muted);padding:14px;">Loading…</div>';
            body.appendChild(pane);
            var schema = strategySchemas[strategyId];
            if (!schema) {
                fetch('/api/strategies/' + strategyId + '/settings/schema')
                    .then(function(r) { return r.json(); })
                    .then(function(s) {
                        strategySchemas[strategyId] = s;
                        renderSchemaFields(pane, strategyId, s);
                        loadStrategyValues(strategyId);
                    })
                    .catch(function() { pane.innerHTML = '<div style="color:var(--accent-red);padding:14px;">Failed to load schema.</div>'; });
            } else {
                renderSchemaFields(pane, strategyId, schema);
                loadStrategyValues(strategyId);
            }
        } else {
            // Re-show + refresh values
            loadStrategyValues(strategyId);
        }
        pane.style.display = '';
    }

    function renderSchemaFields(pane, strategyId, schema) {
        var html = '';
        schema.forEach(function(f) {
            var fieldId = 'sm-' + strategyId + '-' + f.key;
            var input = '';
            switch (f.type) {
                case 'time':
                    input = '<input type="text" id="' + fieldId + '" placeholder="' + (f.default || '') + '">';
                    break;
                case 'int':
                case 'percent':
                    input = '<input type="number" id="' + fieldId + '" step="1" min="0">';
                    break;
                case 'rupees':
                    input = '<input type="number" id="' + fieldId + '" step="500" min="0">';
                    break;
                case 'double':
                    input = '<input type="number" id="' + fieldId + '" step="0.01">';
                    break;
                case 'boolean':
                    input = '<input type="checkbox" id="' + fieldId + '">';
                    break;
                default:
                    input = '<input type="text" id="' + fieldId + '">';
            }
            html += '<div class="sm-field"><label>' + escapeHtml(f.label || f.key) + '</label>' + input;
            if (f.hint) html += '<div class="sm-hint">' + escapeHtml(f.hint) + '</div>';
            html += '</div>';
        });
        pane.innerHTML = html;
    }

    function loadStrategyValues(strategyId) {
        fetch('/api/strategies/' + strategyId + '/settings')
            .then(function(r) { return r.json(); })
            .then(function(values) {
                strategyValues[strategyId] = values || {};
                var schema = strategySchemas[strategyId] || [];
                schema.forEach(function(f) {
                    var fieldId = 'sm-' + strategyId + '-' + f.key;
                    var el = document.getElementById(fieldId);
                    if (!el) return;
                    var v = values && values[f.key] != null ? values[f.key] : f.default;
                    if (f.type === 'boolean') el.checked = (v === true || v === 'true');
                    else                      el.value = v == null ? '' : v;
                });
            })
            .catch(function() {});
    }

    // ── Save ─────────────────────────────────────────────────────────────────
    function saveSettings() {
        if (activeTab && activeTab.indexOf('strategy:') === 0) {
            return saveStrategyTab(activeTab.substring('strategy:'.length));
        }
        if (activeTab === 'charges') {
            return saveChargesTab();
        }
        showBanner('No save action for this tab.', 'info');
    }

    function saveStrategyTab(strategyId) {
        var schema = strategySchemas[strategyId];
        if (!schema) return;
        var body = {};
        schema.forEach(function(f) {
            var el = document.getElementById('sm-' + strategyId + '-' + f.key);
            if (!el) return;
            if (f.type === 'boolean') body[f.key] = el.checked;
            else if (f.type === 'int' || f.type === 'percent') body[f.key] = parseInt(el.value, 10) || 0;
            else if (f.type === 'rupees' || f.type === 'double') body[f.key] = parseFloat(el.value) || 0;
            else body[f.key] = (el.value || '').trim();
        });
        postSettings('/api/strategies/' + strategyId + '/settings', body);
    }

    function saveChargesTab() {
        var body = {
            startingCapital:   parseFloat(document.getElementById('sm-startingCapital').value) || 0,
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

    // ── Charges values — still come from the legacy /api/settings/risk ───────
    function loadChargesValues() {
        fetch('/api/settings/risk').then(function(r) { return r.json(); }).then(function(d) {
            if (!d) return;
            document.getElementById('sm-startingCapital').value   = d.startingCapital != null ? d.startingCapital : 1000000;
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
        // First-time: load strategy list, build tabs, default to first strategy
        if (strategiesList.length === 0) {
            fetch('/api/strategies').then(function(r) { return r.json(); }).then(function(arr) {
                strategiesList = Array.isArray(arr) ? arr : [];
                buildTabs();
                loadChargesValues();
                if (strategiesList.length > 0) switchTab('strategy:' + strategiesList[0].id);
                else                           switchTab('charges');
            }).catch(function() {
                buildTabs();
                loadChargesValues();
                switchTab('charges');
            });
        } else {
            // Re-open: refresh values for the active strategy (or charges)
            if (activeTab && activeTab.indexOf('strategy:') === 0) {
                loadStrategyValues(activeTab.substring('strategy:'.length));
            } else if (activeTab === 'charges') {
                loadChargesValues();
            }
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
        deleteUser: deleteUser
    };
})();
