/**
 * Settings modal — opened from the gear icon in the navbar.
 *
 * Tabs: CAMARILLA · PORTFOLIO RISK · CHARGES · USERS.
 * The CAMARILLA tab for the singleton strategy settings lands in Commit B.
 */
(function() {
    var modalEl = null;
    var activeTab = null;

    function ensureBuilt() {
        if (modalEl) return modalEl;
        var html =
            '<div id="settingsModalOverlay" style="display:none;position:fixed;inset:0;background:rgba(0,0,0,0.6);z-index:999;align-items:center;justify-content:center;">' +
              '<div style="background:var(--bg-card);border:1px solid var(--border);border-radius:12px;width:720px;max-width:94vw;max-height:88vh;display:flex;flex-direction:column;box-shadow:0 16px 48px rgba(0,0,0,0.3);">' +
                '<div style="display:flex;align-items:center;justify-content:space-between;padding:18px 24px;border-bottom:1px solid var(--border);">' +
                  '<div style="font-family:var(--font-mono);font-size:0.92rem;font-weight:700;color:var(--text-primary);">⚙ Settings</div>' +
                  '<button onclick="SettingsModal.close()" style="background:transparent;border:none;color:var(--text-muted);font-size:1.5rem;cursor:pointer;line-height:1;padding:0 4px;">&times;</button>' +
                '</div>' +
                '<div id="sm-tabstrip" style="display:flex;border-bottom:1px solid var(--border);padding:0 24px;overflow-x:auto;"></div>' +
                '<div class="sm-body" id="sm-body" style="flex:1;overflow-y:auto;padding:20px 24px;">' +
                  '<div class="sm-pane" data-pane="camarilla" style="display:none;">' +
                    '<div class="sm-field"><label>Lots per Leg</label><input type="number" id="sm-camarillaLotsPerLeg" step="1" min="1"><div class="sm-hint">1 lot = 75 NIFTY.</div></div>' +
                    '<div class="sm-field"><label>Order Type</label><select id="sm-camarillaOrderType"><option value="INTRADAY">INTRADAY</option><option value="OVERNIGHT">OVERNIGHT</option></select></div>' +
                    '<div class="sm-field"><label>Trading Start Time (HH:mm IST)</label><input type="time" id="sm-camarillaTradingStartTime" step="60"><div class="sm-hint">New entries only fire on candle closes after this time. Default 09:30. Exits and position management run independently.</div></div>' +
                    '<div class="sm-field"><label>Trading End Time (HH:mm IST)</label><input type="time" id="sm-camarillaTradingEndTime" step="60"><div class="sm-hint">No new entries fire on candle closes after this time. Default 13:30. Existing positions keep running until target / SL / squareoff.</div></div>' +
                    '<div class="sm-field"><label>Squareoff Time (HH:mm IST)</label><input type="time" id="sm-camarillaSquareOffTime" step="60"><div class="sm-hint">Hard exit if neither target nor SL has triggered.</div></div>' +
                    '<div class="sm-field"><label>Max Concurrent Positions</label><input type="number" id="sm-camarillaMaxConcurrentPositions" step="1" min="1" max="20"><div class="sm-hint">Hard cap on simultaneously open shorts across all symbols. Default 4.</div></div>' +
                  '</div>' +
                  '<div class="sm-pane" data-pane="portfolio-risk" style="display:none;">' +
                    '<div class="sm-field"><label>Initial Capital (₹)</label><input type="number" id="sm-startingCapital" step="1000" min="0"><div class="sm-hint">Baseline used by the Home analytics page (capital growth %, equity curve, return %). Default ₹10,00,000.</div></div>' +
                    '<div class="sm-field"><label>Portfolio Max Daily Risk (%)</label><input type="number" id="sm-portfolioMaxRiskPct" step="0.1" min="0"><div class="sm-hint">Global kill switch trigger. When net day P&L drops below this % of Initial Capital, the strategy is flattened. 0 disables.</div></div>' +
                    '<div class="sm-field"><label>Max Portfolio Risk (₹)</label><div class="sm-readonly" id="sm-portfolioMaxRiskRupees">—</div><div class="sm-hint">Auto-calculated from Initial Capital × Daily Risk %.</div></div>' +
                  '</div>' +
                  '<div class="sm-pane" data-pane="charges" style="display:none;">' +
                    '<div class="sm-field"><label>Brokerage per Order (₹)</label><input type="number" id="sm-brokeragePerOrder" step="1" min="0"><div class="sm-hint">Flat per-order brokerage. Drives charge estimates on every dashboard + session row.</div></div>' +
                    '<div class="sm-field"><label>STT Rate (%)</label><input type="number" id="sm-sttRate" step="0.0001" min="0"></div>' +
                    '<div class="sm-field"><label>Exchange Rate (%)</label><input type="number" id="sm-exchangeRate" step="0.0001" min="0"></div>' +
                    '<div class="sm-field"><label>GST Rate (%)</label><input type="number" id="sm-gstRate" step="0.01" min="0"></div>' +
                    '<div class="sm-field"><label>SEBI Rate (%)</label><input type="number" id="sm-sebiRate" step="0.0001" min="0"></div>' +
                    '<div class="sm-field"><label>Stamp Duty Rate (%)</label><input type="number" id="sm-stampDutyRate" step="0.0001" min="0"></div>' +
                  '</div>' +
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

        var style = document.createElement('style');
        style.textContent =
            '#sm-tabstrip { scrollbar-width: none; -ms-overflow-style: none; }' +
            '#sm-tabstrip::-webkit-scrollbar { display: none; height: 0; width: 0; }' +
            '.sm-tab { background:transparent;border:none;color:var(--text-secondary);padding:14px 18px;font-family:var(--font-mono);font-size:0.78rem;font-weight:600;cursor:pointer;border-bottom:2px solid transparent;margin-bottom:-1px;white-space:nowrap; }' +
            '.sm-tab.active { color:var(--text-primary);border-bottom:2px solid var(--accent-cyan); }' +
            '.sm-tab:hover { color:var(--text-primary); }' +
            '.sm-field { margin-bottom:14px;font-family:var(--font-mono);font-size:0.78rem; }' +
            '.sm-field label { display:block;color:var(--text-muted);font-size:0.7rem;letter-spacing:0.06em;text-transform:uppercase;margin-bottom:6px; }' +
            '.sm-field input, .sm-field select { width:100%;padding:8px 12px;border-radius:6px;border:1px solid var(--border);background:var(--bg-primary);color:var(--text-primary);font-family:var(--font-mono);font-size:0.82rem;outline:none; }' +
            '.sm-field input[type=checkbox] { width:auto; }' +
            '.sm-readonly { width:100%;padding:8px 12px;border-radius:6px;border:1px dashed var(--border);background:var(--bg-card);color:var(--accent-cyan);font-family:var(--font-mono);font-size:0.82rem;font-weight:700;letter-spacing:0.04em; }' +
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

    function buildTabs() {
        var strip = document.getElementById('sm-tabstrip');
        if (!strip) return;
        var html = '';
        html += '<button class="sm-tab" data-tab="camarilla">CAMARILLA</button>';
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
        if (tab === 'camarilla') {
            var cp = modalEl.querySelector('[data-pane="camarilla"]'); if (cp) cp.style.display = '';
            loadCamarillaValues();
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

    function saveSettings() {
        if (activeTab === 'camarilla')      return saveCamarillaTab();
        if (activeTab === 'portfolio-risk') return savePortfolioRiskTab();
        if (activeTab === 'charges')        return saveChargesTab();
        if (activeTab === 'users')          { showBanner('Use the row buttons to manage users.', 'info'); return; }
        showBanner('No save action for this tab.', 'info');
    }

    function loadCamarillaValues() {
        fetch('/api/settings/risk').then(function(r) { return r.json(); }).then(function(d) {
            if (!d) return;
            var g = id => document.getElementById(id);
            if (g('sm-camarillaLotsPerLeg'))        g('sm-camarillaLotsPerLeg').value = d.camarillaLotsPerLeg != null ? d.camarillaLotsPerLeg : 1;
            if (g('sm-camarillaOrderType'))         g('sm-camarillaOrderType').value = d.camarillaOrderType || 'INTRADAY';
            if (g('sm-camarillaTradingStartTime'))  g('sm-camarillaTradingStartTime').value = d.camarillaTradingStartTime || '09:30';
            if (g('sm-camarillaTradingEndTime'))    g('sm-camarillaTradingEndTime').value = d.camarillaTradingEndTime || '13:30';
            if (g('sm-camarillaSquareOffTime'))     g('sm-camarillaSquareOffTime').value = d.camarillaSquareOffTime || '15:15';
            if (g('sm-camarillaMaxConcurrentPositions')) g('sm-camarillaMaxConcurrentPositions').value = d.camarillaMaxConcurrentPositions != null ? d.camarillaMaxConcurrentPositions : 4;
        }).catch(function() {});
    }

    function saveCamarillaTab() {
        var g = id => document.getElementById(id);
        var body = {
            camarillaLotsPerLeg:        parseInt(g('sm-camarillaLotsPerLeg').value, 10) || 1,
            camarillaOrderType:         g('sm-camarillaOrderType').value,
            camarillaTradingStartTime:  (g('sm-camarillaTradingStartTime').value || '').trim(),
            camarillaTradingEndTime:    (g('sm-camarillaTradingEndTime').value || '').trim(),
            camarillaSquareOffTime:     (g('sm-camarillaSquareOffTime').value || '').trim(),
            camarillaMaxConcurrentPositions: parseInt(g('sm-camarillaMaxConcurrentPositions').value, 10) || 4
        };
        postSettings('/api/settings/risk', body);
    }

    function savePortfolioRiskTab() {
        var body = {
            startingCapital:     parseFloat(document.getElementById('sm-startingCapital').value) || 0,
            portfolioMaxRiskPct: parseFloat(document.getElementById('sm-portfolioMaxRiskPct').value) || 0
        };
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

    function loadPortfolioRiskValues() {
        fetch('/api/settings/risk').then(function(r) { return r.json(); }).then(function(d) {
            if (!d) return;
            var capInput = document.getElementById('sm-startingCapital');
            var pctInput = document.getElementById('sm-portfolioMaxRiskPct');
            if (capInput) capInput.value = d.startingCapital != null ? d.startingCapital : 1000000;
            if (pctInput) pctInput.value = d.portfolioMaxRiskPct != null ? d.portfolioMaxRiskPct : 0;
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
        if (!modalEl.dataset.tabsBuilt) {
            buildTabs();
            loadChargesValues();
            modalEl.dataset.tabsBuilt = '1';
            switchTab('camarilla');
        } else {
            if (activeTab === 'camarilla')           loadCamarillaValues();
            else if (activeTab === 'portfolio-risk') loadPortfolioRiskValues();
            else if (activeTab === 'charges')        loadChargesValues();
            else                                     switchTab('camarilla');
        }
    }

    function closeModal() {
        if (modalEl) modalEl.style.display = 'none';
    }

    // ── Users CRUD ───────────────────────────────────────────────────────────
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
