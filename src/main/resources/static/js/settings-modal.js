/**
 * Settings modal — opened from the gear icon in the navbar.
 *
 * Three tabs:
 *   1. Rolling Straddle  — strategy toggle + 6 tunables
 *   2. Charges           — brokerage per order (+ taxes)
 *   3. Users             — list / add / edit / delete app users (ADMIN only)
 *
 * Drops in as a single script; expose `SettingsModal.open()` / `.close()` globally.
 * Builds the DOM lazily on first open, so pages just need a button that calls open().
 */
(function() {
    var modalEl = null;
    var activeTab = 'straddle';
    var cachedSettings = null;

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
                // Tab strip
                '<div style="display:flex;border-bottom:1px solid var(--border);padding:0 24px;">' +
                  '<button class="sm-tab active" data-tab="straddle">ROLLING STRADDLE</button>' +
                  '<button class="sm-tab" data-tab="risk">RISK</button>' +
                  '<button class="sm-tab" data-tab="charges">CHARGES</button>' +
                  '<button class="sm-tab" data-tab="users">USERS</button>' +
                '</div>' +
                // Body (each tab pane scrolls independently)
                '<div class="sm-body" style="flex:1;overflow-y:auto;padding:20px 24px;">' +
                  // Straddle tab
                  '<div class="sm-pane" data-pane="straddle">' +
                    '<div class="sm-hint" style="margin-bottom:14px;font-style:italic;">Strategy runs every trading day. Lifecycle is governed by entry time, move triggers, max rolls, and the timed squareoff below.</div>' +
                    '<div class="sm-field"><label>Entry Time (HH:mm IST)</label><input type="text" id="sm-straddleEntryTime" placeholder="09:20"></div>' +
                    '<div class="sm-field"><label>Squareoff Time (HH:mm IST)</label><input type="text" id="sm-straddleSquareOffTime" placeholder="15:15"></div>' +
                    '<div class="sm-field"><label>Move % Trigger</label><input type="number" id="sm-straddleMovePctTrigger" step="0.05" min="0.05" max="5.0"><div class="sm-hint">NIFTY move from last entry that fires a roll. Default 0.4%.</div></div>' +
                    '<div class="sm-field"><label>Max Rolls</label><input type="number" id="sm-straddleMaxRolls" step="1" min="0" max="20"><div class="sm-hint">Number of rolls before holding to squareoff. Default 3.</div></div>' +
                    '<div class="sm-field"><label>Lots per Leg</label><input type="number" id="sm-straddleLotsPerLeg" step="1" min="1" max="20"><div class="sm-hint">Qty = lots × NIFTY lot size (65). Default 1.</div></div>' +
                  '</div>' +
                  // Risk tab
                  '<div class="sm-pane" data-pane="risk" style="display:none;">' +
                    '<div class="sm-hint" style="margin-bottom:14px;font-style:italic;">Day-level safety limits. Triggered checks flatten open legs and park the bot DONE_FOR_DAY; manual ↻ Reset to IDLE required to re-arm.</div>' +
                    '<div class="sm-field"><label>Max Daily Loss (₹)</label><input type="number" id="sm-straddleMaxDailyLoss" step="500" min="0"><div class="sm-hint">Net P&L kill-switch. When today\'s loss (realised + open MTM − charges) exceeds this, both legs are flattened and bot parks DONE_FOR_DAY. 0 disables.</div></div>' +
                  '</div>' +
                  // Charges tab
                  '<div class="sm-pane" data-pane="charges" style="display:none;">' +
                    '<div class="sm-field"><label>Brokerage per Order (₹)</label><input type="number" id="sm-brokeragePerOrder" step="1" min="0"><div class="sm-hint">Flat per-order brokerage. Used to estimate daily charges on the Straddle History row.</div></div>' +
                    '<div class="sm-field"><label>STT Rate (%)</label><input type="number" id="sm-sttRate" step="0.0001" min="0"></div>' +
                    '<div class="sm-field"><label>Exchange Rate (%)</label><input type="number" id="sm-exchangeRate" step="0.0001" min="0"></div>' +
                    '<div class="sm-field"><label>GST Rate (%)</label><input type="number" id="sm-gstRate" step="0.01" min="0"></div>' +
                    '<div class="sm-field"><label>SEBI Rate (%)</label><input type="number" id="sm-sebiRate" step="0.0001" min="0"></div>' +
                    '<div class="sm-field"><label>Stamp Duty Rate (%)</label><input type="number" id="sm-stampDutyRate" step="0.0001" min="0"></div>' +
                  '</div>' +
                  // Users tab
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
            '.sm-tab { background:transparent;border:none;color:var(--text-secondary);padding:14px 18px;font-family:var(--font-mono);font-size:0.78rem;font-weight:600;cursor:pointer;border-bottom:2px solid transparent; }' +
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
            // Thin scrollbar to match the rest of the app's modals (4px, themed thumb)
            '.sm-body::-webkit-scrollbar { width: 4px; }' +
            '.sm-body::-webkit-scrollbar-thumb { background: var(--border); border-radius: 4px; }' +
            '.sm-body::-webkit-scrollbar-track { background: transparent; }' +
            '.sm-body { scrollbar-width: thin; scrollbar-color: var(--border) transparent; }';
        document.head.appendChild(style);

        // Wire tab clicks
        modalEl.querySelectorAll('.sm-tab').forEach(function(b) {
            b.addEventListener('click', function() { switchTab(b.getAttribute('data-tab')); });
        });

        return modalEl;
    }

    function switchTab(tab) {
        activeTab = tab;
        modalEl.querySelectorAll('.sm-tab').forEach(function(b) {
            b.classList.toggle('active', b.getAttribute('data-tab') === tab);
        });
        modalEl.querySelectorAll('.sm-pane').forEach(function(p) {
            p.style.display = (p.getAttribute('data-pane') === tab) ? '' : 'none';
        });
        if (tab === 'users') loadUsers();
    }

    function openModal() {
        ensureBuilt();
        modalEl.style.display = 'flex';
        loadSettings();
    }

    function closeModal() {
        if (modalEl) modalEl.style.display = 'none';
    }

    // ── Settings load / save ─────────────────────────────────────────────────
    function loadSettings() {
        fetch('/api/settings/risk').then(r => r.json()).then(d => {
            cachedSettings = d;
            document.getElementById('sm-straddleEntryTime').value        = d.straddleEntryTime || '09:20';
            document.getElementById('sm-straddleSquareOffTime').value    = d.straddleSquareOffTime || '15:15';
            document.getElementById('sm-straddleMovePctTrigger').value   = d.straddleMovePctTrigger != null ? d.straddleMovePctTrigger : 0.4;
            document.getElementById('sm-straddleMaxRolls').value         = d.straddleMaxRolls != null ? d.straddleMaxRolls : 3;
            document.getElementById('sm-straddleLotsPerLeg').value       = d.straddleLotsPerLeg != null ? d.straddleLotsPerLeg : 1;
            document.getElementById('sm-straddleMaxDailyLoss').value     = d.straddleMaxDailyLoss != null ? d.straddleMaxDailyLoss : 0;
            document.getElementById('sm-brokeragePerOrder').value        = d.brokeragePerOrder != null ? d.brokeragePerOrder : 0;
            document.getElementById('sm-sttRate').value                  = d.sttRate != null ? d.sttRate : 0;
            document.getElementById('sm-exchangeRate').value             = d.exchangeRate != null ? d.exchangeRate : 0;
            document.getElementById('sm-gstRate').value                  = d.gstRate != null ? d.gstRate : 0;
            document.getElementById('sm-sebiRate').value                 = d.sebiRate != null ? d.sebiRate : 0;
            document.getElementById('sm-stampDutyRate').value            = d.stampDutyRate != null ? d.stampDutyRate : 0;
        }).catch(() => {});
    }

    function saveSettings() {
        var body = {
            straddleEntryTime:      document.getElementById('sm-straddleEntryTime').value,
            straddleSquareOffTime:  document.getElementById('sm-straddleSquareOffTime').value,
            straddleMovePctTrigger: parseFloat(document.getElementById('sm-straddleMovePctTrigger').value) || 0.4,
            straddleMaxRolls:       parseInt(document.getElementById('sm-straddleMaxRolls').value, 10) || 3,
            straddleLotsPerLeg:     parseInt(document.getElementById('sm-straddleLotsPerLeg').value, 10) || 1,
            straddleMaxDailyLoss:   parseFloat(document.getElementById('sm-straddleMaxDailyLoss').value) || 0,
            brokeragePerOrder:      parseFloat(document.getElementById('sm-brokeragePerOrder').value) || 0,
            sttRate:                parseFloat(document.getElementById('sm-sttRate').value) || 0,
            exchangeRate:           parseFloat(document.getElementById('sm-exchangeRate').value) || 0,
            gstRate:                parseFloat(document.getElementById('sm-gstRate').value) || 0,
            sebiRate:               parseFloat(document.getElementById('sm-sebiRate').value) || 0,
            stampDutyRate:          parseFloat(document.getElementById('sm-stampDutyRate').value) || 0
        };
        var btn = document.getElementById('sm-save-btn');
        if (btn) { btn.disabled = true; btn.textContent = 'Saving…'; }
        clearBanner();
        fetch('/api/settings/risk', {
            method: 'POST',
            headers: Object.assign({ 'Content-Type': 'application/json' }, csrfHeaders()),
            body: JSON.stringify(body)
        }).then(r => r.json()).then(d => {
            if (d && d.ok) {
                showBanner('✓ ' + (d.message || 'Settings saved'), 'success');
            } else {
                showBanner('✗ ' + ((d && d.message) || 'Save failed'), 'error');
            }
        }).catch(err => { showBanner('✗ Save failed: ' + (err.message || err), 'error'); })
          .finally(() => { if (btn) { btn.disabled = false; btn.textContent = '✓ Save Settings'; } });
    }

    function showBanner(msg, kind) {
        var el = document.getElementById('sm-banner');
        if (!el) return;
        el.textContent = msg;
        el.style.color = kind === 'success' ? 'var(--accent-green)'
                       : kind === 'error'   ? 'var(--accent-red, #f87171)'
                       : 'var(--text-secondary)';
        // Auto-fade after 4 seconds
        clearTimeout(showBanner._t);
        showBanner._t = setTimeout(clearBanner, 4000);
    }
    function clearBanner() {
        var el = document.getElementById('sm-banner');
        if (el) el.textContent = '';
    }

    // ── Users CRUD ───────────────────────────────────────────────────────────
    function loadUsers() {
        var list = document.getElementById('sm-users-list');
        list.textContent = 'Loading…';
        fetch('/api/users').then(r => r.json()).then(users => {
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
        }).catch(() => { list.textContent = 'Failed to load users.'; });
    }

    function showUserForm(u) {
        document.getElementById('sm-user-form').style.display = '';
        document.getElementById('sm-user-form-title').textContent = u ? 'Edit User' : 'Add User';
        document.getElementById('sm-user-id').value      = u ? u.id : '';
        document.getElementById('sm-user-email').value   = u ? u.email : '';
        document.getElementById('sm-user-email').disabled = !!u; // email is the natural key
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
        }).then(r => r.json()).then(d => {
            if (d && d.ok) {
                showBanner('✓ ' + (id ? 'User updated' : 'User added'), 'success');
                cancelUserForm();
                loadUsers();
            } else {
                showBanner('✗ ' + ((d && d.error) || 'Save failed'), 'error');
            }
        }).catch(err => { showBanner('✗ Save failed: ' + (err.message || err), 'error'); });
    }
    function deleteUser(id, email) {
        if (!confirm('Delete user "' + email + '"? This cannot be undone.')) return;
        fetch('/api/users/' + id + '/delete', {
            method: 'POST',
            headers: csrfHeaders()
        }).then(r => r.json()).then(d => {
            if (d && d.ok) {
                showBanner('✓ User deleted', 'success');
                loadUsers();
            } else {
                showBanner('✗ ' + ((d && d.error) || 'Delete failed'), 'error');
            }
        }).catch(err => { showBanner('✗ Delete failed: ' + (err.message || err), 'error'); });
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
