/**
 * Settings modal — opened from the gear icon in the navbar.
 *
 * Tabs: GENERAL · NIFTY · SENSEX · HEDGE · CHARGES · USERS · MAINTENANCE.
 * (data-tab / data-pane keys stay as "strangle-adjust" for stable persistence.)
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
                  '<div class="sm-pane" data-pane="strangle-adjust" style="display:none;">' +
                    '<div class="sm-section-title" style="font-family:var(--font-mono);font-size:0.68rem;color:var(--accent-cyan);letter-spacing:0.12em;text-transform:uppercase;margin-bottom:10px;">Sizing &amp; timing</div>' +
                    '<div class="sm-grid-2col">' +
                      '<div class="sm-field"><label>Lots per Leg</label><input type="number" id="sm-strangleAdjustLotsPerLeg" step="1" min="1"><div class="sm-hint">Multiplied by instrument lot size (NIFTY 65, SENSEX 20).</div></div>' +
                      '<div class="sm-field"><label>Order Type</label><select id="sm-strangleAdjustOrderType"><option value="INTRADAY">INTRADAY</option><option value="OVERNIGHT">OVERNIGHT</option></select></div>' +
                      '<div class="sm-field"><label>Entry Time (HH:mm IST)</label><input type="time" id="sm-strangleAdjustEntryTime" step="60"><div class="sm-hint">Strangle fires once at/after this time. Default 09:20.</div></div>' +
                      '<div class="sm-field"><label>Squareoff Time (HH:mm IST)</label><input type="time" id="sm-strangleAdjustSquareOffTime" step="60"><div class="sm-hint">Flatten all legs at market. Default 15:15.</div></div>' +
                    '</div>' +
                    '<div class="sm-section-title" style="font-family:var(--font-mono);font-size:0.68rem;color:var(--accent-cyan);letter-spacing:0.12em;text-transform:uppercase;margin:18px 0 10px;">Capital</div>' +
                    '<div class="sm-grid-2col">' +
                      '<div class="sm-field sm-full"><label>Initial Capital (₹)</label><input type="number" id="sm-strangleAdjustInitialCapital" step="1000" min="0"><div class="sm-hint">Per-strategy capital baseline for equity curve and return %. Default ₹10L.</div></div>' +
                    '</div>' +
                    '<div class="sm-hint" style="margin-top:14px;">Target Premium + SL Multiplier are configured per index — see <b>NIFTY</b> / <b>SENSEX</b> tabs.</div>' +
                  '</div>' +
                  // ── NIFTY tab ────────────────────────────────────────────────
                  '<div class="sm-pane" data-pane="nifty" style="display:none;">' +
                    '<div class="sm-section-title" style="font-family:var(--font-mono);font-size:0.68rem;color:var(--accent-cyan);letter-spacing:0.12em;text-transform:uppercase;margin-bottom:10px;">Weekdays</div>' +
                    '<div class="sm-hint" style="margin:0 0 12px;">Days the strategy will run NIFTY. NIFTY wins ties if a day is also enabled on the SENSEX tab.</div>' +
                    '<div class="sm-grid-2col">' +
                      '<div class="sm-field"><label style="display:flex;align-items:center;gap:10px;"><input type="checkbox" id="sm-strangleAdjustNiftyMonday"    style="width:auto;"><span>Monday</span></label></div>' +
                      '<div class="sm-field"><label style="display:flex;align-items:center;gap:10px;"><input type="checkbox" id="sm-strangleAdjustNiftyTuesday"   style="width:auto;"><span>Tuesday</span></label></div>' +
                      '<div class="sm-field"><label style="display:flex;align-items:center;gap:10px;"><input type="checkbox" id="sm-strangleAdjustNiftyWednesday" style="width:auto;"><span>Wednesday</span></label></div>' +
                      '<div class="sm-field"><label style="display:flex;align-items:center;gap:10px;"><input type="checkbox" id="sm-strangleAdjustNiftyThursday"  style="width:auto;"><span>Thursday</span></label></div>' +
                      '<div class="sm-field sm-full"><label style="display:flex;align-items:center;gap:10px;"><input type="checkbox" id="sm-strangleAdjustNiftyFriday"    style="width:auto;"><span>Friday</span></label></div>' +
                    '</div>' +
                    '<div class="sm-section-title" style="font-family:var(--font-mono);font-size:0.68rem;color:var(--accent-cyan);letter-spacing:0.12em;text-transform:uppercase;margin:18px 0 10px;">NIFTY params</div>' +
                    '<div class="sm-grid-2col">' +
                      '<div class="sm-field"><label>Target Premium (₹)</label><input type="number" id="sm-strangleAdjustNiftyTargetPremium" step="1" min="0"><div class="sm-hint">Pick NIFTY CE + PE strikes near this premium. Default 50.</div></div>' +
                      '<div class="sm-field"><label>SL Multiplier</label><input type="number" id="sm-strangleAdjustNiftySlMultiplier" step="0.1" min="1"><div class="sm-hint">SL price = entryPremium × this. Default 2.0 (= 100 % of received premium).</div></div>' +
                    '</div>' +
                  '</div>' +
                  // ── SENSEX tab ────────────────────────────────────────────────
                  '<div class="sm-pane" data-pane="sensex" style="display:none;">' +
                    '<div class="sm-section-title" style="font-family:var(--font-mono);font-size:0.68rem;color:var(--accent-cyan);letter-spacing:0.12em;text-transform:uppercase;margin-bottom:10px;">Weekdays</div>' +
                    '<div class="sm-hint" style="margin:0 0 12px;">Days the strategy will run SENSEX. Ignored on a day where NIFTY is also enabled (NIFTY takes priority).</div>' +
                    '<div class="sm-grid-2col">' +
                      '<div class="sm-field"><label style="display:flex;align-items:center;gap:10px;"><input type="checkbox" id="sm-strangleAdjustSensexMonday"    style="width:auto;"><span>Monday</span></label></div>' +
                      '<div class="sm-field"><label style="display:flex;align-items:center;gap:10px;"><input type="checkbox" id="sm-strangleAdjustSensexTuesday"   style="width:auto;"><span>Tuesday</span></label></div>' +
                      '<div class="sm-field"><label style="display:flex;align-items:center;gap:10px;"><input type="checkbox" id="sm-strangleAdjustSensexWednesday" style="width:auto;"><span>Wednesday</span></label></div>' +
                      '<div class="sm-field"><label style="display:flex;align-items:center;gap:10px;"><input type="checkbox" id="sm-strangleAdjustSensexThursday"  style="width:auto;"><span>Thursday</span></label></div>' +
                      '<div class="sm-field sm-full"><label style="display:flex;align-items:center;gap:10px;"><input type="checkbox" id="sm-strangleAdjustSensexFriday"    style="width:auto;"><span>Friday</span></label></div>' +
                    '</div>' +
                    '<div class="sm-section-title" style="font-family:var(--font-mono);font-size:0.68rem;color:var(--accent-cyan);letter-spacing:0.12em;text-transform:uppercase;margin:18px 0 10px;">SENSEX params</div>' +
                    '<div class="sm-grid-2col">' +
                      '<div class="sm-field"><label>Target Premium (₹)</label><input type="number" id="sm-strangleAdjustSensexTargetPremium" step="5" min="0"><div class="sm-hint">Pick SENSEX CE + PE strikes near this premium. Default 120.</div></div>' +
                      '<div class="sm-field"><label>SL Multiplier</label><input type="number" id="sm-strangleAdjustSensexSlMultiplier" step="0.1" min="1"><div class="sm-hint">SL price = entryPremium × this. Default 2.0.</div></div>' +
                    '</div>' +
                  '</div>' +
                  '<div class="sm-pane" data-pane="hedge" style="display:none;">' +
                    '<div class="sm-section-title" style="font-family:var(--font-mono);font-size:0.68rem;color:var(--accent-cyan);letter-spacing:0.12em;text-transform:uppercase;margin-bottom:10px;">Master toggle</div>' +
                    '<div class="sm-grid-2col">' +
                      '<div class="sm-field sm-full"><label style="display:flex;align-items:center;gap:10px;"><input type="checkbox" id="sm-strangleAdjustHedgeEnabled" style="width:auto;"><span>Enable hedge</span></label><div class="sm-hint">When off, recovery adjustments run as a NAKED sell — no deep-OTM BUY leg is placed. Broker margin will not be relieved. Default on.</div></div>' +
                    '</div>' +
                    '<div class="sm-section-title" style="font-family:var(--font-mono);font-size:0.68rem;color:var(--accent-cyan);letter-spacing:0.12em;text-transform:uppercase;margin:18px 0 10px;">Hedge params</div>' +
                    '<div class="sm-grid-2col">' +
                      '<div class="sm-field"><label>Hedge Strikes Away</label><input type="number" id="sm-strangleAdjustHedgeStrikesAway" step="1" min="1"><div class="sm-hint">Deep-OTM hedge distance in strike-steps from the new-sell leg. Default 10. Ignored when hedge is disabled.</div></div>' +
                      '<div class="sm-field"><label>Hedge Qty Multiplier</label><input type="number" id="sm-strangleAdjustHedgeQtyMultiplier" step="0.5" min="0"><div class="sm-hint">Hedge qty = base qty × this. Default 2.0. Ignored when hedge is disabled.</div></div>' +
                    '</div>' +
                  '</div>' +
                  '<div class="sm-pane" data-pane="charges" style="display:none;">' +
                    '<div class="sm-grid-2col">' +
                      '<div class="sm-field"><label>Brokerage per Order (₹)</label><input type="number" id="sm-brokeragePerOrder" step="1" min="0"><div class="sm-hint">Flat per-order brokerage.</div></div>' +
                      '<div class="sm-field"><label>STT Rate (%)</label><input type="number" id="sm-sttRate" step="0.0001" min="0"></div>' +
                      '<div class="sm-field"><label>Exchange Rate (%)</label><input type="number" id="sm-exchangeRate" step="0.0001" min="0"></div>' +
                      '<div class="sm-field"><label>GST Rate (%)</label><input type="number" id="sm-gstRate" step="0.01" min="0"></div>' +
                      '<div class="sm-field"><label>SEBI Rate (%)</label><input type="number" id="sm-sebiRate" step="0.0001" min="0"></div>' +
                      '<div class="sm-field"><label>Stamp Duty Rate (%)</label><input type="number" id="sm-stampDutyRate" step="0.0001" min="0"></div>' +
                    '</div>' +
                  '</div>' +
                  '<div class="sm-pane" data-pane="users" style="display:none;">' +
                    '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:14px;">' +
                      '<div class="sm-hint" style="margin:0;">App users with login access. Trader = full admin. Observer = read-only.</div>' +
                      '<button class="sm-btn-primary" onclick="SettingsModal.showUserForm()">+ Add User</button>' +
                    '</div>' +
                    '<div id="sm-users-list" style="font-family:var(--font-mono);font-size:0.78rem;">Loading…</div>' +
                    '<div id="sm-user-form" style="display:none;margin-top:18px;padding:14px;border:1px solid var(--border);border-radius:8px;background:var(--bg-card-hover);">' +
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
                  '<div class="sm-pane" data-pane="maintenance" style="display:none;">' +
                    '<div style="padding:18px;border:1px solid rgba(248,113,113,0.35);border-radius:8px;background:rgba(248,113,113,0.05);margin-bottom:14px;">' +
                      '<div style="font-family:var(--font-mono);font-size:0.92rem;font-weight:700;color:var(--accent-red, #f87171);margin-bottom:8px;">⚠ Clear Today\'s Records</div>' +
                      '<div class="sm-hint" style="margin:0 0 14px;">Wipes today\'s closed-trade records, today\'s event-log entries, and the corresponding DB rows. <b>Open positions are preserved</b> — they keep running at the broker and the bot continues to manage their SL / squareoff. Useful after a test session before going live. <b>Irreversible.</b></div>' +
                      '<div id="sm-clear-today-status" style="font-family:var(--font-mono);font-size:0.78rem;margin-bottom:12px;"></div>' +
                      '<button class="sm-btn-primary" id="sm-clear-today-btn" onclick="SettingsModal.clearToday()" style="background:rgba(248,113,113,0.15);border-color:rgba(248,113,113,0.45);color:var(--accent-red, #f87171);">Clear Today\'s Records</button>' +
                    '</div>' +
                    '<div style="padding:18px;border:1px solid rgba(248,113,113,0.55);border-radius:8px;background:rgba(248,113,113,0.12);">' +
                      '<div style="font-family:var(--font-mono);font-size:0.92rem;font-weight:700;color:var(--accent-red, #f87171);margin-bottom:8px;">☠ Clear ALL Records (database wipe)</div>' +
                      '<div class="sm-hint" style="margin:0 0 14px;">Deletes <b>every closed trade in the database</b>, the full in-memory event log, and any in-flight pending confirmations. Open positions are still preserved. Use this for a hard reset after extensive test sessions. <b>Permanently destroys historical analytics</b> — there is no recovery.</div>' +
                      '<div id="sm-clear-all-status" style="font-family:var(--font-mono);font-size:0.78rem;margin-bottom:12px;"></div>' +
                      '<button class="sm-btn-primary" id="sm-clear-all-btn" onclick="SettingsModal.clearAll()" style="background:rgba(248,113,113,0.22);border-color:rgba(248,113,113,0.65);color:var(--accent-red, #f87171);">Clear ALL Records</button>' +
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
            '.sm-field input { width:100%;padding:8px 12px;border-radius:6px;border:1px solid var(--border);background-color:var(--bg-card-hover);color:var(--text-primary);font-family:var(--font-mono);font-size:0.82rem;outline:none;box-sizing:border-box; }' +
            // Matches the event-log + strategy-settings dropdowns: bg-card background with a
            // two-gradient chevron arrow, appearance:none. Cross-browser consistent — no SVG
            // data URL, no color-scheme dependency.
            '.sm-field select { width:100%;padding:8px 24px 8px 12px;border-radius:6px;border:1px solid var(--border);background-color:var(--bg-card);color:var(--text-primary);font-family:var(--font-mono);font-size:0.82rem;outline:none;cursor:pointer;appearance:none;-webkit-appearance:none;box-sizing:border-box;background-image:linear-gradient(45deg, transparent 50%, var(--text-muted) 50%), linear-gradient(135deg, var(--text-muted) 50%, transparent 50%);background-position:calc(100% - 12px) 50%, calc(100% - 7px) 50%;background-size:5px 5px, 5px 5px;background-repeat:no-repeat; }' +
            '.sm-field select option { background-color:var(--bg-card);color:var(--text-primary); }' +
            '.sm-field select:focus, .sm-field input:focus { border-color:var(--accent-cyan); }' +
            '.sm-field input[type=checkbox] { width:auto; }' +
            '.sm-readonly { width:100%;padding:8px 12px;border-radius:6px;border:1px dashed var(--border);background:var(--bg-card);color:var(--accent-cyan);font-family:var(--font-mono);font-size:0.82rem;font-weight:700;letter-spacing:0.04em; }' +
            '.sm-hint { color:var(--text-muted);font-size:0.7rem;margin-top:4px; }' +
            '.sm-grid-2col { display:grid;grid-template-columns:1fr 1fr;gap:14px 20px; }' +
            '.sm-grid-2col > .sm-field { margin-bottom:0; }' +
            '.sm-grid-2col > .sm-field.sm-full { grid-column: 1 / -1; }' +
            '@media (max-width: 640px) { .sm-grid-2col { grid-template-columns: 1fr; } }' +
            '.sm-btn-primary { background:rgba(52,211,153,0.12);border:1px solid rgba(52,211,153,0.4);color:var(--accent-green);padding:8px 18px;border-radius:6px;font-family:var(--font-mono);font-size:0.74rem;font-weight:700;cursor:pointer; }' +
            '.sm-btn-secondary { background:transparent;border:1px solid var(--border);color:var(--text-secondary);padding:8px 18px;border-radius:6px;font-family:var(--font-mono);font-size:0.74rem;cursor:pointer; }' +
            '.sm-user-row { display:flex;justify-content:space-between;align-items:center;padding:10px 12px;border:1px solid var(--border);border-radius:6px;margin-bottom:8px;background:var(--bg-card-hover); }' +
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
        html += '<button class="sm-tab" data-tab="strangle-adjust">GENERAL</button>';
        html += '<button class="sm-tab" data-tab="nifty">NIFTY</button>';
        html += '<button class="sm-tab" data-tab="sensex">SENSEX</button>';
        html += '<button class="sm-tab" data-tab="hedge">HEDGE</button>';
        html += '<button class="sm-tab" data-tab="charges">CHARGES</button>';
        html += '<button class="sm-tab" data-tab="users">USERS</button>';
        html += '<button class="sm-tab" data-tab="maintenance">MAINTENANCE</button>';
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
        if (tab === 'strangle-adjust') {
            var cp = modalEl.querySelector('[data-pane="strangle-adjust"]'); if (cp) cp.style.display = '';
            loadStrangleAdjustValues();
        } else if (tab === 'nifty') {
            var np = modalEl.querySelector('[data-pane="nifty"]'); if (np) np.style.display = '';
            loadNiftyValues();
        } else if (tab === 'sensex') {
            var xp = modalEl.querySelector('[data-pane="sensex"]'); if (xp) xp.style.display = '';
            loadSensexValues();
        } else if (tab === 'hedge') {
            var hp = modalEl.querySelector('[data-pane="hedge"]'); if (hp) hp.style.display = '';
            loadHedgeValues();
        } else if (tab === 'charges') {
            var pane = modalEl.querySelector('[data-pane="charges"]'); if (pane) pane.style.display = '';
        } else if (tab === 'users') {
            var p2 = modalEl.querySelector('[data-pane="users"]'); if (p2) p2.style.display = '';
            loadUsers();
        } else if (tab === 'maintenance') {
            var mp = modalEl.querySelector('[data-pane="maintenance"]'); if (mp) mp.style.display = '';
        }
    }

    function saveSettings() {
        if (activeTab === 'strangle-adjust') return saveStrangleAdjustTab();
        if (activeTab === 'nifty')           return saveNiftyTab();
        if (activeTab === 'sensex')          return saveSensexTab();
        if (activeTab === 'hedge')           return saveHedgeTab();
        if (activeTab === 'charges')         return saveChargesTab();
        if (activeTab === 'users')           { showBanner('Use the row buttons to manage users.', 'info'); return; }
        showBanner('No save action for this tab.', 'info');
    }

    function loadStrangleAdjustValues() {
        fetch('/api/settings/risk').then(function(r) { return r.json(); }).then(function(d) {
            if (!d) return;
            var g = id => document.getElementById(id);
            if (g('sm-strangleAdjustLotsPerLeg'))     g('sm-strangleAdjustLotsPerLeg').value = d.strangleAdjustLotsPerLeg != null ? d.strangleAdjustLotsPerLeg : 1;
            if (g('sm-strangleAdjustOrderType'))      g('sm-strangleAdjustOrderType').value = d.strangleAdjustOrderType || 'INTRADAY';
            if (g('sm-strangleAdjustEntryTime'))      g('sm-strangleAdjustEntryTime').value = d.strangleAdjustEntryTime || '09:20';
            if (g('sm-strangleAdjustSquareOffTime'))  g('sm-strangleAdjustSquareOffTime').value = d.strangleAdjustSquareOffTime || '15:15';
            if (g('sm-strangleAdjustInitialCapital')) g('sm-strangleAdjustInitialCapital').value = d.strangleAdjustInitialCapital != null ? d.strangleAdjustInitialCapital : 1000000;
        }).catch(function() {});
    }

    function saveStrangleAdjustTab() {
        var g = id => document.getElementById(id);
        var body = {
            strangleAdjustLotsPerLeg:     parseInt(g('sm-strangleAdjustLotsPerLeg').value, 10) || 1,
            strangleAdjustOrderType:      g('sm-strangleAdjustOrderType').value,
            strangleAdjustEntryTime:      (g('sm-strangleAdjustEntryTime').value || '').trim(),
            strangleAdjustSquareOffTime:  (g('sm-strangleAdjustSquareOffTime').value || '').trim(),
            strangleAdjustInitialCapital: parseFloat(g('sm-strangleAdjustInitialCapital').value) || 0
        };
        postSettings('/api/settings/risk', body);
    }

    function loadNiftyValues() {
        fetch('/api/settings/risk').then(function(r) { return r.json(); }).then(function(d) {
            if (!d) return;
            var g = id => document.getElementById(id);
            if (g('sm-strangleAdjustNiftyMonday'))         g('sm-strangleAdjustNiftyMonday').checked    = d.strangleAdjustNiftyMonday    !== false;
            if (g('sm-strangleAdjustNiftyTuesday'))        g('sm-strangleAdjustNiftyTuesday').checked   = d.strangleAdjustNiftyTuesday   !== false;
            if (g('sm-strangleAdjustNiftyWednesday'))      g('sm-strangleAdjustNiftyWednesday').checked = !!d.strangleAdjustNiftyWednesday;
            if (g('sm-strangleAdjustNiftyThursday'))       g('sm-strangleAdjustNiftyThursday').checked  = !!d.strangleAdjustNiftyThursday;
            if (g('sm-strangleAdjustNiftyFriday'))         g('sm-strangleAdjustNiftyFriday').checked    = !!d.strangleAdjustNiftyFriday;
            if (g('sm-strangleAdjustNiftyTargetPremium')) g('sm-strangleAdjustNiftyTargetPremium').value = d.strangleAdjustNiftyTargetPremium != null ? d.strangleAdjustNiftyTargetPremium : 50;
            if (g('sm-strangleAdjustNiftySlMultiplier'))  g('sm-strangleAdjustNiftySlMultiplier').value  = d.strangleAdjustNiftySlMultiplier  != null ? d.strangleAdjustNiftySlMultiplier  : 2.0;
        }).catch(function() {});
    }

    function saveNiftyTab() {
        var g = id => document.getElementById(id);
        var body = {
            strangleAdjustNiftyMonday:        !!g('sm-strangleAdjustNiftyMonday').checked,
            strangleAdjustNiftyTuesday:       !!g('sm-strangleAdjustNiftyTuesday').checked,
            strangleAdjustNiftyWednesday:     !!g('sm-strangleAdjustNiftyWednesday').checked,
            strangleAdjustNiftyThursday:      !!g('sm-strangleAdjustNiftyThursday').checked,
            strangleAdjustNiftyFriday:        !!g('sm-strangleAdjustNiftyFriday').checked,
            strangleAdjustNiftyTargetPremium: parseFloat(g('sm-strangleAdjustNiftyTargetPremium').value) || 0,
            strangleAdjustNiftySlMultiplier:  parseFloat(g('sm-strangleAdjustNiftySlMultiplier').value)  || 2.0
        };
        postSettings('/api/settings/risk', body);
    }

    function loadSensexValues() {
        fetch('/api/settings/risk').then(function(r) { return r.json(); }).then(function(d) {
            if (!d) return;
            var g = id => document.getElementById(id);
            if (g('sm-strangleAdjustSensexMonday'))         g('sm-strangleAdjustSensexMonday').checked    = !!d.strangleAdjustSensexMonday;
            if (g('sm-strangleAdjustSensexTuesday'))        g('sm-strangleAdjustSensexTuesday').checked   = !!d.strangleAdjustSensexTuesday;
            if (g('sm-strangleAdjustSensexWednesday'))      g('sm-strangleAdjustSensexWednesday').checked = d.strangleAdjustSensexWednesday !== false;
            if (g('sm-strangleAdjustSensexThursday'))       g('sm-strangleAdjustSensexThursday').checked  = d.strangleAdjustSensexThursday  !== false;
            if (g('sm-strangleAdjustSensexFriday'))         g('sm-strangleAdjustSensexFriday').checked    = !!d.strangleAdjustSensexFriday;
            if (g('sm-strangleAdjustSensexTargetPremium')) g('sm-strangleAdjustSensexTargetPremium').value = d.strangleAdjustSensexTargetPremium != null ? d.strangleAdjustSensexTargetPremium : 120;
            if (g('sm-strangleAdjustSensexSlMultiplier'))  g('sm-strangleAdjustSensexSlMultiplier').value  = d.strangleAdjustSensexSlMultiplier  != null ? d.strangleAdjustSensexSlMultiplier  : 2.0;
        }).catch(function() {});
    }

    function saveSensexTab() {
        var g = id => document.getElementById(id);
        var body = {
            strangleAdjustSensexMonday:        !!g('sm-strangleAdjustSensexMonday').checked,
            strangleAdjustSensexTuesday:       !!g('sm-strangleAdjustSensexTuesday').checked,
            strangleAdjustSensexWednesday:     !!g('sm-strangleAdjustSensexWednesday').checked,
            strangleAdjustSensexThursday:      !!g('sm-strangleAdjustSensexThursday').checked,
            strangleAdjustSensexFriday:        !!g('sm-strangleAdjustSensexFriday').checked,
            strangleAdjustSensexTargetPremium: parseFloat(g('sm-strangleAdjustSensexTargetPremium').value) || 0,
            strangleAdjustSensexSlMultiplier:  parseFloat(g('sm-strangleAdjustSensexSlMultiplier').value)  || 2.0
        };
        postSettings('/api/settings/risk', body);
    }

    function loadHedgeValues() {
        fetch('/api/settings/risk').then(function(r) { return r.json(); }).then(function(d) {
            if (!d) return;
            var g = id => document.getElementById(id);
            if (g('sm-strangleAdjustHedgeEnabled'))        g('sm-strangleAdjustHedgeEnabled').checked = d.strangleAdjustHedgeEnabled !== false;
            if (g('sm-strangleAdjustHedgeStrikesAway'))    g('sm-strangleAdjustHedgeStrikesAway').value = d.strangleAdjustHedgeStrikesAway != null ? d.strangleAdjustHedgeStrikesAway : 10;
            if (g('sm-strangleAdjustHedgeQtyMultiplier'))  g('sm-strangleAdjustHedgeQtyMultiplier').value = d.strangleAdjustHedgeQtyMultiplier != null ? d.strangleAdjustHedgeQtyMultiplier : 2.0;
        }).catch(function() {});
    }

    function saveHedgeTab() {
        var g = id => document.getElementById(id);
        var body = {
            strangleAdjustHedgeEnabled:         !!g('sm-strangleAdjustHedgeEnabled').checked,
            strangleAdjustHedgeStrikesAway:     parseInt(g('sm-strangleAdjustHedgeStrikesAway').value, 10) || 10,
            strangleAdjustHedgeQtyMultiplier:   parseFloat(g('sm-strangleAdjustHedgeQtyMultiplier').value) || 0
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
            switchTab('strangle-adjust');
        } else {
            if (activeTab === 'strangle-adjust')     loadStrangleAdjustValues();
            else if (activeTab === 'nifty')          loadNiftyValues();
            else if (activeTab === 'sensex')         loadSensexValues();
            else if (activeTab === 'hedge')          loadHedgeValues();
            else if (activeTab === 'charges')        loadChargesValues();
            else                                     switchTab('strangle-adjust');
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

    // Maintenance — clear today's records. Confirms via the themed AppConfirm dialog
    // (loaded from common.js) to match the rest of the app. Reports the result inline
    // beneath the button. Open positions are NOT touched.
    function clearToday() {
        var go = function() {
            var btn    = document.getElementById('sm-clear-today-btn');
            var status = document.getElementById('sm-clear-today-status');
            if (btn) { btn.disabled = true; btn.textContent = 'Clearing…'; }
            if (status) { status.textContent = ''; status.style.color = ''; }
            fetch('/api/maintenance/clear-today', {
                method: 'POST',
                headers: Object.assign({ 'Content-Type': 'application/json' }, (window.csrfHeaders ? window.csrfHeaders() : {}))
            }).then(function(r) { return r.json(); }).then(function(d) {
                if (d && d.ok) {
                    if (status) {
                        status.style.color = 'var(--accent-green, #34d399)';
                        status.textContent = '✓ Cleared — cycles=' + (d.cyclesCleared || 0)
                            + ' events=' + (d.eventsCleared || 0)
                            + ' dbRows=' + (d.dbCleared || 0);
                    }
                } else {
                    if (status) {
                        status.style.color = 'var(--accent-red, #f87171)';
                        status.textContent = '✗ ' + ((d && d.message) || 'Clear failed');
                    }
                }
            }).catch(function(err) {
                if (status) {
                    status.style.color = 'var(--accent-red, #f87171)';
                    status.textContent = '✗ Clear failed: ' + (err && err.message ? err.message : err);
                }
            }).finally(function() {
                if (btn) { btn.disabled = false; btn.textContent = 'Clear Today\'s Records'; }
            });
        };
        if (window.AppConfirm) {
            window.AppConfirm.ask({
                title:        'Clear Today\'s Records',
                message:      'Wipe today\'s closed-trade records, event log, and DB rows?\n\nOpen positions will keep running and the bot will keep managing them.\n\nThis is irreversible.',
                confirmLabel: 'Clear',
                danger:       true
            }).then(function(ok) { if (ok) go(); });
        } else {
            go();
        }
    }

    // Maintenance — clear ALL records (every DB row, all events, all pendings).
    // Parallel to clearToday(); harder confirm message since this destroys
    // historical analytics.
    function clearAll() {
        var go = function() {
            var btn    = document.getElementById('sm-clear-all-btn');
            var status = document.getElementById('sm-clear-all-status');
            if (btn) { btn.disabled = true; btn.textContent = 'Clearing…'; }
            if (status) { status.textContent = ''; status.style.color = ''; }
            fetch('/api/maintenance/clear-all', {
                method: 'POST',
                headers: Object.assign({ 'Content-Type': 'application/json' }, (window.csrfHeaders ? window.csrfHeaders() : {}))
            }).then(function(r) { return r.json(); }).then(function(d) {
                if (d && d.ok) {
                    if (status) {
                        status.style.color = 'var(--accent-green, #34d399)';
                        status.textContent = '✓ Wiped — cycles=' + (d.cyclesCleared || 0)
                            + ' events=' + (d.eventsCleared || 0)
                            + ' dbRows=' + (d.dbCleared || 0);
                    }
                } else {
                    if (status) {
                        status.style.color = 'var(--accent-red, #f87171)';
                        status.textContent = '✗ ' + ((d && d.message) || 'Wipe failed');
                    }
                }
            }).catch(function(err) {
                if (status) {
                    status.style.color = 'var(--accent-red, #f87171)';
                    status.textContent = '✗ Wipe failed: ' + (err && err.message ? err.message : err);
                }
            }).finally(function() {
                if (btn) { btn.disabled = false; btn.textContent = 'Clear ALL Records'; }
            });
        };
        if (window.AppConfirm) {
            window.AppConfirm.ask({
                title:        'Clear ALL Records',
                message:      'PERMANENTLY DELETE every closed trade in the database, the full event log, and all pending confirmations?\n\nOpen positions will keep running and the bot will keep managing them.\n\nHistorical analytics will be destroyed. There is NO recovery.',
                confirmLabel: 'Wipe Everything',
                danger:       true
            }).then(function(ok) { if (ok) go(); });
        } else {
            go();
        }
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
        clearToday: clearToday,
        clearAll:   clearAll
    };
})();
