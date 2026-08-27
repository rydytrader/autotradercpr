/**
 * Settings modal — opened from the gear icon in the navbar.
 *
 * Tabs: ATM VWAP · PORTFOLIO RISK · CHARGES · USERS · MAINTENANCE.
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
                  '<div class="sm-pane" data-pane="vwap-supertrend" style="display:none;">' +
                    '<div class="sm-grid-2col">' +
                      '<div class="sm-field sm-full"><label><input type="checkbox" id="sm-vwapStEnabled" style="margin-right:6px;vertical-align:middle;">Strategy enabled</label><div class="sm-hint">Master kill switch. When OFF the strategy skips new entries; open positions keep running to SL / ST-flip / squareoff.</div></div>' +
                      '<div class="sm-field"><label>Lots per Leg</label><input type="number" id="sm-vwapStLotsPerLeg" step="1" min="1"><div class="sm-hint">1 lot = 65 NIFTY. Per-leg fixed sizing (CE and PE track independently).</div></div>' +
                      '<div class="sm-field"><label>Start Time (HH:mm IST)</label><input type="time" id="sm-vwapStStartTime" step="60"><div class="sm-hint">Earliest time an entry can fire. Prep still runs at 09:15 (spot open + pair pick); entries suppressed until this time. Default 09:15.</div></div>' +
                      '<div class="sm-field"><label>Squareoff Time (HH:mm IST)</label><input type="time" id="sm-vwapStSquareOffTime" step="60"><div class="sm-hint">Hard market-exit of any open leg. Default 15:25.</div></div>' +
                      '<div class="sm-field"><label>Target Premium (₹)</label><input type="number" id="sm-vwapStTargetPremium" step="1" min="1"><div class="sm-hint">After spot open, pick the CE and PE trading closest to this premium as the tracked pair. Default 250.</div></div>' +
                      '<div class="sm-field"><label>Candle Minutes</label><input type="number" id="sm-vwapStCandleMinutes" step="1" min="1"><div class="sm-hint">Timeframe for signal candles + Supertrend calc. Default 3.</div></div>' +
                      '<div class="sm-field"><label>Supertrend ATR Period</label><input type="number" id="sm-vwapStAtrPeriod" step="1" min="2"><div class="sm-hint">Bars in the Supertrend ATR window. Default 10.</div></div>' +
                      '<div class="sm-field"><label>Supertrend Multiplier</label><input type="number" id="sm-vwapStMultiplier" step="0.1" min="0.1"><div class="sm-hint">ATR × this = band distance. Default 3.0.</div></div>' +
                    '</div>' +
                  '</div>' +
                  '<div class="sm-pane" data-pane="portfolio-risk" style="display:none;">' +
                    '<div class="sm-grid-2col">' +
                      '<div class="sm-field"><label>Initial Capital (₹)</label><input type="number" id="sm-startingCapital" step="1000" min="0"><div class="sm-hint">Baseline for Home analytics. Default ₹10L.</div></div>' +
                      '<div class="sm-field"><label>Max Daily Risk (%)</label><input type="number" id="sm-portfolioMaxRiskPct" step="0.1" min="0"><div class="sm-hint">Kill switch when net day P&L drops below this % of capital. 0 = off.</div></div>' +
                      '<div class="sm-field"><label>Max Risk (₹)</label><div class="sm-readonly" id="sm-portfolioMaxRiskRupees">—</div><div class="sm-hint">Auto from Capital × Risk %. Same value shown as \'Risk Budget\' on the positions page.</div></div>' +
                      '<div class="sm-field"><label>SL Buffer (₹)</label><input type="number" id="sm-vwapStSlBufferPoints" step="0.05" min="0"><div class="sm-hint">SL = entry candle low − this many rupees. Wider buffer avoids getting stopped out by a wick that just touches the low. Default 5.0.</div></div>' +
                      '<div class="sm-field"><label>Max SL (points)</label><input type="number" id="sm-vwapStMaxSlPoints" step="0.5" min="0.5"><div class="sm-hint">Hard cap on SL distance from fill. If (fill − entry-candle-low + buffer) exceeds this, SL is capped at fill − this. Default 20.</div></div>' +
                      '<div class="sm-field"><label>Reward : Risk Ratio</label><input type="number" id="sm-vwapStRewardRiskRatio" step="0.1" min="0.1"><div class="sm-hint">Target = fill + N × (fill − SL). 2.0 = 1:2 RR. Default 2.0.</div></div>' +
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
        html += '<button class="sm-tab" data-tab="vwap-supertrend">VWAP + SUPERTREND</button>';
        html += '<button class="sm-tab" data-tab="portfolio-risk">RISK</button>';
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
        if (tab === 'vwap-supertrend') {
            var ob = modalEl.querySelector('[data-pane="vwap-supertrend"]'); if (ob) ob.style.display = '';
            loadVwapStValues();
        } else if (tab === 'portfolio-risk') {
            var pp = modalEl.querySelector('[data-pane="portfolio-risk"]'); if (pp) pp.style.display = '';
            loadPortfolioRiskValues();
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
        if (activeTab === 'vwap-supertrend') return saveVwapStTab();
        if (activeTab === 'portfolio-risk') return savePortfolioRiskTab();
        if (activeTab === 'charges')        return saveChargesTab();
        if (activeTab === 'users')          { showBanner('Use the row buttons to manage users.', 'info'); return; }
        showBanner('No save action for this tab.', 'info');
    }

    function loadVwapStValues() {
        fetch('/api/settings/risk').then(function(r) { return r.json(); }).then(function(d) {
            if (!d) return;
            var g = id => document.getElementById(id);
            if (g('sm-vwapStEnabled'))         g('sm-vwapStEnabled').checked = d.vwapStEnabled !== false;
            if (g('sm-vwapStLotsPerLeg'))      g('sm-vwapStLotsPerLeg').value = d.vwapStLotsPerLeg != null ? d.vwapStLotsPerLeg : 1;
            if (g('sm-vwapStStartTime'))       g('sm-vwapStStartTime').value = d.vwapStStartTime || '09:15';
            if (g('sm-vwapStSquareOffTime'))   g('sm-vwapStSquareOffTime').value = d.vwapStSquareOffTime || '15:25';
            if (g('sm-vwapStTargetPremium'))   g('sm-vwapStTargetPremium').value = d.vwapStTargetPremium != null ? d.vwapStTargetPremium : 250;
            if (g('sm-vwapStCandleMinutes'))   g('sm-vwapStCandleMinutes').value = d.vwapStCandleMinutes != null ? d.vwapStCandleMinutes : 3;
            if (g('sm-vwapStAtrPeriod'))       g('sm-vwapStAtrPeriod').value = d.vwapStAtrPeriod != null ? d.vwapStAtrPeriod : 10;
            if (g('sm-vwapStMultiplier'))      g('sm-vwapStMultiplier').value = d.vwapStMultiplier != null ? d.vwapStMultiplier : 3.0;
        }).catch(function() {});
    }

    function saveVwapStTab() {
        var g = id => document.getElementById(id);
        var body = {
            vwapStEnabled:        !!(g('sm-vwapStEnabled') && g('sm-vwapStEnabled').checked),
            vwapStLotsPerLeg:     parseInt(g('sm-vwapStLotsPerLeg').value, 10) || 1,
            vwapStStartTime:      (g('sm-vwapStStartTime').value || '').trim(),
            vwapStSquareOffTime:  (g('sm-vwapStSquareOffTime').value || '').trim(),
            vwapStTargetPremium:  parseFloat(g('sm-vwapStTargetPremium').value) || 250,
            vwapStCandleMinutes:  parseInt(g('sm-vwapStCandleMinutes').value, 10) || 3,
            vwapStAtrPeriod:      parseInt(g('sm-vwapStAtrPeriod').value, 10) || 10,
            vwapStMultiplier:     parseFloat(g('sm-vwapStMultiplier').value) || 3.0
        };
        postSettings('/api/settings/risk', body);
    }

    function savePortfolioRiskTab() {
        var g = id => document.getElementById(id);
        var body = {
            startingCapital:       parseFloat(g('sm-startingCapital').value) || 0,
            portfolioMaxRiskPct:   parseFloat(g('sm-portfolioMaxRiskPct').value) || 0,
            vwapStSlBufferPoints:  parseFloat(g('sm-vwapStSlBufferPoints').value) || 0,
            vwapStMaxSlPoints:     parseFloat(g('sm-vwapStMaxSlPoints').value) || 20.0,
            vwapStRewardRiskRatio: parseFloat(g('sm-vwapStRewardRiskRatio').value) || 2.0
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
            var g = id => document.getElementById(id);
            var capInput = g('sm-startingCapital');
            var pctInput = g('sm-portfolioMaxRiskPct');
            if (capInput) capInput.value = d.startingCapital != null ? d.startingCapital : 1000000;
            if (pctInput) pctInput.value = d.portfolioMaxRiskPct != null ? d.portfolioMaxRiskPct : 0;
            if (g('sm-vwapStSlBufferPoints'))  g('sm-vwapStSlBufferPoints').value = d.vwapStSlBufferPoints != null ? d.vwapStSlBufferPoints : 5.0;
            if (g('sm-vwapStMaxSlPoints'))     g('sm-vwapStMaxSlPoints').value    = d.vwapStMaxSlPoints    != null ? d.vwapStMaxSlPoints    : 20.0;
            if (g('sm-vwapStRewardRiskRatio')) g('sm-vwapStRewardRiskRatio').value = d.vwapStRewardRiskRatio != null ? d.vwapStRewardRiskRatio : 2.0;
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
            switchTab('vwap-supertrend');
        } else {
            if (activeTab === 'vwap-supertrend')       loadVwapStValues();
            else if (activeTab === 'portfolio-risk') loadPortfolioRiskValues();
            else if (activeTab === 'charges')        loadChargesValues();
            else                                     switchTab('vwap-supertrend');
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
