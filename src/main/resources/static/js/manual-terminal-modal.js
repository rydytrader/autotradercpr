/**
 * Manual NIFTY Options Terminal — navbar ▲ button → modal with minimize / maximize.
 * Pattern: lazy-built IIFE (mirrors option-chain-modal.js). State machine for the wrapper
 * has three visual modes: normal (default ~1100×620 centered), maximized (100vw×100vh),
 * minimized (collapsed to a fixed-bottom-right pill).
 *
 * Public API:  window.ManualTerminal = { open, close };
 */
(function() {
    var overlayEl   = null;   // backdrop
    var modalEl     = null;   // main modal card
    var pillEl      = null;   // minimized floating pill
    var pollTimer   = null;
    var slowPollTimer = null;
    var minimized   = false;
    var maximized   = false;
    var lastData    = null;
    var chainCache  = null;   // cached strikes payload
    // ── Real-time SSE plumbing ────────────────────────────────────────────────
    // tickCache: Fyers-symbol → { ltp, ch, chp } updated on every SSE 'ticker' event.
    // Used to repaint the LTP banner + per-position LTP/P&L instantly between the
    // slower dashboard polls. tickerListener is the attached handler (for detach).
    var tickCache       = {};
    var tickerListener  = null;
    var sseAttachTimer  = null;
    var NIFTY_INDEX_SYM = 'NSE:NIFTY50-INDEX';
    // NIFTY-only after BankNifty was retired. Kept as a variable for
    // signature stability with callers below.
    var currentIndexSym = NIFTY_INDEX_SYM;
    function currentIndexLabel() { return 'NIFTY'; }

    function ensureBuilt() {
        if (overlayEl) return overlayEl;

        // ── Canonical dropdown styling (matches strategy-settings-form / event-log dropdowns) ──
        var styleTag = document.createElement('style');
        styleTag.textContent =
            '.mt-select { width:100%;padding:8px 24px 8px 12px;border-radius:6px;border:1px solid var(--border);background-color:var(--bg-card);color:var(--text-primary);font-family:var(--font-mono);font-size:0.78rem;outline:none;cursor:pointer;appearance:none;-webkit-appearance:none;' +
              'background-image:linear-gradient(45deg, transparent 50%, var(--text-muted) 50%), linear-gradient(135deg, var(--text-muted) 50%, transparent 50%);' +
              'background-position:calc(100% - 12px) 50%, calc(100% - 7px) 50%;background-size:5px 5px, 5px 5px;background-repeat:no-repeat; }' +
            '.mt-select option { background-color:var(--bg-card);color:var(--text-primary); }' +
            '.mt-input  { width:100%;padding:8px 12px;border-radius:6px;border:1px solid var(--border);background-color:var(--bg-primary);color:var(--text-primary);font-family:var(--font-mono);font-size:0.78rem;outline:none; }';
        document.head.appendChild(styleTag);

        // ── Backdrop + modal ──────────────────────────────────────────────
        overlayEl = document.createElement('div');
        overlayEl.id = 'mtOverlay';
        overlayEl.style.cssText = 'display:none;position:fixed;inset:0;background:rgba(0,0,0,0.55);z-index:998;align-items:center;justify-content:center;';
        overlayEl.innerHTML =
            '<div id="mtModal" style="background:var(--bg-card);border:1px solid var(--border);border-radius:12px;width:1120px;max-width:96vw;height:640px;max-height:92vh;display:flex;flex-direction:column;box-shadow:0 16px 48px rgba(0,0,0,0.45);overflow:hidden;">' +
              // Header — title, live NIFTY, current weekly Expiry, running Net P&L + Charges,
              // and the min/max/close window controls.
              '<div style="display:flex;align-items:center;justify-content:space-between;padding:12px 20px;border-bottom:1px solid var(--border);gap:18px;flex-wrap:wrap;">' +
                '<div style="display:flex;align-items:center;gap:22px;flex-wrap:wrap;">' +
                  '<div style="font-family:var(--font-mono);font-size:0.92rem;font-weight:700;color:var(--text-primary);letter-spacing:0.04em;">▲ OPTIONS TERMINAL</div>' +
                  // Expiry dropdown (replaces the old per-row Expiry).
                  '<div style="font-family:var(--font-mono);font-size:0.74rem;display:flex;align-items:center;gap:8px;">' +
                    '<span style="color:var(--text-muted);letter-spacing:0.06em;text-transform:uppercase;font-size:0.62rem;">Expiry</span>' +
                    '<select id="mtExpiry" class="mt-select" style="padding:5px 22px 5px 8px;font-size:0.72rem;min-width:120px;"></select>' +
                  '</div>' +
                  // Running totals — Net P&L (signed, color-coded) and Charges.
                  '<div style="display:flex;align-items:center;gap:18px;font-family:var(--font-mono);font-size:0.74rem;">' +
                    '<div><span style="color:var(--text-muted);letter-spacing:0.06em;text-transform:uppercase;font-size:0.62rem;">Net P&amp;L</span> <span id="mtHdrNetPnl" style="font-weight:700;margin-left:6px;">—</span></div>' +
                    '<div><span style="color:var(--text-muted);letter-spacing:0.06em;text-transform:uppercase;font-size:0.62rem;">Charges</span> <span id="mtHdrCharges" style="color:var(--text-muted);margin-left:6px;">—</span></div>' +
                  '</div>' +
                '</div>' +
                '<div style="display:flex;align-items:center;gap:6px;">' +
                  // Close = collapse back to the persistent pill. There is no full-hide.
                  '<button id="mtCloseBtn" title="Close (collapse to pill)" style="background:transparent;border:1px solid var(--border);color:var(--text-secondary);width:28px;height:26px;border-radius:5px;cursor:pointer;font-size:1rem;line-height:1;font-family:var(--font-mono);">×</button>' +
                '</div>' +
              '</div>' +
              // Body (scrollable)
              '<div id="mtBody" style="flex:1;overflow-y:auto;padding:18px 22px;font-family:var(--font-mono);font-size:0.78rem;color:var(--text-primary);">' +
                buildControlsHtml() +
                buildLtpRowHtml() +
                buildActionRowHtml() +
                buildBottomRowHtml() +
                buildPositionsHtml() +
              '</div>' +
            '</div>';
        document.body.appendChild(overlayEl);
        modalEl = document.getElementById('mtModal');

        // ── Minimized pill (separate floating element) ────────────────────
        pillEl = document.createElement('div');
        pillEl.id = 'mtPill';
        pillEl.style.cssText = 'display:none;position:fixed;bottom:18px;right:18px;z-index:998;background:var(--bg-card);border:1px solid var(--accent-purple, #8b5cf6);border-radius:24px;padding:10px 18px;font-family:var(--font-mono);font-size:0.78rem;color:var(--text-primary);box-shadow:0 8px 24px rgba(0,0,0,0.45);cursor:pointer;';
        pillEl.innerHTML = '<span style="color:var(--accent-purple, #8b5cf6);font-weight:700;letter-spacing:0.04em;">▲ TERMINAL</span> <span id="mtPillText" style="color:var(--text-muted);margin-left:10px;">— pos · ₹0</span>';
        document.body.appendChild(pillEl);
        pillEl.addEventListener('click', restore);

        // ── Wire header buttons ───────────────────────────────────────────
        // × Close = collapse the modal back to the persistent pill.
        document.getElementById('mtCloseBtn').addEventListener('click', minimize);

        // ── Wire dropdowns + buttons ──────────────────────────────────────
        wireControls();
        wireActionButtons();
        wireBottomButtons();
        wireTabs();

        // Backdrop click + ESC collapse the modal back to the persistent pill —
        // there is no full-close, the pill is always present.
        overlayEl.addEventListener('click', function(e) {
            if (e.target === overlayEl && !minimized) minimize();
        });
        document.addEventListener('keydown', function(e) {
            if (e.key === 'Escape' && overlayEl && overlayEl.style.display === 'flex' && !minimized) minimize();
        });

        return overlayEl;
    }

    // ── Layout builders ───────────────────────────────────────────────────────
    function buildControlsHtml() {
        return '<div style="display:grid;grid-template-columns:repeat(6,1fr) auto;gap:12px 16px;align-items:end;margin-bottom:18px;">' +
            '<div><label style="display:block;color:var(--text-muted);font-size:0.66rem;letter-spacing:0.06em;text-transform:uppercase;margin-bottom:5px;">Index</label>' +
              '<div style="padding:8px 12px;border-radius:6px;border:1px solid var(--border);background-color:var(--bg-card);color:var(--text-primary);font-family:var(--font-mono);font-size:0.78rem;">NIFTY</div></div>' +
            '<div><label style="display:block;color:var(--text-muted);font-size:0.66rem;letter-spacing:0.06em;text-transform:uppercase;margin-bottom:5px;">Call Strike</label>' +
              '<select id="mtCeStrike" class="mt-select"></select></div>' +
            '<div><label style="display:block;color:var(--text-muted);font-size:0.66rem;letter-spacing:0.06em;text-transform:uppercase;margin-bottom:5px;">Put Strike</label>' +
              '<select id="mtPeStrike" class="mt-select"></select></div>' +
            // Stop Loss in PREMIUM POINTS from entry. Mandatory, ceiling 50. Default 50;
            // operator can pick anything > 0 and ≤ 50. Breach (SELL → LTP ≥ entry + pts;
            // BUY → LTP ≤ entry − pts).
            '<div><label style="display:block;color:var(--text-muted);font-size:0.66rem;letter-spacing:0.06em;text-transform:uppercase;margin-bottom:5px;" title="Stop loss in premium points from entry. Required; max 50.">SL (pts) *</label>' +
              '<input id="mtStopLoss" type="number" min="5" max="50" step="5" value="25" required class="mt-input"></div>' +
            '<div><label style="display:block;color:var(--text-muted);font-size:0.66rem;letter-spacing:0.06em;text-transform:uppercase;margin-bottom:5px;">Qty (Lots)</label>' +
              '<input id="mtLots" type="number" min="1" value="1" class="mt-input"></div>' +
            '<div><label style="display:block;color:var(--text-muted);font-size:0.66rem;letter-spacing:0.06em;text-transform:uppercase;margin-bottom:5px;">Product</label>' +
              '<select id="mtProduct" class="mt-select">' +
                '<option value="OVERNIGHT" selected>OVERNIGHT</option>' +
                '<option value="INTRADAY">INTRADAY</option>' +
              '</select></div>' +
            // Refresh ATM — re-fetches the option chain and snaps both CE/PE strike
            // dropdowns to the bot's default ATM (AtmTracker baseline, locked to the
            // open-price strike for the session).
            '<div><label style="display:block;color:transparent;font-size:0.66rem;letter-spacing:0.06em;text-transform:uppercase;margin-bottom:5px;">·</label>' +
              '<button id="mtRefreshAtm" type="button" title="Reset both strike dropdowns to the bot’s default ATM (open-price locked)" ' +
                'style="background:transparent;border:1px solid var(--accent-purple, #8b5cf6);color:var(--accent-purple, #8b5cf6);' +
                'padding:8px 14px;border-radius:6px;font-family:var(--font-mono);font-size:0.74rem;font-weight:700;letter-spacing:0.04em;cursor:pointer;white-space:nowrap;">' +
                '↻ ATM</button></div>' +
            '</div>';
    }
    function buildLtpRowHtml() {
        return '<div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:18px;align-items:center;margin-bottom:18px;padding:14px 22px;border:1px solid var(--border);border-radius:8px;background:var(--bg-primary);">' +
            '<div>' +
              '<div style="color:var(--text-muted);font-size:0.66rem;letter-spacing:0.06em;text-transform:uppercase;" id="mtCeLabel">CE</div>' +
              '<div style="display:flex;align-items:baseline;gap:10px;">' +
                '<span style="font-size:1.6rem;font-weight:700;color:var(--accent-green, #34d399);" id="mtCeLtp">—</span>' +
                '<span id="mtCeChange" style="font-family:var(--font-mono);font-size:0.8rem;color:var(--text-muted);">—</span>' +
              '</div>' +
            '</div>' +
            '<div style="text-align:center;">' +
              '<div id="mtNiftyLabel" style="color:var(--text-muted);font-size:0.66rem;letter-spacing:0.06em;text-transform:uppercase;">NIFTY</div>' +
              '<div style="display:flex;align-items:baseline;justify-content:center;gap:10px;">' +
                '<span style="font-size:1.6rem;font-weight:700;color:var(--text-primary);" id="mtNiftyLtp">—</span>' +
                '<span id="mtNiftyChange" style="font-family:var(--font-mono);font-size:0.8rem;color:var(--text-muted);">—</span>' +
              '</div>' +
            '</div>' +
            '<div style="text-align:right;">' +
              '<div style="color:var(--text-muted);font-size:0.66rem;letter-spacing:0.06em;text-transform:uppercase;" id="mtPeLabel">PE</div>' +
              '<div style="display:flex;align-items:baseline;justify-content:flex-end;gap:10px;">' +
                '<span style="font-size:1.6rem;font-weight:700;color:var(--accent-red, #f87171);" id="mtPeLtp">—</span>' +
                '<span id="mtPeChange" style="font-family:var(--font-mono);font-size:0.8rem;color:var(--text-muted);">—</span>' +
              '</div>' +
            '</div>' +
            '</div>';
    }
    function buildActionRowHtml() {
        return '<div style="display:grid;grid-template-columns:1fr 1fr 1fr 1fr;gap:12px;margin-bottom:14px;">' +
            // BUY buttons bracket the row (first + last); SELL buttons sit inside.
            // Arrow + color reflect the market view the trade EXPRESSES:
            // BUY CALL / SELL PUT  → bullish → ↑ green
            // SELL CALL / BUY PUT  → bearish → ↓ red
            '<button id="mtBuyCall"  style="' + bigBtn('green', '↑') + '">↑ BUY CALL</button>' +
            '<button id="mtSellCall" style="' + bigBtn('red',   '↓') + '">↓ SELL CALL</button>' +
            '<button id="mtSellPut"  style="' + bigBtn('green', '↑') + '">↑ SELL PUT</button>' +
            '<button id="mtBuyPut"   style="' + bigBtn('red',   '↓') + '">↓ BUY PUT</button>' +
            '</div>';
    }
    function buildBottomRowHtml() {
        return '<div id="mtStatusBanner" style="display:none;margin:0 0 12px;padding:8px 0;font-family:var(--font-mono);font-size:0.78rem;letter-spacing:0.02em;text-align:center;font-weight:600;"></div>' +
            '<div style="display:flex;justify-content:center;gap:14px;margin-bottom:18px;">' +
                '<button id="mtCloseAll" style="background:transparent;border:1px solid var(--border);color:var(--text-secondary);padding:8px 22px;border-radius:6px;font-family:var(--font-mono);font-size:0.72rem;font-weight:600;cursor:pointer;letter-spacing:0.04em;">✕ CLOSE ALL POSITIONS</button>' +
            '</div>';
    }
    function buildPositionsHtml() {
        // Recent Trades pane retired — the Trades page (/trades) already shows the
        // full completed-trade history. The terminal now shows only open positions.
        return '<div style="border-top:1px solid var(--border);padding-top:14px;">' +
            '<div style="display:flex;align-items:center;justify-content:space-between;border-bottom:1px solid var(--border);margin-bottom:10px;padding-bottom:8px;">' +
              '<div style="font-family:var(--font-mono);font-size:0.72rem;font-weight:700;letter-spacing:0.06em;color:var(--text-primary);">POSITIONS</div>' +
              '<span id="mtMtmSummary" style="color:var(--text-secondary);font-weight:700;font-family:var(--font-mono);font-size:0.74rem;"></span>' +
            '</div>' +
            '<div id="mtPaneMain" data-pane="positions" style="overflow-x:auto;">' +
              '<div id="mtPosTable"></div>' +
            '</div>' +
            '</div>';
    }
    function tabStyle(active) {
        return 'background:transparent;border:none;border-bottom:2px solid ' +
            (active ? 'var(--accent-purple, #8b5cf6)' : 'transparent') + ';color:' +
            (active ? 'var(--text-primary)' : 'var(--text-muted)') +
            ';padding:8px 18px;font-family:var(--font-mono);font-size:0.72rem;font-weight:700;letter-spacing:0.06em;cursor:pointer;';
    }
    function bigBtn(tone, arrow) {
        var bg, border, color;
        if (tone === 'green') {
            bg = 'rgba(52,211,153,0.12)'; border = 'rgba(52,211,153,0.50)'; color = 'var(--accent-green, #34d399)';
        } else {
            bg = 'rgba(248,113,113,0.12)'; border = 'rgba(248,113,113,0.50)'; color = 'var(--accent-red, #f87171)';
        }
        return 'background:' + bg + ';border:1px solid ' + border + ';color:' + color +
               ';padding:14px 18px;border-radius:8px;font-family:var(--font-mono);font-size:0.86rem;font-weight:700;letter-spacing:0.06em;cursor:pointer;';
    }

    // ── Control wiring ────────────────────────────────────────────────────────
    function wireControls() {
        // Index selection is NIFTY-only after BankNifty was retired — no
        // dropdown to wire; currentIndexSym is fixed.
        document.getElementById('mtExpiry').addEventListener('change', function() {
            // (Single-expiry resolution today; placeholder for future weekly switch.)
            refreshSelectedSymbolLtps();
            pollOnce();
        });
        // Strike change → refresh labels AND fire an immediate dashboard poll. Without the
        // poll the LTP banner would wait up to 5 seconds for the next scheduled tick because
        // the SSE channel to the browser doesn't forward every option strike (only main
        // ticker symbols). The server-side WS already has the latest LTP cached for any of
        // the ATM ± 10 strikes, so /api/manual/dashboard returns it instantly.
        document.getElementById('mtCeStrike').addEventListener('change', function() {
            refreshSelectedSymbolLtps();
            pollOnce();
        });
        document.getElementById('mtPeStrike').addEventListener('change', function() {
            refreshSelectedSymbolLtps();
            pollOnce();
        });
        // ↻ ATM — re-fetch the chain so the dropdowns snap back to the bot's default
        // ATM (AtmTracker baseline). Brief visual feedback so the operator knows the
        // click registered.
        document.getElementById('mtRefreshAtm').addEventListener('click', function() {
            var btn = document.getElementById('mtRefreshAtm');
            if (btn) { btn.textContent = '↻ …'; btn.disabled = true; }
            // Preserve the currently selected expiry on refresh — operator may
            // be on next-week and clicking ↻ ATM shouldn't pop them back to
            // current. Falls back to nearest when nothing is picked yet.
            var currentTs = (document.getElementById('mtExpiry') || {}).value || '';
            var qs = [];
            if (currentIndexSym) qs.push('symbol=' + encodeURIComponent(currentIndexSym));
            if (currentTs)       qs.push('expiryTs=' + encodeURIComponent(currentTs));
            var url = '/api/manual/strikes' + (qs.length ? ('?' + qs.join('&')) : '');
            fetch(url, { credentials: 'same-origin' })
                .then(function(r) { return r.json(); })
                .then(function(payload) {
                    chainCache = payload;
                    var opts = (payload.expiryOptions && payload.expiryOptions.length)
                        ? payload.expiryOptions : (payload.expiries || []);
                    populateExpiry(opts, payload.selectedExpiryTs || currentTs || '');
                    populateStrikes(payload.strikes || [], payload.atmStrike);
                    refreshSelectedSymbolLtps(payload);
                })
                .catch(function() { showStatus('Refresh failed', 'error'); })
                .finally(function() {
                    if (btn) { btn.textContent = '↻ ATM'; btn.disabled = false; }
                });
        });
        // Expiry dropdown change → refetch strikes for the picked weekly. The
        // strike grid swaps to that expiry's symbols; any subsequent order placement
        // uses those symbols (so the operator can trade next-week from the same modal).
        var expEl = document.getElementById('mtExpiry');
        if (expEl) expEl.addEventListener('change', function() {
            var ts = expEl.value || '';
            loadChain(ts);
        });
    }
    function wireActionButtons() {
        document.getElementById('mtSellCall').addEventListener('click', function() { placeOrder('SELL', 'CE'); });
        document.getElementById('mtBuyCall').addEventListener('click',  function() { placeOrder('BUY',  'CE'); });
        document.getElementById('mtBuyPut').addEventListener('click',   function() { placeOrder('BUY',  'PE'); });
        document.getElementById('mtSellPut').addEventListener('click',  function() { placeOrder('SELL', 'PE'); });
    }
    function wireBottomButtons() {
        document.getElementById('mtCloseAll').addEventListener('click', closeAllPositions);
    }
    function wireTabs() {
        var tabs = document.querySelectorAll('.mt-tab');
        tabs.forEach(function(t) {
            t.addEventListener('click', function() {
                var which = t.getAttribute('data-tab');
                tabs.forEach(function(b) {
                    var active = b.getAttribute('data-tab') === which;
                    b.classList.toggle('mt-tab-active', active);
                    b.style.cssText = tabStyle(active);
                });
                document.getElementById('mtPaneMain').style.display   = which === 'positions' ? 'block' : 'none';
                document.getElementById('mtPaneRecent').style.display = which === 'recent'    ? 'block' : 'none';
            });
        });
    }

    // ── State machine ────────────────────────────────────────────────────────
    function open() {
        ensureBuilt();
        minimized = false;
        overlayEl.style.display = 'flex';
        pillEl.style.display    = 'none';
        loadChain();
        startPolling();
        attachSse();
    }
    function close() {
        if (!overlayEl) return;
        overlayEl.style.display = 'none';
        if (pillEl) pillEl.style.display = 'none';
        minimized = false;
        stopPolling();
        stopSlowPolling();
        detachSse();
    }
    // ── Real-time SSE: piggyback on the shared window.__tickerSSE EventSource
    //    (managed by ticker.js). On every 'ticker' event we get an array of
    //    {symbol, fyers, lp, ch, chp, position} for every subscribed symbol the
    //    server cares about — including our CE/PE legs (we explicitly subscribed
    //    them) and the NIFTY index. We update the local cache + repaint instantly.
    function attachSse() {
        if (tickerListener) return;
        var src = window.__tickerSSE;
        if (!src) {
            // ticker.js hasn't connected yet (or is reconnecting). Retry shortly.
            if (sseAttachTimer) clearTimeout(sseAttachTimer);
            sseAttachTimer = setTimeout(attachSse, 800);
            return;
        }
        tickerListener = function(evt) {
            var arr;
            try { arr = JSON.parse(evt.data); } catch (e) { return; }
            if (!Array.isArray(arr) || arr.length === 0) return;
            for (var i = 0; i < arr.length; i++) {
                var t = arr[i];
                var key = t && t.fyers ? t.fyers : (t && t.symbol);
                if (!key) continue;
                tickCache[key] = { ltp: Number(t.lp || 0), ch: Number(t.ch || 0), chp: Number(t.chp || 0) };
            }
            applyRealtime();
        };
        src.addEventListener('ticker', tickerListener);
    }
    function detachSse() {
        if (sseAttachTimer) { clearTimeout(sseAttachTimer); sseAttachTimer = null; }
        var src = window.__tickerSSE;
        if (src && tickerListener) {
            try { src.removeEventListener('ticker', tickerListener); } catch (e) {}
        }
        tickerListener = null;
    }
    // Repaint banner + positions + header totals from tickCache (no network).
    function applyRealtime() {
        // NIFTY banner
        var idx = tickCache[currentIndexSym];
        if (idx) renderQuote('mtNiftyLtp', 'mtNiftyChange', idx.ltp, idx.ch, idx.chp);
        // CE / PE banner — drive off the currently-selected dropdown symbols.
        var ceSym = selectedCeSymbol();
        var peSym = selectedPeSymbol();
        var ce = ceSym ? tickCache[ceSym] : null;
        var pe = peSym ? tickCache[peSym] : null;
        if (ce) renderQuote('mtCeLtp', 'mtCeChange', ce.ltp, ce.ch, ce.chp);
        if (pe) renderQuote('mtPeLtp', 'mtPeChange', pe.ltp, pe.ch, pe.chp);
        // Position rows + totals — recompute LTP / P&L per row from the cache.
        if (!lastData || !Array.isArray(lastData.openPositions) || lastData.openPositions.length === 0) {
            if (minimized) updatePillText();
            return;
        }
        var totalMtm = 0;
        var anyChanged = false;
        for (var i = 0; i < lastData.openPositions.length; i++) {
            var p = lastData.openPositions[i];
            var c = tickCache[p.symbol];
            if (c && c.ltp > 0) {
                p.ltp = c.ltp;
                if (p.avgPrice > 0) {
                    p.pnl = (p.side === 'BUY' ? (c.ltp - p.avgPrice) : (p.avgPrice - c.ltp)) * p.qty;
                }
                anyChanged = true;
            }
            totalMtm += Number(p.pnl || 0);
        }
        if (anyChanged) {
            lastData.totalMtm = totalMtm;
            // Net P&L = realised + unrealised − charges; charges come from the last poll.
            var realised = Number(lastData.realisedPnl || 0);
            var charges  = Number(lastData.charges || 0);
            lastData.netPnl = realised + totalMtm - charges;
            if (minimized) {
                updatePillText();
            } else {
                renderPositions(lastData.openPositions);
                renderHeaderTotals(lastData);
                renderMtm(totalMtm);
            }
        } else if (minimized) {
            updatePillText();
        }
    }
    function minimize() {
        if (!overlayEl) return;
        minimized = true;
        overlayEl.style.display = 'none';
        positionPill();
        updatePillText();
        startSlowPolling();
        attachSse();
    }
    // Park the pill inline BEFORE the "Welcome, <user>" text on the same line in
    // the page header, with extra top spacing for breathing room below the
    // ticker bar above. Fallback (no Welcome line on page) → top-right fixed chip.
    function positionPill() {
        var userNameSpan = document.getElementById('userName');
        var welcomeLine  = userNameSpan ? userNameSpan.parentElement : null;
        if (welcomeLine) {
            pillEl.style.position      = 'static';
            pillEl.style.top           = 'auto';
            pillEl.style.left          = 'auto';
            pillEl.style.right         = 'auto';
            pillEl.style.bottom        = 'auto';
            pillEl.style.transform     = 'none';
            pillEl.style.marginTop     = '0';
            pillEl.style.marginBottom  = '0';
            pillEl.style.marginRight   = '12px';
            pillEl.style.verticalAlign = 'middle';
            pillEl.style.padding       = '6px 14px';
            pillEl.style.fontSize      = '0.72rem';
            pillEl.style.display       = 'inline-block';
            if (welcomeLine.firstChild !== pillEl) {
                welcomeLine.insertBefore(pillEl, welcomeLine.firstChild);
            }
            return;
        }
        if (pillEl.parentElement !== document.body) document.body.appendChild(pillEl);
        pillEl.style.position     = 'fixed';
        pillEl.style.top          = '60px';
        pillEl.style.left          = 'auto';
        pillEl.style.right        = '18px';
        pillEl.style.bottom       = 'auto';
        pillEl.style.transform    = 'none';
        pillEl.style.marginTop    = '0';
        pillEl.style.marginRight  = '0';
        pillEl.style.marginBottom = '0';
        pillEl.style.padding      = '10px 18px';
        pillEl.style.fontSize     = '0.78rem';
        pillEl.style.display      = 'block';
    }
    function restore() {
        if (!overlayEl) return;
        minimized = false;
        pillEl.style.display    = 'none';
        overlayEl.style.display = 'flex';
        stopSlowPolling();
        startPolling();
        attachSse();
    }
    function toggleMaximize() {
        if (!modalEl) return;
        maximized = !maximized;
        if (maximized) {
            modalEl.style.width = '100vw';
            modalEl.style.height = '100vh';
            modalEl.style.maxWidth = '100vw';
            modalEl.style.maxHeight = '100vh';
            modalEl.style.borderRadius = '0';
            document.getElementById('mtMaxBtn').textContent = '▭';
        } else {
            modalEl.style.width = '1120px';
            modalEl.style.height = '640px';
            modalEl.style.maxWidth = '96vw';
            modalEl.style.maxHeight = '92vh';
            modalEl.style.borderRadius = '12px';
            document.getElementById('mtMaxBtn').textContent = '▢';
        }
    }

    // ── Strike chain loading + LTP refresh ───────────────────────────────────
    // expiryTs is optional. Blank/null = nearest (current week). When the operator
    // picks "next" in the dropdown the change-handler calls loadChain(ts) with
    // the epoch-second timestamp from expiryOptions.
    function loadChain(expiryTs) { refreshStrikes(expiryTs || ''); }

    /** Shared chain fetch — passes the current instrument symbol AND the
     *  optional expiry timestamp. Called by boot, index-swap, refresh-ATM,
     *  and expiry-change flows. */
    function refreshStrikes(expiryTs) {
        var qs = [];
        if (currentIndexSym) qs.push('symbol=' + encodeURIComponent(currentIndexSym));
        if (expiryTs)        qs.push('expiryTs=' + encodeURIComponent(expiryTs));
        var url = '/api/manual/strikes' + (qs.length ? ('?' + qs.join('&')) : '');
        fetch(url, { credentials: 'same-origin' })
            .then(function(r) { return r.json(); })
            .then(function(payload) {
                chainCache = payload;
                var opts = (payload.expiryOptions && payload.expiryOptions.length)
                    ? payload.expiryOptions : (payload.expiries || []);
                populateExpiry(opts, payload.selectedExpiryTs || expiryTs || '');
                populateStrikes(payload.strikes || [], payload.atmStrike);
                refreshSelectedSymbolLtps(payload);
            })
            .catch(function() { /* leave dropdowns empty */ });
    }
    // Populate the expiry dropdown. New shape (preferred): expiryOptions = [{date, ts}, …]
    // Each option's value carries the epoch-second timestamp so a change-handler
    // can refetch the strikes for that specific expiry. Falls back to the legacy
    // expiries string list when expiryOptions isn't provided (older backend).
    function populateExpiry(opts, selectedTs) {
        var sel = document.getElementById('mtExpiry');
        if (!sel) return;
        if (!opts || opts.length === 0) { sel.innerHTML = '<option>—</option>'; return; }
        if (typeof opts[0] === 'string') {
            // back-compat path
            sel.innerHTML = opts.map(function(e) { return '<option value="' + e + '">' + e + '</option>'; }).join('');
            return;
        }
        sel.innerHTML = opts.map(function(o, i) {
            var ts = o.ts || '';
            var label = o.date + (i === 0 ? ' · current' : ' · next');
            var isSel = selectedTs ? (selectedTs === ts) : (i === 0);
            return '<option value="' + ts + '"' + (isSel ? ' selected' : '') + '>' + label + '</option>';
        }).join('');
    }
    function populateStrikes(strikes, atmStrike) {
        var ce = document.getElementById('mtCeStrike');
        var pe = document.getElementById('mtPeStrike');
        if (!ce || !pe) return;
        var opts = strikes.map(function(r) {
            return '<option value="' + r.strike + '" data-ce="' + r.ce + '" data-pe="' + r.pe + '"' +
                (r.strike === atmStrike ? ' selected' : '') + '>' + r.strike + (r.strike === atmStrike ? ' (ATM)' : '') + '</option>';
        }).join('');
        ce.innerHTML = opts;
        pe.innerHTML = opts;
    }
    function selectedCeSymbol() {
        var sel = document.getElementById('mtCeStrike');
        if (!sel || !sel.selectedOptions[0]) return '';
        return sel.selectedOptions[0].getAttribute('data-ce') || '';
    }
    function selectedPeSymbol() {
        var sel = document.getElementById('mtPeStrike');
        if (!sel || !sel.selectedOptions[0]) return '';
        return sel.selectedOptions[0].getAttribute('data-pe') || '';
    }
    function refreshSelectedSymbolLtps(payload) {
        var ceSym = selectedCeSymbol();
        var peSym = selectedPeSymbol();
        document.getElementById('mtCeLabel').textContent = ceSym ? abbrev(ceSym) : 'CE';
        document.getElementById('mtPeLabel').textContent = peSym ? abbrev(peSym) : 'PE';
    }
    function abbrev(symbol) {
        // Pull the strike + optType tail. NIFTY weekly format: NSE:NIFTY<YY><M><DD><strike><CE/PE>
        // e.g. NSE:NIFTY2660923300CE → 23300 CE. NIFTY strikes are 4–5 digits; capping
        // the regex at 5 prevents it from sucking in the trailing day-digit ("9" here)
        // and rendering "923300 CE" instead of "23300 CE".
        var m = symbol.match(/(\d{4,5})(CE|PE)$/);
        return m ? (m[1] + ' ' + m[2]) : symbol;
    }

    // ── Status banner (inline error / success below action row) ──────────────
    var statusTimer = null;
    function showStatus(message, kind) {
        var el = document.getElementById('mtStatusBanner');
        if (!el || !message) return;
        var color;
        if (kind === 'error')        color = 'var(--accent-red, #f87171)';
        else if (kind === 'success') color = 'var(--accent-green, #34d399)';
        else                          color = 'var(--accent-purple, #8b5cf6)';
        el.style.color = color;
        el.textContent = message;
        el.style.display = 'block';
        if (statusTimer) { clearTimeout(statusTimer); statusTimer = null; }
        // Errors linger longer than successes — broker reject messages deserve a longer read.
        var dwell = kind === 'error' ? 12000 : 5000;
        statusTimer = setTimeout(function() {
            if (el) el.style.display = 'none';
        }, dwell);
    }

    // ── Order placement ──────────────────────────────────────────────────────
    function placeOrder(side, leg) {
        var symbol = leg === 'CE' ? selectedCeSymbol() : selectedPeSymbol();
        if (!symbol) {
            showStatus('Strike not resolved yet — wait for the chain to load.', 'error');
            return;
        }
        var lots        = parseInt(document.getElementById('mtLots').value, 10) || 1;
        var product     = document.getElementById('mtProduct').value || 'OVERNIGHT';
        var slRaw       = document.getElementById('mtStopLoss').value;
        var stopLossPts = parseFloat(slRaw);
        // SL is mandatory (cannot be empty) and must be > 0 and ≤ 50. Server enforces
        // the same range; this client check fails fast on clearly invalid input.
        if (slRaw === '' || !(stopLossPts > 0)) {
            showStatus('SL is required — please enter a stop-loss value.', 'error');
            return;
        }
        if (stopLossPts > 50) {
            showStatus('SL cannot exceed 50 points.', 'error');
            return;
        }
        // No confirm modal — operator clicks Buy/Sell and we fire immediately. The inline
        // status banner below shows the broker's response (or rejection reason).
        fetch('/api/manual/order', {
            method: 'POST',
            headers: Object.assign({ 'Content-Type': 'application/json' }, csrfHeaders()),
            body: JSON.stringify({ side: side, symbol: symbol, lots: lots, product: product, stopLossPts: stopLossPts })
        }).then(function(r) { return r.json(); }).then(function(resp) {
            // Silent on success — the position appears in the table on the next pollOnce.
            // Errors still surface so the operator knows when something failed.
            if (!resp.success) showStatus(resp.message || 'Order failed', 'error');
            pollOnce();
        }).catch(function(err) {
            showStatus('Order failed: ' + (err && err.message ? err.message : err), 'error');
        });
    }
    function closeOnePosition(orderId, symbol) {
        var doIt = function() {
            fetch('/api/manual/close/' + encodeURIComponent(orderId), {
                method: 'POST',
                headers: Object.assign({ 'Content-Type': 'application/json' }, csrfHeaders())
            }).then(function(r) { return r.json(); }).then(function(resp) {
                if (window.showToast) showToast(resp.message || 'Close submitted', resp.success ? 'success' : 'error');
                pollOnce();
            });
        };
        if (window.AppConfirm) {
            window.AppConfirm.ask({
                title:        'Close Position',
                message:      'Close ' + symbol + ' at market?\n\nPlaces an opposite-side order to flatten the position.',
                confirmLabel: 'Close',
                danger:       true
            }).then(function(ok) { if (ok) doIt(); });
        } else {
            doIt();   // fallback when common.js hasn't loaded
        }
    }
    function closeAllPositions() {
        var doIt = function() {
            fetch('/api/manual/close-all', {
                method: 'POST',
                headers: Object.assign({ 'Content-Type': 'application/json' }, csrfHeaders())
            }).then(function(r) { return r.json(); }).then(function(resp) {
                if (window.showToast) showToast(resp.message || 'Close All submitted', 'success');
                pollOnce();
            });
        };
        if (window.AppConfirm) {
            window.AppConfirm.ask({
                title:        'Close All Manual Positions',
                message:      'Places an opposite-side market order for every open manual position.\n\nAlgo (Camarilla) positions are untouched.',
                confirmLabel: 'Close All',
                danger:       true
            }).then(function(ok) { if (ok) doIt(); });
        } else {
            doIt();
        }
    }
    // ── Polling + render ─────────────────────────────────────────────────────
    // SSE drives the per-tick refresh (LTPs, P&L). The dashboard poll only carries
    // fields SSE doesn't — filled-flag, slTrigger, charges, recent trades — so it
    // can run at a much slower cadence than before.
    function startPolling() {
        stopPolling();
        pollOnce();
        pollTimer = setInterval(pollOnce, 5000);
    }
    function stopPolling() {
        if (pollTimer) { clearInterval(pollTimer); pollTimer = null; }
    }
    function startSlowPolling() {
        stopSlowPolling();
        pollOnce();
        slowPollTimer = setInterval(pollOnce, 15000);
    }
    function stopSlowPolling() {
        if (slowPollTimer) { clearInterval(slowPollTimer); slowPollTimer = null; }
    }
    function pollOnce() {
        var ce = selectedCeSymbol();
        var pe = selectedPeSymbol();
        var qs = [];
        if (currentIndexSym) qs.push('symbol=' + encodeURIComponent(currentIndexSym));
        if (ce) qs.push('ceSymbol=' + encodeURIComponent(ce));
        if (pe) qs.push('peSymbol=' + encodeURIComponent(pe));
        var url = '/api/manual/dashboard' + (qs.length ? ('?' + qs.join('&')) : '');
        fetch(url, { credentials: 'same-origin' })
            .then(function(r) { return r.json(); })
            .then(function(data) {
                lastData = data;
                renderHeaderTotals(data);
                if (minimized) { updatePillText(); applyRealtime(); return; }
                renderLtps(data);
                renderPositions(data.openPositions || []);
                // Recent Trades pane retired — full history lives on the Trades page.
                renderMtm(data.totalMtm || 0);
                // Overlay the freshest SSE ticks on top of the just-rendered server values
                // so we never display anything staler than the most recent tick.
                applyRealtime();
            })
            .catch(function() {});
    }
    function renderQuote(ltpId, changeId, ltp, change, changePct) {
        var ltpEl = document.getElementById(ltpId);
        var chEl  = document.getElementById(changeId);
        if (ltpEl) ltpEl.textContent = ltp > 0 ? Number(ltp).toFixed(2) : '—';
        if (!chEl) return;
        if (ltp <= 0 || (change === 0 && changePct === 0)) {
            chEl.textContent = '—';
            chEl.style.color = 'var(--text-muted)';
            return;
        }
        var sign = change > 0 ? '+' : (change < 0 ? '−' : '');
        var pctStr = changePct === 0 ? '' : ' (' + sign + Math.abs(changePct).toFixed(2) + '%)';
        chEl.textContent = sign + Math.abs(change).toFixed(2) + pctStr;
        chEl.style.color = change > 0 ? 'var(--accent-green, #34d399)'
            : (change < 0 ? 'var(--accent-red, #f87171)' : 'var(--text-muted)');
    }
    function renderLtps(data) {
        renderQuote('mtNiftyLtp', 'mtNiftyChange',
            Number(data && data.niftyLtp || 0),
            Number(data && data.niftyChange || 0),
            Number(data && data.niftyChangePct || 0));
        var ce = (data && data.selectedCe) || {};
        var pe = (data && data.selectedPe) || {};
        renderQuote('mtCeLtp', 'mtCeChange',
            Number(ce.ltp || 0), Number(ce.change || 0), Number(ce.changePct || 0));
        renderQuote('mtPeLtp', 'mtPeChange',
            Number(pe.ltp || 0), Number(pe.change || 0), Number(pe.changePct || 0));
    }
    function renderPositions(rows) {
        var wrap = document.getElementById('mtPosTable');
        if (!wrap) return;
        // Don't blow away an in-progress inline SL edit. Every tick / poll calls this; if
        // the operator has clicked an SL value to edit it, the input lives inside #mtPosTable
        // until they Enter / Esc / blur it. Re-rendering destroys it mid-keystroke.
        if (wrap.querySelector('input[data-sl-editing="1"]')) return;
        // table-layout:fixed + percentage widths so columns spread across the modal width.
        // ACTION is sized just for the small ✕ button — extra width goes to the data
        // columns where it's actually useful.
        var header = '<table style="width:100%;border-collapse:collapse;table-layout:fixed;font-family:var(--font-mono);font-size:0.76rem;">' +
            '<thead><tr style="color:var(--text-muted);font-size:0.66rem;letter-spacing:0.06em;text-transform:uppercase;">' +
                '<th style="text-align:left;padding:6px 8px;width:5%;">#</th>' +
                '<th style="text-align:left;padding:6px 8px;width:14%;">SYMBOL</th>' +
                '<th style="text-align:center;padding:6px 8px;width:8%;">SIDE</th>' +
                '<th style="text-align:right;padding:6px 8px;width:8%;">QTY</th>' +
                '<th style="text-align:right;padding:6px 8px;width:11%;">AVG</th>' +
                '<th style="text-align:right;padding:6px 8px;width:11%;">LTP</th>' +
                '<th style="text-align:center;padding:6px 8px;width:21%;" title="Stop-loss trigger price = entry ± SL pts">SL</th>' +
                '<th style="text-align:right;padding:6px 8px;width:15%;">P&amp;L</th>' +
                '<th style="text-align:center;padding:6px 8px;width:7%;">ACTION</th>' +
            '</tr></thead><tbody>';
        var body = '';
        if (rows.length === 0) {
            body = '<tr><td colspan="9" style="padding:18px 8px;text-align:center;color:var(--text-muted);font-size:0.74rem;">No open manual positions. Use the Buy / Sell buttons above to place an order.</td></tr>';
        } else {
            rows.forEach(function(p, idx) {
                var pnl = Number(p.pnl || 0);
                var pnlCls = pnl > 0 ? 'color:var(--accent-green, #34d399);' : (pnl < 0 ? 'color:var(--accent-red, #f87171);' : '');
                var sideCol = p.side === 'BUY' ? 'var(--accent-green, #34d399)' : 'var(--accent-red, #f87171)';
                var slTrigger = Number(p.slTrigger || 0);
                var slPts     = Number(p.stopLossPts || 0);
                // Inline SL +/- buttons (1 pt per click). The SL trigger value itself is
                // click-to-edit: clicking it swaps the span for an input pre-filled with
                // the current value. Enter or blur commits the change via the same /sl
                // endpoint using a computed deltaPts. Qty adjustments use the main
                // BUY/SELL action row above — it routes through placeManual's merge logic
                // (same direction adds, opposite direction reduces).
                var idAttr = escapeHtml(String(p.orderId || ''));
                var slEditableSpan = slTrigger > 0
                    ? ('<span data-sl-edit="1" data-id="' + idAttr + '" data-current="' + slTrigger.toFixed(2)
                        + '" title="Click to edit — Enter saves, Esc cancels (entry '
                        + (p.side === 'SELL' ? '+' : '−') + ' ' + slPts.toFixed(0) + ' pts)" '
                        + 'style="margin:0 6px;cursor:pointer;">'
                        + slTrigger.toFixed(2) + '</span>')
                    : (slPts > 0 ? ('<span style="color:var(--text-muted);margin:0 6px;" title="Will compute on fill">+' + slPts.toFixed(0) + ' pts</span>') : '<span style="color:var(--text-muted);margin:0 6px;">—</span>');
                // "→C" Move SL to Cost — rendered in the Action column (see below) since
                // the operator wanted row-level actions grouped there. Click computes
                // deltaPts = avg − slTrigger and POSTs to /api/manual/sl/{orderId}.
                var costPx = Number(p.avgPrice || 0);
                var moveToCostBtn = (slTrigger > 0 && costPx > 0 && costPx !== slTrigger)
                    ? '<button data-sl-cost="1" data-id="' + idAttr
                        + '" data-cost="' + costPx.toFixed(2)
                        + '" data-current="' + slTrigger.toFixed(2)
                        + '" title="Move SL to cost (' + costPx.toFixed(2) + ')" '
                        + 'style="' + adjustBtn('amber') + 'margin-right:4px;width:auto;padding:0 6px;">→C</button>'
                    : '';
                var slAdjust = slTrigger > 0
                    ? '<button data-sl="-1" data-id="' + idAttr + '" title="Lower SL 1 pt" style="' + adjustBtn('red')   + '">−</button>' +
                      slEditableSpan +
                      '<button data-sl="+1" data-id="' + idAttr + '" title="Raise SL 1 pt" style="' + adjustBtn('green') + '">+</button>'
                    : slEditableSpan;
                body += '<tr style="border-top:1px solid rgba(128,128,128,0.10);">' +
                    '<td style="padding:8px;color:var(--text-muted);">' + (idx + 1) + '</td>' +
                    '<td style="padding:8px;font-size:0.74rem;white-space:nowrap;" title="' + escapeHtml(p.symbol) + '">' + escapeHtml(abbrev(p.symbol)) + '</td>' +
                    '<td style="padding:8px;text-align:center;color:' + sideCol + ';font-weight:700;">' + p.side + '</td>' +
                    '<td style="padding:8px;text-align:right;">' + p.qty + '</td>' +
                    '<td style="padding:8px;text-align:right;">' + (p.avgPrice > 0 ? Number(p.avgPrice).toFixed(2) : (p.filled ? '—' : '⏳')) + '</td>' +
                    '<td style="padding:8px;text-align:right;">' + (p.ltp > 0 ? Number(p.ltp).toFixed(2) : '—') + '</td>' +
                    '<td style="padding:8px;text-align:center;color:var(--accent-amber, #fbbf24);white-space:nowrap;">' + slAdjust + '</td>' +
                    '<td style="padding:8px;text-align:right;font-weight:700;' + pnlCls + '">' + fmtRs(pnl) + '</td>' +
                    '<td style="padding:8px;text-align:center;white-space:nowrap;">'
                        + moveToCostBtn
                        + '<button data-close="' + p.orderId + '" data-sym="' + escapeHtml(p.symbol) + '" title="Close position" style="background:transparent;border:1px solid rgba(248,113,113,0.35);color:var(--accent-red, #f87171);width:24px;height:24px;border-radius:4px;font-family:var(--font-mono);font-size:0.85rem;font-weight:700;cursor:pointer;line-height:1;padding:0;">✕</button>'
                        + '</td>' +
                '</tr>';
            });
        }
        wrap.innerHTML = header + body + '</tbody></table>';
        // Wire per-row close buttons.
        wrap.querySelectorAll('button[data-close]').forEach(function(btn) {
            btn.addEventListener('click', function() {
                closeOnePosition(btn.getAttribute('data-close'), btn.getAttribute('data-sym'));
            });
        });
        // Wire inline SL +/- buttons (1 pt per click). Qty changes go through the main
        // 4-button row above which calls placeManual — its merge logic handles same-
        // direction adds and opposite-direction reduces automatically.
        wrap.querySelectorAll('button[data-sl]').forEach(function(btn) {
            btn.addEventListener('click', function() {
                var id = btn.getAttribute('data-id');
                var deltaPts = parseInt(btn.getAttribute('data-sl'), 10);
                fetch('/api/manual/sl/' + encodeURIComponent(id), {
                    method: 'POST',
                    headers: Object.assign({ 'Content-Type': 'application/json' }, csrfHeaders()),
                    body: JSON.stringify({ deltaPts: deltaPts })
                }).then(function(r) { return r.json(); }).then(function(resp) {
                    // SL adjust doesn't place an order — it just mutates the in-memory trigger
                    // level. No success message; only error feedback.
                    if (!resp.success) showStatus(resp.message || 'SL adjust failed', 'error');
                    pollOnce();
                }).catch(function(err) {
                    showStatus('SL adjust failed: ' + (err && err.message ? err.message : err), 'error');
                });
            });
        });
        // "→C" Move SL to Cost — sends deltaPts = avgPrice − currentSlTrigger so the
        // trigger lands exactly at the entry. Same backend endpoint as the +/- buttons.
        wrap.querySelectorAll('button[data-sl-cost]').forEach(function(btn) {
            btn.addEventListener('click', function() {
                var id      = btn.getAttribute('data-id');
                var cost    = parseFloat(btn.getAttribute('data-cost'));
                var current = parseFloat(btn.getAttribute('data-current'));
                if (!(cost > 0) || !(current > 0)) return;
                var deltaPts = cost - current;
                if (Math.abs(deltaPts) < 0.005) return;   // already at cost
                fetch('/api/manual/sl/' + encodeURIComponent(id), {
                    method: 'POST',
                    headers: Object.assign({ 'Content-Type': 'application/json' }, csrfHeaders()),
                    body: JSON.stringify({ deltaPts: deltaPts })
                }).then(function(r) { return r.json(); }).then(function(resp) {
                    if (!resp.success) showStatus(resp.message || 'Move-SL-to-cost failed', 'error');
                    pollOnce();
                }).catch(function(err) {
                    showStatus('Move-SL-to-cost failed: ' + (err && err.message ? err.message : err), 'error');
                });
            });
        });
        // Click-to-edit on the SL trigger value. Replaces the span with an inline number
        // input pre-filled with the current trigger price. Enter commits via the same /sl
        // endpoint (computed deltaPts = newValue − currentValue); Esc cancels.
        wrap.querySelectorAll('span[data-sl-edit]').forEach(function(span) {
            span.addEventListener('click', function() {
                if (span.dataset.editing === '1') return;        // already editing
                span.dataset.editing = '1';
                var id      = span.getAttribute('data-id');
                var current = parseFloat(span.getAttribute('data-current')) || 0;
                var input = document.createElement('input');
                input.type = 'number';
                input.step = '0.05';
                input.min  = '0.05';
                input.value = current.toFixed(2);
                input.setAttribute('data-sl-editing', '1');   // flags renderPositions to skip re-render
                input.style.cssText = 'width:70px;padding:2px 4px;margin:0 6px;font-family:var(--font-mono);'
                    + 'font-size:0.76rem;text-align:center;border:1px solid var(--accent-amber, #fbbf24);'
                    + 'border-radius:4px;background:var(--bg-card);color:var(--text-primary);outline:none;';
                span.replaceWith(input);
                input.focus();
                input.select();
                var committed = false;
                function commit() {
                    if (committed) return;
                    committed = true;
                    // Clear the editing flag so the next renderPositions() doesn't bail —
                    // we WANT the table to repaint and replace this input with a fresh span.
                    input.removeAttribute('data-sl-editing');
                    var newVal = parseFloat(input.value);
                    if (!(newVal > 0) || newVal === current) {
                        // Invalid or unchanged — just refresh to restore the span.
                        pollOnce();
                        return;
                    }
                    var deltaPts = newVal - current;
                    fetch('/api/manual/sl/' + encodeURIComponent(id), {
                        method: 'POST',
                        headers: Object.assign({ 'Content-Type': 'application/json' }, csrfHeaders()),
                        body: JSON.stringify({ deltaPts: deltaPts })
                    }).then(function(r) { return r.json(); }).then(function(resp) {
                        if (!resp.success) showStatus(resp.message || 'SL adjust failed', 'error');
                        pollOnce();
                    }).catch(function(err) {
                        showStatus('SL adjust failed: ' + (err && err.message ? err.message : err), 'error');
                        pollOnce();
                    });
                }
                input.addEventListener('keydown', function(e) {
                    if (e.key === 'Enter')      { e.preventDefault(); commit(); }
                    else if (e.key === 'Escape') {
                        committed = true;
                        input.removeAttribute('data-sl-editing');
                        pollOnce();
                    }
                });
                input.addEventListener('blur', commit);
            });
        });
    }

    /** Style for the small inline +/- / move-to-cost buttons on the positions table.
     *  Matches the close button's visual weight but smaller, with tone-coded accent. */
    function adjustBtn(tone) {
        var color, border;
        if      (tone === 'green') { color = 'var(--accent-green, #34d399)'; border = 'rgba(52,211,153,0.45)'; }
        else if (tone === 'amber') { color = 'var(--accent-amber, #fbbf24)'; border = 'rgba(251,191,36,0.55)'; }
        else                       { color = 'var(--accent-red, #f87171)';    border = 'rgba(248,113,113,0.45)'; }
        return 'background:transparent;border:1px solid ' + border + ';color:' + color +
               ';width:22px;height:22px;border-radius:4px;font-family:var(--font-mono);font-size:0.78rem;' +
               'font-weight:700;cursor:pointer;line-height:1;padding:0;vertical-align:middle;';
    }
    function renderRecent(rows) {
        var wrap = document.getElementById('mtRecentTable');
        if (!wrap) return;
        var header = '<table style="width:100%;border-collapse:collapse;font-family:var(--font-mono);font-size:0.72rem;">' +
            '<thead><tr style="color:var(--text-muted);font-size:0.64rem;letter-spacing:0.06em;text-transform:uppercase;">' +
                '<th style="text-align:left;padding:5px 8px;width:36px;">#</th>' +
                '<th style="text-align:left;padding:5px 8px;">SYMBOL</th>' +
                '<th style="text-align:center;padding:5px 8px;">SIDE</th>' +
                '<th style="text-align:right;padding:5px 8px;">QTY</th>' +
                '<th style="text-align:right;padding:5px 8px;">OPEN</th>' +
                '<th style="text-align:right;padding:5px 8px;">CLOSE</th>' +
                '<th style="text-align:right;padding:5px 8px;">P&amp;L</th>' +
            '</tr></thead><tbody>';
        var body = '';
        if (rows.length === 0) {
            body = '<tr><td colspan="7" style="padding:14px 8px;text-align:center;color:var(--text-muted);">No recent trades.</td></tr>';
        } else {
            rows.slice(0, 10).forEach(function(t, idx) {
                var pnl = Number(t.pnl || 0);
                var pnlCls = pnl > 0 ? 'color:var(--accent-green, #34d399);' : (pnl < 0 ? 'color:var(--accent-red, #f87171);' : '');
                var sideCol = t.side === 'BUY' ? 'var(--accent-green, #34d399)' : 'var(--accent-red, #f87171)';
                body += '<tr style="border-top:1px solid rgba(128,128,128,0.08);">' +
                    '<td style="padding:5px 8px;color:var(--text-muted);">' + (idx + 1) + '</td>' +
                    '<td style="padding:5px 8px;white-space:nowrap;" title="' + escapeHtml(t.symbol) + '">' + escapeHtml(abbrev(t.symbol)) + '</td>' +
                    '<td style="padding:5px 8px;text-align:center;color:' + sideCol + ';font-weight:700;">' + t.side + '</td>' +
                    '<td style="padding:5px 8px;text-align:right;">' + t.qty + '</td>' +
                    '<td style="padding:5px 8px;text-align:right;">' + (t.openPrice > 0 ? Number(t.openPrice).toFixed(2) : '—') + '</td>' +
                    '<td style="padding:5px 8px;text-align:right;">' + (t.closePrice > 0 ? Number(t.closePrice).toFixed(2) : '—') + '</td>' +
                    '<td style="padding:5px 8px;text-align:right;font-weight:700;' + pnlCls + '">' + fmtRs(pnl) + '</td>' +
                '</tr>';
            });
        }
        wrap.innerHTML = header + body + '</tbody></table>';
    }
    function renderMtm(mtm) {
        var el = document.getElementById('mtMtmSummary');
        if (el) {
            var cls = mtm > 0 ? 'color:var(--accent-green, #34d399);' : (mtm < 0 ? 'color:var(--accent-red, #f87171);' : '');
            el.style.cssText = 'float:right;font-weight:700;' + cls;
            el.textContent = 'MTM: ' + fmtRs(mtm);
        }
    }
    /** Net P&L + Charges in the modal header. Net P&L color-coded; Charges always muted. */
    function renderHeaderTotals(data) {
        var net = Number(data && data.netPnl  || 0);
        var ch  = Number(data && data.charges || 0);
        var netEl = document.getElementById('mtHdrNetPnl');
        var chEl  = document.getElementById('mtHdrCharges');
        if (netEl) {
            netEl.textContent = fmtRs(net);
            netEl.style.color = net > 0 ? 'var(--accent-green, #34d399)'
                : (net < 0 ? 'var(--accent-red, #f87171)' : 'var(--text-primary)');
        }
        if (chEl) chEl.textContent = fmtRs(ch);
    }
    function updatePillText() {
        if (!pillEl) return;
        var n   = (lastData && lastData.openPositions) ? lastData.openPositions.length : 0;
        var net = (lastData && lastData.netPnl)  || 0;
        var ch  = (lastData && lastData.charges) || 0;
        var el = document.getElementById('mtPillText');
        if (el) el.innerHTML =
            '— ' + n + ' pos · Net <span style="color:' +
            (net > 0 ? 'var(--accent-green, #34d399)' : (net < 0 ? 'var(--accent-red, #f87171)' : 'var(--text-primary)')) +
            ';font-weight:700;">' + fmtRs(net) + '</span> · Charges ' + fmtRs(ch);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────
    function fmtRs(v) {
        var n = Number(v) || 0;
        var sign = n < 0 ? '-' : (n > 0 ? '+' : '');
        var abs = Math.abs(n);
        return sign + '₹' + abs.toLocaleString('en-IN', { minimumFractionDigits: 0, maximumFractionDigits: 0 });
    }
    function escapeHtml(s) {
        return String(s == null ? '' : s)
            .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
    }
    function csrfHeaders() {
        // Most pages already define csrfHeaders() globally; if not, return empty object.
        if (typeof window.csrfHeaders === 'function') return window.csrfHeaders();
        return {};
    }

    // ── Auto-init ─ pill is always visible on every page ─────────────────────
    // No navbar button: on DOM ready we build the modal in the minimized state so
    // the pill appears in the page header from the moment the page loads. The pill
    // click → expands; the modal's − collapses back; there is no "close" because
    // the pill never goes away.
    function autoInit() {
        ensureBuilt();
        minimized = true;
        overlayEl.style.display = 'none';
        positionPill();
        updatePillText();
        loadChain();
        startSlowPolling();
        attachSse();
    }
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', autoInit);
    } else {
        autoInit();
    }

    // Public API kept for compatibility: open()/restore() both expand the pill into
    // the modal. close() collapses back to the pill (does NOT hide it).
    window.ManualTerminal = { open: restore, close: minimize, restore: restore };
})();
