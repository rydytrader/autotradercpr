// ── OI Bias Filter Effectiveness modal ──────────────────────────────────
// Shown from the Trades page period-picker row. Fetches the current period's
// summary from /api/analytics/summary?strategyId=atmvwap&from=..&to=.. and
// renders the against-bias vs everything-else split so the operator can
// decide whether enabling atmVwapOiBiasFilterEnabled is worth it.
//
// Overlay skeleton mirrors AppConfirm in common.js — same fixed dark backdrop
// + centered card + Esc / backdrop-click / Close-button dismiss. Independent
// of AppConfirm so a Close does not affect any in-flight confirm dialog.
window.OiBiasEffectivenessModal = (function() {
    var overlayEl = null;

    function build() {
        if (overlayEl) return;
        overlayEl = document.createElement('div');
        overlayEl.id = 'oiBiasEffOverlay';
        overlayEl.style.cssText =
            'display:none;position:fixed;inset:0;background:rgba(0,0,0,0.55);'
          + 'z-index:9999;align-items:center;justify-content:center;';
        overlayEl.innerHTML =
            '<div id="oiBiasEffCard" style="background:var(--bg-card);border:1px solid var(--border);'
          +   'border-radius:12px;width:620px;max-width:94vw;box-shadow:0 16px 48px rgba(0,0,0,0.45);'
          +   'overflow:hidden;">'
          +   '<div style="display:flex;align-items:center;justify-content:space-between;'
          +     'padding:14px 22px;border-bottom:1px solid var(--border);'
          +     'font-family:var(--font-mono);font-weight:700;letter-spacing:0.04em;'
          +     'color:var(--text-primary);">'
          +     '<span>⚖ OI BIAS — FILTER EFFECTIVENESS <span id="oiBiasEffPeriod" style="color:var(--text-muted);font-weight:500;margin-left:8px;"></span></span>'
          +     '<button id="oiBiasEffClose" type="button" style="background:transparent;border:none;'
          +       'color:var(--text-muted);font-size:1.2rem;cursor:pointer;line-height:1;padding:0 4px;">×</button>'
          +   '</div>'
          +   '<div id="oiBiasEffBody" style="padding:18px 22px;color:var(--text-secondary);'
          +     'font-size:0.84rem;line-height:1.55;">Loading…</div>'
          +   '<div style="display:flex;justify-content:flex-end;gap:10px;padding:14px 22px;'
          +     'border-top:1px solid var(--border);background:rgba(0,0,0,0.12);">'
          +     '<button id="oiBiasEffDone" type="button" style="border:1px solid var(--accent-cyan);'
          +       'color:var(--accent-cyan);background:rgba(125,211,252,0.10);padding:7px 18px;'
          +       'border-radius:6px;font-family:var(--font-mono);font-size:0.74rem;font-weight:700;'
          +       'letter-spacing:0.04em;cursor:pointer;">Close</button>'
          +   '</div>'
          + '</div>';
        document.body.appendChild(overlayEl);
        overlayEl.addEventListener('click', function(e) {
            if (e.target === overlayEl) close();
        });
        document.addEventListener('keydown', function(e) {
            if (overlayEl.style.display !== 'flex') return;
            if (e.key === 'Escape') { e.preventDefault(); close(); }
        });
        document.getElementById('oiBiasEffClose').addEventListener('click', close);
        document.getElementById('oiBiasEffDone').addEventListener('click', close);
    }

    function close() {
        if (overlayEl) overlayEl.style.display = 'none';
    }

    // Format a rupees value with sign + commas — Indian grouping.
    function fmtRs(v) {
        var n = Number(v || 0);
        var sign = n < 0 ? '−' : (n > 0 ? '+' : '');
        var abs = Math.abs(n);
        try {
            return sign + '₹' + abs.toLocaleString('en-IN', {
                minimumFractionDigits: 0, maximumFractionDigits: 0
            });
        } catch (e) { return sign + '₹' + Math.round(abs); }
    }
    function pnlClass(v) { return Number(v) >= 0 ? 'pnl-positive' : 'pnl-negative'; }
    function pctText(v)  { return (Number(v || 0)).toFixed(1) + '%'; }

    /** Client-side verdict rules — matches the plan file. */
    function verdictFor(against) {
        var t = Number(against.trades || 0);
        var net = Number(against.netPnl || 0);
        if (t < 5) {
            return { text: 'Not enough against-bias trades yet to judge — need more data.',
                     color: 'var(--text-muted)' };
        }
        if (net < 0) {
            return { text: '✓ Filter looks beneficial — against-bias trades are losing money.',
                     color: 'var(--accent-green, #34d399)' };
        }
        return { text: '✗ Filter is unnecessary — against-bias trades are still profitable.',
                 color: 'var(--accent-amber, #fbbf24)' };
    }

    function renderBody(data, rangeLabel) {
        var eff = (data && data.oiBiasEffectiveness) || null;
        if (!eff) {
            document.getElementById('oiBiasEffBody').innerHTML =
                '<div style="color:var(--text-muted);text-align:center;padding:24px;">'
              + 'No effectiveness data available for this period.</div>';
            return;
        }
        var against = eff.against || {};
        var other   = eff.other   || {};
        var impact  = eff.projectedFilterImpact || {};
        var ce = against.ceSell || {};
        var pe = against.peSell || {};
        var verdict = verdictFor(against);

        var rowStyle = 'padding:8px 6px;border-bottom:1px solid rgba(128,128,128,0.10);';
        var subRowStyle = 'padding:6px 6px 6px 24px;font-size:0.78rem;color:var(--text-muted);';
        var againstNet = Number(against.netPnl || 0);
        var netColor = againstNet > 0 ? 'var(--accent-green, #34d399)'
                     : againstNet < 0 ? 'var(--accent-red, #f87171)'
                     : 'var(--text-muted)';
        var savings = Number(impact.netPnlIfEnabled || 0);
        var savingsColor = savings > 0 ? 'var(--accent-green, #34d399)'
                        : savings < 0 ? 'var(--accent-red, #f87171)'
                        : 'var(--text-muted)';

        var html =
            '<table style="width:100%;border-collapse:collapse;font-family:var(--font-mono);font-size:0.82rem;">'
          +   '<thead>'
          +     '<tr style="color:var(--text-muted);text-align:left;font-size:0.72rem;letter-spacing:0.08em;">'
          +       '<th style="padding:6px;text-transform:uppercase;">Bucket</th>'
          +       '<th style="padding:6px;text-align:right;text-transform:uppercase;">Trades</th>'
          +       '<th style="padding:6px;text-align:right;text-transform:uppercase;">Win Rate</th>'
          +       '<th style="padding:6px;text-align:right;text-transform:uppercase;">Net P&amp;L</th>'
          +     '</tr>'
          +   '</thead>'
          +   '<tbody>'
          +     '<tr style="' + rowStyle + '">'
          +       '<td style="padding:6px;color:#f87171;font-weight:700;">Against bias <span style="color:var(--text-muted);font-weight:400;font-size:0.72rem;margin-left:6px;">← would block</span></td>'
          +       '<td style="padding:6px;text-align:right;font-weight:700;">' + (against.trades || 0) + '</td>'
          +       '<td style="padding:6px;text-align:right;font-weight:700;">' + pctText(against.winRate) + '</td>'
          +       '<td style="padding:6px;text-align:right;font-weight:700;color:' + netColor + ';">' + fmtRs(against.netPnl) + '</td>'
          +     '</tr>'
          +     '<tr style="' + subRowStyle + '">'
          +       '<td style="padding:4px 6px 4px 24px;">↳ CE_SELL</td>'
          +       '<td style="padding:4px 6px;text-align:right;">' + (ce.trades || 0) + '</td>'
          +       '<td style="padding:4px 6px;text-align:right;">' + pctText(ce.winRate) + '</td>'
          +       '<td style="padding:4px 6px;text-align:right;">' + fmtRs(ce.netPnl) + '</td>'
          +     '</tr>'
          +     '<tr style="' + subRowStyle + '">'
          +       '<td style="padding:4px 6px 4px 24px;">↳ PE_SELL</td>'
          +       '<td style="padding:4px 6px;text-align:right;">' + (pe.trades || 0) + '</td>'
          +       '<td style="padding:4px 6px;text-align:right;">' + pctText(pe.winRate) + '</td>'
          +       '<td style="padding:4px 6px;text-align:right;">' + fmtRs(pe.netPnl) + '</td>'
          +     '</tr>'
          +     '<tr>'
          +       '<td style="padding:10px 6px;color:var(--text-secondary);">All other <span style="color:var(--text-muted);font-weight:400;font-size:0.72rem;margin-left:6px;">(with-bias · neutral · historical)</span></td>'
          +       '<td style="padding:10px 6px;text-align:right;">' + (other.trades || 0) + '</td>'
          +       '<td style="padding:10px 6px;text-align:right;">' + pctText(other.winRate) + '</td>'
          +       '<td style="padding:10px 6px;text-align:right;color:' + (Number(other.netPnl) >= 0 ? '#34d399' : '#f87171') + ';">' + fmtRs(other.netPnl) + '</td>'
          +     '</tr>'
          +   '</tbody>'
          + '</table>'
          + '<div style="margin-top:16px;padding:12px 14px;background:rgba(125,211,252,0.06);'
          +   'border:1px solid rgba(125,211,252,0.20);border-radius:8px;'
          +   'font-family:var(--font-mono);font-size:0.78rem;color:var(--text-secondary);">'
          +   'If enabled: blocks <strong style="color:var(--text-primary);">' + (impact.wouldBlockTrades || 0) + '</strong> trades · '
          +   'net impact <strong style="color:' + savingsColor + ';">' + fmtRs(savings) + '</strong>'
          + '</div>'
          + '<div style="margin-top:12px;padding:10px 14px;font-size:0.82rem;font-weight:700;color:' + verdict.color + ';">'
          +   verdict.text
          + '</div>';

        document.getElementById('oiBiasEffBody').innerHTML = html;
        var periodEl = document.getElementById('oiBiasEffPeriod');
        if (periodEl) periodEl.textContent = '· ' + rangeLabel;
    }

    function open() {
        build();
        overlayEl.style.display = 'flex';
        document.getElementById('oiBiasEffBody').innerHTML = 'Loading…';
        var periodEl = document.getElementById('oiBiasEffPeriod');
        if (periodEl) periodEl.textContent = '';

        // Trades page keeps `currentRange` as a page-scoped var and exposes
        // `rangeBounds(key)` returning {from, to} ISO dates. Use them so this
        // modal scopes to whatever period pill is currently active.
        var key = (typeof currentRange !== 'undefined') ? currentRange : 'today';
        var bounds = (typeof rangeBounds === 'function') ? rangeBounds(key) : null;
        var labels = (typeof LABELS === 'object' && LABELS) ? LABELS : {};
        var rangeLabel = labels[key] || key;

        if (!bounds) {
            document.getElementById('oiBiasEffBody').innerHTML =
                '<div style="color:var(--accent-red, #f87171);">Could not resolve period bounds — reload the trades page and try again.</div>';
            return;
        }
        var url = '/api/analytics/summary?strategyId=atmvwap'
                + '&from=' + encodeURIComponent(bounds.from)
                + '&to='   + encodeURIComponent(bounds.to);
        fetch(url).then(function(r) { return r.json(); }).then(function(d) {
            renderBody(d, rangeLabel);
        }).catch(function(err) {
            document.getElementById('oiBiasEffBody').innerHTML =
                '<div style="color:var(--accent-red, #f87171);">Failed to load analytics: '
              + (err && err.message ? err.message : 'unknown error') + '</div>';
        });
    }

    return { open: open, close: close };
})();
