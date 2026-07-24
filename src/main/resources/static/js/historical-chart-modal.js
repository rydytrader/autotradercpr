// ── Historical Chart modal ─────────────────────────────────────────────
// Opens from the calendar page when the operator clicks the chart-icon
// button in a day cell. Fetches /api/chart/historical?date=YYYY-MM-DD,
// renders NIFTY spot + ATM CE + ATM PE as three stacked TradingView
// Lightweight Charts panels (candlestick + VWAP overlay, same look as the
// live /chart page).
//
// Overlay skeleton mirrors OiBiasEffectivenessModal + AppConfirm — fixed
// dark backdrop, centered card, Esc / backdrop / close-button dismissal.
window.HistoricalChartModal = (function() {
    var overlayEl = null;
    var charts = {};              // panelKey -> chart
    var candleSeries = {};
    var vwapSeries = {};
    var IST_OFFSET_S = 5.5 * 3600; // shift epoch so LightweightCharts renders IST wall-clock

    function build() {
        if (overlayEl) return;
        overlayEl = document.createElement('div');
        overlayEl.id = 'histChartOverlay';
        overlayEl.style.cssText =
            'display:none;position:fixed;inset:0;background:rgba(0,0,0,0.62);'
          + 'z-index:9999;align-items:center;justify-content:center;';
        overlayEl.innerHTML =
            '<div id="histChartCard" style="background:var(--bg-card);border:1px solid var(--border);'
          +   'border-radius:12px;width:1100px;max-width:96vw;max-height:92vh;'
          +   'box-shadow:0 16px 48px rgba(0,0,0,0.5);display:flex;flex-direction:column;'
          +   'overflow:hidden;">'
          +   '<div style="display:flex;align-items:center;justify-content:space-between;'
          +     'padding:14px 22px;border-bottom:1px solid var(--border);'
          +     'font-family:var(--font-mono);font-weight:700;letter-spacing:0.04em;'
          +     'color:var(--text-primary);">'
          +     '<span>↗ HISTORICAL CHART · <span id="histChartDate" style="color:var(--text-muted);font-weight:500;margin-left:6px;">—</span>'
          +     ' · <span id="histChartAtm" style="color:var(--text-secondary);font-weight:500;">—</span></span>'
          +     '<button id="histChartClose" type="button" style="background:transparent;border:none;'
          +       'color:var(--text-muted);font-size:1.35rem;cursor:pointer;line-height:1;padding:0 4px;">×</button>'
          +   '</div>'
          +   '<div id="histChartBody" style="flex:1;overflow:auto;padding:14px 18px;">'
          +     '<div id="histChartLoading" style="color:var(--text-muted);text-align:center;padding:60px 20px;'
          +       'font-family:var(--font-mono);font-size:0.82rem;">Loading…</div>'
          +     '<div id="histChartPanels" style="display:none;">'
          +       '<div class="hist-panel" data-key="nifty" style="margin-bottom:14px;">'
          +         '<div class="hist-panel-hdr">NIFTY 50 <span class="hist-hdr-note">(spot)</span></div>'
          +         '<div class="hist-panel-body"><div id="histChartNifty" style="position:absolute;inset:0;"></div></div>'
          +       '</div>'
          +       '<div class="hist-panel" data-key="ce" style="margin-bottom:14px;">'
          +         '<div class="hist-panel-hdr"><span style="color:#34d399;">■</span> ATM CE <span id="histCeSym" class="hist-hdr-note"></span></div>'
          +         '<div class="hist-panel-body"><div id="histChartCe" style="position:absolute;inset:0;"></div></div>'
          +       '</div>'
          +       '<div class="hist-panel" data-key="pe">'
          +         '<div class="hist-panel-hdr"><span style="color:#f87171;">■</span> ATM PE <span id="histPeSym" class="hist-hdr-note"></span></div>'
          +         '<div class="hist-panel-body"><div id="histChartPe" style="position:absolute;inset:0;"></div></div>'
          +       '</div>'
          +     '</div>'
          +   '</div>'
          + '</div>';
        document.body.appendChild(overlayEl);

        // Scoped styles for the panel headers + fixed-height chart boxes.
        var style = document.createElement('style');
        style.textContent =
            '.hist-panel-hdr { font-family:var(--font-mono);font-size:0.72rem;font-weight:700;'
          + '  letter-spacing:0.06em;color:var(--text-secondary);padding:6px 4px;text-transform:uppercase; }'
          + '.hist-panel-hdr .hist-hdr-note { color:var(--text-muted);font-weight:400;margin-left:8px;text-transform:none; }'
          + '.hist-panel-body { position:relative;height:280px;border:1px solid var(--border);border-radius:6px;overflow:hidden; }';
        document.head.appendChild(style);

        overlayEl.addEventListener('click', function(e) {
            if (e.target === overlayEl) close();
        });
        document.addEventListener('keydown', function(e) {
            if (overlayEl.style.display !== 'flex') return;
            if (e.key === 'Escape') { e.preventDefault(); close(); }
        });
        document.getElementById('histChartClose').addEventListener('click', close);
    }

    function close() {
        if (!overlayEl) return;
        overlayEl.style.display = 'none';
        // Tear down charts so the next open starts from a clean slate — LightweightCharts
        // instances don't survive a hidden container being resized.
        ['nifty','ce','pe'].forEach(function(k) {
            if (charts[k]) { try { charts[k].remove(); } catch (e) {} }
            charts[k] = null; candleSeries[k] = null; vwapSeries[k] = null;
        });
    }

    function themeColors() {
        var t = document.documentElement.getAttribute('data-theme') || 'dark';
        var isDark = t !== 'light';
        return {
            text:  isDark ? '#94a3b8' : '#475569',
            grid:  'rgba(128,128,128,0.08)',
            vwap:  '#fbbf24',
            up:    '#34d399',
            down:  '#f87171'
        };
    }

    function renderPanel(panelKey, containerId, candles, priceDecimals) {
        var container = document.getElementById(containerId);
        if (!container || typeof LightweightCharts === 'undefined') return;
        var col = themeColors();
        var minMove = priceDecimals >= 1 ? Math.pow(10, -priceDecimals) : 1;
        var chart = LightweightCharts.createChart(container, {
            autoSize: true,
            layout: {
                background: { type: 'solid', color: 'transparent' },
                textColor: col.text, fontFamily: 'Roboto Mono, monospace', fontSize: 10
            },
            grid: { vertLines: { color: col.grid }, horzLines: { color: col.grid } },
            rightPriceScale: { borderVisible: false, scaleMargins: { top: 0.08, bottom: 0.08 } },
            timeScale: { borderVisible: false, timeVisible: true, secondsVisible: false, rightOffset: 4 },
            crosshair: { mode: LightweightCharts.CrosshairMode.Normal },
            handleScroll: false, handleScale: false
        });
        var cs = chart.addCandlestickSeries({
            upColor: col.up, downColor: col.down,
            wickUpColor: col.up, wickDownColor: col.down,
            borderVisible: false,
            priceFormat: { type: 'price', precision: priceDecimals, minMove: minMove }
        });
        var vs = chart.addLineSeries({
            color: col.vwap, lineWidth: 2,
            priceLineVisible: false, lastValueVisible: true, crosshairMarkerVisible: false,
            priceFormat: { type: 'price', precision: priceDecimals, minMove: minMove }
        });

        var bars = [], vwaps = [];
        (candles || []).forEach(function(c) {
            var t = Math.floor(Number(c.startMillis) / 1000) + IST_OFFSET_S;
            bars.push({ time: t, open: Number(c.open), high: Number(c.high),
                        low: Number(c.low), close: Number(c.close) });
            var v = Number(c.vwap || 0);
            if (v > 0) vwaps.push({ time: t, value: v });
        });
        // NIFTY spot has no ATP → derive VWAP from typical price like the live chart does.
        if (vwaps.length === 0 && bars.length > 0) {
            var cumTyp = 0, cnt = 0;
            bars.forEach(function(b) {
                cumTyp += (b.high + b.low + b.close) / 3; cnt++;
                vwaps.push({ time: b.time, value: cumTyp / cnt });
            });
        }
        cs.setData(bars);
        vs.setData(vwaps);
        try { chart.timeScale().fitContent(); } catch (e) {}

        charts[panelKey] = chart;
        candleSeries[panelKey] = cs;
        vwapSeries[panelKey] = vs;
    }

    function open(dateStr) {
        build();
        overlayEl.style.display = 'flex';
        document.getElementById('histChartLoading').style.display = 'block';
        document.getElementById('histChartPanels').style.display = 'none';
        document.getElementById('histChartDate').textContent = dateStr;
        document.getElementById('histChartAtm').textContent = '—';
        document.getElementById('histCeSym').textContent = '';
        document.getElementById('histPeSym').textContent = '';

        // Tear down previous charts if the modal is being re-opened without a full close.
        ['nifty','ce','pe'].forEach(function(k) {
            if (charts[k]) { try { charts[k].remove(); } catch (e) {} charts[k] = null; }
        });

        var url = '/api/chart/historical?date=' + encodeURIComponent(dateStr);
        fetch(url).then(function(r) {
            if (r.status === 404) return null;
            return r.json();
        }).then(function(d) {
            var loading = document.getElementById('histChartLoading');
            var panels  = document.getElementById('histChartPanels');
            if (!d) {
                loading.textContent = 'No chart data stored for ' + dateStr
                    + ' — the bot wasn’t running at 15:32 that day, or it was a market holiday.';
                return;
            }
            loading.style.display = 'none';
            panels.style.display  = 'block';

            document.getElementById('histChartAtm').textContent = 'ATM ' + (d.atmStrike || '—');
            document.getElementById('histCeSym').textContent = d.ceSymbol ? '· ' + d.ceSymbol : '';
            document.getElementById('histPeSym').textContent = d.peSymbol ? '· ' + d.peSymbol : '';

            var byS = d.candlesBySymbol || {};
            // Render in DOM-visible order: NIFTY, CE, PE. Small setTimeout so LWC sees the
            // container after its display:block flush.
            setTimeout(function() {
                renderPanel('nifty', 'histChartNifty', byS['NSE:NIFTY50-INDEX'], 2);
                if (d.ceSymbol) renderPanel('ce', 'histChartCe', byS[d.ceSymbol], 2);
                if (d.peSymbol) renderPanel('pe', 'histChartPe', byS[d.peSymbol], 2);
            }, 30);
        }).catch(function(err) {
            document.getElementById('histChartLoading').textContent =
                'Failed to load: ' + (err && err.message ? err.message : 'unknown error');
        });
    }

    return { open: open, close: close };
})();
