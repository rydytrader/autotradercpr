// ── Historical Chart modal ─────────────────────────────────────────────
// Opens from the calendar page when the operator clicks the chart-icon
// button in a day cell. Fetches /api/chart/historical?date=YYYY-MM-DD
// and renders the day's chosen CE + PE 3-min bars as two TradingView
// Lightweight Charts (candlestick + VWAP overlay, same look as the live
// /chart page).
//
// Overlay skeleton mirrors AppConfirm — fixed dark backdrop, centered card,
// Esc / backdrop / close-button dismissal.
window.HistoricalChartModal = (function() {
    var overlayEl = null;
    var charts = { ce: null, pe: null };
    var candleSeries = { ce: null, pe: null };
    var vwapSeries   = { ce: null, pe: null };
    var stUpSeries   = { ce: null, pe: null };
    var stDnSeries   = { ce: null, pe: null };
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
          +       '<div class="hist-panel" data-key="ce">'
          +         '<div class="hist-panel-hdr"><span style="color:#34d399;">■</span> CE (3-min) <span id="histCeSym" class="hist-hdr-note"></span></div>'
          +         '<div class="hist-panel-body"><div id="histChartCe" style="position:absolute;inset:0;"></div></div>'
          +       '</div>'
          +       '<div class="hist-panel" data-key="pe" style="margin-top:14px;">'
          +         '<div class="hist-panel-hdr"><span style="color:#f87171;">■</span> PE (3-min) <span id="histPeSym" class="hist-hdr-note"></span></div>'
          +         '<div class="hist-panel-body"><div id="histChartPe" style="position:absolute;inset:0;"></div></div>'
          +       '</div>'
          +     '</div>'
          +   '</div>'
          + '</div>';
        document.body.appendChild(overlayEl);

        var style = document.createElement('style');
        style.textContent =
            '.hist-panel-hdr { font-family:var(--font-mono);font-size:0.72rem;font-weight:700;'
          + '  letter-spacing:0.06em;color:var(--text-secondary);padding:6px 4px;text-transform:uppercase; }'
          + '.hist-panel-hdr .hist-hdr-note { color:var(--text-muted);font-weight:400;margin-left:8px;text-transform:none; }'
          + '.hist-panel-body { position:relative;height:340px;border:1px solid var(--border);border-radius:6px;overflow:hidden; }';
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
        // Tear down charts so the next open starts from a clean slate.
        ['ce', 'pe'].forEach(function(k) {
            if (charts[k]) { try { charts[k].remove(); } catch (e) {} }
            charts[k] = null; candleSeries[k] = null; vwapSeries[k] = null;
            stUpSeries[k] = null; stDnSeries[k] = null;
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

    function renderPanel(panelKey, containerId, candles, stArr, priceDecimals) {
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
        var stUp = chart.addLineSeries({
            color: col.up, lineWidth: 2,
            priceLineVisible: false, lastValueVisible: false, crosshairMarkerVisible: false,
            priceFormat: { type: 'price', precision: priceDecimals, minMove: minMove }
        });
        var stDn = chart.addLineSeries({
            color: col.down, lineWidth: 2,
            priceLineVisible: false, lastValueVisible: false, crosshairMarkerVisible: false,
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
        // Split ST into up-runs (green) and down-runs (red), patching each
        // flip point into BOTH so the two colored segments visually meet.
        var stUpData = [], stDnData = [], prevUp = null;
        (stArr || []).forEach(function(p) {
            var t = Math.floor(Number(p.t) / 1000) + IST_OFFSET_S;
            var v = Number(p.line);
            if (p.isUp) {
                stUpData.push({ time: t, value: v });
                if (prevUp === false) stDnData.push({ time: t, value: v });
            } else {
                stDnData.push({ time: t, value: v });
                if (prevUp === true) stUpData.push({ time: t, value: v });
            }
            prevUp = !!p.isUp;
        });
        cs.setData(bars);
        vs.setData(vwaps);
        stUp.setData(stUpData);
        stDn.setData(stDnData);
        try { chart.timeScale().fitContent(); } catch (e) {}

        charts[panelKey] = chart;
        candleSeries[panelKey] = cs;
        vwapSeries[panelKey] = vs;
        stUpSeries[panelKey] = stUp;
        stDnSeries[panelKey] = stDn;
    }

    function open(dateStr) {
        build();
        overlayEl.style.display = 'flex';
        document.getElementById('histChartLoading').style.display = 'block';
        document.getElementById('histChartPanels').style.display = 'none';
        document.getElementById('histChartDate').textContent = dateStr;
        document.getElementById('histChartAtm').textContent = '';
        var ceSymEl = document.getElementById('histCeSym');
        var peSymEl = document.getElementById('histPeSym');
        if (ceSymEl) ceSymEl.textContent = '';
        if (peSymEl) peSymEl.textContent = '';

        ['ce', 'pe'].forEach(function(k) {
            if (charts[k]) { try { charts[k].remove(); } catch (e) {} charts[k] = null; }
        });

        fetch('/api/chart/historical?date=' + encodeURIComponent(dateStr))
            .then(function(r) { return r.status === 404 ? null : r.json(); })
            .then(function(d) {
                var loading = document.getElementById('histChartLoading');
                var panels  = document.getElementById('histChartPanels');
                if (!d) {
                    loading.textContent = 'No chart data stored for ' + dateStr
                        + ' — the bot wasn’t running at 15:45 that day, or it was a market holiday.';
                    return;
                }
                loading.style.display = 'none';
                panels.style.display  = 'block';

                var ceSym = d.ceSymbol || '';
                var peSym = d.peSymbol || '';
                var ceBars = d.ceCandles || [];
                var peBars = d.peCandles || [];
                // Header — spot open + ATM + both symbols.
                var hdrBits = [];
                if (Number(d.spotOpen) > 0) hdrBits.push('spot open ' + Number(d.spotOpen).toFixed(2));
                if (d.atmStrike > 0)        hdrBits.push('ATM ' + d.atmStrike);
                document.getElementById('histChartAtm').textContent = hdrBits.join(' · ');
                if (ceSymEl) ceSymEl.textContent = ceSym ? '· ' + ceSym : '';
                if (peSymEl) peSymEl.textContent = peSym ? '· ' + peSym : '';

                // Render both panels after a tick so LWC sees the container.
                setTimeout(function() {
                    renderPanel('ce', 'histChartCe', ceBars, d.ceStSeries || [], 1);
                    renderPanel('pe', 'histChartPe', peBars, d.peStSeries || [], 1);
                }, 30);
            })
            .catch(function(err) {
                document.getElementById('histChartLoading').textContent =
                    'Failed to load: ' + (err && err.message ? err.message : 'unknown error');
            });
    }

    return { open: open, close: close };
})();
