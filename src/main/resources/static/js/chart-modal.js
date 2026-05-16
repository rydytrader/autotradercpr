// ══════════════════════════════════════════════════════════════════════
// Chart Modal — live candlestick chart with CPR/VWAP/EMA overlays.
// Shared between /scanner and /positions pages.
// Requires Lightweight Charts (loaded via CDN <script> on each page).
// Exposes:  openChartModal(symbol, shortName)  / closeChartModal()
// ══════════════════════════════════════════════════════════════════════

var chartState = { modal: null, chart: null, series: null, vwapSeries: null, ema20Series: null, levelSeries: [], levelPriceLines: [], symbol: null, interval: null, resizeHandler: null, lastData: null };

// Chart visibility preferences (persisted in localStorage)
var chartPrefs = (function() {
    var defaults = {
        cprBand: true, r1pdhBand: true, s1pdlBand: true,
        cprLines: true, rLines: true, sLines: true, pdhPdl: true, pivot: true,
        vwap: true, ema20: true, trades: true
    };
    try {
        var stored = JSON.parse(localStorage.getItem('chartPrefs') || '{}');
        return Object.assign({}, defaults, stored);
    } catch (e) { return defaults; }
})();
function saveChartPrefs() {
    try { localStorage.setItem('chartPrefs', JSON.stringify(chartPrefs)); } catch (e) {}
}

function toggleChartSettings() {
    var panel = document.getElementById('chartSettingsPanel');
    if (!panel) return;
    if (panel.style.display === 'none') {
        renderChartSettings();
        panel.style.display = 'block';
        setTimeout(function() {
            document.addEventListener('mousedown', closeSettingsOnOutsideClick, true);
        }, 0);
    } else {
        panel.style.display = 'none';
        document.removeEventListener('mousedown', closeSettingsOnOutsideClick, true);
    }
}

function closeSettingsOnOutsideClick(e) {
    var panel = document.getElementById('chartSettingsPanel');
    var btn = document.getElementById('chartSettingsBtn');
    if (!panel || panel.style.display === 'none') {
        document.removeEventListener('mousedown', closeSettingsOnOutsideClick, true);
        return;
    }
    if (panel.contains(e.target) || (btn && btn.contains(e.target))) return;
    panel.style.display = 'none';
    document.removeEventListener('mousedown', closeSettingsOnOutsideClick, true);
}

function renderChartSettings() {
    var panel = document.getElementById('chartSettingsPanel');
    var groups = [
        { label: 'Bands', items: [
            { key: 'cprBand', label: 'CPR band', color: '#6496ff' },
            { key: 'r1pdhBand', label: 'R1/PDH band', color: '#26a69a' },
            { key: 's1pdlBand', label: 'S1/PDL band', color: '#ef5350' }
        ]},
        { label: 'CPR & Pivot', items: [
            { key: 'cprLines', label: 'CPR Top/Bot', color: '#6496ff' },
            { key: 'pivot', label: 'Pivot', color: '#6496ff' }
        ]},
        { label: 'Resistance / Support', items: [
            { key: 'rLines', label: 'R1 / R2 / R3 / R4', color: '#26a69a' },
            { key: 'sLines', label: 'S1 / S2 / S3 / S4', color: '#ef5350' },
            { key: 'pdhPdl', label: 'PDH / PDL', color: '#ffa726' }
        ]},
        { label: 'Indicators', items: [
            { key: 'vwap', label: 'VWAP', color: '#ec407a' },
            { key: 'ema20', label: 'EMA 20', color: '#66bb6a' }
        ]},
        { label: 'Trades', items: [
            { key: 'trades', label: 'Trade markers', color: '#888' }
        ]}
    ];
    var html = '';
    groups.forEach(function(g) {
        html += '<div style="color:var(--text-secondary);font-size:0.7rem;text-transform:uppercase;letter-spacing:0.5px;margin:6px 0 4px;font-weight:700;">' + g.label + '</div>';
        g.items.forEach(function(it) {
            var checked = chartPrefs[it.key] ? 'checked' : '';
            html += '<label style="display:flex;align-items:center;gap:8px;padding:3px 0;cursor:pointer;color:var(--text-primary);">' +
                    '<input type="checkbox" ' + checked + ' data-key="' + it.key + '" onchange="onChartPrefChange(this)" style="accent-color:' + it.color + ';" />' +
                    '<span style="width:10px;height:2px;background:' + it.color + ';display:inline-block;"></span>' +
                    '<span>' + it.label + '</span>' +
                    '</label>';
        });
    });
    panel.innerHTML = html;
}

function onChartPrefChange(el) {
    chartPrefs[el.dataset.key] = el.checked;
    saveChartPrefs();
    if (chartState.lastData) {
        applyChartLines(chartState.lastData);
        applyTradeMarkers(chartState.lastData);
        if (chartState.vwapSeries) chartState.vwapSeries.applyOptions({ visible: chartPrefs.vwap });
        if (chartState.ema20Series) chartState.ema20Series.applyOptions({ visible: chartPrefs.ema20 });
    }
}

function openChartModal(symbol, shortName) {
    var modal = document.getElementById('chartModal');
    if (!modal) modal = buildChartModal();
    chartState.symbol = symbol;
    var titleEl = document.getElementById('chartModalTitle');
    if (titleEl) titleEl.textContent = shortName || symbol;
    modal.style.display = 'flex';
    loadChartData(true).then(function(isLive) {
        if (chartState.interval) clearInterval(chartState.interval);
        if (isLive) {
            chartState.interval = setInterval(function() { loadChartData(false); }, 1000);
        }
    });
}

function closeChartModal() {
    var modal = document.getElementById('chartModal');
    if (modal) modal.style.display = 'none';
    var panel = document.getElementById('chartSettingsPanel');
    if (panel) panel.style.display = 'none';
    document.removeEventListener('mousedown', closeSettingsOnOutsideClick, true);
    if (chartState.interval) { clearInterval(chartState.interval); chartState.interval = null; }
    if (chartState.resizeHandler) { window.removeEventListener('resize', chartState.resizeHandler); chartState.resizeHandler = null; }
    if (chartState.chart) {
        try { chartState.chart.remove(); } catch (e) {}
        chartState.chart = null;
        chartState.series = null;
        chartState.vwapSeries = null;
        chartState.ema20Series = null;
        chartState.levelSeries = [];
        chartState.levelPriceLines = [];
        chartState.tooltip = null;
    }
    chartState.symbol = null;
}

function buildChartModal() {
    var m = document.createElement('div');
    m.id = 'chartModal';
    m.style.cssText = 'display:none;position:fixed;top:0;left:0;width:100%;height:100%;background:rgba(0,0,0,0.7);z-index:9999;align-items:center;justify-content:center;';
    m.innerHTML =
        '<div style="background:var(--bg-card);border:1px solid var(--border);border-radius:8px;width:92%;max-width:1200px;height:85vh;display:flex;flex-direction:column;overflow:hidden;box-sizing:border-box;">' +
        '  <div style="display:flex;align-items:center;justify-content:space-between;padding:12px 16px;border-bottom:1px solid var(--border);flex-shrink:0;position:relative;">' +
        '    <div style="display:flex;align-items:center;gap:8px;">' +
        '      <div id="chartModalTitle" style="font-family:var(--font-mono);font-weight:700;font-size:0.95rem;color:var(--text-primary);"></div>' +
        '      <button id="chartSettingsBtn" onclick="toggleChartSettings()" title="Chart settings" style="background:transparent;border:1px solid var(--border);border-radius:4px;padding:3px 6px;color:var(--text-secondary);cursor:pointer;line-height:0;display:inline-flex;align-items:center;">' +
        '        <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 0 1 0 2.83 2 2 0 0 1-2.83 0l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-2 2 2 2 0 0 1-2-2v-.09a1.65 1.65 0 0 0-1-1.51 1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 0 1-2.83 0 2 2 0 0 1 0-2.83l.06-.06a1.65 1.65 0 0 0 .33-1.82 1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1-2-2 2 2 0 0 1 2-2h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 0 1 0-2.83 2 2 0 0 1 2.83 0l.06.06a1.65 1.65 0 0 0 1.82.33H9a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 2-2 2 2 0 0 1 2 2v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 0 1 2.83 0 2 2 0 0 1 0 2.83l-.06.06a1.65 1.65 0 0 0-.33 1.82V9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 2 2 2 2 0 0 1-2 2h-.09a1.65 1.65 0 0 0-1.51 1z"/></svg>' +
        '      </button>' +
        '    </div>' +
        '    <button onclick="closeChartModal()" style="background:transparent;border:1px solid var(--border);border-radius:4px;padding:4px 10px;color:var(--text-secondary);cursor:pointer;font-size:1rem;line-height:1;">&times;</button>' +
        '    <div id="chartSettingsPanel" style="display:none;position:absolute;top:46px;left:16px;background:var(--bg-card);border:1px solid var(--border);border-radius:6px;box-shadow:0 4px 16px rgba(0,0,0,0.3);padding:12px;z-index:10;min-width:240px;max-height:60vh;overflow-y:auto;font-family:var(--font-mono);font-size:0.78rem;"></div>' +
        '  </div>' +
        '  <div style="flex:1;min-height:0;padding:12px 16px;box-sizing:border-box;display:flex;">' +
        '    <div id="chartContainer" style="flex:1;min-height:0;min-width:0;position:relative;overflow:hidden;"></div>' +
        '  </div>' +
        '</div>';
    m.addEventListener('click', function(e) { if (e.target === m) closeChartModal(); });
    document.body.appendChild(m);
    return m;
}

function loadChartData(isFirst) {
    if (!chartState.symbol) return Promise.resolve(false);
    return fetch('/api/scanner/chart?symbol=' + encodeURIComponent(chartState.symbol))
        .then(function(r) { return r.json(); })
        .then(function(d) { renderChart(d, isFirst); return d.tradingDay === true; })
        .catch(function(e) { console.error('Chart load failed', e); return false; });
}

function chartFmt(v) { return (v == null || v === 0) ? '--' : Number(v).toFixed(2); }

function toLwcBars(ohlc) {
    var seen = {};
    return ohlc.map(function(c) {
        return { time: Math.floor(c.t / 1000), open: c.o, high: c.h, low: c.l, close: c.c };
    }).filter(function(b) {
        if (seen[b.time]) return false;
        seen[b.time] = true;
        return true;
    }).sort(function(a, b) { return a.time - b.time; });
}

function buildVolumeMap(ohlc) {
    var m = {};
    (ohlc || []).forEach(function(c) {
        m[Math.floor(c.t / 1000)] = c.v || 0;
    });
    return m;
}

function fmtChartVol(v) {
    if (v == null || v <= 0) return '--';
    if (v >= 10000000) return (v / 10000000).toFixed(2) + ' Cr';
    if (v >= 100000) return (v / 100000).toFixed(2) + ' L';
    if (v >= 1000) return (v / 1000).toFixed(1) + ' K';
    return String(Math.round(v));
}

function toLwcLine(series) {
    if (!series || !series.length) return [];
    var seen = {};
    return series.map(function(p) {
        return { time: Math.floor(p.t / 1000), value: p.v };
    }).filter(function(b) {
        if (seen[b.time] || b.value == null || b.value === 0) return false;
        seen[b.time] = true;
        return true;
    }).sort(function(a, b) { return a.time - b.time; });
}

function renderChart(d, isFirst) {
    var candles = d.candles || [];
    chartState.lastData = d;
    var isNifty = d.symbol === 'NSE:NIFTY50-INDEX';

    var container = document.getElementById('chartContainer');

    if (candles.length === 0 && d.dataSource === 'error') {
        container.innerHTML = '<div style="display:flex;align-items:center;justify-content:center;height:100%;color:var(--text-secondary);font-family:var(--font-mono);font-size:0.9rem;text-align:center;padding:20px;">'
            + 'Failed to fetch historical candles. Please ensure you are logged in to Fyers.<br/>Error: ' + (d.error || 'unknown')
            + '</div>';
        if (chartState.chart) { chartState.chart.destroy(); chartState.chart = null; }
        return;
    }

    var bars = toLwcBars(candles);
    var volMap = buildVolumeMap(candles);
    chartState.volMap = volMap;
    var vwapLine = toLwcLine(d.vwapSeries);
    var ema20Line = toLwcLine(d.ema20Series);

    var theme = document.documentElement.getAttribute('data-theme');
    var dark = theme !== 'light';

    if (isFirst || !chartState.chart) {
        container.innerHTML = '';
        var bg = dark ? '#0f1418' : '#ffffff';
        var text = dark ? '#d0d4d8' : '#2a2e33';
        var grid = dark ? 'rgba(255,255,255,0.06)' : 'rgba(0,0,0,0.06)';
        var istTime = function(unixSec) {
            return new Date(unixSec * 1000).toLocaleTimeString('en-GB', { timeZone: 'Asia/Kolkata', hour: '2-digit', minute: '2-digit', hour12: false });
        };
        var istDateTime = function(unixSec) {
            var d = new Date(unixSec * 1000);
            var date = d.toLocaleDateString('en-GB', { timeZone: 'Asia/Kolkata', day: '2-digit', month: 'short' });
            var time = istTime(unixSec);
            return date + ' ' + time;
        };
        chartState.chart = LightweightCharts.createChart(container, {
            autoSize: true,
            layout: { background: { type: 'solid', color: bg }, textColor: text, fontFamily: 'Roboto Mono, monospace' },
            // Hide the chart's auto vertLines — we draw our own at 15-min boundaries via a
            // DOM overlay (see redraw15MinGridlines). LWC's auto tick spacing falls back to
            // ~1h at this chart width, which is too coarse for intraday CPR-level analysis.
            grid: { vertLines: { color: 'transparent' }, horzLines: { color: grid } },
            rightPriceScale: { borderColor: grid, scaleMargins: { top: 0.08, bottom: 0.08 } },
            timeScale: {
                borderColor: grid, timeVisible: true, secondsVisible: false,
                rightOffset: 0, minBarSpacing: 0.5,
                tickMarkFormatter: function(time) { return istTime(time); }
            },
            localization: {
                locale: 'en-IN',
                timeFormatter: function(time) { return istDateTime(time); }
            },
            crosshair: { mode: LightweightCharts.CrosshairMode.Normal, vertLine: { color: dark ? '#aab0b6' : '#888', labelBackgroundColor: dark ? '#1a1f25' : '#fff' }, horzLine: { color: dark ? '#aab0b6' : '#888', labelBackgroundColor: dark ? '#1a1f25' : '#fff' } },
            handleScroll: { mouseWheel: true, pressedMouseMove: true, horzTouchDrag: true, vertTouchDrag: true },
            handleScale: { axisPressedMouseMove: { time: true, price: true }, mouseWheel: true, pinch: true }
        });

        chartState.series = chartState.chart.addCandlestickSeries({
            upColor: '#26a69a', downColor: '#ef5350',
            borderUpColor: '#26a69a', borderDownColor: '#ef5350',
            wickUpColor: '#26a69a', wickDownColor: '#ef5350',
            priceFormat: { type: 'price', precision: 2, minMove: 0.01 }
        });
        chartState.series.setData(bars);

        if (!isNifty) {
            chartState.vwapSeries = chartState.chart.addLineSeries({
                color: '#ec407a', lineWidth: 2, lineStyle: LightweightCharts.LineStyle.Dashed,
                priceLineVisible: false, lastValueVisible: true, title: 'VWAP',
                crosshairMarkerVisible: false, visible: chartPrefs.vwap
            });
            chartState.vwapSeries.setData(vwapLine);
        }

        chartState.ema20Series = chartState.chart.addLineSeries({
            color: '#66bb6a', lineWidth: 2,
            priceLineVisible: false, lastValueVisible: true, title: 'EMA20',
            crosshairMarkerVisible: false, visible: chartPrefs.ema20
        });
        chartState.ema20Series.setData(ema20Line);

        chartState.levelSeries = [];
        chartState.levelPriceLines = [];
        applyChartLines(d);
        applyTradeMarkers(d);

        var sessDateMs = candles.length > 0 ? candles[0].t : Date.now();
        var sessDate = new Date(sessDateMs);
        var sessStart = Math.floor(Date.UTC(sessDate.getUTCFullYear(), sessDate.getUTCMonth(), sessDate.getUTCDate(), 3, 45, 0) / 1000);
        var sessEnd = sessStart + (6 * 3600 + 15 * 60);
        chartState.sessStart = sessStart;
        chartState.sessEnd   = sessEnd;
        var resizeChart = function() {
            if (!chartState.chart) return;
            var w = container.clientWidth || 0;
            var h = container.clientHeight || 0;
            if (w > 0 && h > 0) {
                try { chartState.chart.resize(w, h); } catch (e) {}
                var pStart = sessStart - (30 * 60);
                var pEnd   = sessEnd   + (30 * 60);
                try {
                    chartState.chart.timeScale().setVisibleRange({ from: pStart, to: pEnd });
                } catch (e) {
                    try { chartState.chart.timeScale().fitContent(); } catch (e2) {}
                }
                redraw15MinGridlines();
            }
        };
        requestAnimationFrame(function() { requestAnimationFrame(resizeChart); });

        // Re-draw 15-min vertical gridlines whenever the user pans/zooms the time axis.
        try {
            chartState.chart.timeScale().subscribeVisibleTimeRangeChange(function() {
                redraw15MinGridlines();
            });
        } catch (e) {}

        var tooltip = document.createElement('div');
        tooltip.id = 'chartTooltip';
        tooltip.style.cssText = 'position:absolute;display:none;pointer-events:none;z-index:1000;background:' + (dark ? 'rgba(26,31,37,0.95)' : 'rgba(255,255,255,0.98)') + ';border:1px solid ' + (dark ? '#2a2e33' : '#ccc') + ';border-radius:4px;padding:6px 10px;font-family:Roboto Mono, monospace;font-size:11px;line-height:1.5;color:' + text + ';box-shadow:0 2px 8px rgba(0,0,0,0.3);white-space:nowrap;';
        container.appendChild(tooltip);
        chartState.tooltip = tooltip;

        chartState.chart.subscribeCrosshairMove(function(param) {
            if (!param.time || !param.point || param.point.x < 0 || param.point.y < 0) {
                tooltip.style.display = 'none';
                return;
            }
            var priceData = param.seriesData.get(chartState.series);
            if (!priceData) { tooltip.style.display = 'none'; return; }
            var vwapData = param.seriesData.get(chartState.vwapSeries);
            var ema20Data = param.seriesData.get(chartState.ema20Series);
            var changePct = priceData.open > 0 ? ((priceData.close - priceData.open) / priceData.open * 100) : 0;
            var changeColor = priceData.close >= priceData.open ? '#26a69a' : '#ef5350';
            var vol = chartState.volMap ? chartState.volMap[param.time] : null;
            var html =
                '<div style="color:' + (dark ? '#aab0b6' : '#666') + ';margin-bottom:3px;">' + istDateTime(param.time) + '</div>' +
                '<div>O: <span style="color:' + changeColor + ';">' + priceData.open.toFixed(2) + '</span></div>' +
                '<div>H: <span style="color:' + changeColor + ';">' + priceData.high.toFixed(2) + '</span></div>' +
                '<div>L: <span style="color:' + changeColor + ';">' + priceData.low.toFixed(2) + '</span></div>' +
                '<div>C: <span style="color:' + changeColor + ';">' + priceData.close.toFixed(2) + '</span> <span style="color:' + changeColor + ';font-size:10px;">(' + (changePct >= 0 ? '+' : '') + changePct.toFixed(2) + '%)</span></div>' +
                '<div style="color:' + (dark ? '#aab0b6' : '#666') + ';">Vol: ' + fmtChartVol(vol) + '</div>';
            if (vwapData && vwapData.value) html += '<div style="color:#ec407a;">VWAP: ' + vwapData.value.toFixed(2) + '</div>';
            if (ema20Data && ema20Data.value) html += '<div style="color:#66bb6a;">EMA20: ' + ema20Data.value.toFixed(2) + '</div>';
            tooltip.innerHTML = html;
            var rect = container.getBoundingClientRect();
            var left = param.point.x + 15;
            var top = param.point.y + 15;
            if (left + 200 > rect.width) left = param.point.x - 200;
            if (top + 150 > rect.height) top = param.point.y - 150;
            tooltip.style.left = Math.max(5, left) + 'px';
            tooltip.style.top = Math.max(5, top) + 'px';
            tooltip.style.display = 'block';
        });
        return;
    }

    // Live updates
    var lastBar = bars[bars.length - 1];
    if (lastBar) chartState.series.update(lastBar);
    if (chartState.vwapSeries) chartState.vwapSeries.setData(vwapLine);
    if (chartState.ema20Series) chartState.ema20Series.setData(ema20Line);
    applyChartLines(d);
    applyTradeMarkers(d);
}

// Draw a thin vertical line at every 15-min boundary inside the trading session
// (9:15 → 15:30 IST). LWC's auto-grid was hidden in createChart so the only
// vertical lines on the canvas are the ones we put here. Re-runs on resize and
// on every visible-range change so lines track the bars when the user pans/zooms.
function redraw15MinGridlines() {
    var container = document.getElementById('chartContainer');
    if (!container || !chartState.chart) return;
    if (!chartState.sessStart || !chartState.sessEnd) return;

    // Clear previous overlay lines.
    var existing = container.querySelectorAll('.lwc-gridline-15m');
    for (var i = 0; i < existing.length; i++) existing[i].remove();

    var theme = document.documentElement.getAttribute('data-theme');
    var dark = theme !== 'light';
    var lineColor = dark ? 'rgba(255,255,255,0.06)' : 'rgba(0,0,0,0.06)';

    var ts = chartState.chart.timeScale();
    var w = container.clientWidth || 0;
    for (var t = chartState.sessStart; t <= chartState.sessEnd; t += 15 * 60) {
        var x;
        try { x = ts.timeToCoordinate(t); } catch (e) { continue; }
        if (x == null || x < 0 || x > w) continue;
        var line = document.createElement('div');
        line.className = 'lwc-gridline-15m';
        line.style.cssText = 'position:absolute;top:0;bottom:0;left:' + Math.round(x) + 'px;width:1px;background:' + lineColor + ';pointer-events:none;z-index:1;';
        container.appendChild(line);
    }
}

function applyTradeMarkers(d) {
    if (!chartState.series) return;
    if (!chartPrefs.trades) {
        try { chartState.series.setMarkers([]); } catch (e) {}
        return;
    }
    var trades = d.trades || [];
    var markers = [];
    trades.forEach(function(t) {
        var isLong = t.side === 'LONG';
        var win = t.netPnl >= 0;
        if (t.entryTime) {
            markers.push({
                time: Math.floor(t.entryTime / 1000),
                position: isLong ? 'belowBar' : 'aboveBar',
                color: isLong ? '#26a69a' : '#ef5350',
                shape: isLong ? 'arrowUp' : 'arrowDown',
                text: (window.prettySetup ? window.prettySetup(t.setup) : t.setup) + ' @ ' + t.entryPrice.toFixed(2)
            });
        }
        if (t.exitTime) {
            markers.push({
                time: Math.floor(t.exitTime / 1000),
                position: isLong ? 'aboveBar' : 'belowBar',
                color: win ? '#26a69a' : '#ef5350',
                shape: isLong ? 'arrowDown' : 'arrowUp',
                text: (t.exitReason || 'EXIT') + ' ' + (win ? '+' : '') + t.netPnl.toFixed(0)
            });
        }
    });
    markers.sort(function(a, b) { return a.time - b.time; });
    try { chartState.series.setMarkers(markers); } catch (e) { console.warn('setMarkers failed', e); }
}

function applyChartLines(d) {
    if (!chartState.chart) return;
    if (chartState.levelSeries && chartState.levelSeries.length) {
        chartState.levelSeries.forEach(function(s) {
            try { chartState.chart.removeSeries(s); } catch (e) {}
        });
    }
    chartState.levelSeries = [];
    if (chartState.levelPriceLines && chartState.levelPriceLines.length && chartState.series) {
        chartState.levelPriceLines.forEach(function(pl) {
            try { chartState.series.removePriceLine(pl); } catch (e) {}
        });
    }
    chartState.levelPriceLines = [];

    var candles = d.candles || [];
    var sessAnchorMs = candles.length > 0 ? candles[0].t : Date.now();
    var sessAnchor = new Date(sessAnchorMs);
    // 9:15 IST = 03:45 UTC; 15:30 IST = 10:00 UTC.
    var sessionStartSec = Math.floor(Date.UTC(sessAnchor.getUTCFullYear(), sessAnchor.getUTCMonth(), sessAnchor.getUTCDate(), 3, 45, 0) / 1000);
    var sessionEndSec   = sessionStartSec + (6 * 3600 + 15 * 60);
    // Padding keeps a 30-min margin on the visible time-axis so the bars don't hug the
    // canvas edges. Phantom series uses the padded range so fitContent reserves room for it.
    var paddingStart = sessionStartSec - (30 * 60);
    var paddingEnd   = sessionEndSec   + (30 * 60);
    // CPR / R / S level lines and bands are drawn strictly inside the trading session
    // (9:15 → 15:30 IST). Past 15:30 the levels simply stop — matches TradingView and avoids
    // implying the levels are still active during the pre-/post-market padding window.
    var startSec = sessionStartSec;
    var endSec   = sessionEndSec;

    var phantom = chartState.chart.addLineSeries({
        color: 'rgba(0,0,0,0)', lineWidth: 1,
        priceLineVisible: false, lastValueVisible: false, crosshairMarkerVisible: false
    });
    var phantomY = (d.cpr && d.cpr.pivot) ? d.cpr.pivot : (d.ltp || 0);
    phantom.setData([
        { time: paddingStart, value: phantomY },
        { time: paddingEnd,   value: phantomY }
    ]);
    chartState.levelSeries.push(phantom);

    var cpr = d.cpr || {};

    var addLine = function(value, color, label) {
        if (!value || value <= 0) return;
        var s = chartState.chart.addLineSeries({
            color: color,
            lineWidth: 1,
            priceLineVisible: false,
            lastValueVisible: true,
            title: label,
            crosshairMarkerVisible: false
        });
        var data = [];
        for (var t = startSec; t <= endSec; t += 300) {
            data.push({ time: t, value: value });
        }
        s.setData(data);
        chartState.levelSeries.push(s);
    };

    var addBand = function(top, bottom, fillColor1, fillColor2) {
        if (!top || !bottom || top <= 0 || bottom <= 0) return;
        var hi = Math.max(top, bottom);
        var lo = Math.min(top, bottom);
        var s = chartState.chart.addBaselineSeries({
            baseValue: { type: 'price', price: lo },
            topLineColor: 'transparent',
            topFillColor1: fillColor1,
            topFillColor2: fillColor2,
            bottomLineColor: 'transparent',
            bottomFillColor1: 'transparent',
            bottomFillColor2: 'transparent',
            priceLineVisible: false, lastValueVisible: false, crosshairMarkerVisible: false
        });
        s.setData([
            { time: startSec, value: hi },
            { time: endSec, value: hi }
        ]);
        chartState.levelSeries.push(s);
    };

    if (chartPrefs.cprBand) addBand(cpr.top, cpr.bottom, 'rgba(100,150,255,0.20)', 'rgba(100,150,255,0.10)');
    if (chartPrefs.r1pdhBand && cpr.r1 && cpr.pdh) addBand(cpr.r1, cpr.pdh, 'rgba(38,166,154,0.22)', 'rgba(38,166,154,0.10)');
    if (chartPrefs.s1pdlBand && cpr.s1 && cpr.pdl) addBand(cpr.s1, cpr.pdl, 'rgba(239,83,80,0.22)', 'rgba(239,83,80,0.10)');

    if (chartPrefs.cprLines) {
        addLine(cpr.top, '#6496ff', 'CPR Top');
        addLine(cpr.bottom, '#6496ff', 'CPR Bot');
    }
    if (chartPrefs.pivot) addLine(cpr.pivot, '#6496ff', 'Pivot');
    if (chartPrefs.rLines) {
        addLine(cpr.r1, '#26a69a', 'R1');
        addLine(cpr.r2, '#26a69a', 'R2');
        addLine(cpr.r3, '#26a69a', 'R3');
        addLine(cpr.r4, '#26a69a', 'R4');
    }
    if (chartPrefs.sLines) {
        addLine(cpr.s1, '#ef5350', 'S1');
        addLine(cpr.s2, '#ef5350', 'S2');
        addLine(cpr.s3, '#ef5350', 'S3');
        addLine(cpr.s4, '#ef5350', 'S4');
    }
    if (chartPrefs.pdhPdl) {
        addLine(cpr.pdh, '#ffa726', 'PDH');
        addLine(cpr.pdl, '#ffa726', 'PDL');
    }

    if (d.oi) {
        if (+d.oi.maxCallStrike > 0) addLine(+d.oi.maxCallStrike, '#e91e63', 'Max Call OI');
        if (+d.oi.maxPutStrike  > 0) addLine(+d.oi.maxPutStrike,  '#9c27b0', 'Max Put OI');
    }
}

// ESC key closes chart modal
document.addEventListener('keydown', function(e) {
    if (e.key === 'Escape' && chartState.symbol) closeChartModal();
});
