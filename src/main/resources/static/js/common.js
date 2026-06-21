var THEME_KEY = 'traderedge-theme';
var THEME_ORDER = ['dark', 'light', 'forest'];
var THEME_ICONS = { dark: '\uD83C\uDF19', light: '\u2600\uFE0F', forest: '\uD83C\uDF32' };

// \u2500\u2500 In-page confirm modal \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
// Replaces browser window.confirm() with a themed modal matching the rest of
// the app (dark card, accent border, monospace title, primary/danger buttons).
// Promise-based: AppConfirm.ask({\u2026}).then(ok => { if (ok) doIt(); }). Reusable
// for any destructive action \u2014 close position, square off all, etc.
//
// Options:
//   title   \u2014 short label (default "Confirm")
//   message \u2014 body text; supports multiple lines
//   confirmLabel \u2014 button text on the confirm side (default "Confirm")
//   cancelLabel  \u2014 button text on the cancel  side (default "Cancel")
//   danger  \u2014 when true, paints the confirm button red (default false)
window.AppConfirm = (function() {
    var overlayEl = null;
    var resolver  = null;
    function build() {
        if (overlayEl) return;
        overlayEl = document.createElement('div');
        overlayEl.id = 'appConfirmOverlay';
        overlayEl.style.cssText =
            'display:none;position:fixed;inset:0;background:rgba(0,0,0,0.55);' +
            'z-index:9999;align-items:center;justify-content:center;';
        overlayEl.innerHTML =
            '<div id="appConfirmCard" style="background:var(--bg-card);border:1px solid var(--border);' +
              'border-radius:12px;width:420px;max-width:92vw;box-shadow:0 16px 48px rgba(0,0,0,0.45);' +
              'overflow:hidden;">' +
              '<div id="appConfirmTitle" style="padding:14px 22px;border-bottom:1px solid var(--border);' +
                'font-family:var(--font-mono);font-size:0.86rem;font-weight:700;letter-spacing:0.04em;' +
                'color:var(--text-primary);"></div>' +
              '<div id="appConfirmBody" style="padding:18px 22px;color:var(--text-secondary);' +
                'font-size:0.84rem;line-height:1.55;white-space:pre-wrap;"></div>' +
              '<div style="display:flex;justify-content:flex-end;gap:10px;padding:14px 22px;' +
                'border-top:1px solid var(--border);background:rgba(0,0,0,0.12);">' +
                '<button id="appConfirmCancel" type="button" style="background:transparent;' +
                  'border:1px solid var(--border);color:var(--text-secondary);padding:7px 16px;' +
                  'border-radius:6px;font-family:var(--font-mono);font-size:0.74rem;font-weight:600;' +
                  'letter-spacing:0.04em;cursor:pointer;"></button>' +
                '<button id="appConfirmOk" type="button" style="border:1px solid var(--accent-cyan);' +
                  'color:var(--accent-cyan);background:rgba(125,211,252,0.10);padding:7px 18px;' +
                  'border-radius:6px;font-family:var(--font-mono);font-size:0.74rem;font-weight:700;' +
                  'letter-spacing:0.04em;cursor:pointer;"></button>' +
              '</div>' +
            '</div>';
        document.body.appendChild(overlayEl);
        // Backdrop click + Esc cancel; Enter confirms.
        overlayEl.addEventListener('click', function(e) {
            if (e.target === overlayEl) finish(false);
        });
        document.addEventListener('keydown', function(e) {
            if (overlayEl.style.display !== 'flex' || !resolver) return;
            if (e.key === 'Escape') { e.preventDefault(); finish(false); }
            else if (e.key === 'Enter') { e.preventDefault(); finish(true); }
        });
        document.getElementById('appConfirmCancel').addEventListener('click', function() { finish(false); });
        document.getElementById('appConfirmOk').addEventListener('click',     function() { finish(true);  });
    }
    function finish(answer) {
        if (overlayEl) overlayEl.style.display = 'none';
        var r = resolver; resolver = null;
        if (r) r(answer);
    }
    function ask(opts) {
        build();
        opts = opts || {};
        var titleText   = opts.title   || 'Confirm';
        var messageText = opts.message || '';
        var confirmText = opts.confirmLabel || 'Confirm';
        var cancelText  = opts.cancelLabel  || 'Cancel';
        document.getElementById('appConfirmTitle').textContent = titleText;
        document.getElementById('appConfirmBody').textContent  = messageText;
        var okBtn = document.getElementById('appConfirmOk');
        var cnBtn = document.getElementById('appConfirmCancel');
        okBtn.textContent = confirmText;
        cnBtn.textContent = cancelText;
        // Danger styling \u2014 destructive actions (close position, etc.) get the red palette.
        if (opts.danger) {
            okBtn.style.borderColor = 'rgba(248,113,113,0.55)';
            okBtn.style.color       = 'var(--accent-red, #f87171)';
            okBtn.style.background  = 'rgba(248,113,113,0.10)';
        } else {
            okBtn.style.borderColor = 'var(--accent-cyan)';
            okBtn.style.color       = 'var(--accent-cyan)';
            okBtn.style.background  = 'rgba(125,211,252,0.10)';
        }
        overlayEl.style.display = 'flex';
        // Focus the OK so Enter confirms by default; Esc still cancels.
        setTimeout(function() { try { okBtn.focus(); } catch (e) {} }, 0);
        return new Promise(function(resolve) { resolver = resolve; });
    }
    return { ask: ask };
})();

// \u2500\u2500 Fyers "Not Connected" gate \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
// Polls /api/fyers/status every 10 s; when the broker token isn't available,
// shows the same centered "Fyers Broker Not Connected" card the parent branch
// used \u2014 lightning bolt, headline, cyan "Login to Fyers \u2192" button. Inserted
// into the main content area so it sits inside the page layout (not over it).
function isFyersGateSuppressed() {
    var p = (window.location && window.location.pathname) || '';
    // The Fyers gate is only required on pages that consume live broker data (the Trade page).
    // Pre-auth screens (app login + Fyers OAuth handoff) and information-only screens (Home
    // analytics, Calendar) work fine without a broker token, so the gate is suppressed there.
    if (p === '/login' || p.indexOf('/fyers/login') === 0) return true;
    if (p === '/home' || p.indexOf('/home') === 0) return true;
    if (p === '/calendar' || p.indexOf('/calendar') === 0) return true;
    if (p === '/trades') return true;
    if (p === '/' || p === '') return true;
    return false;
}
function ensureFyersGateStyle() {
    if (document.getElementById('fyersGateStyle')) return;
    var style = document.createElement('style');
    style.id = 'fyersGateStyle';
    // When the gate is shown, hide every other direct child of .app-main so the user only sees
    // the connect prompt. Navigation (top navbar + side rail + sidebar) stays visible so the
    // operator can switch pages, log out, toggle theme, etc.
    style.textContent =
        'html.fyers-disconnected .app-main > *:not(#fyersGate) { display:none !important; }';
    document.head.appendChild(style);
}
function ensureFyersBanner() {
    if (document.getElementById('fyersGate')) return;
    var gate = document.createElement('div');
    gate.id = 'fyersGate';
    gate.style.cssText = 'display:none;margin:20px 24px;';
    gate.innerHTML =
        '<div class="section-card" style="margin-bottom:0;">' +
            '<div style="padding:60px 28px;text-align:center;">' +
                '<div style="font-size:2.5rem;margin-bottom:16px;opacity:0.3;">\u26A1</div>' +
                '<div style="font-family:var(--font-mono);font-size:1.1rem;color:var(--text-primary);font-weight:600;margin-bottom:8px;">Fyers Broker Not Connected</div>' +
                '<div style="font-family:var(--font-mono);font-size:0.82rem;color:var(--text-muted);margin-bottom:24px;">Connect your Fyers account to start trading and view live data.</div>' +
                '<a href="/fyers/login" style="' +
                    'display:inline-flex;align-items:center;gap:10px;' +
                    'background:rgba(99,179,237,0.08);border:1px solid rgba(99,179,237,0.25);' +
                    'color:var(--accent-blue);padding:14px 32px;border-radius:8px;' +
                    'font-family:var(--font-mono);font-size:0.9rem;font-weight:700;' +
                    'letter-spacing:0.06em;text-decoration:none;">' +
                    'Login to Fyers <span style="font-size:1rem;">\u2192</span>' +
                '</a>' +
            '</div>' +
        '</div>';
    // Prefer the main content column if the page has one; fall back to body.
    var host = document.querySelector('.app-main') || document.body;
    if (host.firstChild) host.insertBefore(gate, host.firstChild);
    else host.appendChild(gate);
}
function refreshFyersBanner() {
    if (isFyersGateSuppressed()) {
        // Make sure no stale gate / class lingers on pre-auth screens.
        document.documentElement.classList.remove('fyers-disconnected');
        var existing = document.getElementById('fyersGate');
        if (existing) existing.style.display = 'none';
        return;
    }
    ensureFyersGateStyle();
    ensureFyersBanner();
    fetch('/api/fyers/status').then(function(r) {
        if (!r.ok || (r.headers.get('content-type') || '').indexOf('json') < 0) return null;
        return r.json();
    }).then(function(d) {
        var gate = document.getElementById('fyersGate');
        if (!gate) return;
        var ok = !!(d && d.connected);
        gate.style.display = ok ? 'none' : 'block';
        document.documentElement.classList.toggle('fyers-disconnected', !ok);
    }).catch(function() {});
}
if (typeof window !== 'undefined') {
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', refreshFyersBanner);
    } else {
        refreshFyersBanner();
    }
    setInterval(refreshFyersBanner, 10000);
}

function applyTheme(theme) {
    if (THEME_ORDER.indexOf(theme) === -1) theme = 'dark';
    document.documentElement.setAttribute('data-theme', theme);
    var btn = document.getElementById('themeToggle');
    if (btn) btn.textContent = THEME_ICONS[theme];
    localStorage.setItem(THEME_KEY, theme);
}

function toggleTheme() {
    var current = document.documentElement.getAttribute('data-theme') || 'dark';
    var idx = THEME_ORDER.indexOf(current);
    var next = THEME_ORDER[(idx + 1) % THEME_ORDER.length];
    applyTheme(next);
}

// App logout (Spring Security — CSRF exempt)
function logout() {
    var form = document.createElement('form');
    form.method = 'POST';
    form.action = '/app-logout';
    document.body.appendChild(form);
    form.submit();
}

// CSRF header helper for fetch() calls
function csrfHeaders() {
    var headers = {'Content-Type': 'application/json'};
    var csrfCookie = document.cookie.split('; ').find(c => c.startsWith('XSRF-TOKEN='));
    if (csrfCookie) {
        headers['X-XSRF-TOKEN'] = decodeURIComponent(csrfCookie.split('=')[1]);
    }
    return headers;
}

// User role (loaded on every page)
var userRole = '';
var userType = '';

function loadUserRole() {
    fetch('/api/user/me').then(function(r) { return r.json(); }).then(function(d) {
        userRole = d.role || '';
        userType = d.type || '';
        console.log('[loadUserRole] role=' + userRole + ' type=' + userType);
        if (userRole === 'ROLE_ADMIN') {
            document.querySelectorAll('.admin-only').forEach(function(el) { el.classList.add('admin-visible'); });
        } else {
            document.querySelectorAll('a[href="/settings"]').forEach(function(el) { el.style.display = 'none'; });
        }
    }).catch(function(e) { console.error('[loadUserRole] failed', e); });
}

// Sidebar collapse
var SIDEBAR_KEY = 'traderedge-sidebar-collapsed';

function toggleSidebar() {
    var sidebar = document.querySelector('.sidebar');
    var main = document.querySelector('.app-main');
    if (!sidebar) return;
    var collapsed = sidebar.classList.toggle('collapsed');
    if (main) main.classList.toggle('sidebar-collapsed', collapsed);
    localStorage.setItem(SIDEBAR_KEY, collapsed ? '1' : '0');
    document.documentElement.classList.toggle('sidebar-is-collapsed', collapsed);
}

function restoreSidebar() {
    if (localStorage.getItem(SIDEBAR_KEY) === '1') {
        var sidebar = document.querySelector('.sidebar');
        var main = document.querySelector('.app-main');
        if (sidebar) sidebar.classList.add('collapsed');
        if (main) main.classList.add('sidebar-collapsed');
    }
}

// Apply sidebar state immediately to prevent flash
(function() {
    if (localStorage.getItem(SIDEBAR_KEY) === '1') {
        document.documentElement.classList.add('sidebar-is-collapsed');
    }
})();

document.addEventListener('DOMContentLoaded', function() {
    var yr = document.getElementById('footerYear');
    if (yr) yr.textContent = new Date().getFullYear();
    applyTheme(localStorage.getItem(THEME_KEY) || 'dark');
    restoreSidebar();
    loadUserRole();
});
