var THEME_KEY = 'traderedge-theme';
var THEME_ORDER = ['dark', 'light', 'forest'];
var THEME_ICONS = { dark: '\uD83C\uDF19', light: '\u2600\uFE0F', forest: '\uD83C\uDF32' };

// \u2500\u2500 Fyers connection banner \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
// Polls /api/fyers/status every 10 s; when the broker token isn't available,
// shows a fixed top banner with a "Connect Fyers" button that kicks off the
// OAuth flow (/fyers/login \u2192 Fyers authcode redirect \u2192 /fyers/callback).
function ensureFyersBanner() {
    if (document.getElementById('fyersConnectBanner')) return;
    var bar = document.createElement('div');
    bar.id = 'fyersConnectBanner';
    bar.style.cssText = 'display:none;position:fixed;top:0;left:0;right:0;z-index:1000;'
        + 'background:rgba(251,191,36,0.18);border-bottom:1px solid rgba(251,191,36,0.55);'
        + 'color:var(--accent-amber, #fbbf24);font-family:var(--font-mono);font-size:0.78rem;'
        + 'padding:9px 18px;display:flex;align-items:center;gap:14px;justify-content:center;';
    bar.innerHTML =
        '<span>\u26A0 Fyers broker is not connected \u2014 strategy actions are paused.</span>' +
        '<a href="/fyers/login" style="background:rgba(251,191,36,0.25);border:1px solid rgba(251,191,36,0.55);'
        + 'color:var(--accent-amber, #fbbf24);text-decoration:none;padding:5px 14px;border-radius:6px;'
        + 'font-weight:700;letter-spacing:0.06em;">CONNECT FYERS</a>';
    document.body.appendChild(bar);
}
function refreshFyersBanner() {
    ensureFyersBanner();
    fetch('/api/fyers/status').then(function(r) { return r.json(); }).then(function(d) {
        var bar = document.getElementById('fyersConnectBanner');
        if (!bar) return;
        var ok = !!(d && d.connected);
        bar.style.display = ok ? 'none' : 'flex';
        document.body.style.paddingTop = ok ? '' : (bar.offsetHeight + 'px');
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
