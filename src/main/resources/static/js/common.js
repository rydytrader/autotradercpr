var THEME_KEY = 'traderedge-theme';
var THEME_ORDER = ['dark', 'light', 'forest'];
var THEME_ICONS = { dark: '\uD83C\uDF19', light: '\u2600\uFE0F', forest: '\uD83C\uDF32' };

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

document.addEventListener('DOMContentLoaded', function() {
    var yr = document.getElementById('footerYear');
    if (yr) yr.textContent = new Date().getFullYear();
    applyTheme(localStorage.getItem(THEME_KEY) || 'dark');
});
