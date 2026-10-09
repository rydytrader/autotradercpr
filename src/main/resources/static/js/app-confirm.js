// In-app confirm modal — replaces the native browser confirm() everywhere.
// Usage: showConfirm({ title, body, confirmLabel, onConfirm })
// Loaded on every page (after ticker.js); strategy pages, calendar, home and the
// settings modal all share this one overlay.
(function () {
    function showConfirm(opts) {
        opts = opts || {};
        var ov = document.getElementById('appConfirmOverlay');
        if (!ov) {
            ov = document.createElement('div');
            ov.id = 'appConfirmOverlay';
            ov.style.cssText = 'display:none;position:fixed;inset:0;background:rgba(0,0,0,0.6);z-index:99999;align-items:center;justify-content:center;';
            ov.innerHTML =
                '<div style="background:var(--bg-card);border:1px solid var(--border);border-radius:10px;max-width:480px;width:92vw;font-family:var(--font-mono);color:var(--text-primary);box-shadow:0 16px 48px rgba(0,0,0,0.4);">' +
                  '<div id="appConfirmTitle" style="padding:18px 22px;border-bottom:1px solid var(--border);font-size:0.92rem;font-weight:700;">—</div>' +
                  '<div id="appConfirmBody" style="padding:18px 22px;font-size:0.82rem;line-height:1.55;color:var(--text-secondary);white-space:pre-line;">—</div>' +
                  '<div style="padding:14px 22px;border-top:1px solid var(--border);display:flex;justify-content:flex-end;gap:10px;">' +
                    '<button id="appConfirmCancel" style="background:transparent;border:1px solid var(--border);color:var(--text-secondary);padding:8px 22px;border-radius:6px;font-family:var(--font-mono);font-size:0.76rem;font-weight:600;cursor:pointer;letter-spacing:0.04em;">No</button>' +
                    '<button id="appConfirmYes" style="background:rgba(248,113,113,0.12);border:1px solid rgba(248,113,113,0.45);color:var(--accent-red,#f87171);padding:8px 22px;border-radius:6px;font-family:var(--font-mono);font-size:0.76rem;font-weight:700;cursor:pointer;letter-spacing:0.04em;">Yes</button>' +
                  '</div>' +
                '</div>';
            document.body.appendChild(ov);
            ov.addEventListener('click', function (e) { if (e.target === ov) hide(); });
        }
        function hide() { ov.style.display = 'none'; document.removeEventListener('keydown', esc); }
        function cancel() { hide(); if (typeof opts.onCancel === 'function') opts.onCancel(); }
        function esc(e)  { if (e.key === 'Escape') cancel(); }
        ov.onclick = function (e) { if (e.target === ov) cancel(); };
        document.getElementById('appConfirmTitle').textContent = opts.title || 'Confirm';
        document.getElementById('appConfirmBody').textContent  = opts.body  || '';
        var yes = document.getElementById('appConfirmYes');
        yes.textContent = opts.confirmLabel || 'Yes';
        // Clear any prior listener by cloning the node
        var fresh = yes.cloneNode(true);
        yes.parentNode.replaceChild(fresh, yes);
        fresh.addEventListener('click', function () {
            hide();
            if (typeof opts.onConfirm === 'function') opts.onConfirm();
        });
        document.getElementById('appConfirmCancel').onclick = cancel;
        document.addEventListener('keydown', esc);
        ov.style.display = 'flex';
    }
    window.showConfirm = showConfirm;
})();
