'use strict';
(() => {
    const backend = document.body.dataset.backend;
    const csrf = document.body.dataset.csrf;
    const status = document.getElementById('status');
    const start = document.getElementById('start');
    const cancel = document.getElementById('cancel');
    const input = document.getElementById('input');
    const output = document.getElementById('output');
    const managed = document.body.dataset.setup === 'true';
    let vpnReady = !managed;
    let socket = null, attempt = null;
    const active = new Set(['waiting', 'authenticating']);
    const labels = {waiting: 'Waiting for terminal', authenticating: 'Authenticating',
        connected: 'Connected', failed: 'Authentication failed — check the host profile or use terminal login',
        cancelled: 'Cancelled', timed_out: 'Authentication timed out'};
    async function request(path, mutation = false) {
        const response = await fetch(path, {method: mutation ? 'POST' : 'GET', cache: 'no-store',
            headers: mutation ? {'X-CSRF-Token': csrf} : {}});
        if (!response.ok) throw new Error('Request rejected; refresh the page or check the backend configuration.');
        return response.json();
    }
    function finish(value) {
        if (attempt) attempt.state = value;
        status.textContent = labels[value] || value;
        input.disabled = !active.has(value);
        cancel.disabled = !active.has(value);
        start.disabled = !vpnReady || active.has(value) || value === 'connected';
        if (!active.has(value)) {output.textContent = ''; input.value = '';}
    }
    function send(data) {
        if (socket && socket.readyState === WebSocket.OPEN && !input.disabled)
            socket.send(JSON.stringify({type: 'input', data}));
    }
    if (managed) {
        const panel = document.getElementById('setup');
        const label = document.getElementById('setup-status');
        const connect = document.getElementById('setup-connect');
        const desktop = document.getElementById('setup-desktop');
        const phases = {stopped: 'Connection services stopped', starting: 'Starting VPN',
            vpn_login_required: 'VPN login needed', vpn_connecting: 'VPN connecting',
            ssh_login_required: 'VPN connected — SSH login needed', connected: 'Connected',
            scheduler_unconfigured: 'Connected — Slurm account not configured'};
        panel.hidden = false; start.disabled = true;
        let pending = false;
        async function refresh(mutate = false) {
            if (pending) return;
            pending = true;
            try {
                const value = await request(`/login/backend/${encodeURIComponent(backend)}/setup`, mutate);
                label.textContent = phases[value.phase];
                desktop.href = value.desktop_url;
                desktop.hidden = !['vpn_login_required', 'vpn_connecting'].includes(value.phase);
                vpnReady = ['ssh_login_required', 'connected', 'scheduler_unconfigured'].includes(value.phase);
                connect.disabled = vpnReady || value.phase === 'vpn_connecting';
                if (!attempt || !active.has(attempt.state)) {
                    start.disabled = !vpnReady || value.phase !== 'ssh_login_required';
                }
            } catch (_) {
                label.textContent = 'Connection status unavailable — retry or refresh';
                vpnReady = false; start.disabled = true; connect.disabled = false;
            } finally {pending = false;}
        }
        connect.addEventListener('click', () => refresh(true));
        refresh();
        const timer = setInterval(() => {if (!document.hidden) refresh();}, 5000);
        window.addEventListener('pagehide', () => clearInterval(timer));
    }
    start.addEventListener('click', async () => {
        start.disabled = true;
        output.textContent = '';
        try {
            attempt = await request(`/login/backend/${encodeURIComponent(backend)}/start`, true);
            finish(attempt.state);
            if (!active.has(attempt.state)) return;
            if (attempt.state === 'authenticating') {
                input.disabled = true;
                status.textContent = 'Authentication is active in another window. Keep that window open or cancel.';
                return;
            }
            socket = new WebSocket(`${location.protocol === 'https:' ? 'wss:' : 'ws:'}//${location.host}/login/attempt/${attempt.id}/terminal`);
            socket.onopen = () => {
                socket.send(JSON.stringify({csrf}));
                finish('authenticating'); input.focus();
            };
            socket.onmessage = event => {
                const message = JSON.parse(event.data);
                if (message.type === 'output') {
                    // Render text, never HTML or terminal escape commands. Keep
                    // only a bounded live view; there is no replay or storage.
                    const text = message.data.replace(/\x1b\[[0-?]*[ -/]*[@-~]/g, '')
                        .replace(/[\x00-\x08\x0b-\x1f\x7f]/g, '');
                    output.textContent = (output.textContent + text).slice(-8192);
                    output.scrollTop = output.scrollHeight;
                }
            };
            socket.onclose = async () => {
                input.disabled = true; input.value = ''; output.textContent = '';
                try {finish((await request(`/login/attempt/${attempt.id}`)).state);}
                catch (_) {finish('failed');}
            };
            socket.onerror = () => {status.textContent = 'Connection interrupted';};
        } catch (error) {status.textContent = error.message; start.disabled = false;}
    });
    cancel.addEventListener('click', async () => {
        if (!attempt) return;
        cancel.disabled = true;
        try {finish((await request(`/login/attempt/${attempt.id}/cancel`, true)).state);}
        catch (_) {status.textContent = 'Cancellation interrupted; closing the terminal.';}
        if (socket) socket.close();
    });
    input.addEventListener('keydown', event => {
        const special = {Enter: '\n', Backspace: '\x7f', Tab: '\t', Escape: '\x1b'};
        if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === 'v') return;
        if (event.ctrlKey && event.key.length === 1) {
            event.preventDefault(); send(String.fromCharCode(event.key.toUpperCase().charCodeAt(0) & 31));
        } else if (special[event.key]) {
            event.preventDefault(); send(special[event.key]);
        } else if (event.key.length === 1 && !event.metaKey && !event.altKey) {
            event.preventDefault(); send(event.key);
        }
        input.value = '';
    });
    input.addEventListener('input', () => {send(input.value); input.value = '';});
    input.addEventListener('paste', event => {
        event.preventDefault(); send(event.clipboardData.getData('text')); input.value = '';
    });
    window.addEventListener('pagehide', () => {
        input.value = ''; output.textContent = ''; if (socket) socket.close();
    });
})();
