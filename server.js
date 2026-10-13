'use strict';

const express = require('express');
const http    = require('http');
const { WebSocketServer, OPEN } = require('ws');
const { v4: uuidv4 } = require('uuid');
const path    = require('path');
const fs      = require('fs');

// ── Config ────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
const PING_INTERVAL = 30_000;

// ── In-memory device + pin store (no DB needed) ───────────────
// token → { deviceName, createdAt, pins: Map }
const deviceStore = new Map();

// ── Live WS connections ────────────────────────────────────────
const hardwareClients  = new Map(); // token → WebSocket
const dashboardClients = new Map(); // token → Set<WebSocket>

// ── Helpers ───────────────────────────────────────────────────
function sendJSON(ws, obj) {
  if (ws.readyState === OPEN) ws.send(JSON.stringify(obj));
}

function broadcastToDashboards(token, obj) {
  const set = dashboardClients.get(token);
  if (set) set.forEach(ws => sendJSON(ws, obj));
}

// ── Express ───────────────────────────────────────────────────
const app    = express();
const server = http.createServer(app);

app.use(express.json());

// Serve static files — works whether files are in /public or root
const publicDir = fs.existsSync(path.join(__dirname, 'public'))
  ? path.join(__dirname, 'public')
  : __dirname;

app.use(express.static(publicDir));

// ── REST API ──────────────────────────────────────────────────

// GET /api/health — uptime check
app.get('/api/health', (_req, res) => {
  res.json({ ok: true, ts: new Date().toISOString(), devices: hardwareClients.size });
});

// POST /api/devices — create new device with a token
app.post('/api/devices', (req, res) => {
  const { deviceName } = req.body;
  if (!deviceName?.trim()) return res.status(400).json({ error: 'deviceName required' });
  const token = uuidv4();
  deviceStore.set(token, { deviceName: deviceName.trim(), createdAt: new Date().toISOString(), pins: new Map() });
  console.log(`[API] Device created: "${deviceName.trim()}" token=${token}`);
  res.json({ token, deviceName: deviceName.trim(), createdAt: new Date().toISOString() });
});

// GET /api/devices — list all devices
app.get('/api/devices', (_req, res) => {
  const list = Array.from(deviceStore.entries()).map(([token, d]) => ({
    token,
    deviceName : d.deviceName,
    createdAt  : d.createdAt,
    online     : hardwareClients.has(token),
  }));
  res.json(list);
});

// DELETE /api/devices/:token — remove a device
app.delete('/api/devices/:token', (req, res) => {
  const { token } = req.params;
  if (!deviceStore.has(token)) return res.status(404).json({ error: 'Not found' });
  const hw = hardwareClients.get(token);
  if (hw) hw.terminate();
  const dbs = dashboardClients.get(token);
  if (dbs) dbs.forEach(ws => ws.terminate());
  deviceStore.delete(token);
  res.json({ ok: true });
});

// GET /api/devices/:token/pins — current pin state
app.get('/api/devices/:token/pins', (req, res) => {
  const d = deviceStore.get(req.params.token);
  if (!d) return res.status(404).json({ error: 'Not found' });
  res.json({
    deviceName : d.deviceName,
    online     : hardwareClients.has(req.params.token),
    pins       : Object.fromEntries(d.pins),
  });
});

// Fallback — serve index.html for all non-API routes
app.get('*', (req, res) => {
  if (req.path.startsWith('/api')) return res.status(404).json({ error: 'Not found' });
  const indexPath = path.join(publicDir, 'index.html');
  if (fs.existsSync(indexPath)) res.sendFile(indexPath);
  else res.status(404).send('index.html not found');
});

// ── WebSocket Server ──────────────────────────────────────────
const wss = new WebSocketServer({ server });

wss.on('connection', (ws) => {
  ws._token      = null;
  ws._clientType = null;
  ws._alive      = true;

  ws.on('pong', () => { ws._alive = true; });

  ws.on('message', async (raw) => {
    let msg;
    try   { msg = JSON.parse(raw.toString()); }
    catch { return sendJSON(ws, { type: 'error', message: 'Invalid JSON' }); }

    // ── AUTH ────────────────────────────────────────────────
    if (msg.type === 'auth') {
      const { token, clientType } = msg;
      const device = deviceStore.get(token);

      if (!device) {
        sendJSON(ws, { type: 'auth_error', message: 'Invalid auth token' });
        return ws.terminate();
      }

      ws._token      = token;
      ws._clientType = clientType;

      if (clientType === 'hardware') {
        const prev = hardwareClients.get(token);
        if (prev && prev.readyState === OPEN) prev.terminate();
        hardwareClients.set(token, ws);
        console.log(`[WS] HW auth OK: "${device.deviceName}"`);
        sendJSON(ws, { type: 'auth_ok', deviceName: device.deviceName });
        // Replay last pin states to hardware
        device.pins.forEach((value, pin) => sendJSON(ws, { type: 'vpin_update', pin, value }));
        broadcastToDashboards(token, { type: 'device_status', online: true, deviceName: device.deviceName });

      } else {
        if (!dashboardClients.has(token)) dashboardClients.set(token, new Set());
        dashboardClients.get(token).add(ws);
        const online = hardwareClients.has(token);
        console.log(`[WS] Dashboard auth OK: "${device.deviceName}" hw=${online}`);
        sendJSON(ws, { type: 'auth_ok', deviceName: device.deviceName, online });
        // Push current pin state snapshot to dashboard
        device.pins.forEach((value, pin) => sendJSON(ws, { type: 'vpin_update', pin, value }));
      }
      return;
    }

    // Require auth for all further messages
    if (!ws._token) return sendJSON(ws, { type: 'error', message: 'Not authenticated' });

    const token  = ws._token;
    const device = deviceStore.get(token);

    // ── VPIN WRITE ──────────────────────────────────────────
    if (msg.type === 'vpin_write') {
      const { pin, value } = msg;
      if (!pin || value === undefined) return;

      // Save state
      if (device) device.pins.set(pin, value);

      if (ws._clientType === 'hardware') {
        // Hardware → push to all dashboards
        broadcastToDashboards(token, { type: 'vpin_update', pin, value });
        console.log(`[VP] HW ${token.slice(0,8)} → ${pin}=${value}`);
      } else {
        // Dashboard → push to hardware
        const hw = hardwareClients.get(token);
        if (hw && hw.readyState === OPEN) {
          sendJSON(hw, { type: 'vpin_update', pin, value });
          console.log(`[VP] Dashboard → HW: ${pin}=${value}`);
        } else {
          sendJSON(ws, { type: 'device_status', online: false });
        }
      }
      return;
    }

    // ── PING ────────────────────────────────────────────────
    if (msg.type === 'ping') { sendJSON(ws, { type: 'pong' }); return; }
  });

  ws.on('close', () => {
    if (!ws._token) return;
    const token  = ws._token;
    const device = deviceStore.get(token);

    if (ws._clientType === 'hardware') {
      if (hardwareClients.get(token) === ws) {
        hardwareClients.delete(token);
        console.log(`[WS] HW disconnected: "${device?.deviceName}"`);
        broadcastToDashboards(token, { type: 'device_status', online: false, deviceName: device?.deviceName });
      }
    } else {
      const set = dashboardClients.get(token);
      if (set) { set.delete(ws); if (!set.size) dashboardClients.delete(token); }
    }
  });

  ws.on('error', (err) => console.error('[WS] Error:', err.message));
});

// ── 30s ping keep-alive ───────────────────────────────────────
const pingInterval = setInterval(() => {
  wss.clients.forEach(ws => {
    if (!ws._alive) return ws.terminate();
    ws._alive = false;
    ws.ping();
  });
}, PING_INTERVAL);

wss.on('close', () => clearInterval(pingInterval));

// ── Start ─────────────────────────────────────────────────────
server.listen(PORT, () => {
  console.log(`✅ IoT Platform running on port ${PORT}`);
  console.log(`📁 Serving static from: ${publicDir}`);
});
