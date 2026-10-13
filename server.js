/**
 * ============================================================
 *   Custom IoT Platform v2 — server.js
 *   Multi-device · Neon DB · WebSocket · Virtual Pins
 * ============================================================
 *
 *  ENV VARS required:
 *    DATABASE_URL  — Neon PostgreSQL connection string
 *    PORT          — set automatically by Render
 *
 *  Architecture:
 *    • Single HTTP+WS server on one port
 *    • Neon DB (pg) for persistent device & pin storage
 *    • In-memory maps for live WebSocket connections
 *    • 30-second native WS ping/pong keep-alive
 *    • Multi-device: unlimited ESP8266/ESP32 devices
 * ============================================================
 */

'use strict';

const express              = require('express');
const http                 = require('http');
const { WebSocketServer, OPEN } = require('ws');
const { Pool }             = require('pg');
const { v4: uuidv4 }      = require('uuid');
const path                 = require('path');

// ── Config ───────────────────────────────────────────────────
const PORT            = process.env.PORT || 3000;
const PING_INTERVAL   = 30_000;
const DATABASE_URL    = process.env.DATABASE_URL;

// ── Neon DB Pool ─────────────────────────────────────────────
let pool = null;

if (DATABASE_URL) {
  pool = new Pool({
    connectionString: DATABASE_URL,
    ssl: { rejectUnauthorized: false }, // required for Neon
    max: 10,
  });
  pool.on('error', (err) => console.error('[DB] Pool error:', err.message));
} else {
  console.warn('[DB] WARNING: DATABASE_URL not set — running in memory-only mode');
}

// ── DB helpers ────────────────────────────────────────────────
async function dbQuery(sql, params = []) {
  if (!pool) return null;
  const client = await pool.connect();
  try {
    return await client.query(sql, params);
  } finally {
    client.release();
  }
}

// ── Initialize DB schema ─────────────────────────────────────
async function initDB() {
  if (!pool) return;
  try {
    await dbQuery(`
      CREATE TABLE IF NOT EXISTS devices (
        token       TEXT PRIMARY KEY,
        device_name TEXT NOT NULL,
        created_at  TIMESTAMPTZ DEFAULT NOW(),
        last_seen   TIMESTAMPTZ
      )
    `);
    await dbQuery(`
      CREATE TABLE IF NOT EXISTS pin_states (
        device_token TEXT NOT NULL REFERENCES devices(token) ON DELETE CASCADE,
        pin          TEXT NOT NULL,
        value        DOUBLE PRECISION NOT NULL,
        updated_at   TIMESTAMPTZ DEFAULT NOW(),
        PRIMARY KEY  (device_token, pin)
      )
    `);
    await dbQuery(`
      CREATE TABLE IF NOT EXISTS pin_history (
        id           BIGSERIAL PRIMARY KEY,
        device_token TEXT NOT NULL REFERENCES devices(token) ON DELETE CASCADE,
        pin          TEXT NOT NULL,
        value        DOUBLE PRECISION NOT NULL,
        recorded_at  TIMESTAMPTZ DEFAULT NOW()
      )
    `);
    console.log('[DB] Schema ready ✓');
  } catch (err) {
    console.error('[DB] Schema init error:', err.message);
  }
}

// ── In-memory fallback (used when no DB) ─────────────────────
const memDevices = new Map(); // token → { device_name, created_at, pinState: Map }

// ── Live connection registries (always in-memory) ─────────────
const hardwareClients  = new Map(); // token → WebSocket
const dashboardClients = new Map(); // token → Set<WebSocket>

// ── Device CRUD (DB-backed with mem fallback) ─────────────────
async function createDevice(token, deviceName) {
  if (pool) {
    await dbQuery(
      `INSERT INTO devices (token, device_name) VALUES ($1, $2) ON CONFLICT (token) DO NOTHING`,
      [token, deviceName]
    );
  } else {
    memDevices.set(token, { device_name: deviceName, created_at: new Date().toISOString(), pinState: new Map() });
  }
}

async function getDevice(token) {
  if (pool) {
    const r = await dbQuery(`SELECT * FROM devices WHERE token = $1`, [token]);
    return r?.rows[0] || null;
  }
  const d = memDevices.get(token);
  return d ? { token, device_name: d.device_name, created_at: d.created_at } : null;
}

async function getAllDevices() {
  if (pool) {
    const r = await dbQuery(`SELECT * FROM devices ORDER BY created_at DESC`);
    return r?.rows || [];
  }
  return Array.from(memDevices.entries()).map(([token, d]) => ({
    token, device_name: d.device_name, created_at: d.created_at,
  }));
}

async function deleteDevice(token) {
  if (pool) {
    await dbQuery(`DELETE FROM devices WHERE token = $1`, [token]);
  } else {
    memDevices.delete(token);
  }
}

async function updateLastSeen(token) {
  if (pool) {
    await dbQuery(`UPDATE devices SET last_seen = NOW() WHERE token = $1`, [token]);
  }
}

async function savePinState(token, pin, value) {
  if (pool) {
    await dbQuery(`
      INSERT INTO pin_states (device_token, pin, value, updated_at)
      VALUES ($1, $2, $3, NOW())
      ON CONFLICT (device_token, pin) DO UPDATE SET value = $3, updated_at = NOW()
    `, [token, pin, value]);
    // Store history (keep last 1000 per device)
    await dbQuery(`
      INSERT INTO pin_history (device_token, pin, value) VALUES ($1, $2, $3)
    `, [token, pin, value]);
    await dbQuery(`
      DELETE FROM pin_history WHERE id IN (
        SELECT id FROM pin_history WHERE device_token = $1
        ORDER BY recorded_at DESC OFFSET 1000
      )
    `, [token]);
  } else {
    const d = memDevices.get(token);
    if (d) d.pinState.set(pin, value);
  }
}

async function getPinStates(token) {
  if (pool) {
    const r = await dbQuery(`SELECT pin, value FROM pin_states WHERE device_token = $1`, [token]);
    const m = new Map();
    (r?.rows || []).forEach(row => m.set(row.pin, row.value));
    return m;
  }
  return memDevices.get(token)?.pinState || new Map();
}

async function getPinHistory(token, pin, limit = 50) {
  if (!pool) return [];
  const r = await dbQuery(`
    SELECT value, recorded_at FROM pin_history
    WHERE device_token = $1 AND pin = $2
    ORDER BY recorded_at DESC LIMIT $3
  `, [token, pin, limit]);
  return r?.rows || [];
}

// ── Express App ───────────────────────────────────────────────
const app    = express();
const server = http.createServer(app);

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// ─────────────────────────────────────────────────────────────
//  REST API
// ─────────────────────────────────────────────────────────────

// POST /api/devices — create new device + token
app.post('/api/devices', async (req, res) => {
  const { deviceName } = req.body;
  if (!deviceName?.trim()) return res.status(400).json({ error: 'deviceName required' });
  const token = uuidv4();
  await createDevice(token, deviceName.trim());
  console.log(`[API] Device created: "${deviceName.trim()}" token=${token}`);
  res.json({ token, deviceName: deviceName.trim(), createdAt: new Date().toISOString() });
});

// GET /api/devices — list all devices with online status
app.get('/api/devices', async (_req, res) => {
  try {
    const devices = await getAllDevices();
    const result  = devices.map(d => ({
      token      : d.token,
      deviceName : d.device_name,
      createdAt  : d.created_at,
      lastSeen   : d.last_seen,
      online     : hardwareClients.has(d.token),
    }));
    res.json(result);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// DELETE /api/devices/:token — remove device
app.delete('/api/devices/:token', async (req, res) => {
  const { token } = req.params;
  const device = await getDevice(token);
  if (!device) return res.status(404).json({ error: 'Device not found' });

  // Disconnect any live hardware/dashboard
  const hw = hardwareClients.get(token);
  if (hw) hw.terminate();
  const dbs = dashboardClients.get(token);
  if (dbs) dbs.forEach(ws => ws.terminate());

  await deleteDevice(token);
  console.log(`[API] Device deleted: ${token}`);
  res.json({ ok: true });
});

// GET /api/devices/:token/pins — get current pin state
app.get('/api/devices/:token/pins', async (req, res) => {
  const { token } = req.params;
  const device = await getDevice(token);
  if (!device) return res.status(404).json({ error: 'Device not found' });
  const pins = await getPinStates(token);
  res.json({
    deviceName : device.device_name,
    online     : hardwareClients.has(token),
    pins       : Object.fromEntries(pins),
  });
});

// GET /api/devices/:token/history/:pin — pin history
app.get('/api/devices/:token/history/:pin', async (req, res) => {
  const { token, pin } = req.params;
  const limit = Math.min(parseInt(req.query.limit) || 50, 200);
  const history = await getPinHistory(token, pin, limit);
  res.json(history);
});

// GET /api/health — uptime check for UptimeRobot
app.get('/api/health', (_req, res) => {
  res.json({ ok: true, ts: new Date().toISOString(), devices: hardwareClients.size });
});

// Legacy compat
app.post('/api/token/new', async (req, res) => {
  req.url = '/api/devices'; return app._router.handle(req, res, () => {});
});
app.get('/api/tokens', async (req, res) => {
  req.url = '/api/devices'; return app._router.handle(req, res, () => {});
});

// ─────────────────────────────────────────────────────────────
//  WebSocket Server
// ─────────────────────────────────────────────────────────────

/**
 * Message protocol (JSON):
 *
 *  → auth:         { type:"auth",       token, clientType:"hardware"|"dashboard" }
 *  ← auth_ok:      { type:"auth_ok",    deviceName, online }
 *  ← auth_error:   { type:"auth_error", message }
 *  → vpin_write:   { type:"vpin_write", pin:"V2", value:23.5 }
 *  ← vpin_update:  { type:"vpin_update",pin:"V2", value:23.5 }
 *  ← device_status:{ type:"device_status", online, deviceName }
 *  ← devices_list: { type:"devices_list", devices:[...] }
 */

const wss = new WebSocketServer({ server });

wss.on('connection', (ws, req) => {
  const ip = req.socket.remoteAddress;
  console.log(`[WS] New connection from ${ip}`);

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

      const device = await getDevice(token);
      if (!device) {
        sendJSON(ws, { type: 'auth_error', message: 'Invalid auth token' });
        return ws.terminate();
      }

      if (!['hardware', 'dashboard'].includes(clientType)) {
        sendJSON(ws, { type: 'auth_error', message: 'clientType must be hardware or dashboard' });
        return ws.terminate();
      }

      ws._token      = token;
      ws._clientType = clientType;

      if (clientType === 'hardware') {
        const prev = hardwareClients.get(token);
        if (prev && prev.readyState === OPEN) { prev.terminate(); }
        hardwareClients.set(token, ws);
        await updateLastSeen(token);

        console.log(`[WS] Hardware auth OK: "${device.device_name}" (${token.slice(0,8)}…)`);
        sendJSON(ws, { type: 'auth_ok', deviceName: device.device_name });

        // Replay last pin states
        const pins = await getPinStates(token);
        pins.forEach((value, pin) => sendJSON(ws, { type: 'vpin_update', pin, value }));

        // Notify all dashboards of this device
        broadcastToDashboards(token, { type: 'device_status', online: true, deviceName: device.device_name });

        // Also push updated devices list to ALL connected dashboards
        broadcastDevicesList();

      } else {
        // Dashboard
        if (!dashboardClients.has(token)) dashboardClients.set(token, new Set());
        dashboardClients.get(token).add(ws);

        const online = hardwareClients.has(token);
        console.log(`[WS] Dashboard subscribed: token ${token.slice(0,8)}… (hw ${online ? 'ONLINE' : 'OFFLINE'})`);
        sendJSON(ws, { type: 'auth_ok', deviceName: device.device_name, online });

        // Push current pin state snapshot
        const pins = await getPinStates(token);
        pins.forEach((value, pin) => sendJSON(ws, { type: 'vpin_update', pin, value }));
      }
      return;
    }

    // All further messages require auth
    if (!ws._token) return sendJSON(ws, { type: 'error', message: 'Not authenticated' });

    const token  = ws._token;
    const device = await getDevice(token);

    // ── VPIN WRITE ──────────────────────────────────────────
    if (msg.type === 'vpin_write') {
      const { pin, value } = msg;
      if (!pin || value === undefined) return sendJSON(ws, { type: 'error', message: 'pin and value required' });

      await savePinState(token, pin, value);

      if (ws._clientType === 'hardware') {
        console.log(`[VP] HW ${token.slice(0,8)}… wrote ${pin}=${value}`);
        broadcastToDashboards(token, { type: 'vpin_update', pin, value });
        await updateLastSeen(token);
      } else {
        console.log(`[VP] Dashboard → HW: ${pin}=${value}`);
        const hw = hardwareClients.get(token);
        if (hw && hw.readyState === OPEN) {
          sendJSON(hw, { type: 'vpin_update', pin, value });
        } else {
          sendJSON(ws, { type: 'device_status', online: false });
        }
      }
      return;
    }

    // ── PING ────────────────────────────────────────────────
    if (msg.type === 'ping') {
      sendJSON(ws, { type: 'pong' }); return;
    }

    sendJSON(ws, { type: 'error', message: `Unknown type: ${msg.type}` });
  });

  ws.on('close', async () => {
    if (!ws._token) return;
    const token  = ws._token;
    const device = await getDevice(token);

    if (ws._clientType === 'hardware') {
      if (hardwareClients.get(token) === ws) {
        hardwareClients.delete(token);
        console.log(`[WS] Hardware disconnected: "${device?.device_name}" (${token.slice(0,8)}…)`);
        broadcastToDashboards(token, { type: 'device_status', online: false, deviceName: device?.device_name });
        broadcastDevicesList();
      }
    } else if (ws._clientType === 'dashboard') {
      const set = dashboardClients.get(token);
      if (set) { set.delete(ws); if (!set.size) dashboardClients.delete(token); }
    }
  });

  ws.on('error', (err) => {
    console.error(`[WS] Error (${ws._token?.slice(0,8) || 'unauth'}):`, err.message);
  });
});

// ── 30-second ping keep-alive ─────────────────────────────────
const pingInterval = setInterval(() => {
  wss.clients.forEach((ws) => {
    if (!ws._alive) return ws.terminate();
    ws._alive = false;
    ws.ping();
  });
}, PING_INTERVAL);

wss.on('close', () => clearInterval(pingInterval));

// ── Helpers ───────────────────────────────────────────────────
function sendJSON(ws, obj) {
  if (ws.readyState === OPEN) ws.send(JSON.stringify(obj));
}

function broadcastToDashboards(token, obj) {
  const set = dashboardClients.get(token);
  if (set) set.forEach(ws => sendJSON(ws, obj));
}

/**
 * Push the latest device list (with online status) to every
 * connected dashboard client so the sidebar stays in sync.
 */
async function broadcastDevicesList() {
  try {
    const devices = await getAllDevices();
    const list = devices.map(d => ({
      token      : d.token,
      deviceName : d.device_name,
      online     : hardwareClients.has(d.token),
      lastSeen   : d.last_seen,
    }));
    const msg = JSON.stringify({ type: 'devices_list', devices: list });
    wss.clients.forEach(ws => {
      if (ws._clientType === 'dashboard' && ws.readyState === OPEN) {
        ws.send(msg);
      }
    });
  } catch (err) {
    console.error('[WS] broadcastDevicesList error:', err.message);
  }
}

// ── Start ─────────────────────────────────────────────────────
async function start() {
  await initDB();
  server.listen(PORT, () => {
    console.log('╔══════════════════════════════════════════════╗');
    console.log(`║  Custom IoT Platform v2 — port ${PORT}          ║`);
    console.log(`║  DB: ${DATABASE_URL ? 'Neon PostgreSQL ✓' : 'In-memory (no DATABASE_URL)'}      ║`);
    console.log('╚══════════════════════════════════════════════╝');
  });
}

start().catch(console.error);
