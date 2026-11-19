'use strict';
/*
 * Grapple Gatling server.
 * Serves the game page and relays every player's state to everyone else over WebSockets.
 * No dependencies: the WebSocket protocol (RFC 6455) is implemented below, so `npm install` has nothing to fetch.
 */
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const PORT = parseInt(process.env.PORT, 10) || 3000;
const MAX_PLAYERS = 24;
const MAX_MSG = 4096;
const TICK_MS = 50;            // broadcast 20 times a second
const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
const INDEX = path.join(__dirname, 'index.html');

const clients = new Map();     // id -> { id, socket, presence, buf, alive, msgs }
let dirty = false;
let nextId = 1;

/* ---------- http ---------- */
const server = http.createServer((req, res) => {
  const url = (req.url || '/').split('?')[0];
  if (url === '/healthz') { res.writeHead(200, { 'Content-Type': 'text/plain' }); res.end('ok'); return; }
  if (url === '/' || url === '/index.html') {
    fs.readFile(INDEX, (err, data) => {
      if (err) { res.writeHead(500, { 'Content-Type': 'text/plain' }); res.end('Game page missing'); return; }
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' });
      res.end(data);
    });
    return;
  }
  res.writeHead(404, { 'Content-Type': 'text/plain' }); res.end('Not found');
});

/* ---------- websocket framing ---------- */
function frame(opcode, payload) {
  const len = payload.length;
  let head;
  if (len < 126) head = Buffer.from([0x80 | opcode, len]);
  else if (len < 65536) { head = Buffer.alloc(4); head[0] = 0x80 | opcode; head[1] = 126; head.writeUInt16BE(len, 2); }
  else { head = Buffer.alloc(10); head[0] = 0x80 | opcode; head[1] = 127; head.writeBigUInt64BE(BigInt(len), 2); }
  return Buffer.concat([head, payload]);
}
function sendText(c, str) {
  if (c.socket.destroyed || !c.socket.writable) return;
  c.socket.write(frame(0x1, Buffer.from(str)));
}
function drop(c) {
  if (!clients.delete(c.id)) return;
  dirty = true;
  try { c.socket.destroy(); } catch (e) { /* already gone */ }
}

/* Pull complete frames out of the client's buffer. Returns false if the client broke the protocol. */
function readFrames(c) {
  for (;;) {
    const b = c.buf;
    if (b.length < 2) return true;
    const opcode = b[0] & 0x0f;
    const masked = (b[1] & 0x80) !== 0;
    let len = b[1] & 0x7f, off = 2;
    if (len === 126) { if (b.length < 4) return true; len = b.readUInt16BE(2); off = 4; }
    else if (len === 127) { if (b.length < 10) return true; const big = b.readBigUInt64BE(2); if (big > BigInt(MAX_MSG)) return false; len = Number(big); off = 10; }
    if (!masked || len > MAX_MSG) return false;
    if (b.length < off + 4 + len) return true;
    const mask = b.subarray(off, off + 4);
    const data = Buffer.from(b.subarray(off + 4, off + 4 + len));
    for (let i = 0; i < data.length; i++) data[i] ^= mask[i & 3];
    c.buf = b.subarray(off + 4 + len);
    if (opcode === 0x8) { try { c.socket.write(frame(0x8, Buffer.alloc(0))); } catch (e) { /* closing */ } return false; }
    if (opcode === 0x9) { try { c.socket.write(frame(0xA, data)); } catch (e) { /* closing */ } continue; }
    if (opcode === 0xA) { c.alive = true; continue; }
    if (opcode === 0x1) onMessage(c, data.toString('utf8'));
  }
}

function onMessage(c, text) {
  c.alive = true;
  if (++c.msgs > 60) return;           // soft cap: 60 messages per second per player
  let m;
  try { m = JSON.parse(text); } catch (e) { return; }
  if (!m || m.t !== 'p' || !m.d || typeof m.d !== 'object' || Array.isArray(m.d)) return;
  for (const k of Object.keys(m.d)) {
    if (!/^[a-z]{1,3}$/.test(k)) continue;          // the game only uses short lowercase keys
    if (m.d[k] === null) delete c.presence[k]; else c.presence[k] = m.d[k];
  }
  dirty = true;
}

/* ---------- upgrade ---------- */
server.on('upgrade', (req, socket) => {
  const url = (req.url || '').split('?')[0];
  const key = req.headers['sec-websocket-key'];
  if (url !== '/ws' || !key || String(req.headers.upgrade || '').toLowerCase() !== 'websocket') { socket.destroy(); return; }
  if (clients.size >= MAX_PLAYERS) { socket.write('HTTP/1.1 503 Service Unavailable\r\nConnection: close\r\n\r\n'); socket.destroy(); return; }
  const accept = crypto.createHash('sha1').update(key + GUID).digest('base64');
  socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + '\r\n\r\n');
  socket.setNoDelay(true);
  const c = { id: 'p' + (nextId++), socket, presence: {}, buf: Buffer.alloc(0), alive: true, msgs: 0 };
  clients.set(c.id, c);
  sendText(c, JSON.stringify({ t: 'hello', id: c.id }));
  dirty = true;
  socket.on('data', (chunk) => {
    c.buf = Buffer.concat([c.buf, chunk]);
    if (c.buf.length > MAX_MSG * 4 || !readFrames(c)) drop(c);
  });
  socket.on('close', () => drop(c));
  socket.on('error', () => drop(c));
});

/* ---------- loops ---------- */
setInterval(() => {
  for (const c of clients.values()) c.msgs = 0;
  if (!dirty) return;
  dirty = false;
  const peers = [];
  for (const c of clients.values()) peers.push({ id: c.id, p: c.presence });
  const out = JSON.stringify({ t: 's', peers });
  for (const c of clients.values()) sendText(c, out);
}, TICK_MS);

/* Ping every 25 s so Render's proxy keeps idle sockets open, and clear out players who stopped answering. */
setInterval(() => {
  for (const c of clients.values()) {
    if (!c.alive) { drop(c); continue; }
    c.alive = false;
    try { c.socket.write(frame(0x9, Buffer.alloc(0))); } catch (e) { drop(c); }
  }
}, 25000);

server.listen(PORT, () => console.log('Grapple Gatling listening on port ' + PORT));
