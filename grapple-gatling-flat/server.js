'use strict';
/*
 * Grapple Gatling server.
 * Serves the game page, pairs players for 1v1 matches, runs the round flow (first to 3 points, upgrade picks)
 * and relays each player's state to their opponent over WebSockets.
 * No dependencies: the WebSocket protocol (RFC 6455) is implemented below, so there is nothing to install.
 */
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const PORT = parseInt(process.env.PORT, 10) || 3000;
const MAX_CLIENTS = 300;
const MAX_MSG = 4096;
const TICK_MS = 50;              // match state goes out 20 times a second
const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
const INDEX = path.join(__dirname, 'index.html');

const WIN_SCORE = 3;
const COUNTDOWN_MS = 3000;       // before round 1
const NEXT_MS = 2500;            // before every later round
const PICK_MS = 12000;           // time the loser has to choose a card
const CARDS = ['shotgun', 'poison', 'shield'];

const clients = new Map();       // id -> client
const queue = [];                // clients waiting for an opponent, oldest first
const matches = new Map();       // id -> match
let nextClient = 1, nextMatch = 1;
let lastStats = '';

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
function send(c, obj) {
  if (!c || c.socket.destroyed || !c.socket.writable) return;
  c.socket.write(frame(0x1, Buffer.from(typeof obj === 'string' ? obj : JSON.stringify(obj))));
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

/* ---------- matchmaking and match flow ---------- */
const cleanName = (s) => String(s == null ? '' : s).replace(/[^\w \-]/g, '').trim().slice(0, 12) || 'Player';
const other = (m, c) => (m.p[0] === c ? m.p[1] : m.p[0]);
const toBoth = (m, obj) => { send(m.p[0], obj); send(m.p[1], obj); };

function leaveQueue(c) {
  const i = queue.indexOf(c);
  if (i >= 0) queue.splice(i, 1);
  if (c.state === 'queue') c.state = 'menu';
}

function tryMatch() {
  while (queue.length >= 2) {
    const a = queue.shift(), b = queue.shift();
    startMatch(a, b);
  }
}

function startMatch(a, b) {
  if (a.name.toLowerCase() === b.name.toLowerCase()) b.name = (b.name.slice(0, 10) + ' 2');
  const m = {
    id: 'm' + (nextMatch++), p: [a, b], round: 0, phase: 'countdown', loser: null, timer: null, ended: false, dirty: true,
    score: { [a.id]: 0, [b.id]: 0 },
    ups: { [a.id]: { shotgun: 0, poison: 0, shield: 0 }, [b.id]: { shotgun: 0, poison: 0, shield: 0 } }
  };
  matches.set(m.id, m);
  a.state = 'match'; b.state = 'match'; a.match = m; b.match = m; a.presence = {}; b.presence = {};
  send(a, { t: 'match', you: a.id, side: 0, color: 0, to: WIN_SCORE, opp: { id: b.id, name: b.name, color: 4 } });
  send(b, { t: 'match', you: b.id, side: 1, color: 4, to: WIN_SCORE, opp: { id: a.id, name: a.name, color: 0 } });
  beginRound(m, null, COUNTDOWN_MS);
}

function beginRound(m, picked, ms) {
  if (m.ended) return;
  m.round++;
  m.phase = 'countdown'; m.loser = null;
  toBoth(m, { t: 'round', n: m.round, cd: ms, scores: m.score, ups: m.ups, picked });
  clearTimeout(m.timer);
  m.timer = setTimeout(() => {
    if (m.ended) return;
    m.phase = 'fight';
    toBoth(m, { t: 'go' });
  }, ms);
}

function onDead(c) {
  const m = c.match;
  if (!m || m.ended || m.phase !== 'fight') return;
  const winner = other(m, c);
  m.score[winner.id]++;
  if (m.score[winner.id] >= WIN_SCORE) { endMatch(m, winner, 'score'); return; }
  m.phase = 'pick'; m.loser = c;
  toBoth(m, { t: 'point', winner: winner.id, loser: c.id, scores: m.score });
  send(c, { t: 'offer', cards: CARDS, ms: PICK_MS });
  send(winner, { t: 'wait' });
  clearTimeout(m.timer);
  m.timer = setTimeout(() => {
    if (m.ended || m.phase !== 'pick') return;
    pickCard(m, m.loser, CARDS[Math.floor(Math.random() * CARDS.length)]);
  }, PICK_MS + 1500);
}

function pickCard(m, c, card) {
  if (m.ended || m.phase !== 'pick' || m.loser !== c || CARDS.indexOf(card) < 0) return;
  clearTimeout(m.timer);
  m.ups[c.id][card]++;
  beginRound(m, { by: c.id, card }, NEXT_MS);
}

function endMatch(m, winner, reason) {
  if (m.ended) return;
  m.ended = true;
  clearTimeout(m.timer);
  matches.delete(m.id);
  for (const p of m.p) {
    send(p, { t: 'over', winner: winner.id, scores: m.score, reason });
    p.state = 'menu'; p.match = null; p.presence = {};
  }
}

function leaveMatch(c) {
  const m = c.match;
  if (m && !m.ended) endMatch(m, other(m, c), 'left');
}

function drop(c) {
  if (!clients.delete(c.id)) return;
  leaveQueue(c);
  leaveMatch(c);
  try { c.socket.destroy(); } catch (e) { /* already gone */ }
}

function onMessage(c, text) {
  c.alive = true;
  let m;
  try { m = JSON.parse(text); } catch (e) { return; }
  if (!m || typeof m !== 'object') return;

  if (m.t === 'p') {                                  // presence patch: only relayed while in a match
    if (++c.msgs > 120) return;                       // soft cap: 120 state updates a second per player
    if (!m.d || typeof m.d !== 'object' || Array.isArray(m.d)) return;
    for (const k of Object.keys(m.d)) {
      if (!/^[a-z]{1,3}$/.test(k)) continue;
      if (m.d[k] === null) delete c.presence[k]; else c.presence[k] = m.d[k];
    }
    if (c.match) c.match.dirty = true;
  } else if (m.t === 'q') {
    if (c.state !== 'menu') return;
    c.name = cleanName(m.n);
    c.state = 'queue';
    queue.push(c);
    tryMatch();
  } else if (m.t === 'uq') {
    leaveQueue(c);
  } else if (m.t === 'dead') {
    onDead(c);
  } else if (m.t === 'pick') {
    if (c.match) pickCard(c.match, c, m.c);
  } else if (m.t === 'leave') {
    leaveQueue(c);
    leaveMatch(c);
  }
}

/* ---------- upgrade ---------- */
server.on('upgrade', (req, socket) => {
  const url = (req.url || '').split('?')[0];
  const key = req.headers['sec-websocket-key'];
  if (url !== '/ws' || !key || String(req.headers.upgrade || '').toLowerCase() !== 'websocket') { socket.destroy(); return; }
  if (clients.size >= MAX_CLIENTS) { socket.write('HTTP/1.1 503 Service Unavailable\r\nConnection: close\r\n\r\n'); socket.destroy(); return; }
  const accept = crypto.createHash('sha1').update(key + GUID).digest('base64');
  socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + '\r\n\r\n');
  socket.setNoDelay(true);
  const c = { id: 'p' + (nextClient++), socket, presence: {}, buf: Buffer.alloc(0), alive: true, msgs: 0, name: 'Player', state: 'menu', match: null };
  clients.set(c.id, c);
  send(c, { t: 'hello', id: c.id });
  send(c, statsMessage());
  socket.on('data', (chunk) => {
    c.buf = Buffer.concat([c.buf, chunk]);
    if (c.buf.length > MAX_MSG * 4 || !readFrames(c)) drop(c);
  });
  socket.on('close', () => drop(c));
  socket.on('error', () => drop(c));
});

/* ---------- loops ---------- */
function statsMessage() {
  let playing = 0;
  for (const m of matches.values()) playing += m.p.length;
  return { t: 'stats', online: clients.size, q: queue.length, m: playing };
}

setInterval(() => {                                   // each player sees only themselves and their opponent
  for (const m of matches.values()) {
    if (!m.dirty) continue;
    m.dirty = false;
    const [a, b] = m.p;
    send(a, { t: 's', peers: [{ id: a.id, p: a.presence }, { id: b.id, p: b.presence }] });
    send(b, { t: 's', peers: [{ id: b.id, p: b.presence }, { id: a.id, p: a.presence }] });
  }
}, TICK_MS);

setInterval(() => {
  for (const c of clients.values()) c.msgs = 0;
  const s = JSON.stringify(statsMessage());
  if (s === lastStats) return;
  lastStats = s;
  for (const c of clients.values()) send(c, s);
}, 1000);

/* Ping every 25 s so Render's proxy keeps idle sockets open, and clear out players who stopped answering. */
setInterval(() => {
  for (const c of Array.from(clients.values())) {
    if (!c.alive) { drop(c); continue; }
    c.alive = false;
    try { c.socket.write(frame(0x9, Buffer.alloc(0))); } catch (e) { drop(c); }
  }
}, 25000);

server.listen(PORT, () => console.log('Grapple Gatling listening on port ' + PORT));
