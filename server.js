/* Neon Royale — servidor multijugador
   Sirve el juego (carpeta public/) y mantiene el mundo compartido por WebSocket en /ws.
   Arranque: npm install && npm start   (puerto: variable PORT o 3000) */
const http = require('http'), fs = require('fs'), path = require('path');
const { WebSocketServer } = require('ws');
const PORT = process.env.PORT || 3000, PUB = path.join(__dirname, 'public');
const TYPES = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript', '.css': 'text/css', '.png': 'image/png', '.json': 'application/json' };

const server = http.createServer((req, res) => {
  let f = decodeURIComponent(req.url.split('?')[0]); if (f === '/' || f === '') f = '/index.html';
  if (f === '/health') { res.writeHead(200); res.end('ok'); return; }
  const fp = path.join(PUB, path.normalize(f).replace(/^(\.\.[\/\\])+/, ''));
  if (!fp.startsWith(PUB)) { res.writeHead(403); res.end(); return; }
  fs.readFile(fp, (err, data) => {
    if (err) { res.writeHead(404); res.end('No encontrado'); return; }
    res.writeHead(200, { 'Content-Type': TYPES[path.extname(fp)] || 'application/octet-stream', 'Cache-Control': 'no-cache' }); res.end(data);
  });
});

const wss = new WebSocketServer({ server, path: '/ws', maxPayload: 16 * 1024 });
const players = new Map(); // id -> { ws, state, lk, dirty, lkDirty, n, t0 }
const clean = s => String(s || 'Jugador').replace(/[<>&"']/g, '').trim().slice(0, 16) || 'Jugador';
const num = (v, lim = 1e6) => { v = +v; return Number.isFinite(v) ? Math.max(-lim, Math.min(lim, v)) : 0; };
const SCENES = new Set(['city', 'avenida', 'casino', 'premier', 'vip', 'roof', 'burger', 'salera', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'h7', 'h8']);

wss.on('connection', ws => {
  let me = null;
  ws.isAlive = true; ws.on('pong', () => ws.isAlive = true);
  ws.on('message', raw => {
    let m; try { m = JSON.parse(raw); } catch (e) { return; }
    if (!m || typeof m !== 'object') return;
    if (m.t === 'hello') {
      const id = String(m.id || '').replace(/[^a-z0-9]/gi, '').slice(0, 16) || 'p' + Math.random().toString(36).slice(2, 10);
      const old = players.get(id); if (old && old.ws !== ws) try { old.ws.close(); } catch (e) { }
      me = { id, ws, state: { id, name: clean(m.name), scene: 'city', x: 0, y: 0, z: 0, ry: 0, mv: 0, m: 0, car: null }, lk: '', dirty: true, lkDirty: true, n: 0, t0: Date.now() };
      players.set(id, me); sendSnap(ws); broadcastCount(); jxTo(ws); return;
    }
    if (!me) return;
    // límite de mensajes por segundo
    const now = Date.now(); if (now - me.t0 > 1000) { me.t0 = now; me.n = 0; } if (++me.n > 40) return;
    if (m.t === 'state') {
      const s = me.state;
      if (SCENES.has(m.scene)) s.scene = m.scene;
      s.x = num(m.x); s.y = num(m.y, 100); s.z = num(m.z); s.ry = num(m.ry, 100); s.mv = num(m.mv, 20); s.m = Math.round(num(m.m, 1e15));
      s.g = typeof m.g === 'string' && /^[a-z]{0,12}$/.test(m.g) ? m.g : '';
      s.name = clean(m.name || s.name); s.car = typeof m.car === 'string' ? m.car.slice(0, 12) : null;
      s.cc = typeof m.cc === 'string' ? m.cc.slice(0, 9) : undefined; s.ct = m.ct && typeof m.ct === 'object' ? JSON.parse(JSON.stringify(m.ct).slice(0, 300) || '{}') : undefined;
      if (typeof m.lk === 'string' && m.lk.length < 1200 && m.lk !== me.lk) { me.lk = m.lk; me.lkDirty = true; }
      me.dirty = true;
    } else if (m.t === 'name') { me.state.name = clean(m.name); me.dirty = true; }
    else if (m.t === 'sync') { sendSnap(ws); jxTo(ws); }
    else if (m.t === 'tx') feed(me, m);
    else if (m.t === 'jxbet') {
      const c = Math.round(num(m.c, 1e12));
      if (JX.ph !== 'wait' || c <= 0 || JX.bets.has(me.id)) { send(ws, { t: 'jxno', c }); return; }
      const ax = num(m.ax, 1000); JX.bets.set(me.id, { id: me.id, ws, n: me.state.name, c, ax: ax >= 1.01 ? ax : 0, x: 0 }); JX.bd = true;
    } else if (m.t === 'jxcancel') { if (JX.ph === 'wait' && JX.bets.delete(me.id)) JX.bd = true; }
    else if (m.t === 'jxcash') { const b = JX.bets.get(me.id); if (JX.ph === 'fly' && b && !b.x) { const x = Math.floor(jxM(Date.now()) * 100) / 100; if (x < JX.crash) jxPay(b, x); } }
  });
  ws.on('close', () => { if (me && players.get(me.id) === me) { players.delete(me.id); broadcast({ t: 'leave', id: me.id }); broadcastCount(); } });
});

function send(ws, o) { try { if (ws.readyState === 1) ws.send(JSON.stringify(o)); } catch (e) { } }

// Avisos de apuestas y premios para todos (máx. 2 por segundo por jugador; premios grandes siempre)
function feed(me, m) {
  if ((m.type !== 'bet' && m.type !== 'win') || typeof m.game !== 'string' || !/^[a-z]{2,12}$/.test(m.game)) return;
  const c = Math.abs(Math.round(num(m.c, 1e15))); if (!c) return;
  const now = Date.now(); me.fq = (me.fq || []).filter(t => now - t < 1000);
  if (me.fq.length >= 2 && !(m.type === 'win' && c >= 2000)) return; me.fq.push(now);
  broadcast({ t: 'feed', pid: me.id, n: me.state.name, g: m.game, k: m.type, c });
}

// JetX compartida: una sola ronda para todos, la decide el servidor
const JX_WAIT = 6000, JX_PAUSE = 3000, JX = { ph: 'wait', t0: Date.now(), r: 0, crash: 2, bets: new Map(), hist: [], bd: false, lastB: 0 };
const jxM = now => Math.exp(.11 * (now - JX.t0) / 1000);
function jxMsg() { return { t: 'jx', ph: JX.ph, el: Date.now() - JX.t0, r: JX.r, crash: JX.ph === 'crash' ? JX.crash : 0, hist: JX.hist }; }
function jxList() { return { t: 'jxb', r: JX.r, list: [...JX.bets.values()].map(b => ({ n: b.n, c: b.c, x: b.x })) }; }
function jxTo(ws) { send(ws, jxMsg()); send(ws, jxList()); }
function jxPay(b, x) { b.x = x; JX.bd = true; send(b.ws, { t: 'jxok', x, r: JX.r }); }
function jxNew() { JX.r++; JX.ph = 'wait'; JX.t0 = Date.now(); JX.bets = new Map(); JX.bd = true; const u = Math.random(); JX.crash = Math.min(1000, Math.max(1, Math.floor(99 / (1 - u)) / 100)); broadcast(jxMsg()); }
jxNew();
setInterval(() => {
  const now = Date.now();
  if (JX.ph === 'wait' && now - JX.t0 >= JX_WAIT) { JX.ph = 'fly'; JX.t0 = now; broadcast(jxMsg()); }
  else if (JX.ph === 'fly') {
    const m = jxM(now);
    for (const b of JX.bets.values()) if (!b.x && b.ax && b.ax < JX.crash && m >= b.ax) jxPay(b, b.ax);
    if (m >= JX.crash) { JX.ph = 'crash'; JX.t0 = now; JX.hist.unshift(JX.crash); JX.hist = JX.hist.slice(0, 14); JX.bd = true; broadcast(jxMsg()); }
  } else if (JX.ph === 'crash' && now - JX.t0 >= JX_PAUSE) jxNew();
  if (JX.bd && now - JX.lastB > 200) { JX.bd = false; JX.lastB = now; broadcast(jxList()); }
}, 40);

function full(p) { return Object.assign({}, p.state, { lk: p.lk }); }
function sendSnap(ws) { try { ws.send(JSON.stringify({ t: 'snap', list: [...players.values()].map(full) })); } catch (e) { } }
function broadcast(obj) { const s = JSON.stringify(obj); for (const p of players.values()) if (p.ws.readyState === 1) p.ws.send(s); }
function broadcastCount() { broadcast({ t: 'count', n: players.size }); }

// 10 veces por segundo: un solo paquete con lo que ha cambiado
setInterval(() => {
  const out = [];
  for (const p of players.values()) if (p.dirty) { const o = Object.assign({ t: 'state' }, p.state); if (p.lkDirty) { o.lk = p.lk; p.lkDirty = false; } out.push(o); p.dirty = false; }
  if (out.length) { const s = JSON.stringify(out); for (const p of players.values()) if (p.ws.readyState === 1) p.ws.send(s); }
}, 100);
// conexiones muertas
setInterval(() => { for (const ws of wss.clients) { if (!ws.isAlive) { ws.terminate(); continue; } ws.isAlive = false; try { ws.ping(); } catch (e) { } } }, 15000);

server.listen(PORT, () => console.log('Neon Royale escuchando en el puerto ' + PORT));
