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
const SCENES = new Set(['city', 'avenida', 'casino', 'premier', 'vip', 'roof', 'burger', 'salera']);

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
      players.set(id, me); sendSnap(ws); broadcastCount(); return;
    }
    if (!me) return;
    // límite de mensajes por segundo
    const now = Date.now(); if (now - me.t0 > 1000) { me.t0 = now; me.n = 0; } if (++me.n > 40) return;
    if (m.t === 'state') {
      const s = me.state;
      if (SCENES.has(m.scene)) s.scene = m.scene;
      s.x = num(m.x); s.y = num(m.y, 100); s.z = num(m.z); s.ry = num(m.ry, 100); s.mv = num(m.mv, 20); s.m = Math.round(num(m.m, 1e15));
      s.name = clean(m.name || s.name); s.car = typeof m.car === 'string' ? m.car.slice(0, 12) : null;
      s.cc = typeof m.cc === 'string' ? m.cc.slice(0, 9) : undefined; s.ct = m.ct && typeof m.ct === 'object' ? JSON.parse(JSON.stringify(m.ct).slice(0, 300) || '{}') : undefined;
      if (typeof m.lk === 'string' && m.lk.length < 1200 && m.lk !== me.lk) { me.lk = m.lk; me.lkDirty = true; }
      me.dirty = true;
    } else if (m.t === 'name') { me.state.name = clean(m.name); me.dirty = true; }
    else if (m.t === 'sync') sendSnap(ws);
  });
  ws.on('close', () => { if (me && players.get(me.id) === me) { players.delete(me.id); broadcast({ t: 'leave', id: me.id }); broadcastCount(); } });
});

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
