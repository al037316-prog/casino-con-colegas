/* Neon Royale — servidor multijugador
   Sirve el juego (carpeta public/) y mantiene el mundo compartido por WebSocket en /ws.
   Arranque: npm install && npm start   (puerto: variable PORT o 3000) */
const http = require('http'), fs = require('fs'), path = require('path');
const { WebSocketServer } = require('ws');
const PORT = process.env.PORT || 3000, PUB = path.join(__dirname, 'public');
// versión del juego: cambia cada vez que se sube un index.html nuevo (los jugadores se recargan solos)
let VER = '0'; try { VER = require('crypto').createHash('md5').update(fs.readFileSync(path.join(PUB, 'index.html'))).digest('hex').slice(0, 10); } catch (e) { }
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
const SCENES = new Set(['city', 'avenida', 'casino', 'premier', 'vip', 'roof', 'burger', 'salera', 'sur', 'sotano', 'club', 'norte', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'h7', 'h8', 'poli', 'carcel', 'armeria', 'growshop']);

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
      players.set(id, me); sendSnap(ws); broadcastCount(); jxTo(ws); send(ws, topMsg()); send(ws, cityMsg()); send(ws, bizMsg()); send(ws, homesMsg()); send(ws, sugMsgFor(id)); send(ws, { t: 'news', list: DB.news }); if (DB.el.votes[id]) send(ws, { t: 'myvote', id: DB.el.votes[id] }); if (DB.refunds[id]) { give(me, DB.refunds[id], 'Devolución pendiente'); delete DB.refunds[id]; saveDb(); } crJoin(me); send(ws, { t: 'chathist', list: CHATLOG.concat(PRIVLOG[me.id] || []).sort((a, b) => a.at - b.at).slice(-40) }); if (CAB) send(ws, cabMsg()); if (UFO) send(ws, { t: 'ufo', at: Date.now() - UFO.at, x: UFO.x, z: UFO.z }); send(ws, { t: 'ver', v: VER }); for (const k in RL) send(ws, rlMsg(k)); send(ws, occMsg()); for (const [k, L] of LIVE) send(ws, Object.assign({ t: 'live', k }, L)); return;
    }
    if (!me) return;
    // límite de mensajes por segundo
    const now = Date.now(); if (now - me.t0 > 1000) { me.t0 = now; me.n = 0; } if (++me.n > 40) return;
    if (m.t === 'state') {
      const s = me.state;
      if (SCENES.has(m.scene)) s.scene = m.scene;
      s.x = num(m.x); s.y = num(m.y, 100); s.z = num(m.z); s.ry = num(m.ry, 100); s.mv = num(m.mv, 20); s.m = Math.round(num(m.m, 1e15));
      s.w = typeof m.w === 'string' && /^[a-z]{0,8}$/.test(m.w) ? m.w : ''; s.g = typeof m.g === 'string' && /^[a-z]{0,12}$/.test(m.g) ? m.g : '';
      s.name = clean(m.name || s.name); s.car = typeof m.car === 'string' ? m.car.slice(0, 12) : null;
      s.cc = typeof m.cc === 'string' ? m.cc.slice(0, 9) : undefined; s.ct = m.ct && typeof m.ct === 'object' ? JSON.parse(JSON.stringify(m.ct).slice(0, 300) || '{}') : undefined;
      if (typeof m.lk === 'string' && m.lk.length < 1200 && m.lk !== me.lk) { me.lk = m.lk; me.lkDirty = true; crLook(me); }
      me.dirty = true;
    } else if (m.t === 'name') { me.state.name = clean(m.name); me.dirty = true; }
    else if (m.t === 'sync') { sendSnap(ws); jxTo(ws); }
    else if (m.t === 'tx') feed(me, m);
    else if (m.t === 'stats') stats(me, m);
    else if (m.t === 'adm') admAct(me, m);
    else if (m.t === 'hbuy') homeBuy(me, m);
    else if (m.t === 'hsell') homeSell(me, m);
    else if (m.t === 'hclaim') homeClaim(me, m);
    else if (m.t === 'rlbet') rlBet(me, m);
    else if (m.t === 'occ') occ(me, m);
    else if (m.t === 'live') live(me, m);
    else if (m.t === 'seed') seed(m);
    else if (m.t === 'run') runFor(me, m);
    else if (m.t === 'vote') vote(me, m);
    else if (m.t === 'gov') govSet(me, m);
    else if (m.t === 'fund') fund(me, m);
    else if (m.t === 'tax') tax(me, m);
    else if (m.t === 'aid') aid(me);
    else if (m.t === 'bizbuy') bizBuy(me, m);
    else if (m.t === 'bizsell') bizSell(me, m);
    else if (m.t === 'bizclaim') bizClaim(me, m);
    else if (m.t === 'bid') bid(me, m);
    else if (m.t === 'bizspend') { const b = DB.biz[String(m.id)], c = Math.round(num(m.c, 5000)); const now = Date.now(); if (b && c > 0 && b.owner !== me.id && now - (me.lastSpend || 0) > 1500) { me.lastSpend = now; owe(b.owner, Math.round(c / 2), 'Clientes en tu negocio'); } }
    else if (m.t === 'chat') chat(me, m);
    else if (m.t === 'sugpost') sugPost(me, m);
    else if (m.t === 'sugvote') sugVote(me, m);
    else if (m.t === 'ufoclaim') ufoClaim(me, m);
    else if (m.t === 'cabpick') cabPick(me, m);
    else if (m.t === 'bz') bizum(me, m);
    else if (m.t === 'role') crRoleMsg(me, m);
    else if (m.t === 'd') crDeal(me, m);
    else if (m.t === 'c') crCop(me, m);
    else if (m.t === 'lic') crLic(me, m);
    else if (m.t === 'shot') crShot(me, m);
    else if (m.t === 'jxbet') {
      const c = Math.round(num(m.c, 1e12));
      if (JX.ph !== 'wait' || c <= 0 || JX.bets.has(me.id)) { send(ws, { t: 'jxno', c }); return; }
      const ax = num(m.ax, 1000); JX.bets.set(me.id, { id: me.id, ws, n: me.state.name, c, ax: ax >= 1.01 ? ax : 0, x: 0 }); JX.bd = true;
    } else if (m.t === 'jxcancel') { if (JX.ph === 'wait' && JX.bets.delete(me.id)) JX.bd = true; }
    else if (m.t === 'jxcash') { const b = JX.bets.get(me.id); if (JX.ph === 'fly' && b && !b.x) { const x = Math.floor(jxM(Date.now()) * 100) / 100; if (x < JX.crash) jxPay(b, x); } }
  });
  ws.on('close', () => { if (me && players.get(me.id) === me) { admOff(me); occFree(me.id); crOff(me); players.delete(me.id); broadcast({ t: 'leave', id: me.id }); broadcastCount(); } });
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

// ---------- Datos permanentes: ranking, alcaldía, gobierno, negocios, subastas, sugerencias y noticias ----------
// Se guardan en data.json y, si existen las variables UPSTASH_REDIS_REST_URL y UPSTASH_REDIS_REST_TOKEN,
// también en una base de datos Redis gratuita (Upstash) para que no se pierdan al reiniciar Render.
const DATA = process.env.DATA_FILE || path.join(__dirname, 'data.json');
const UP = process.env.UPSTASH_REDIS_REST_URL && process.env.UPSTASH_REDIS_REST_TOKEN ? { url: process.env.UPSTASH_REDIS_REST_URL.replace(/\/$/, ''), tok: process.env.UPSTASH_REDIS_REST_TOKEN } : null;
let DB = { homes: {}, top: {}, mayor: null, el: null, gov: null, biz: {}, auc: null, refunds: {}, sug: [], news: [], fin: {} };
for (const f of [DATA, DATA + '.bak']) { try { DB = Object.assign(DB, JSON.parse(fs.readFileSync(f, 'utf8'))); break; } catch (e) { } }
let dbDirty = false; const saveDb = () => { dbDirty = true; };
async function upCmd(args) { const r = await fetch(UP.url, { method: 'POST', headers: { Authorization: 'Bearer ' + UP.tok, 'Content-Type': 'application/json' }, body: JSON.stringify(args) }); return (await r.json()).result; }
async function loadDb() { if (!UP) return; try { const v = await upCmd(['GET', 'neonroyale']); if (v) { DB = Object.assign(DB, JSON.parse(v)); console.log('Datos cargados de Upstash'); } } catch (e) { console.log('Upstash no disponible:', e.message); } }
const flushSync = () => { try { const j = JSON.stringify(DB); fs.writeFileSync(DATA + '.tmp', j); try { fs.copyFileSync(DATA, DATA + '.bak'); } catch (e) { } fs.renameSync(DATA + '.tmp', DATA); } catch (e) { } };
setInterval(() => { if (!dbDirty) return; dbDirty = false; flushSync(); if (UP) upCmd(['SET', 'neonroyale', JSON.stringify(DB)]).catch(() => { dbDirty = true; }); }, UP ? 15000 : 8000);
let _bye = false; const bye = () => { if (_bye) return; _bye = true; flushSync(); const j = JSON.stringify(DB); if (UP) upCmd(['SET', 'neonroyale', j]).catch(() => { }).finally(() => process.exit(0)); else process.exit(0); setTimeout(() => process.exit(0), 3000); };
process.on('SIGTERM', bye); process.on('SIGINT', bye);
const news = (txt, k = 'evento') => { DB.news.unshift({ txt, k, at: Date.now() }); DB.news = DB.news.slice(0, 40); saveDb(); broadcast({ t: 'news', list: DB.news }); };
const byName = n => { n = String(n || '').toLowerCase(); for (const p of players.values()) if (p.state.name.toLowerCase() === n) return p; return null; };
const give = (p, c, why) => send(p.ws, { t: 'gift', c, why });
function owe(id, c, why) { const p = players.get(id); if (p) give(p, c, why); else { DB.refunds[id] = (DB.refunds[id] || 0) + c; saveDb(); } }

// Ranking: solo jugadores reales (cada uno envía sus números al conectarse y cada poco)
const rec = (id, o) => { const r = DB.top[id] || (DB.top[id] = { id }); return Object.assign(r, o); };
function stats(me, m) {
  const day = Math.floor(num(m.tday, 1e7));
  rec(me.id, { n: me.state.name, net: Math.round(num(m.net, 1e15)), lvl: Math.round(num(m.lvl, 1e4)), best: Math.round(num(m.best, 1e15)), tour: Math.round(num(m.tour, 1e9)), tday: day, bizv: Math.round(num(m.bizv, 1e15)), homes: Math.round(num(m.homes, 100)), at: Date.now() });
  pruneTop(); saveDb(); dirtyTop();
}
function seed(m) {
  if (!Array.isArray(m.list)) return; let n = 0;
  for (const x of m.list.slice(0, 100)) { const id = String(x.id || '').replace(/[^a-z0-9]/gi, '').slice(0, 16); if (!id || DB.top[id]) continue; rec(id, { n: clean(x.n), net: Math.round(num(x.net, 1e15)), lvl: Math.round(num(x.lvl, 1e4)), best: Math.round(num(x.best, 1e15)), tour: Math.round(num(x.tour, 1e9)), tday: Math.floor(num(x.tday, 1e7)), bizv: Math.round(num(x.bizv, 1e15)), homes: Math.round(num(x.homes, 100)), at: Math.round(num(x.at, 1e13)) }); n++; }
  if (n) { pruneTop(); saveDb(); dirtyTop(); }
}
function pruneTop() { const L = Object.values(DB.top); if (L.length <= 300) return; L.sort((a, b) => b.net - a.net); for (const r of L.slice(300)) delete DB.top[r.id]; }
function topMsg() { return { t: 'top', list: Object.values(DB.top) }; }
let topD = false, lastTop = 0; const dirtyTop = () => { topD = true; };

// ---------- Alcaldía: elecciones semanales (cierran el domingo a las 20:00, hora de Madrid en verano) ----------
const BONUS = new Set(['wage', 'biz', 'xp']);
const LIM = { tax: [0, 15], prize: [0, 5], aid: [0, 5000], salary: [0, 50000] }; // impuesto % sobre premios del casino, extra % pagado por el ayuntamiento, ayuda diaria y sueldo/hora del alcalde (céntimos)
function nextSunday() { const d = new Date(); d.setUTCHours(18, 0, 0, 0); let add = (7 - d.getUTCDay()) % 7; if (add === 0 && d.getTime() <= Date.now()) add = 7; d.setUTCDate(d.getUTCDate() + add); return d.getTime(); }
function newElection() { DB.el = { ends: nextSunday(), cands: {}, votes: {} }; saveDb(); }
if (!DB.el || !DB.el.ends || DB.el.ends - Date.now() > 8 * 864e5) newElection();
if (!DB.gov) DB.gov = { tax: 3, prize: 0, aid: 0, salary: 5000, treasury: 0, log: [], aidDay: {}, paidAt: Date.now() };
function runFor(me, m) {
  const e = DB.el; if (Object.keys(e.cands).length >= 12 || e.cands[me.id]) return;
  e.cands[me.id] = { id: me.id, n: me.state.name, slogan: clean(m.slogan).slice(0, 40) || 'Por una ciudad con más neón', bonus: BONUS.has(m.bonus) ? m.bonus : 'wage', fund: 0 }; dirtyCity();
  news('🗳️ ' + me.state.name + ' se presenta a las elecciones a alcalde', 'alcaldia');
}
// financiar la campaña: solo da visibilidad (carteles), nunca votos; máximo 1.000 € por candidato
function fund(me, m) { const e = DB.el, c = e.cands[me.id], v = Math.round(num(m.c, 1e9)); if (!c || v <= 0) return; const ok = Math.min(v, 100000 - c.fund); if (ok <= 0) { give(me, v, 'Campaña: límite de 1.000 € alcanzado'); return; } c.fund += ok; if (ok < v) give(me, v - ok, 'Campaña: límite de 1.000 €'); dirtyCity(); }
function vote(me, m) { const e = DB.el, id = String(m.id || ''); if (!e.cands[id]) return; e.votes[me.id] = id; dirtyCity(); send(me.ws, { t: 'myvote', id }); }
function cityMsg() {
  const e = DB.el, v = {}; for (const k in e.votes) v[e.votes[k]] = (v[e.votes[k]] || 0) + 1;
  const g = DB.gov;
  return { t: 'city', mayor: DB.mayor, gov: { tax: g.tax, prize: g.prize, aid: g.aid, salary: g.salary, treasury: Math.round(g.treasury), log: g.log.slice(0, 12), lim: LIM }, el: { ends: e.ends, left: e.ends - Date.now(), cands: Object.values(e.cands).map(c => Object.assign({ v: v[c.id] || 0 }, c)), total: Object.keys(e.votes).length } };
}
function govSet(me, m) {
  if (!DB.mayor || DB.mayor.id !== me.id) return; const g = DB.gov, ch = [];
  for (const k of ['tax', 'prize', 'aid', 'salary']) if (m[k] != null) { const v = Math.round(Math.max(LIM[k][0], Math.min(LIM[k][1], num(m[k], 1e9)))); if (v !== g[k]) { ch.push(k); g[k] = v; } }
  if (BONUS.has(m.bonus) && m.bonus !== DB.mayor.bonus) { DB.mayor.bonus = m.bonus; ch.push('bonus'); }
  if (!ch.length) return;
  const txt = { tax: 'impuesto sobre premios ' + g.tax + ' %', prize: 'extra en premios ' + g.prize + ' %', aid: 'ayuda diaria ' + (g.aid / 100).toFixed(2) + ' €', salary: 'sueldo del alcalde ' + (g.salary / 100).toFixed(2) + ' €/h', bonus: 'nueva ventaja de ciudad' };
  const line = '👑 ' + me.state.name + ' cambia: ' + ch.map(k => txt[k]).join(', ');
  g.log.unshift({ txt: line, at: Date.now() }); g.log = g.log.slice(0, 30); dirtyCity(); news(line, 'alcaldia');
}
// impuestos: los clientes declaran lo retenido de cada premio del casino; el extra lo paga el ayuntamiento si hay fondos
function tax(me, m) { const c = Math.round(num(m.c, 1e9)); if (c > 0) { DB.gov.treasury += c; saveDb(); cityD = true; } const x = Math.round(num(m.x, 1e9)); if (x > 0) { DB.gov.treasury = Math.max(0, DB.gov.treasury - x); saveDb(); cityD = true; } }
function aid(me) {
  const g = DB.gov, day = Math.floor(Date.now() / 864e5); if (!g.aid) { send(me.ws, { t: 'aidno', why: 'El alcalde no ha aprobado ayudas' }); return; }
  if ((g.aidDay[me.id] || 0) === day) { send(me.ws, { t: 'aidno', why: 'Ya has cobrado la ayuda de hoy' }); return; }
  if (g.treasury < g.aid) { send(me.ws, { t: 'aidno', why: 'El ayuntamiento no tiene fondos' }); return; }
  g.treasury -= g.aid; g.aidDay[me.id] = day; give(me, g.aid, 'Ayuda municipal'); dirtyCity();
}
let cityD = false; const dirtyCity = () => { cityD = true; saveDb(); };

// ---------- Negocios: un solo dueño por negocio. Kiosco y Cafetería para el primero que los compre; el resto, en subasta diaria ----------
const BIZ = { kiosk: 150000, cafe: 800000, phones: 2500000, club: 15000000, bowling: 9000000, gym: 6000000, wash: 4500000, beach: 2000000 };
const FIRST = new Set(['kiosk', 'cafe']), AUC_MS = 15 * 60e3, AUC_ADD = 60e3, AUC_GAP = 45 * 60e3;
const bizMsg = () => ({ t: 'biz', own: DB.biz, auc: DB.auc && Object.assign({}, DB.auc, { left: DB.auc.ends - Date.now() }) });
let bizD = false; const dirtyBiz = () => { bizD = true; saveDb(); };
function bizBuy(me, m) {
  const id = String(m.id || ''); if (!FIRST.has(id)) return send(me.ws, { t: 'bizno', id, c: Math.round(num(m.c, 1e12)), why: 'Este negocio solo se consigue en subasta' });
  if (DB.biz[id]) return send(me.ws, { t: 'bizno', id, c: Math.round(num(m.c, 1e12)), why: 'Ya tiene dueño: ' + DB.biz[id].n });
  if (Math.round(num(m.c, 1e12)) < BIZ[id]) return; DB.biz[id] = { owner: me.id, n: me.state.name, since: Date.now() }; dirtyBiz(); news('🏪 ' + me.state.name + ' compra ' + id, 'negocios');
}
function bizSell(me, m) { const id = String(m.id || ''), b = DB.biz[id]; if (!b || b.owner !== me.id) return; delete DB.biz[id]; dirtyBiz(); }
function bizClaim(me, m) { // compras hechas antes de que existieran los dueños únicos
  const id = String(m.id || ''); if (!(id in BIZ)) return; if (!DB.biz[id] && FIRST.has(id)) { DB.biz[id] = { owner: me.id, n: me.state.name, since: Date.now() }; dirtyBiz(); }
  else if (!DB.biz[id] || DB.biz[id].owner !== me.id) send(me.ws, { t: 'bizlost', id });
}
const aucEv = (k, txt) => broadcast({ t: 'aucev', k, txt });
function nextAuction() {
  if (DB.auc) return;
  const free = Object.keys(BIZ).filter(id => !FIRST.has(id) && !DB.biz[id]); if (!free.length) { DB.auc = null; return; }
  const last = DB.aucLast || ''; const id = free[(free.indexOf(last) + 1) % free.length] || free[0];
  DB.aucLast = id; DB.auc = { id, min: BIZ[id], ends: Date.now() + AUC_MS, top: null, n: 0 }; dirtyBiz(); aucEv('start', '🔨 SUBASTA: ' + id + ' · precio inicial ' + (BIZ[id] / 100).toLocaleString('es-ES') + ' € · 15 min'); news('🔨 Nueva subasta: ' + id + ' · puja mínima ' + (BIZ[id] / 100).toLocaleString('es-ES') + ' €', 'subasta');
}
function bid(me, m) {
  const a = DB.auc, c = Math.round(num(m.c, 1e13)); if (!a || String(m.id) !== a.id) return send(me.ws, { t: 'bidno', c, why: 'Esa subasta ya terminó' });
  const need = a.top ? Math.ceil(a.top.c * 1.05) : a.min; if (c < need) return send(me.ws, { t: 'bidno', c, why: 'La puja mínima ahora es ' + (need / 100).toFixed(2) + ' €' });
  const prev = a.top;
  if (prev) { owe(prev.id, prev.c, 'Devolución de tu puja en ' + a.id); const pp = players.get(prev.id); pp && send(pp.ws, { t: 'aucev', k: 'out', txt: '⚠️ ' + me.state.name + ' te ha superado en ' + a.id + ' con ' + (c / 100).toLocaleString('es-ES') + ' €. Tu puja (' + (prev.c / 100).toLocaleString('es-ES') + ' €) te ha sido devuelta.' }); }
  a.top = { id: me.id, n: me.state.name, c }; a.n++; a.ends += AUC_ADD; dirtyBiz();
  aucEv('bid', '💰 ' + me.state.name + ' puja ' + (c / 100).toLocaleString('es-ES') + ' € por ' + a.id + ' (+1 min)');
}
if ((DB.auc === null || DB.auc === undefined) && !DB.aucNextAt) DB.aucNextAt = Date.now() + 3 * 60e3;

// ---------- Casas: cada casa tiene un solo dueño (compra o alquiler de 7 días) ----------
const HOME_ID = /^h([1-9]|[1-9][0-9])$/, homesMsg = () => ({ t: 'homes', own: DB.homes || (DB.homes = {}) });
function homeBuy(me, m) {
  const id = String(m.id || ''); if (!HOME_ID.test(id)) return; const mode = m.m === 'rent' ? 'rent' : 'buy', cur = DB.homes[id], n = Date.now();
  if (cur && cur.o !== me.id) return send(me.ws, { t: 'hno', id, why: 'Esa casa ya tiene dueño' });
  if (cur && cur.m === 'buy') return send(me.ws, { t: 'hno', id, why: 'Esa casa ya es tuya' });
  const until = mode === 'rent' ? Math.max(n, (cur && cur.until) || 0) + 7 * 864e5 : 0;
  DB.homes[id] = { o: me.id, n: me.state.name, m: mode, until }; saveDb(); broadcast(homesMsg()); send(me.ws, { t: 'hok', id, m: mode, until });
}
function homeSell(me, m) { const id = String(m.id || ''), c = DB.homes[id]; if (c && c.o === me.id) { delete DB.homes[id]; saveDb(); broadcast(homesMsg()); } }
function homeClaim(me, m) { // al conectar: casas que el jugador ya tenía antes de este sistema
  const id = String(m.id || ''); if (!HOME_ID.test(id) || DB.homes[id]) return; const n = Date.now(), mode = m.m === 'rent' ? 'rent' : 'buy';
  DB.homes[id] = { o: me.id, n: me.state.name, m: mode, until: mode === 'rent' ? Math.min(num(m.until, 1e15), n + 7 * 864e5) : 0 }; saveDb(); broadcast(homesMsg());
}
const homeFree = id => { if (id && DB.homes[id]) { delete DB.homes[id]; saveDb(); broadcast(homesMsg()); } };
function homesTick(now) { let ch = false; for (const id in DB.homes) { const c = DB.homes[id]; if (c.m === 'rent' && c.until < now) { delete DB.homes[id]; ch = true; } } if (ch) { saveDb(); broadcast(homesMsg()); } }

// ---------- Chat, privados y comandos ----------
const ADMIN = process.env.ADMIN_PASS || ''; // la contraseña se pone en Render (Environment), nunca en el código
const CHATLOG = [], PRIVLOG = {}; // historial en memoria para quien entra o se reconecta
function chat(me, m) {
  let txt = String(m.txt || '').replace(/[<>]/g, '').trim().slice(0, 200); if (!txt) return;
  const now = Date.now(); if (now - (me.lastChat || 0) < 700) return; me.lastChat = now;
  if (txt[0] === '/') return command(me, txt);
  if (m.to) { const p = players.get(String(m.to)); if (!p) return send(me.ws, { t: 'chat', sys: true, txt: 'Ese jugador no está conectado' }); const o = { t: 'chat', id: now.toString(36) + Math.random().toString(36).slice(2, 5), priv: true, pid: me.id, n: me.state.name, to: p.id, tn: p.state.name, txt, at: now }; send(p.ws, o); send(me.ws, o); for (const k of [me.id, p.id]) { const L = PRIVLOG[k] || (PRIVLOG[k] = []); L.push(o); if (L.length > 30) L.shift(); } return; }
  const o = { t: 'chat', id: now.toString(36) + Math.random().toString(36).slice(2, 5), pid: me.id, n: me.state.name, txt, at: now }; CHATLOG.push(o); if (CHATLOG.length > 40) CHATLOG.shift(); broadcast(o);
}
function admOff(me) {
  if (!me.isAdm) return; me.isAdm = false; if (me.inv) { me.inv = false; me.dirty = true; broadcast(Object.assign({ t: 'state' }, me.state)); }
  if (isDeal(me)) { CRM.deal = null; crSave(); } if (isCop(me)) { CRM.cop = null; crSave(); copId(); } delete CRM.lic[me.id]; crRoleMsg0(me);
  send(me.ws, { t: 'adm', on: false });
}
function admAct(me, m) {
  if (!me.isAdm) return; const n = Date.now();
  if (m.a === 'inv') { me.inv = !!m.on; if (me.inv) broadcast({ t: 'leave', id: me.id }); else { me.dirty = true; broadcast(Object.assign({ t: 'state' }, me.state, { lk: me.lk })); } }
  else if (m.a === 'dealer') { if (isCop(me)) { CRM.cop = null; copId(); } CRM.deal = { id: me.id, n: me.state.name, since: n, inv: { seeds: 0, plants: [], prod: 0, lamp: 1 }, house: null, sales: 0, msales: 0, changes: 0, first: 0, mask: false, last: 0, spots: {}, off: 0 }; crSave(); crRoleMsg0(me); }
  else if (m.a === 'cop') { if (isDeal(me)) CRM.deal = null; CRM.cop = { id: me.id, n: me.state.name, at: n, paid: n, ev: [], off: 0 }; crSave(); copId(); crRoleMsg0(me); }
  else if (m.a === 'lic') { CRM.lic[me.id] = n + 7200000; send(me.ws, { t: 'lic', until: CRM.lic[me.id] }); }
  else if (m.a === 'goods' && isDeal(me)) { CRM.deal.inv.seeds += 50; CRM.deal.inv.prod += 100; CRM.deal.inv.lamp = 3; crDealMsg(me); }
}
function command(me, txt) {
  const [cmd, ...a] = txt.split(/\s+/), sys = t => send(me.ws, { t: 'chat', sys: true, txt: t });
  if (cmd === '/help' || cmd === '/ayuda') return sys('Comandos: /online · /w NOMBRE mensaje (privado) · /givemoney NOMBRE CANTIDAD CONTRASEÑA · /ufo CONTRASEÑA · /ayuntamiento CONTRASEÑA · /admin CONTRASEÑA (administración)');
  if (cmd === '/online') return sys('Conectados (' + players.size + '): ' + [...players.values()].map(p => p.state.name).join(', '));
  if (cmd === '/w' || cmd === '/msg') { const p = byName(a[0]); if (!p) return sys('No hay nadie conectado con ese nombre'); return chat(me, { txt: a.slice(1).join(' '), to: p.id }); }
  if (cmd === '/admin') {
    if (a[0] === 'off') { admOff(me); return sys('Modo admin desactivado'); }
    const now = Date.now(); me.adm = (me.adm || []).filter(t => now - t < 600000); if (me.adm.length >= 5) return sys('Demasiados intentos. Espera 10 minutos.');
    if (!ADMIN) return sys('La administración no está activada en este servidor (falta ADMIN_PASS en Render)');
    if (a[0] !== ADMIN) { me.adm.push(now); return sys('Contraseña incorrecta'); }
    me.isAdm = true; send(me.ws, { t: 'adm', on: true }); console.log('admin on', me.state.name); return sys('Modo admin activado. Nada de lo que hagas se guarda; /admin off para salir.');
  }
  if (cmd === '/givemoney' || cmd === '/ufo' || cmd === '/ayuntamiento') {
    const now = Date.now(); me.adm = (me.adm || []).filter(t => now - t < 600000); if (me.adm.length >= 5) return sys('Demasiados intentos. Espera 10 minutos.');
    const pass = a[a.length - 1];
    if (!ADMIN) return sys('La administración no está activada en este servidor (falta ADMIN_PASS en Render)');
    if (pass !== ADMIN) { me.adm.push(now); return sys('Contraseña incorrecta'); }
    if (cmd === '/ufo') { ufoStart(); return sys('OVNI en camino'); }
    if (cmd === '/ayuntamiento') return sys(cabStart() ? 'Pleno del Ayuntamiento convocado' : 'Ya hay un pleno en marcha');
    const p = byName(a[0]), c = Math.round(num(String(a[1] || '').replace(',', '.'), 1e9) * 100);
    if (!p || !(c > 0) || a.length < 3) return sys('Uso: /givemoney NOMBRE CANTIDAD CONTRASEÑA (el jugador tiene que estar conectado)');
    give(p, c, 'Administración'); console.log('givemoney', me.state.name, '->', p.state.name, c); return sys('Has dado ' + (c / 100).toFixed(2) + ' € a ' + p.state.name);
  }
  sys('Comando desconocido. Escribe /help');
}

// ---------- Sugerencias (foro con votos únicos) ----------
function sugMsgFor(id) { return { t: 'sug', list: DB.sug.slice(0, 60).map(s => ({ id: s.id, a: s.a, txt: s.txt, at: s.at, v: s.v.length, me: s.v.includes(id), mine: s.pid === id })) }; }
function sugAll() { for (const p of players.values()) send(p.ws, sugMsgFor(p.id)); }
function sugPost(me, m) { const txt = String(m.txt || '').replace(/[<>]/g, '').trim().slice(0, 280); if (txt.length < 5) return; const now = Date.now(); if (now - (me.lastSug || 0) < 120000) return send(me.ws, { t: 'chat', sys: true, txt: 'Espera 2 minutos entre sugerencias' }); me.lastSug = now; DB.sug.unshift({ id: now.toString(36) + Math.random().toString(36).slice(2, 5), a: me.state.name, pid: me.id, txt, at: now, v: [] }); DB.sug = DB.sug.slice(0, 200); saveDb(); sugAll(); }
function sugVote(me, m) { const s = DB.sug.find(x => x.id === m.id); if (!s || s.v.includes(me.id)) return; s.v.push(me.id); DB.sug.sort((a, b) => b.v.length - a.v.length || b.at - a.at); saveDb(); sugAll(); }


// ---------- Pleno del Ayuntamiento (/ayuntamiento CONTRASEÑA): teletransporta a todos y el alcalde decide ----------
const CABO = { reparto: 'Repartir el tesoro entre todos', suerte: 'Semana de suerte (0 % impuestos, +5 % premios)', ayuda: 'Ayuda diaria de 20 €', levy: 'Impuesto extraordinario de 3 € a todos', robo: 'Quedarme la mitad del tesoro', subida: 'Subir impuestos al máximo (15 %)' };
const CABG = ['reparto', 'suerte', 'ayuda'];
let CAB = null;
function cabStart() {
  if (CAB) return false; const m = DB.mayor && players.get(DB.mayor.id);
  CAB = { end: Date.now() + 60000, votes: {}, vote: !m, mid: m ? DB.mayor.id : null };
  broadcast(cabMsg()); news('🏛️ ¡Pleno extraordinario en el Ayuntamiento! ' + (m ? 'El alcalde ' + DB.mayor.n + ' tiene que decidir.' : 'No hay alcalde: decide el pueblo.'), 'alcaldia'); return true;
}
function cabMsg() { return { t: 'cab', left: CAB.end - Date.now(), vote: CAB.vote, mid: CAB.mid, mayor: DB.mayor ? DB.mayor.n : '', opts: CABO, good: CABG }; }
function cabPick(me, m) {
  if (!CAB) return; const k = String(m.k || ''); if (!CABO[k]) return;
  if (CAB.vote) { CAB.votes[me.id] = k; send(me.ws, { t: 'chat', sys: true, txt: 'Voto registrado' }); return; }
  if (me.id !== CAB.mid) return; cabEnd(k);
}
function cabEnd(k) {
  const g = DB.gov, ps = [...players.values()], n = ps.length || 1; let txt = '';
  if (!k) { const c = {}; for (const id in CAB.votes) c[CAB.votes[id]] = (c[CAB.votes[id]] || 0) + 1; k = Object.keys(c).sort((a, b) => c[b] - c[a])[0]; }
  if (!k) txt = 'Nadie decidió nada y el pleno se cierra sin cambios.';
  else {
    const who = DB.mayor && !CAB.vote ? '👑 ' + DB.mayor.n : 'El pueblo';
    if (k === 'reparto') { const a = Math.min(1000, Math.floor(g.treasury / n)); if (a > 0) { for (const p of ps) give(p, a, 'Reparto del ayuntamiento'); g.treasury -= a * n; } txt = who + ' reparte el tesoro: ' + (a / 100).toFixed(2) + ' € para cada ciudadano'; }
    else if (k === 'suerte') { g.tax = 0; g.prize = 5; txt = who + ' declara la Semana de suerte: 0 % impuestos y +5 % en premios'; }
    else if (k === 'ayuda') { g.aid = 2000; txt = who + ' aprueba una ayuda diaria de 20 € para todos'; }
    else if (k === 'levy') { broadcast({ t: 'levy', c: 300 }); txt = who + ' impone un impuesto extraordinario de 3 € a todos'; }
    else if (k === 'robo') { const mp = DB.mayor && players.get(DB.mayor.id), a = Math.floor(g.treasury / 2); if (mp && a > 0) { g.treasury -= a; give(mp, a, 'Desvío de fondos'); } txt = '💥 ESCÁNDALO: ' + (DB.mayor ? DB.mayor.n : 'alguien') + ' se queda con ' + (a / 100).toFixed(2) + ' € del tesoro'; }
    else if (k === 'subida') { g.tax = 15; g.prize = 0; txt = who + ' sube los impuestos del casino al 15 %'; }
    g.log.unshift({ txt: '🏛️ ' + txt, at: Date.now() }); g.log = g.log.slice(0, 30);
  }
  broadcast({ t: 'cabend', txt }); news('🏛️ ' + txt, 'alcaldia'); CAB = null; dirtyCity(); saveDb();
}

// ---------- Crimen: distribuidor (rol secreto único), policía único, pruebas, cárcel, licencias de armas y Bizum ----------
// Todo lo que decide el resultado (roles, pruebas, detenciones, condenas, recompensas, ventas y precios) se calcula aquí.
if (!DB.cr) DB.cr = {};
for (const k of ['jail', 'ban', 'dban', 'lic', 'hist', 'shots']) if (!DB.cr[k]) DB.cr[k] = k === 'shots' ? [] : {};
if (!DB.cr.clues) DB.cr.clues = [];
const CRM = DB.cr, crSave = saveDb;
const COLS = [['rojo', 255, 0, 0], ['naranja', 255, 140, 0], ['amarillo', 240, 220, 0], ['verde', 0, 170, 60], ['turquesa', 0, 200, 200], ['azul', 20, 60, 220], ['morado', 130, 50, 200], ['rosa', 255, 80, 170], ['marrón', 110, 70, 30], ['negro', 20, 20, 20], ['gris', 130, 130, 130], ['blanco', 240, 240, 240]];
function colName(h) { const m = /^#?([0-9a-f]{6})$/i.exec(String(h || '')); if (!m) return 'oscuro'; const v = parseInt(m[1], 16), r = v >> 16, g = v >> 8 & 255, b = v & 255; let best = 'gris', bd = 1e9; for (const [n, R, G, B] of COLS) { const d = (r - R) ** 2 + (g - G) ** 2 + (b - B) ** 2; if (d < bd) { bd = d; best = n; } } return best; }
function sigOf(p) { let l = {}; try { l = JSON.parse(p.lk || '{}') || {}; } catch (e) { } return { mk: !!l.mk, s: colName(l.shirt), p: colName(l.pants), h: colName(l.hair) }; }
const descOf = g => g.mk ? 'Persona enmascarada con sudadera ' + g.s : 'Camiseta ' + g.s + ', pantalón ' + g.p + ', pelo ' + g.h;
const SPOTS = [[8, 56, 'Plaza del Neón'], [-50, 45, 'Parque Central'], [14, 114, 'la playa'], [0, -45, 'Neon Park'], [32, -72, 'el Karting'], [-40, -52, 'los recreativos']];
const isCop = p => !!(CRM.cop && CRM.cop.id === p.id), isDeal = p => !!(CRM.deal && CRM.deal.id === p.id);
const jailed = id => { const j = CRM.jail[id]; return !!(j && j.until > Date.now()); };
const sysTo = (me, t) => send(me.ws, { t: 'chat', sys: true, txt: t });
const hasLic = id => (CRM.lic[id] || 0) > Date.now();
function near(me, r, scene) { let best = null, bd = r; for (const p of players.values()) { if (p === me || p.state.scene !== me.state.scene) continue; const d = Math.hypot(p.state.x - me.state.x, p.state.z - me.state.z); if (d < bd) { bd = d; best = p; } } return best; }
const copId = () => broadcast({ t: 'copid', id: CRM.cop ? CRM.cop.id : '' });
function crRoleMsg0(me) { send(me.ws, { t: 'role', r: isCop(me) ? 'cop' : isDeal(me) ? 'dealer' : '', until: CRM.ban[me.id] || 0 }); if (isCop(me)) crCopMsg(me); if (isDeal(me)) crDealMsg(me); }
function crCopMsg(me) { const n = Date.now(); send(me.ws, { t: 'copinfo', deal: !!CRM.deal, ev: (CRM.cop.ev || []).length, clues: CRM.clues.filter(c => (CRM.cop.known || []).includes(c.id) && n - c.t < 1800000).slice(0, 25).map(clueOut), shots: CRM.shots.filter(x => n - x.t < 600000).slice(0, 10).map(x => ({ t: x.t, zone: x.zone, desc: x.desc })) }); }
const clueOut = c => ({ id: c.id, t: c.t, spot: SPOTS[c.spot][2], desc: c.desc, q: c.q });
function crDealMsg(me) { const D = CRM.deal, I = D.inv, n = Date.now(); send(me.ws, { t: 'dstate', seeds: I.seeds, prod: I.prod, lamp: I.lamp, slots: [0, 3, 6, 10][I.lamp], plants: I.plants.map(t => Math.max(0, Math.ceil((GROW - (n - t)) / 1000))), mask: D.mask, house: D.house, sales: D.sales, cool: SPOTS.map((x, i) => Math.max(0, Math.ceil(((D.spots[i] || 0) - n) / 1000))) }); }
const FAST = !!process.env.GROW_MS, GROW = +process.env.GROW_MS || 150000; // GROW_MS solo para pruebas
function crRoleMsg(me, m) {
  const r = m.r, on = !!m.on, n = Date.now();
  if (jailed(me.id)) return sysTo(me, 'Estás en la cárcel');
  if (r === 'cop') {
    if (!on) { if (isCop(me)) { CRM.cop = null; news('👮 ' + me.state.name + ' deja el cuerpo de policía'); crSave(); copId(); } return crRoleMsg0(me); }
    if (CRM.cop && CRM.cop.id !== me.id) return sysTo(me, 'Ya hay un policía de servicio: ' + CRM.cop.n);
    if ((CRM.ban[me.id] || 0) > n) return sysTo(me, 'Estás sancionado: no puedes ser policía durante ' + Math.ceil((CRM.ban[me.id] - n) / 60000) + ' min');
    if (isDeal(me)) return sysTo(me, 'No puedes ser policía y distribuidor a la vez');
    if (!CRM.cop) { CRM.cop = { id: me.id, n: me.state.name, at: n, paid: n, ev: [], off: 0 }; news('👮 ' + me.state.name + ' es ahora el policía de la ciudad', 'policia'); crSave(); } copId();
    return crRoleMsg0(me);
  }
  if (r === 'dealer') {
    if (!on) { if (isDeal(me)) { CRM.deal = null; crSave(); } return crRoleMsg0(me); }
    if (CRM.deal && CRM.deal.id !== me.id) return sysTo(me, 'Ese puesto no está disponible ahora mismo');
    if ((CRM.dban[me.id] || 0) > n) return sysTo(me, 'Te vigilan de cerca: espera ' + Math.ceil((CRM.dban[me.id] - n) / 60000) + ' min');
    if (isCop(me)) return sysTo(me, 'No puedes ser policía y distribuidor a la vez');
    if (!m.house && !CRM.deal) return sysTo(me, 'Necesitas una casa propia para operar');
    if (!CRM.deal) { CRM.deal = { id: me.id, n: me.state.name, since: n, inv: { seeds: 0, plants: [], prod: 0, lamp: 1 }, house: null, sales: 0, msales: 0, changes: 0, first: 0, mask: false, last: 0, spots: {}, off: 0 }; crSave(); }
    return crRoleMsg0(me);
  }
}
function crDeal(me, m) {
  const D = CRM.deal; if (!D || D.id !== me.id || jailed(me.id)) return; const n = Date.now(), a = m.a, I = D.inv, slots = [0, 3, 6, 10][I.lamp];
  if (a === 'seed') I.seeds += Math.max(1, Math.min(10, Math.floor(num(m.n, 1e3))));
  else if (a === 'lamp') { if (I.lamp < 3) I.lamp++; }
  else if (a === 'plant') { const h = String(m.house || ''); if (!/^h\d+$/.test(h) || (DB.homes[h] && DB.homes[h].o !== me.id)) return sysTo(me, 'Solo puedes plantar dentro de tu casa'); const k = Math.min(Math.floor(num(m.n, 1e3)), I.seeds, slots - I.plants.length); if (k <= 0) return sysTo(me, 'No tienes semillas o espacio'); I.seeds -= k; for (let i = 0; i < k; i++) I.plants.push(n); D.house = h; }
  else if (a === 'harvest') { let k = 0; I.plants = I.plants.filter(t => { if (n - t >= GROW) { k++; return false; } return true; }); I.prod += k * [0, 3, 4, 5][I.lamp]; if (!k) return sysTo(me, 'Todavía no hay plantas listas'); }
  else if (a === 'mask') D.mask = !!m.on;
  else if (a === 'sell') {
    const s = m.s | 0, sp = SPOTS[s]; if (!sp) return; if (Math.hypot(me.state.x - sp[0], me.state.z - sp[1]) > 9) return sysTo(me, 'Acércate al comprador');
    if (n - D.last < (FAST ? 200 : 8000)) return sysTo(me, 'Espera unos segundos: llamas la atención'); if ((D.spots[s] || 0) > n) return sysTo(me, 'Este comprador ya ha comprado hace poco'); if (I.prod <= 0) return sysTo(me, 'No te queda producto');
    const q = Math.min(I.prod, 1 + Math.floor(Math.random() * 2)), price = q * (4000 + Math.floor(Math.random() * 2001));
    I.prod -= q; D.last = n; D.spots[s] = n + (FAST ? 300 : 60000); D.sales += q; if (!D.first) D.first = n; if (D.mask) D.msales += q;
    give(me, price, 'Venta de hierba'); send(me.ws, { t: 'sold', s, q });
    if (Math.random() < .85) { const g = sigOf(me); CRM.clues.unshift({ id: n.toString(36) + Math.random().toString(36).slice(2, 4), t: n, spot: s, sig: g, q: g.mk ? 1 : 2, desc: descOf(g) }); CRM.clues = CRM.clues.slice(0, 60); if (CRM.cop) { const cp = players.get(CRM.cop.id); if (cp) send(cp.ws, { t: 'alert', txt: '📞 Aviso vecinal: movimiento sospechoso cerca de ' + sp[2] + '. Interroga a los testigos de la zona.' }); } }
  }
  crSave(); crDealMsg(me);
}
function crJail(p, ms, why) { if (FAST) ms = Math.min(ms, 6000); CRM.jail[p.id] = { until: Date.now() + ms, why }; send(p.ws, { t: 'jail', ms, why }); if (isDeal(p)) { CRM.deal = null; send(p.ws, { t: 'role', r: '', until: 0 }); } crSave(); }
function crSanction(me, why) {
  const n = Date.now(); CRM.ban[me.id] = n + 900000; if (isCop(me)) CRM.cop = null; copId(); crJail(me, 120000, why); send(me.ws, { t: 'fine', c: 100000 }); send(me.ws, { t: 'role', r: '', until: CRM.ban[me.id] });
  news('🚨 ' + me.state.name + ' pierde la placa por una detención sin pruebas válidas (15 min fuera del cuerpo)', 'policia');
}
const matchPts = (T, evs) => { const g = sigOf(T); let pts = 0; const kn = (CRM.cop && CRM.cop.known) || []; for (const c of CRM.clues) { if (Date.now() - c.t > 1800000 || !kn.includes(c.id)) continue; const o = c.sig; if (o.mk ? o.s === g.s : (o.s === g.s && o.p === g.p)) pts += c.q; } for (const e of evs) if (e.pid === T.id && Date.now() - e.at < 1800000) pts += e.q; return pts; };
function crCop(me, m) {
  if (!isCop(me) || jailed(me.id)) return; const C = CRM.cop, n = Date.now(), a = m.a;
  if (a === 'sync') return crCopMsg(me);
  if (a === 'ask') { const sp = SPOTS[m.s | 0]; if (!sp || Math.hypot(me.state.x - sp[0], me.state.z - sp[1]) > 9) return sysTo(me, 'Acércate al testigo'); if (n - (C.lq || 0) < (FAST ? 100 : 8000)) return sysTo(me, 'Dale un momento al testigo'); C.lq = n; C.known = C.known || []; const c = CRM.clues.find(x => x.spot === (m.s | 0) && n - x.t < 1800000 && !C.known.includes(x.id)); if (!c) return send(me.ws, { t: 'cev', txt: '🗣️ El testigo no ha visto nada raro por aquí (o ya te lo contó).' }); C.known.push(c.id); crCopMsg(me); return send(me.ws, { t: 'cev', txt: '🗣️ Testigo: «Vi a alguien sospechoso: ' + c.desc.toLowerCase() + '». Anotado en tu cuaderno.' }); }
  if (a === 'search') {
    const sc = me.state.scene; if (!/^h\d+$/.test(sc)) return sysTo(me, 'Tienes que estar dentro de una casa para registrarla'); if (n - (C.ls || 0) < (FAST ? 200 : 15000)) return sysTo(me, 'Espera 15 segundos entre registros'); C.ls = n; const D = CRM.deal;
    if (D && D.house === sc && D.inv.plants.length) { C.ev.push({ k: 'plantacion', pid: D.id, q: 6, at: n }); send(me.ws, { t: 'cev', txt: '🌿 ¡Plantación de marihuana encontrada! Propietario de la casa: ' + D.n + '. Prueba guardada en tu cuaderno.' }); return crCopMsg(me); }
    return send(me.ws, { t: 'cev', txt: 'Registras la casa: no encuentras nada sospechoso.' });
  }
  const T = near(me, a === 'arrest' ? 4 : 6); if (!T) return sysTo(me, 'No hay nadie lo bastante cerca');
  if (a === 'inspect') { const g = sigOf(T); return send(me.ws, { t: 'insp', n: g.mk ? '??? (enmascarado)' : T.state.name, desc: descOf(g), w: T.state.w || '', lic: hasLic(T.id) }); }
  if (a === 'arrest') {
    if (n - (C.la || 0) < (FAST ? 300 : 20000)) return sysTo(me, 'Espera 20 segundos entre detenciones'); C.la = n; if (isCop(T)) return;
    const armed = !!T.state.w && !hasLic(T.id), shots = CRM.shots.some(x => x.pid === T.id && n - x.t < 300000);
    if (isDeal(T)) {
      const D = CRM.deal, pts = matchPts(T, C.ev);
      if (pts >= 5) {
        const mins = D.first ? Math.min(60, Math.floor((n - D.first) / 60000)) : 0, reward = 5000 + mins * 250 + Math.min(10, D.changes) * 1500 + Math.min(20, D.msales) * 300;
        const bh = D.house || Object.keys(DB.homes).find(i => DB.homes[i].o === T.id); homeFree(bh); broadcast({ t: 'scare', x: T.state.x, z: T.state.z }); crJail(T, 300000, 'Detenido por tráfico de marihuana'); send(T.ws, { t: 'fine', c: 500000 }); send(T.ws, { t: 'bust', house: bh }); CRM.dban[T.id] = n + 600000; CRM.deal = null; CRM.clues = []; C.ev = []; C.won = (C.won || 0) + 1;
        give(me, reward * 100, 'Recompensa por detener al distribuidor'); news('👮 ¡' + me.state.name + ' detiene a ' + T.state.name + ', el distribuidor de marihuana! Recompensa: ' + reward.toLocaleString('es-ES') + ' €', 'policia'); crSave(); return crCopMsg(me);
      }
      return crSanction(me, 'Detención sin pruebas válidas');
    }
    if (armed || shots) { broadcast({ t: 'scare', x: T.state.x, z: T.state.z }); crJail(T, 120000, armed ? 'Arma sin licencia' : 'Disparos injustificados'); send(T.ws, { t: 'fine', c: 100000 }); send(T.ws, { t: 'seize' }); give(me, 50000, 'Detención válida'); news('👮 ' + T.state.name + ' detenido por ' + (armed ? 'portar un arma sin licencia' : 'disparar en la calle'), 'policia'); return; }
    return crSanction(me, 'Detención sin pruebas válidas');
  }
}
function crLic(me, m) { const n = Date.now(); if (m.a === 'buy') { CRM.lic[me.id] = n + 7200000; send(me.ws, { t: 'lic', until: CRM.lic[me.id] }); crSave(); } }
function crShot(me, m) {
  if (m.ok) return; const n = Date.now(); let zone = 'la ciudad', bd = 60; for (const [x, z, nm] of SPOTS) { const d = Math.hypot(me.state.x - x, me.state.z - z); if (d < bd) { bd = d; zone = nm; } }
  const g = sigOf(me); CRM.shots.unshift({ pid: me.id, t: n, zone, desc: descOf(g) }); CRM.shots = CRM.shots.slice(0, 30);
  if (CRM.cop) { const cp = players.get(CRM.cop.id); if (cp) send(cp.ws, { t: 'alert', txt: '🔫 Disparos cerca de ' + zone + ' · ' + descOf(g) }); } broadcast({ t: 'shotfx', x: me.state.x, z: me.state.z });
}
function bizum(me, m) {
  const c = Math.round(num(m.c, 1e9)), nonce = String(m.n || '').slice(0, 24), n = Date.now(), fail = why => send(me.ws, { t: 'bzr', ok: false, why, c, n: nonce });
  if (!(c >= 100 && c <= 500000)) return fail('Importe no válido (de 1 € a 5.000 €)'); if (!nonce) return fail('Operación no válida');
  me.bzn = me.bzn || []; if (me.bzn.includes(nonce)) return; if (n - (me.bzt || 0) < (FAST ? 100 : 2000)) return fail('Espera 2 segundos entre transferencias'); me.bzt = n;
  let to = players.get(String(m.to || '')), tid = to ? to.id : String(m.to || '').replace(/[^a-z0-9]/gi, ''), tn = to ? to.state.name : (DB.top[tid] || {}).n;
  if (!to && !tn) { const q = String(m.to || '').toLowerCase(); to = byName(q); if (to) { tid = to.id; tn = to.state.name; } else for (const r of Object.values(DB.top)) if ((r.n || '').toLowerCase() === q) { tid = r.id; tn = r.n; } }
  if (!tn) return fail('No encuentro a ese jugador'); if (tid === me.id) return fail('No puedes enviarte dinero a ti mismo');
  me.bzn.push(nonce); me.bzn = me.bzn.slice(-60);
  if (to) give(to, c, 'Bizum de ' + me.state.name); else { DB.refunds[tid] = (DB.refunds[tid] || 0) + c; }
  const H = (id, o) => { const L = CRM.hist[id] || (CRM.hist[id] = []); L.unshift(Object.assign({ t: n }, o)); CRM.hist[id] = L.slice(0, 30); };
  H(me.id, { k: 'out', n: tn, c }); H(tid, { k: 'in', n: me.state.name, c }); crSave();
  send(me.ws, { t: 'bzr', ok: true, c, to: tn, n: nonce, hist: CRM.hist[me.id] }); if (to) send(to.ws, { t: 'bzh', hist: CRM.hist[tid] });
}
function crJoin(me) {
  const n = Date.now(); if (CRM.cop && CRM.cop.id === me.id) CRM.cop.off = 0; if (CRM.deal && CRM.deal.id === me.id) CRM.deal.off = 0;
  crRoleMsg0(me); send(me.ws, { t: 'copid', id: CRM.cop ? CRM.cop.id : '' }); send(me.ws, { t: 'bzh', hist: CRM.hist[me.id] || [] }); if (jailed(me.id)) send(me.ws, { t: 'jail', ms: CRM.jail[me.id].until - n, why: CRM.jail[me.id].why }); if (hasLic(me.id)) send(me.ws, { t: 'lic', until: CRM.lic[me.id] });
}
function crOff(me) { const n = Date.now(); if (CRM.cop && CRM.cop.id === me.id) CRM.cop.off = n; if (CRM.deal && CRM.deal.id === me.id) CRM.deal.off = n; }
function crLook(me) { if (!isDeal(me)) return; const g = sigOf(me), o = CRM.deal.sg; if (o && (o.s !== g.s || o.p !== g.p || o.h !== g.h)) { CRM.deal.changes = Math.min(10, CRM.deal.changes + 1); } CRM.deal.sg = g; }
setInterval(() => {
  const n = Date.now();
  for (const id in CRM.jail) if (CRM.jail[id].until <= n) { delete CRM.jail[id]; const p = players.get(id); if (p) send(p.ws, { t: 'free' }); crSave(); }
  if (CRM.cop) { if (CRM.cop.off && n - CRM.cop.off > 600000) { CRM.cop = null; crSave(); copId(); } else if (n - CRM.cop.paid >= 600000) { CRM.cop.paid = n; const p = players.get(CRM.cop.id); if (p) give(p, 83300, 'Sueldo de policía'); } }
  if (CRM.deal && CRM.deal.off && n - CRM.deal.off > 600000) { CRM.deal = null; crSave(); }
}, 5000);

// ---------- Evento OVNI: cada 40-70 minutos (o con /ufo) aterriza en la ciudad ----------
const UFO_SPOTS = [[0, 56], [0, 1], [-14, 116], [60, 2]];
let UFO = null, ufoNext = Date.now() + (40 + Math.random() * 30) * 60000;
function ufoStart() { const [x, z] = UFO_SPOTS[Math.floor(Math.random() * UFO_SPOTS.length)]; UFO = { at: Date.now(), x, z, claims: {} }; broadcast({ t: 'ufo', at: 0, x, z }); news('🛸 ¡Un OVNI ha aterrizado en la ciudad!', 'evento'); ufoNext = Date.now() + (40 + Math.random() * 30) * 60000; }
function ufoClaim(me, m) { if (!UFO || Date.now() - UFO.at > 100000) return; const i = Math.floor(num(m.i, 10)); if (i < 0 || i > 5) return; const c = UFO.claims[me.id] || (UFO.claims[me.id] = []); if (c.includes(i)) return; c.push(i); give(me, 1000 + Math.floor(Math.random() * 1500), 'Contacto alienígena'); }

setInterval(() => {
  const now = Date.now(), e = DB.el;
  if (now >= e.ends) {
    const v = {}; for (const k in e.votes) v[e.votes[k]] = (v[e.votes[k]] || 0) + 1;
    const best = Object.values(e.cands).sort((a, b) => (v[b.id] || 0) - (v[a.id] || 0) || b.fund - a.fund)[0];
    if (best && (v[best.id] || 0) > 0) { DB.mayor = { id: best.id, n: best.n, bonus: best.bonus, slogan: best.slogan, votes: v[best.id] || 0, since: now }; broadcast({ t: 'mayornew', n: best.n }); news('👑 ' + best.n + ' gana las elecciones con ' + (v[best.id] || 0) + ' votos', 'alcaldia'); }
    newElection(); cityD = true;
  }
  // sueldo del alcalde (sale del tesoro municipal), cada 10 minutos si está conectado
  const g = DB.gov; if (now - g.paidAt >= 600000) { g.paidAt = now; const mp = DB.mayor && players.get(DB.mayor.id), pay = Math.min(Math.round(g.salary / 6), Math.floor(g.treasury)); if (mp && pay > 0) { g.treasury -= pay; give(mp, pay, 'Sueldo de alcalde'); cityD = true; } }
  // subastas
  if (DB.auc && now >= DB.auc.ends) { const a = DB.auc; if (a.top) { DB.biz[a.id] = { owner: a.top.id, n: a.top.n, since: now }; const t = (a.top.c / 100).toLocaleString('es-ES') + ' €'; news('🔨 ' + a.top.n + ' gana la subasta de ' + a.id + ' por ' + t, 'subasta'); aucEv('end', '🏆 ' + a.top.n + ' gana ' + a.id + ' por ' + t); const p = players.get(a.top.id); p && send(p.ws, { t: 'chat', sys: true, txt: '¡Has ganado la subasta! El negocio ya es tuyo.' }); } else { news('🔨 Subasta de ' + a.id + ' sin pujas: se adjudica al precio inicial al Ayuntamiento y vuelve a salir más adelante', 'subasta'); aucEv('end', '🔨 ' + a.id + ': sin pujas, adjudicado al precio inicial (' + (a.min / 100).toLocaleString('es-ES') + ' €) al Ayuntamiento'); } DB.auc = null; dirtyBiz(); DB.aucNextAt = now + AUC_GAP; }
  else if (!DB.auc && now >= (DB.aucNextAt || 0)) nextAuction();
  if (now >= ufoNext && players.size) ufoStart();
  if (UFO && now - UFO.at > 100000) UFO = null;
  if (CAB && now >= CAB.end) cabEnd(CAB.vote ? null : '');
  if (cityD) { cityD = false; broadcast(cityMsg()); }
  homesTick(now);
  if (bizD) { bizD = false; broadcast(bizMsg()); }
  if (topD && now - lastTop > 2000) { topD = false; lastTop = now; broadcast(topMsg()); }
}, 1000);

// ---------- Ruleta compartida: gira sola cada 15 s para todos (apuestas abiertas 15 s, «no va más», giro y resultado) ----------
const RL = {}, RL_BET = 15000, RL_CLOSE = 1200, RL_SPIN = 6600, RL_RES = 4200;
const REDS = new Set([1, 3, 5, 7, 9, 12, 14, 16, 18, 19, 21, 23, 25, 27, 30, 32, 34, 36]);
const rlT = k => RL[k] || (RL[k] = { ph: 'bet', t0: Date.now(), n: null, bets: new Map(), hist: [], win: [] });
function rlPay(key, n) { // mismo reparto que el juego: devuelve el multiplicador total (apuesta incluida)
  const [t, a] = key.split(':');
  if (t === 'n') return +a === n ? 36 : 0; if (n === 0) return 0;
  if (t === 'col') return n % 3 === (+a % 3) ? 3 : 0; if (t === 'doz') return Math.ceil(n / 12) === +a ? 3 : 0;
  return ({ low: n <= 18, high: n >= 19, even: n % 2 === 0, odd: n % 2 === 1, red: REDS.has(n), black: !REDS.has(n) })[t] ? 2 : 0;
}
const RL_KEY = /^(n:([0-9]|[12][0-9]|3[0-6])|col:[123]|doz:[123]|low|high|even|odd|red|black)$/;
function rlMsg(k) { const T = rlT(k); return { t: 'rl', k, ph: T.ph, el: Date.now() - T.t0, n: T.ph === 'spin' || T.ph === 'res' ? T.n : null, bets: [...T.bets.entries()].map(([id, b]) => ({ id, n: b.n, c: b.c, cells: b.cells })), hist: T.hist, win: T.ph === 'res' ? T.win : [] }; }
function rlBet(me, m) {
  const k = String(m.k || 'main').replace(/[^a-z0-9]/gi, '').slice(0, 12) || 'main', T = rlT(k);
  if (T.ph !== 'bet' || !Array.isArray(m.cells)) { send(me.ws, { t: 'rlno', k, c: Math.round(num(m.c, 1e12)) }); return; }
  const b = T.bets.get(me.id) || { n: me.state.name, c: 0, cells: {} }; let add = 0;
  for (const [key, v] of m.cells.slice(0, 60)) { const a = Math.round(num(v, 1e12)); if (!RL_KEY.test(key) || a <= 0) continue; b.cells[key] = (b.cells[key] || 0) + a; add += a; }
  if (!add) return; b.c += add; T.bets.set(me.id, b); broadcast(rlMsg(k));
}
setInterval(() => {
  const now = Date.now();
  for (const k in RL) { const T = RL[k];
    if (T.ph === 'bet' && now - T.t0 >= RL_BET) { T.ph = 'closed'; T.t0 = now; broadcast(rlMsg(k)); }
    else if (T.ph === 'closed' && now - T.t0 >= RL_CLOSE) { T.ph = 'spin'; T.t0 = now; T.n = Math.floor(Math.random() * 37); broadcast(rlMsg(k)); }
    else if (T.ph === 'spin' && now - T.t0 >= RL_SPIN) {
      T.ph = 'res'; T.t0 = now; T.hist.unshift(T.n); T.hist = T.hist.slice(0, 12);
      T.win = []; for (const b of T.bets.values()) { let w = 0; for (const key in b.cells) w += b.cells[key] * rlPay(key, T.n); if (w > 0) T.win.push({ n: b.n, w }); }
      T.win.sort((a, b) => b.w - a.w); broadcast(rlMsg(k));
    }
    else if (T.ph === 'res' && now - T.t0 >= RL_RES) { T.ph = 'bet'; T.t0 = now; T.bets = new Map(); T.win = []; broadcast(rlMsg(k)); } }
}, 100);
rlT('main');

// ---------- Pantallas en directo: cada máquina muestra la partida de quien la está usando ----------
const LIVE = new Map(); // máquina -> { g, n, d }
function live(me, m) {
  const k = String(m.k || '').slice(0, 40), o = OCC.get(k); if (!k || !o || o.id !== me.id) return;
  const now = Date.now(); if (now - (me.lt || 0) < 200) return; me.lt = now;
  let d = null; try { d = m.d && JSON.stringify(m.d).length < 900 ? m.d : null; } catch (e) { }
  const g = String(m.g || '').replace(/[^a-z]/g, '').slice(0, 12); LIVE.set(k, { g, n: me.state.name, d });
  broadcast({ t: 'live', k, g, n: me.state.name, d });
}
const liveDrop = k => { if (LIVE.delete(k)) broadcast({ t: 'live', k, d: null }); };

// ---------- Máquinas: solo puede usarlas un jugador a la vez, como en un casino de verdad ----------
const OCC = new Map(); // máquina -> { id, n }
const occMsg = () => ({ t: 'occ', list: [...OCC.entries()].map(([k, o]) => [k, o.id, o.n]) });
function occ(me, m) {
  const k = String(m.k || '').slice(0, 40); if (!k) return;
  if (m.on) { const o = OCC.get(k); if (o && o.id !== me.id && players.has(o.id)) { send(me.ws, { t: 'occno', k, n: o.n }); return; } for (const [kk, oo] of OCC) if (oo.id === me.id) { OCC.delete(kk); liveDrop(kk); } OCC.set(k, { id: me.id, n: me.state.name }); }
  else { const o = OCC.get(k); if (o && o.id === me.id) { OCC.delete(k); liveDrop(k); } }
  broadcast(occMsg());
}
function occFree(id) { let ch = false; for (const [k, o] of OCC) if (o.id === id) { OCC.delete(k); liveDrop(k); ch = true; } if (ch) broadcast(occMsg()); }

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
function sendSnap(ws) { try { ws.send(JSON.stringify({ t: 'snap', list: [...players.values()].filter(p => !p.inv).map(full) })); } catch (e) { } }
function broadcast(obj) { const s = JSON.stringify(obj); for (const p of players.values()) if (p.ws.readyState === 1) p.ws.send(s); }
function broadcastCount() { broadcast({ t: 'count', n: players.size }); }

// 10 veces por segundo: un solo paquete con lo que ha cambiado
setInterval(() => {
  const out = [];
  for (const p of players.values()) if (p.dirty && p.inv) p.dirty = false; else if (p.dirty) { const o = Object.assign({ t: 'state' }, p.state); if (p.lkDirty) { o.lk = p.lk; p.lkDirty = false; } out.push(o); p.dirty = false; }
  if (out.length) { const s = JSON.stringify(out); for (const p of players.values()) if (p.ws.readyState === 1) p.ws.send(s); }
}, 100);
// conexiones muertas
setInterval(() => { for (const ws of wss.clients) { if (!ws.isAlive) { ws.terminate(); continue; } ws.isAlive = false; try { ws.ping(); } catch (e) { } } }, 15000);

loadDb().then(() => server.listen(PORT, () => console.log('Neon Royale escuchando en el puerto ' + PORT + (UP ? ' · guardado en Upstash' : ' · guardado en data.json'))));
