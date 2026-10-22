// Bermuda Explorer — servidor online (Node 18+). Serve o jogo + WebSocket (login Google, jogadores online, fila de partida).
const http = require('http'), fs = require('fs'), path = require('path');
const { WebSocketServer } = require('ws');
const { OAuth2Client } = require('google-auth-library');

// carrega .env simples (sem dependência)
try { fs.readFileSync(path.join(__dirname, '.env'), 'utf8').split(/\r?\n/).forEach(l => {
  const m = l.match(/^\s*([A-Z_]+)\s*=\s*(.*)\s*$/); if (m && !(m[1] in process.env)) process.env[m[1]] = m[2];
}); } catch (e) {}

const PORT = +process.env.PORT || 3000;
const CLIENT_ID = process.env.GOOGLE_CLIENT_ID || '';
const ALLOW_GUEST = process.env.ALLOW_GUEST === '1';
const QUEUE_MS = (+process.env.QUEUE_SECONDS || 60) * 1000; // tempo mostrado na busca; não inicia com bots
const MAX_ROOM = 2;   // 1 contra 1
const oauth = new OAuth2Client(CLIENT_ID);

// ---------- dados dos jogadores ----------
// Com DATABASE_URL (Postgres, ex.: Neon) os dados ficam num banco FORA do Render e sobrevivem a qualquer atualização.
// Sem DATABASE_URL usa um arquivo local (só para testar no PC; no Render grátis o arquivo some a cada deploy).
const crypto = require('crypto');
const DATABASE_URL = process.env.DATABASE_URL || '';
let pool = null;
if (DATABASE_URL) {
  const { Pool } = require('pg');
  pool = new Pool({ connectionString: DATABASE_URL, ssl: { rejectUnauthorized: false }, max: 5 });
  pool.query('CREATE TABLE IF NOT EXISTS players(sub text PRIMARY KEY, id text UNIQUE NOT NULL, data jsonb NOT NULL)').catch(e => console.error('db', e.message));
}
const DB_FILE = path.join(process.env.DATA_DIR || path.join(__dirname, 'data'), 'users.json');
const blank = id => ({ id, nick: '', coins: 0, diamonds: 0, items: { skins: [], banners: [] } });
const strip = u => { const { id, ...rest } = u; return rest; };
const users = {};                 // cache: sub -> jogador
const dirty = new Set();
if (!pool) { try { const o = JSON.parse(fs.readFileSync(DB_FILE, 'utf8')); for (const k in o) users[k] = Object.assign(blank(o[k].id), o[k]); } catch (e) {} }
async function flush() {
  if (!dirty.size) return;
  const subs = [...dirty]; dirty.clear();
  try {
    if (pool) { for (const s of subs) await pool.query('UPDATE players SET data=$2 WHERE sub=$1', [s, JSON.stringify(strip(users[s]))]); }
    else { fs.mkdirSync(path.dirname(DB_FILE), { recursive: true }); fs.writeFileSync(DB_FILE, JSON.stringify(users)); }
  } catch (e) { console.error('save', e.message); subs.forEach(s => dirty.add(s)); }
}
setInterval(flush, 5000);
let ft = null;
const touch = sub => { dirty.add(sub); if (!ft) ft = setTimeout(() => { ft = null; flush(); }, 300); };   // salva quase na hora a cada mudança
for (const sig of ['SIGTERM', 'SIGINT']) process.on(sig, () => flush().finally(() => process.exit(0)));
async function loadBySub(sub) {
  const r = await pool.query('SELECT id,data FROM players WHERE sub=$1', [sub]);
  return r.rows[0] ? (users[sub] = Object.assign(blank(r.rows[0].id), r.rows[0].data)) : null;
}
async function userFor(sub) {
  if (users[sub]) return users[sub];
  if (pool) { const u = await loadBySub(sub); if (u) return u; }
  for (;;) {
    const id = String(10000000 + Math.floor(Math.random() * 90000000));
    if (Object.values(users).some(u => u.id === id)) continue;
    const u = blank(id);
    if (pool) {
      try { await pool.query('INSERT INTO players(sub,id,data) VALUES($1,$2,$3)', [sub, id, JSON.stringify(strip(u))]); }
      catch (e) { if (e.code === '23505') { const x = await loadBySub(sub); if (x) return x; continue; } throw e; }
    }
    users[sub] = u; touch(sub); return u;
  }
}
async function findById(id) {
  for (const s in users) if (users[s].id === id) return { sub: s, u: users[s] };
  if (pool) { const r = await pool.query('SELECT sub FROM players WHERE id=$1', [id]); if (r.rows[0]) { const u = await loadBySub(r.rows[0].sub); return { sub: r.rows[0].sub, u }; } }
  return null;
}
const profileMsg = u => ({ t: 'profile', id: u.id, nick: u.nick, coins: u.coins, diamonds: u.diamonds, items: u.items });

// ---------- servidor HTTP (serve /public) ----------
const MIME = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8', '.png': 'image/png', '.jpg': 'image/jpeg', '.webp': 'image/webp', '.glb': 'model/gltf-binary', '.json': 'application/json', '.ico': 'image/x-icon' };
const server = http.createServer((req, res) => {
  let url = decodeURIComponent((req.url || '/').split('?')[0]);
  if (url === '/config.js') {
    res.writeHead(200, { 'Content-Type': MIME['.js'], 'Cache-Control': 'no-store' });
    return res.end('window.BX_CONFIG=' + JSON.stringify({ GOOGLE_CLIENT_ID: CLIENT_ID, ALLOW_GUEST }) + ';');
  }
  if (url === '/admin/grant') {   // /admin/grant?key=SUA_CHAVE&id=12345678&coins=100&diamonds=50&skin=7&banner=3
    const q = new URL(req.url, 'http://x').searchParams;
    if (!process.env.ADMIN_KEY || q.get('key') !== process.env.ADMIN_KEY) { res.writeHead(403); return res.end('chave invalida'); }
    findById(String(q.get('id'))).then(r => {
      if (!r) { res.writeHead(404); return res.end('jogador nao encontrado'); }
      const u = r.u;
      u.coins += parseInt(q.get('coins')) || 0; u.diamonds += parseInt(q.get('diamonds')) || 0;
      const sk = q.get('skin'), bn = q.get('banner');
      if (sk && !u.items.skins.includes(sk)) u.items.skins.push(sk);
      if (bn && !u.items.banners.includes(bn)) u.items.banners.push(bn);
      touch(r.sub);
      const c = online.get(u.id); if (c) send(c.ws, profileMsg(u));
      res.writeHead(200, { 'Content-Type': 'application/json' }); res.end(JSON.stringify(profileMsg(u)));
    }).catch(e => { res.writeHead(500); res.end('erro'); });
    return;
  }
  if (url === '/health') { res.writeHead(200); return res.end('ok'); }
  if (url === '/') url = '/index.html';
  if (url === '/index.html') {   // aceita index.html na raiz OU dentro de /public
    const f = [path.join(__dirname, 'public', 'index.html'), path.join(__dirname, 'index.html')].find(p => fs.existsSync(p));
    if (!f) { res.writeHead(404); return res.end('index.html nao encontrado'); }
    res.writeHead(200, { 'Content-Type': MIME['.html'], 'Cache-Control': 'no-cache' });
    return fs.createReadStream(f).pipe(res);
  }
  const file = path.join(__dirname, 'public', path.normalize(url).replace(/^(\.\.[\/\\])+/, ''));
  if (!file.startsWith(path.join(__dirname, 'public'))) { res.writeHead(403); return res.end(); }
  fs.readFile(file, (err, buf) => {
    if (err) { res.writeHead(404); return res.end('not found'); }
    res.writeHead(200, { 'Content-Type': MIME[path.extname(file)] || 'application/octet-stream', 'Cache-Control': url === '/index.html' ? 'no-cache' : 'public, max-age=3600' });
    res.end(buf);
  });
});

// ---------- WebSocket ----------
const wss = new WebSocketServer({ server, path: '/ws', maxPayload: 16 * 1024 });
const online = new Map();      // userId -> cliente {ws,id,nick,st,room}
let queue = [];                // [{c, timer}]
const rooms = new Map();       // roomId -> Set(userId)
let roomSeq = 1;

const send = (ws, o) => { if (ws.readyState === 1) ws.send(JSON.stringify(o)); };
const pub = c => ({ id: c.id, nick: c.nick, st: c.st });
function broadcastPlayers() {
  const all = [...online.values()];
  for (const c of all) send(c.ws, { t: 'players', list: all.filter(o => o !== c).map(pub) });
}
let bpTimer = null;
const scheduleBroadcast = () => { if (bpTimer) return; bpTimer = setTimeout(() => { bpTimer = null; broadcastPlayers(); }, 300); };

function dequeue(c) { queue = queue.filter(q => { if (q.c === c) { clearTimeout(q.timer); return false; } return true; }); }

function startMatch(list, bots) {
  const room = 'r' + (roomSeq++);
  const set = new Set(list.map(q => q.c.id)); rooms.set(room, set);
  for (const q of list) { clearTimeout(q.timer); q.c.room = room; q.c.st = 'playing'; }
  queue = queue.filter(q => !list.includes(q));
  for (const q of list) send(q.c.ws, { t: 'match', room, bots, peers: list.filter(o => o !== q).map(o => pub(o.c)) });
  scheduleBroadcast();
}
function enqueue(c) {
  if (c.room) leaveRoom(c);   // saiu da partida anterior: pode procurar outra
  dequeue(c);

  // Não pareia o mesmo cliente duas vezes e não cria partida solo.
  const entry = { c, timer: null };
  queue.push(entry);

  // Só começa quando existem DOIS clientes reais, autenticados e ainda conectados.
  const waiting = queue.filter(q =>
    q.c && q.c.ws && q.c.ws.readyState === 1 &&
    online.get(q.c.id) === q.c && !q.c.room
  );
  queue = waiting;

  if (queue.length >= MAX_ROOM) {
    return startMatch(queue.slice(0, MAX_ROOM), false);
  }

  // Mantém a busca aberta indefinidamente. O valor de 60 s é apenas para a interface;
  // expirar esse contador nunca deve iniciar a partida com NPCs.
  send(c.ws, { t: 'searching', seconds: QUEUE_MS / 1000 });
}
function leaveRoom(c) {
  if (!c.room) return;
  const set = rooms.get(c.room);
  if (set) { set.delete(c.id); for (const id of set) { const o = online.get(id); if (o) send(o.ws, { t: 'left', id: c.id }); } if (!set.size) rooms.delete(c.room); }
  c.room = null; c.st = 'online'; scheduleBroadcast();
}

wss.on('connection', ws => {
  let c = null, last = 0, count = 0;
  ws.isAlive = true; ws.on('pong', () => ws.isAlive = true);
  ws.on('message', async raw => {
    const now = Date.now(); if (now - last > 1000) { last = now; count = 0; } if (++count > 60) return; // limite 60 msg/s
    let m; try { m = JSON.parse(raw); } catch (e) { return; }
    if (!m || typeof m.t !== 'string') return;

    if (m.t === 'auth') {
      try {
        let sub, name;
        if (m.credential && CLIENT_ID) {
          const tk = await oauth.verifyIdToken({ idToken: String(m.credential), audience: CLIENT_ID });
          const p = tk.getPayload(); sub = 'g:' + p.sub; name = p.name || '';
        } else if (m.guest) {
          sub = 'guest:' + crypto.createHash('sha256').update(String(m.guest).slice(0, 80)).digest('hex'); name = 'Visitante';
        } else return send(ws, { t: 'authFail', reason: CLIENT_ID ? 'credencial inválida' : 'servidor sem GOOGLE_CLIENT_ID' });
        const u = await userFor(sub);
        if (c) { online.delete(c.id); }
        const old = online.get(u.id); if (old && old.ws !== ws) { try { old.ws.close(); } catch (e) {} }
        c = { ws, id: u.id, nick: u.nick || '', st: 'online', room: null, sub };
        online.set(c.id, c);
        send(ws, { t: 'authed', id: c.id, name, nick: c.nick }); send(ws, profileMsg(u));
        scheduleBroadcast();
      } catch (e) { send(ws, { t: 'authFail', reason: 'login Google recusado' }); }
      return;
    }
    if (!c) return;

    if (m.t === 'nick') {
      const n = String(m.nick || '').trim().slice(0, 20); if (!n) return;
      c.nick = n; users[c.sub].nick = n; touch(c.sub); scheduleBroadcast();
    } else if (m.t === 'players') { send(ws, { t: 'players', list: [...online.values()].filter(o => o !== c).map(pub) });
    } else if (m.t === 'queue') { if (!c.nick) c.nick = 'Jogador'; enqueue(c);
    } else if (m.t === 'cancel') { dequeue(c);
    } else if (m.t === 'leave') { leaveRoom(c);
    } else if (m.t === 'invite') { const o = online.get(String(m.to)); if (o) send(o.ws, { t: 'invite', from: pub(c) });
    } else if (m.t === 'st' && c.room) {         // estado do personagem (posição, animação, skin...) repassado aos colegas da sala
      const set = rooms.get(c.room); if (!set) return;
      const out = JSON.stringify({ t: 'st', from: c.id, d: m.d });
      for (const id of set) if (id !== c.id) { const o = online.get(id); if (o && o.ws.readyState === 1) o.ws.send(out); }
    }
  });
  ws.on('close', () => { if (!c) return; dequeue(c); leaveRoom(c); if (online.get(c.id) === c) online.delete(c.id); scheduleBroadcast(); });
});
setInterval(() => wss.clients.forEach(w => { if (!w.isAlive) return w.terminate(); w.isAlive = false; w.ping(); }), 30000);

server.listen(PORT, () => console.log('Bermuda Explorer online na porta ' + PORT + (CLIENT_ID ? '' : '  (AVISO: GOOGLE_CLIENT_ID vazio)')));
