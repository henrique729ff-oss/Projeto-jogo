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
const MODES = new Set(['glad', 'conf', 'contra']);   // cada modo tem a sua fila (Confrontos não mistura com Gladiadores)
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
const blank = id => ({ id, nick: '', coins: 0, diamonds: 0, items: { skins: [], banners: [] }, friends: [] });
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
const addFriend = (u, id) => { u.friends = u.friends || []; if (!u.friends.includes(id) && u.friends.length < 200) u.friends.push(id); };
async function friendsMsg(u) {   // lista de amigos salvos (com apelido), online ou não
  const list = [];
  for (const fid of (u.friends || []).slice(0, 200)) { const r = await findById(fid); if (r) list.push({ id: fid, nick: r.u.nick || 'Jogador' }); }
  return { t: 'friends', list };
}
const profileMsg = u => ({ t: 'profile', id: u.id, nick: u.nick, coins: u.coins, diamonds: u.diamonds, items: u.items });

// ---------- servidor HTTP (serve /public) ----------
const MIME = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8', '.png': 'image/png', '.jpg': 'image/jpeg', '.webp': 'image/webp', '.glb': 'model/gltf-binary', '.json': 'application/json', '.ico': 'image/x-icon', '.txt': 'text/plain; charset=utf-8' };
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
  // procura em /public e, se não achar, na raiz do projeto (ex.: confronto_map.txt solto ao lado do server.js)
  const rel = path.normalize(url).replace(/^(\.\.[\/\\])+/, '');
  const ext = path.extname(rel).toLowerCase();
  const cands = [path.join(__dirname, 'public', rel)];
  if (MIME[ext] && !/^[\/\\]?(server\.js|package(-lock)?\.json)$/i.test(rel)) cands.push(path.join(__dirname, rel));
  const file = cands.find(p => p.startsWith(__dirname) && fs.existsSync(p) && fs.statSync(p).isFile());
  if (!file) { res.writeHead(404); return res.end('not found'); }
  fs.readFile(file, (err, buf) => {
    if (err) { res.writeHead(404); return res.end('not found'); }
    res.writeHead(200, { 'Content-Type': MIME[path.extname(file)] || 'application/octet-stream', 'Cache-Control': 'public, max-age=3600' });
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

// ---------- EQUIPES (lobby) E FILA ----------
// Solo  : sem amigos no lobby; 1 x 1 contra outro Solo.
// Duo / Duelo : pode chamar 1 amigo pro lobby (equipe de 2). Sempre 2 x 2:
//    equipe de 2 + equipe de 2 | equipe de 2 + dois sozinhos | 4 sozinhos (forma 1+1 e 1+1).
const SUBS = new Set(['solo', 'duo', 'duelo', 'squad']);
const MIX_2V2 = false;          // true = Duo e Duelo compartilham a mesma fila
const poolKey = c => c.mode + ':' + (c.sm === 'solo' ? 'solo' : (MIX_2V2 ? '2v2' : c.sm));
const partyOf = c => c.party || { members: [c], leader: c };
const partyMsg = c => ({ t: 'party', leader: c.party ? c.party.leader.id : c.id, members: c.party ? c.party.members.map(pub) : [], mode: c.party ? c.party.leader.mode : c.mode, sub: c.party ? c.party.leader.sm : c.sm });
function sendParty(p) { for (const m of p.members) send(m.ws, partyMsg(m)); }
function disband(p, why) {
  for (const m of p.members) { m.party = null; send(m.ws, { t: 'party', leader: m.id, members: [] }); if (why) send(m.ws, { t: 'partyEnd', why }); }
}
function leaveParty(c, why) {
  const p = c.party; if (!p) return;
  dequeueParty(p);
  p.members = p.members.filter(m => m !== c); c.party = null; send(c.ws, { t: 'party', leader: c.id, members: [] });
  if (p.members.length <= 1) { disband(p, why); } else { if (p.leader === c) p.leader = p.members[0]; sendParty(p); }
}
function dequeueParty(p) { const ms = p.members || [p]; queue = queue.filter(q => !q.members.some(m => ms.includes(m))); }
function dequeue(c) { dequeueParty(c.party || { members: [c] }); }

function startMatch(teams) {
  const all = teams.flat();
  const room = 'r' + (roomSeq++);
  rooms.set(room, new Set(all.map(c => c.id)));
  queue = queue.filter(q => !q.members.some(m => all.includes(m)));
  teams.forEach((team, ti) => team.forEach((c, i) => {
    const foes = teams[1 - ti], mates = team.filter(x => x !== c);
    c.room = room; c.st = 'playing'; c.team = ti;
    c.pair = (foes[i] || foes[0]).id;   // a engine do jogo mostra 1 adversário por jogador: cada um enfrenta o seu par
    send(c.ws, { t: 'match', room, mode: c.mode, sub: c.sm, bots: false, team: ti, size: team.length, peers: foes.concat(mates).map(pub), foes: foes.map(pub), mates: mates.map(pub) });
  }));
  scheduleBroadcast();
}
function tryMatch(key) {
  const q = queue.filter(e => e.key === key && e.members.every(m => m.ws.readyState === 1 && online.get(m.id) === m && !m.room));
  queue = queue.filter(e => e.key !== key || q.includes(e));
  if (key.endsWith(':solo')) { if (q.length >= 2) startMatch([q[0].members, q[1].members]); return; }
  if (key.endsWith(':duelo')) {   // Duelo = X1: equipe de 2 cai na hora um contra o outro; 2 sozinhos também se enfrentam
    const par = q.find(e => e.members.length === 2);
    if (par) return startMatch([[par.members[0]], [par.members[1]]]);
    const um = q.filter(e => e.members.length === 1);
    if (um.length >= 2) startMatch([um[0].members, um[1].members]);
    return;
  }
  const duos = q.filter(e => e.members.length === 2), ones = q.filter(e => e.members.length === 1);
  if (duos.length >= 2) return startMatch([duos[0].members, duos[1].members]);
  if (duos.length >= 1 && ones.length >= 2) return startMatch([duos[0].members, [ones[0].members[0], ones[1].members[0]]]);
  if (ones.length >= 4) return startMatch([[ones[0].members[0], ones[1].members[0]], [ones[2].members[0], ones[3].members[0]]]);
  // equipe de 2 sem rivais há alguns segundos: os dois entram juntos contra NPCs
  if (duos.length >= 1 && !ones.length && duos.length === 1 && Date.now() - duos[0].t >= FALLBACK_BOTS_MS) return startBotMatch(duos[0].members);
  // só 2 jogadores sozinhos esperando há um tempo: não deixa travado, joga 1 x 1
  if (ones.length >= 2 && !duos.length && Date.now() - ones[1].t >= FALLBACK_1V1_MS) return startMatch([ones[0].members, ones[1].members]);
}
const FALLBACK_BOTS_MS = (+process.env.FALLBACK_BOTS_SECONDS || 12) * 1000;
function startBotMatch(team) {   // equipe junta contra NPCs: cada um recebe o colega em peers e o jogo cria os NPCs adversários
  const room = 'r' + (roomSeq++);
  rooms.set(room, new Set(team.map(c => c.id)));
  queue = queue.filter(q => !q.members.some(m => team.includes(m)));
  for (const c of team) {
    const mate = team.find(x => x !== c);
    c.room = room; c.st = 'playing'; c.team = 0; c.pair = mate.id;
    send(c.ws, { t: 'match', room, mode: c.mode, sub: c.sm, bots: false, vsBots: true, team: 0, size: team.length, peers: [pub(mate)], foes: [], mates: [pub(mate)] });
  }
  scheduleBroadcast();
}
const FALLBACK_1V1_MS = (+process.env.FALLBACK_1V1_SECONDS || 12) * 1000;
setInterval(() => { for (const k of new Set(queue.map(e => e.key))) tryMatch(k); }, 2000);
function enqueue(c) {
  if (c.room) leaveRoom(c);
  const p = partyOf(c);
  if (c.party && c.party.leader !== c) return send(c.ws, { t: 'queueFail', reason: 'Só o dono da equipe inicia a partida' });
  if (c.sm === 'solo' && c.party) leaveParty(c);   // Solo não tem equipe
  const ms = c.party ? c.party.members : [c];
  if (ms.some(m => m.room)) ms.forEach(m => m.room && leaveRoom(m));
  dequeueParty({ members: ms });
  queue.push({ key: poolKey(c), members: ms.slice(), t: Date.now() });
  for (const m of ms) send(m.ws, { t: 'searching', seconds: QUEUE_MS / 1000 });
  tryMatch(poolKey(c));   // espera indefinidamente por jogadores reais, nunca completa com bots
}
function leaveRoom(c) {
  if (!c.room) return;
  const set = rooms.get(c.room);
  if (set) { set.delete(c.id); for (const id of set) { const o = online.get(id); if (o) send(o.ws, { t: 'left', id: c.id }); } if (!set.size) rooms.delete(c.room); }
  c.room = null; c.pair = null; c.st = 'online'; scheduleBroadcast();
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
        c = { ws, id: u.id, nick: u.nick || '', st: 'online', room: null, sub, mode: 'glad', sm: 'solo', party: null };
        online.set(c.id, c);
        send(ws, { t: 'authed', id: c.id, name, nick: c.nick }); send(ws, profileMsg(u));
        send(ws, await friendsMsg(u));
        scheduleBroadcast();
      } catch (e) { send(ws, { t: 'authFail', reason: 'login Google recusado' }); }
      return;
    }
    if (!c) return;

    if (m.t === 'nick') {
      const n = String(m.nick || '').trim().slice(0, 20); if (!n) return;
      c.nick = n; users[c.sub].nick = n; touch(c.sub); scheduleBroadcast();
    } else if (m.t === 'friendsSync' && Array.isArray(m.ids)) {   // traz para o servidor os amigos que já estavam salvos só no celular
      const u = users[c.sub]; if (!u) return; let ch = false;
      for (const x of m.ids.slice(0, 200)) { const id = String(x); if (id === c.id || (u.friends || []).includes(id)) continue; const r = await findById(id); if (r) { addFriend(u, id); ch = true; } }
      if (ch) touch(c.sub);
      send(ws, await friendsMsg(u));
    } else if (m.t === 'players') { send(ws, { t: 'players', list: [...online.values()].filter(o => o !== c).map(pub) });
    } else if (m.t === 'mode') {   // o lobby avisa o modo/submodo escolhido (Solo / Duo / Duelo)
      const nm = MODES.has(m.mode) ? m.mode : 'glad', ns = SUBS.has(m.sub) ? m.sub : 'solo';
      if (nm !== c.mode || ns !== c.sm) {
        c.mode = nm; c.sm = ns;
        if (c.party) { if (ns === 'solo' && c.party.leader === c) disband(c.party, 'Solo não permite equipe'); else if (c.party.leader === c) { dequeueParty(c.party); c.party.members.forEach(x => { if (x !== c) { x.mode = nm; x.sm = ns; } }); sendParty(c.party); } else leaveParty(c); }
        dequeue(c);
      }
    } else if (m.t === 'queue') {
      if (!c.nick) c.nick = 'Jogador';
      c.mode = MODES.has(m.mode) ? m.mode : 'glad'; c.sm = SUBS.has(m.sub) ? m.sub : (c.sm || 'solo');
      if (c.party && c.party.leader === c) c.party.members.forEach(x => { x.mode = c.mode; x.sm = c.sm; });
      enqueue(c);
    } else if (m.t === 'cancel') { dequeue(c);
    } else if (m.t === 'partyLeave') { leaveParty(c, 'Você saiu da equipe'); 
    } else if (m.t === 'leave') { leaveRoom(c);
    } else if (m.t === 'invite') {   // pedido de amizade ('friend') ou chamada pra equipe do lobby ('x1')
      const kind = m.kind === 'friend' ? 'friend' : 'x1';
      const o = online.get(String(m.to)); if (!o || o === c) return;
      const no = msg => send(ws, { t: 'inviteReply', kind, ok: false, msg, from: pub(o) });
      if (kind === 'x1') {
        if (c.sm === 'solo' || !c.sm) return no('No Solo não dá pra chamar amigo');
        if (c.party && c.party.leader !== c) return no('Só o dono da equipe chama');
        if (c.party && c.party.members.length >= 2) return no('Equipe cheia (máx. 2)');
        if (queue.some(q => q.members.includes(c))) return no('Cancele a busca antes de chamar');
        if (o.room) return no('Está em partida');
        if (o.party) return no('Já está em outra equipe');
      }
      send(o.ws, { t: 'invite', kind, from: pub(c) });
    } else if (m.t === 'inviteReply') {   // resposta de quem recebeu: repassa para quem convidou
      const kind = m.kind === 'friend' ? 'friend' : 'x1';
      const o = online.get(String(m.to)); if (!o || o === c) return;
      send(o.ws, { t: 'inviteReply', kind, ok: !!m.ok, msg: String(m.msg || '').slice(0, 80), from: pub(c) });
      if (kind === 'friend' && m.ok) {   // amizade aceita: salva nos dois jogadores
        const a = users[c.sub], b = users[o.sub];
        if (a && b) { addFriend(a, o.id); addFriend(b, c.id); touch(c.sub); touch(o.sub); friendsMsg(a).then(x => send(ws, x)); friendsMsg(b).then(x => send(o.ws, x)); }
      }
      if (kind === 'x1' && m.ok) {   // aceitou: entra na equipe do lobby de quem chamou (NÃO inicia a partida)
        if (!o.sm || o.sm === 'solo' || c.room || o.room || c.party || (o.party && o.party.members.length >= 2)) return;
        dequeue(c); dequeue(o);
        const p = o.party || (o.party = { leader: o, members: [o] });
        p.members.push(c); c.party = p; c.mode = o.mode; c.sm = o.sm;
        sendParty(p);
      }
    } else if (m.t === 'st' && c.room) {         // estado do personagem (posição, animação, skin...) repassado aos colegas da sala
      const set = rooms.get(c.room); if (!set) return;
      const o = online.get(c.pair); if (o && o.room === c.room && o.ws.readyState === 1) o.ws.send(JSON.stringify({ t: 'st', from: c.id, d: m.d }));   // só o adversário pareado (a engine mostra 1 por vez)
    }
  });
  ws.on('close', () => { if (!c) return; dequeue(c); leaveParty(c, 'Seu amigo saiu'); leaveRoom(c); if (online.get(c.id) === c) online.delete(c.id); scheduleBroadcast(); });
});
setInterval(() => wss.clients.forEach(w => { if (!w.isAlive) return w.terminate(); w.isAlive = false; w.ping(); }), 30000);

server.listen(PORT, () => console.log('Bermuda Explorer online na porta ' + PORT + (CLIENT_ID ? '' : '  (AVISO: GOOGLE_CLIENT_ID vazio)')));
