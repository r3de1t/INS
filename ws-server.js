// Aurora server: WebSocket-комнаты + выдача токенов LiveKit (GET /token). Без истории и авторизации (in-memory).
import 'dotenv/config';
import http from 'http'
import crypto from 'crypto'
import { WebSocketServer } from 'ws'
import { AccessToken } from 'livekit-server-sdk'
const PORT = process.env.PORT || 8080;
const KEY = process.env.LIVEKIT_API_KEY;
const SECRET = process.env.LIVEKIT_API_SECRET;
const cors = { 'Access-Control-Allow-Origin': '*' }
const server = http.createServer(async (req, res) => {
  const u = new URL(req.url, 'http://x')
  if (u.pathname === '/token') {
    const room = u.searchParams.get('room'), name = u.searchParams.get('name')
    if (!KEY || !SECRET) { res.writeHead(500, cors); return res.end('{"error":"LIVEKIT keys not set"}') }
    if (!room || !name) { res.writeHead(400, cors); return res.end('{"error":"room and name required"}') }
    const at = new AccessToken(KEY, SECRET, { identity: name, ttl: '2h' })
    at.addGrant({ roomJoin: true, room, canPublish: true, canSubscribe: true })
    res.writeHead(200, { ...cors, 'Content-Type': 'application/json' })
    return res.end(JSON.stringify({ token: await at.toJwt() }))
  }
  res.writeHead(200, cors); res.end('aurora ok')
})
const wss = new WebSocketServer({ server }); const rooms = new Map(); const names = new Map(); const avatars = new Map() // юзернеймы (в памяти, сбрасываются при перезапуске)
// ---- Лобби совместного просмотра. Пароль проверяется на сервере (scrypt), в список он не попадает ----
const lobbies = new Map()
const hashPw = (pw, salt) => crypto.scryptSync(String(pw), salt, 32).toString('hex')
const okPw = (l, pw) => { const a = Buffer.from(hashPw(pw ?? '', l.salt)), b = Buffer.from(l.hash); return a.length === b.length && crypto.timingSafeEqual(a, b) }
const pub = l => ({ id: l.id, name: l.name, priv: !!l.hash, count: l.members.size, owner: l.owner })
const broadcastLobbies = () => { const m = JSON.stringify({ t: 'lobby_list', rooms: [...lobbies.values()].map(pub) }); wss.clients.forEach(c => c.readyState === 1 && c.send(m)) }
const enter = (ws, l) => { l.members.add(ws); ws.lobbies.add(l.id); for (const r of ['watch:' + l.id, 'watchchat:' + l.id]) { if (!rooms.has(r)) rooms.set(r, new Set()); rooms.get(r).add(ws); ws.rooms.add(r) } }
const exit = (ws, id) => {
  const l = lobbies.get(id); if (!l) return
  l.members.delete(ws); ws.lobbies.delete(id)
  for (const r of ['watch:' + id, 'watchchat:' + id]) { rooms.get(r)?.delete(ws); ws.rooms.delete(r) }
  if (!l.members.size) lobbies.delete(id)
  broadcastLobbies()
}
wss.on('connection', ws => {
  ws.rooms = new Set(); ws.lobbies = new Set()
  ws.on('message', raw => {
    let d; try { d = JSON.parse(raw) } catch { return }
    if (d.t === 'username_set') {
      const n = String(d.name), k = n.toLowerCase(), ok = /^[A-Za-z0-9_]{3,32}$/.test(n) && (!names.has(k) || names.get(k) === ws)
      if (ok) { if (ws.user) names.delete(ws.user.toLowerCase()); names.set(k, ws); ws.user = n }
      return ws.send(JSON.stringify({ t: 'username_result', name: n, ok }))
    }
    if (d.t === 'join' && !/^watch(chat)?:/.test(String(d.room))) { /* каналы просмотра — только через lobby_join */ if (!rooms.has(d.room)) rooms.set(d.room, new Set()); rooms.get(d.room).add(ws); ws.rooms.add(d.room) }
    if (d.t === 'lobby_list') return ws.send(JSON.stringify({ t: 'lobby_list', rooms: [...lobbies.values()].map(pub) }))
    if (d.t === 'lobby_create') {
      const name = String(d.name || '').trim().slice(0, 60); if (!name) return
      const id = crypto.randomBytes(4).toString('hex'); const l = { id, name, owner: ws.user || String(d.owner || '').slice(0, 40), members: new Set() }
      if (d.password) { l.salt = crypto.randomBytes(8).toString('hex'); l.hash = hashPw(d.password, l.salt) }
      lobbies.set(id, l); enter(ws, l); ws.send(JSON.stringify({ t: 'lobby_join_result', ok: true, id, name })); return broadcastLobbies()
    }
    if (d.t === 'lobby_join') {
      const l = lobbies.get(String(d.id)); if (!l) return ws.send(JSON.stringify({ t: 'lobby_join_result', ok: false, error: 'Комната не найдена' }))
      if (l.hash && !l.members.has(ws) && !okPw(l, d.password)) return ws.send(JSON.stringify({ t: 'lobby_join_result', ok: false, id: l.id, error: 'Неверный пароль' }))
      enter(ws, l); ws.send(JSON.stringify({ t: 'lobby_join_result', ok: true, id: l.id, name: l.name })); return broadcastLobbies()
    }
    if (d.t === 'lobby_leave') return exit(ws, String(d.id))
    // Аватар: хранится в памяти по юзернейму (нужен занятый юзернейм, до ~2 МБ)
    if (d.t === 'avatar' && ws.user && typeof d.data === 'string' && d.data.length < 3_000_000) { avatars.set(ws.user.toLowerCase(), d.data); return ws.send(JSON.stringify({ t: 'avatar_ok' })) }
    // Совместный просмотр: события play/pause/seek/url пересылаются остальным участникам комнаты
    if (d.t === 'watch' && ws.rooms.has(d.room)) {
      const out = JSON.stringify({ t: 'watch', room: d.room, from: ws.user || String(d.from || '').slice(0, 40), ev: d.ev })
      rooms.get(d.room).forEach(c => c !== ws && c.readyState === 1 && c.send(out))
    }
    if (d.t === 'msg' && ws.rooms.has(d.room)) {
      const out = JSON.stringify({ t: 'msg', room: d.room, from: String(d.from).slice(0, 40), user: ws.user || '', text: String(d.text).slice(0, 4000), ts: Date.now() })
      rooms.get(d.room).forEach(c => c !== ws && c.readyState === 1 && c.send(out))
    }
  })
  ws.on('close', () => { if (ws.user && names.get(ws.user.toLowerCase()) === ws) names.delete(ws.user.toLowerCase()); ws.rooms.forEach(r => rooms.get(r)?.delete(ws)); [...ws.lobbies].forEach(id => exit(ws, id)) })
})
server.listen(PORT, () => console.log('listening on', PORT))
