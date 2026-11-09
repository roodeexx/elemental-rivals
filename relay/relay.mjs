// Room relay for up to 10 players: node relay.mjs (locally, and on Render).
// Client -> {"t":"create"} | {"t":"join","code":"ABCD"} | {"t":"quick"} | anything else = game message.
// Game messages are broadcast to the rest of the room wrapped as {"f":<senderId>,"m":<msg>}.
import { createServer } from "node:http";
import { WebSocketServer } from "ws";

const PORT = process.env.PORT || 9080;
const MAX_PLAYERS = 10;
const rooms = new Map(); // code -> {clients: Map(id->ws), next, started}

function send(ws, obj) {
  if (ws.readyState === 1) ws.send(JSON.stringify(obj));
}
function bcast(room, text, exceptWs) {
  for (const ws of room.clients.values()) {
    if (ws !== exceptWs && ws.readyState === 1) ws.send(text);
  }
}
function roster(room) {
  return JSON.stringify({ t: "roster", ids: [...room.clients.keys()] });
}
// A public room's server number (#1, #2, ...): the smallest one free, so friends can say
// "join server #1" (the user's ask); a number comes free again when its room closes.
function freeNum() {
  const used = new Set([...rooms.values()].map((r) => r.num));
  let n = 1;
  while (used.has(n)) n++;
  return n;
}
// public: listed for Quick Play. quick: tells the new host to start at once (with bots).
function create(ws, isPublic) {
  let code;
  do {
    code = Array.from({ length: 4 }, () => "ABCDEFGHJKLMNPQRSTUVWXYZ"[(Math.random() * 24) | 0]).join("");
  } while (rooms.has(code));
  ws.room = code;
  ws.id = 1;
  rooms.set(code, { clients: new Map([[1, ws]]), next: 2, started: false, public: isPublic, host: 1, num: isPublic ? freeNum() : 0 });
  send(ws, { t: "room", code, id: 1, quick: isPublic, host: 1, num: rooms.get(code).num });
  send(ws, { t: "roster", ids: [1] });
}
// Joining a running match is allowed (live sessions): "started" tells the newcomer to go
// straight into the game, where they wait for the round to end.
function join(ws, code, room) {
  ws.room = code;
  ws.id = room.next++;
  room.clients.set(ws.id, ws);
  send(ws, { t: "room", code, id: ws.id, started: room.started, host: room.host, num: room.num });
  bcast(room, roster(room));
}

// Reports (the user's ask): POST /report with a game's report - a part every 5 min of play,
// at a match's end, and the last part / crash report at the next start (JSON, sent
// as text/plain so browsers skip the CORS preflight). Saved as a file in a PRIVATE GitHub repo
// (REPORTS_REPO, written with GITHUB_TOKEN, set in Render's dashboard - never in this code), or,
// for local tests, into REPORTS_DIR. Nothing else is kept: no IP, no headers, nothing typed.
const REPORTS_REPO = process.env.REPORTS_REPO || "roodeexx/elemental-rivals-reports";
const REPORTS_DIR = process.env.REPORTS_DIR || "";
const MAX_REPORT = 2 * 1024 * 1024;  // bytes; a 5-minute part is ~0.5 MB
const PER_HOUR = 60;             // reports filed per hour per internet connection (a part every 5 min for a few friends at one home); stops floods
const sent = new Map();          // sender -> [times], in memory only, cleared hourly
setInterval(() => sent.clear(), 3600 * 1000).unref();

async function storeReport(text, report) {
  const now = new Date();
  const time = now.toISOString().slice(11, 19).replaceAll(":", "");
  // one session's parts sit side by side: <session>-part<N>-<time>.json (ids checked, never trusted as paths)
  const sid = typeof report.session === "string" && /^[a-z0-9]{8}$/.test(report.session) ? report.session : "";
  const n = Number.isInteger(report.part) && report.part > 0 && report.part < 1000 ? report.part : 0;
  const file = sid ? `${sid}-part${n}-${time}` : `${time}-${Math.random().toString(36).slice(2, 8)}`;
  const name = `reports/${now.toISOString().slice(0, 10)}/${file}.json`;
  if (REPORTS_DIR) {
    const { mkdir, writeFile } = await import("node:fs/promises");
    const path = `${REPORTS_DIR}/${name}`;
    await mkdir(path.slice(0, path.lastIndexOf("/")), { recursive: true });
    await writeFile(path, text);
    return true;
  }
  if (!process.env.GITHUB_TOKEN) return false;
  const r = await fetch(`https://api.github.com/repos/${REPORTS_REPO}/contents/${name}`, {
    method: "PUT",
    headers: { Authorization: `Bearer ${process.env.GITHUB_TOKEN}`, Accept: "application/vnd.github+json", "User-Agent": "elemental-rivals-relay" },
    body: JSON.stringify({ message: "crash report", content: Buffer.from(text).toString("base64") }),
  });
  return r.ok;
}

function handleReport(req, res) {
  const who = req.socket.remoteAddress || "?";  // only to count floods, never stored
  const times = sent.get(who) || [];
  if (times.length >= PER_HOUR) { res.writeHead(429, CORS); return res.end("too many reports"); }
  let size = 0;
  const parts = [];
  req.on("data", (c) => {
    size += c.length;
    if (size > MAX_REPORT) { res.writeHead(413, CORS); res.end("too big"); req.destroy(); }
    else parts.push(c);
  });
  req.on("end", async () => {
    if (size > MAX_REPORT) return;
    const text = Buffer.concat(parts).toString("utf8");
    let report;
    try { report = JSON.parse(text); } catch { res.writeHead(400, CORS); return res.end("not json"); }
    if (!report || typeof report !== "object" || !report.how_it_ended) { res.writeHead(400, CORS); return res.end("not a report"); }
    try {
      const ok = await storeReport(JSON.stringify(report, null, 1), report);
      if (ok) { times.push(Date.now()); sent.set(who, times); }  // only filed reports count toward the limit
      res.writeHead(ok ? 201 : 503, CORS);
      res.end(ok ? "saved" : "reports are not set up on this relay");
    } catch {
      res.writeHead(502, CORS);
      res.end("could not save");
    }
  });
}
const CORS = { "Content-Type": "text/plain", "Access-Control-Allow-Origin": "*" };

// A plain visit answers too: Render's health check, and the game page waking a sleeping server.
const http = createServer((req, res) => {
  if (req.method === "POST" && req.url === "/report") return handleReport(req, res);
  if (req.url === "/rooms") {
    // the lobby's "live now" line: how many public games run and how many play in them (counts only)
    const live = [...rooms.values()].filter((r) => r.public);
    res.writeHead(200, { ...CORS, "Content-Type": "application/json", "Cache-Control": "no-store" });
    // list: each public server by its number, to pick one in the lobby (counts and its code only)
    const list = [...rooms.entries()].filter(([, r]) => r.public).sort((a, b) => a[1].num - b[1].num)
      .map(([code, r]) => ({ num: r.num, code, players: r.clients.size, bots: r.bots || 0, started: r.started, full: r.clients.size >= MAX_PLAYERS }));
    return res.end(JSON.stringify({ games: live.length, players: live.reduce((n, r) => n + r.clients.size, 0),
      bots: live.reduce((n, r) => n + (r.bots || 0), 0),
      open: live.filter((r) => r.clients.size < MAX_PLAYERS).length, list }));
  }
  res.writeHead(200, CORS);
  res.end("Elemental Rivals relay is running.");
}).listen(PORT);

const wss = new WebSocketServer({ server: http });
// Heartbeat (the user's catch: games listed with players who were long gone): every 15 s each
// connection must answer a ping (browsers answer by themselves, even from a background tab);
// one that doesn't is dropped, which takes its player out of the room - and an empty room closes.
setInterval(() => {
  for (const ws of wss.clients) {
    if (ws.alive === false) { ws.terminate(); continue; }
    ws.alive = false;
    ws.ping();
  }
}, 15000).unref();
wss.on("connection", (ws) => {
  ws.alive = true;
  ws.on("pong", () => { ws.alive = true; });
  ws.on("message", (raw, isBinary) => {
    if (isBinary) return;
    raw = raw.toString();
    if (raw.startsWith('{"t":"ping"')) {  // the game's ping: answered at once, never broadcast
      if (ws.readyState === 1) ws.send(raw.replace('"ping"', '"pong"'));
      return;
    }
    if (ws.room) {
      const room = rooms.get(ws.room);
      if (!room) return;
      // shortcut: detects match start by substring, not JSON parse; fine while only the host sends "begin"
      if (raw.includes('"begin"')) room.started = true;
      if (ws.id === room.host && raw.includes('"meta"')) {  // the host's bot count, for the lobby's "live games" line
        try { const m = JSON.parse(raw); if (m.t === "meta") room.bots = Math.max(0, Math.min(8, m.bots | 0)); } catch {}
      }
      bcast(room, `{"f":${ws.id},"m":${raw}}`, ws);
      return;
    }
    let m;
    try { m = JSON.parse(raw); } catch { return; }
    if (m.t === "create") {
      create(ws, false);
    } else if (m.t === "join") {
      const code = String(m.code || "").toUpperCase();
      const room = rooms.get(code);
      if (!room) return send(ws, { t: "err", msg: "no such room" });
      if (room.clients.size >= MAX_PLAYERS) return send(ws, { t: "err", msg: "room is full" });
      join(ws, code, room);
    } else if (m.t === "new") {
      create(ws, true);  // "New game": always my own public game (with my bots), others can drop in
    } else if (m.t === "quick") {
      // Quick Play (the user's ask): join the fullest public game with a free spot - only
      // join; with none running it says so (New game starts one)
      let best = null;
      for (const [code, room] of rooms) {
        if (room.public && room.clients.size < MAX_PLAYERS && (!best || room.clients.size > best[1].clients.size)) best = [code, room];
      }
      if (best) join(ws, best[0], best[1]);
      else send(ws, { t: "err", msg: "no live games right now" });
    }
  });
  ws.on("close", () => {
    const room = rooms.get(ws.room);
    if (!room) return;
    // the room lives on while anyone is in it (the user's ask): a host who leaves hands
    // the game to the player who has been in it longest (the lowest id)
    room.clients.delete(ws.id);
    if (room.clients.size === 0) {
      rooms.delete(ws.room);
      return;
    }
    bcast(room, `{"t":"left","id":${ws.id}}`);
    if (ws.id === room.host) {
      room.host = Math.min(...room.clients.keys());
      room.bots = 0;  // the old host's bots ran on its machine
      bcast(room, `{"t":"host","id":${room.host}}`);
    }
    bcast(room, roster(room));
  });
});
console.log("relay listening on ws://localhost:" + PORT);
