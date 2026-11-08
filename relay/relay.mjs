// Room relay for up to 10 players: node relay.mjs (locally, and on Render).
// Client -> {"t":"create"} | {"t":"join","code":"ABCD"} | anything else = game message.
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

// Crash reports (the user's ask): POST /report with a game's crash/freeze report (JSON, sent
// as text/plain so browsers skip the CORS preflight). Saved as a file in a PRIVATE GitHub repo
// (REPORTS_REPO, written with GITHUB_TOKEN, set in Render's dashboard - never in this code), or,
// for local tests, into REPORTS_DIR. Nothing else is kept: no IP, no headers, nothing typed.
const REPORTS_REPO = process.env.REPORTS_REPO || "roodeexx/elemental-rivals-reports";
const REPORTS_DIR = process.env.REPORTS_DIR || "";
const MAX_REPORT = 600 * 1024;   // bytes; a full report is well under this
const PER_HOUR = 30;             // reports filed per hour per internet connection (friends at one home share one); stops floods
const sent = new Map();          // sender -> [times], in memory only, cleared hourly
setInterval(() => sent.clear(), 3600 * 1000).unref();

async function storeReport(text) {
  const now = new Date();
  const name = `reports/${now.toISOString().slice(0, 10)}/${now.toISOString().slice(11, 19).replaceAll(":", "")}-${Math.random().toString(36).slice(2, 8)}.json`;
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
      const ok = await storeReport(JSON.stringify(report, null, 1));
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
  res.writeHead(200, CORS);
  res.end("Elemental Rivals relay is running.");
}).listen(PORT);

new WebSocketServer({ server: http }).on("connection", (ws) => {
  ws.on("message", (raw, isBinary) => {
    if (isBinary) return;
    raw = raw.toString();
    if (ws.room) {
      const room = rooms.get(ws.room);
      if (!room) return;
      // shortcut: detects match start by substring, not JSON parse; fine while only the host sends "begin"
      if (raw.includes('"begin"')) room.started = true;
      bcast(room, `{"f":${ws.id},"m":${raw}}`, ws);
      return;
    }
    let m;
    try { m = JSON.parse(raw); } catch { return; }
    if (m.t === "create") {
      let code;
      do {
        code = Array.from({ length: 4 }, () => "ABCDEFGHJKLMNPQRSTUVWXYZ"[(Math.random() * 24) | 0]).join("");
      } while (rooms.has(code));
      ws.room = code;
      ws.id = 1;
      rooms.set(code, { clients: new Map([[1, ws]]), next: 2, started: false });
      send(ws, { t: "room", code, id: 1 });
      send(ws, { t: "roster", ids: [1] });
    } else if (m.t === "join") {
      const code = String(m.code || "").toUpperCase();
      const room = rooms.get(code);
      if (!room) return send(ws, { t: "err", msg: "no such room" });
      if (room.started) return send(ws, { t: "err", msg: "match already started" });
      if (room.clients.size >= MAX_PLAYERS) return send(ws, { t: "err", msg: "room is full" });
      ws.room = code;
      ws.id = room.next++;
      room.clients.set(ws.id, ws);
      send(ws, { t: "room", code, id: ws.id });
      bcast(room, roster(room));
    }
  });
  ws.on("close", () => {
    const room = rooms.get(ws.room);
    if (!room) return;
    if (ws.id === 1) {
      rooms.delete(ws.room);
      bcast(room, '{"t":"host_left"}', ws);
      return;
    }
    room.clients.delete(ws.id);
    bcast(room, `{"t":"left","id":${ws.id}}`);
    bcast(room, roster(room));
  });
});
console.log("relay listening on ws://localhost:" + PORT);
