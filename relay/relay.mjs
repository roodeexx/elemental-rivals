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

// A plain visit answers too: Render's health check, and the game page waking a sleeping server.
const http = createServer((req, res) => {
  res.writeHead(200, { "Content-Type": "text/plain", "Access-Control-Allow-Origin": "*" });
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
