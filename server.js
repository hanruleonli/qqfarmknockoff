const path = require('path');
const http = require('http');
const fs = require('fs');
const WebSocket = require('ws');

const PORT = process.env.PORT ? Number(process.env.PORT) : 8080;
const INDEX_PATH = path.join(__dirname, 'index.html');
const STATIC_DIR = __dirname;

const server = http.createServer((req, res) => {
  try {
    let filePath = path.join(STATIC_DIR, req.url === '/' ? 'index.html' : req.url.split('?')[0]);
    if (!filePath.startsWith(STATIC_DIR)) {
      res.writeHead(403);
      return res.end('Forbidden');
    }
    if (fs.existsSync(filePath) && fs.statSync(filePath).isDirectory()) {
      filePath = INDEX_PATH;
    }
    if (!fs.existsSync(filePath)) {
      res.writeHead(404);
      return res.end('Not Found');
    }
    const ext = path.extname(filePath).toLowerCase();
    const contentType = ext === '.js' ? 'application/javascript' : ext === '.css' ? 'text/css' : ext === '.html' ? 'text/html' : 'application/octet-stream';
    res.writeHead(200, { 'Content-Type': contentType });
    fs.createReadStream(filePath).pipe(res);
  } catch (err) {
    res.writeHead(500);
    res.end('Server error');
  }
});

const wss = new WebSocket.Server({ server });
// Shared state in memory. All clients connected here can sync.
let sharedState = {};
const clientByGame = new Map();

function mergeState(localState, incomingState) {
  if (!incomingState || typeof incomingState !== 'object') return localState;
  const merged = { ...localState };
  merged.players = { ...localState.players, ...incomingState.players };
  merged.steals = { ...localState.steals, ...incomingState.steals };
  merged.battles = { ...localState.battles, ...incomingState.battles };
  merged.userAuth = { userHashes: { ...(localState.userAuth && localState.userAuth.userHashes ? localState.userAuth.userHashes : {}), ...(incomingState.userAuth && incomingState.userAuth.userHashes ? incomingState.userAuth.userHashes : {}) } };
  merged.admin = { pendingSignups: Array.isArray(localState.admin && localState.admin.pendingSignups ? localState.admin.pendingSignups : []) };
  if (Array.isArray(incomingState.admin && incomingState.admin.pendingSignups ? incomingState.admin.pendingSignups : [])) {
    incomingState.admin.pendingSignups.forEach((item) => {
      if (!merged.admin.pendingSignups.some((r) => r.id === item.id)) merged.admin.pendingSignups.push(item);
    });
  }
  merged.admin.adminId = incomingState.admin && incomingState.admin.adminId ? incomingState.admin.adminId : localState.admin && localState.admin.adminId;
  merged.weather = (incomingState.weather && incomingState.weather.setAt > (localState.weather && localState.weather.setAt ? localState.weather.setAt : 0)) ? incomingState.weather : localState.weather;
  return merged;
}

function sendToClient(ws, data) {
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(data));
  }
}

wss.on('connection', (ws) => {
  let subscribedGameId = null;

  ws.on('message', (message) => {
    let data;
    try { data = JSON.parse(message); } catch (_) { return; }
    if (!data || typeof data !== 'object') return;
    if (data.type === 'hello' && data.gameId) {
      subscribedGameId = data.gameId;
      clientByGame.set(ws, subscribedGameId);
      if (!sharedState[subscribedGameId]) {
        sharedState[subscribedGameId] = { players: {}, steals: {}, battles: {}, userAuth: { userHashes: {} }, admin: { pendingSignups: [], adminId: '' }, weather: { setAt: Date.now() } };
      }
      sendToClient(ws, { type: 'state', gameId: subscribedGameId, state: sharedState[subscribedGameId] });
    } else if (data.type === 'update' && data.gameId) {
      const gameId = data.gameId;
      if (!sharedState[gameId]) {
        sharedState[gameId] = { players: {}, steals: {}, battles: {}, userAuth: { userHashes: {} }, admin: { pendingSignups: [], adminId: '' }, weather: { setAt: Date.now() } };
      }
      sharedState[gameId] = mergeState(sharedState[gameId], data.state || {});
      // broadcast updated state to all clients in the same game room
      wss.clients.forEach((client) => {
        if (client.readyState !== WebSocket.OPEN) return;
        const clientGameId = clientByGame.get(client);
        if (clientGameId === gameId) {
          sendToClient(client, { type: 'state', gameId, state: sharedState[gameId] });
        }
      });
    }
  });

  ws.on('close', () => {
    clientByGame.delete(ws);
  });
});

server.listen(PORT, () => {
  console.log(`Server listening on http://localhost:${PORT}`);
});
