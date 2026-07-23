const path = require('path');
const http = require('http');
const fs = require('fs');
const WebSocket = require('ws');

const PORT = process.env.PORT ? Number(process.env.PORT) : 8080;
const ROOT_DIR = path.join(__dirname, '..');
const INDEX_PATH = path.join(ROOT_DIR, 'index.html');
const STATIC_DIR = ROOT_DIR;
const STATE_FILE = path.join(__dirname, 'server-state.json');

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
// Shared state is persisted to disk so the same game room stays available
// across device reconnects and server restarts.
let sharedState = {};
const clientByGame = new Map();

function getDefaultGameState() {
  return {
    players: {},
    steals: {},
    battles: {},
    userAuth: { userHashes: {} },
    admin: { pendingSignups: [], adminId: '' },
    weather: { setAt: Date.now() }
  };
}

function loadPersistedState() {
  try {
    if (!fs.existsSync(STATE_FILE)) return {};
    const raw = fs.readFileSync(STATE_FILE, 'utf8');
    if (!raw.trim()) return {};
    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== 'object') return {};
    return parsed;
  } catch (_) {
    return {};
  }
}

function persistSharedState() {
  try {
    fs.writeFileSync(STATE_FILE, JSON.stringify(sharedState, null, 2));
  } catch (err) {
    console.error('Failed to persist shared state:', err);
  }
}

sharedState = loadPersistedState();

function ensureGameState(gameId) {
  if (!sharedState[gameId]) {
    sharedState[gameId] = getDefaultGameState();
  }
  return sharedState[gameId];
}

function mergeState(localState, incomingState) {
  if (!incomingState || typeof incomingState !== 'object') return localState;
  const merged = { ...localState };
  merged.players = { ...localState.players, ...incomingState.players };
  merged.steals = { ...localState.steals, ...incomingState.steals };
  merged.battles = { ...localState.battles, ...incomingState.battles };
  merged.userAuth = { userHashes: { ...(localState.userAuth && localState.userAuth.userHashes ? localState.userAuth.userHashes : {}), ...(incomingState.userAuth && incomingState.userAuth.userHashes ? incomingState.userAuth.userHashes : {}) } };

  // FIX: the parens around Array.isArray(...) previously wrapped the whole
  // ternary, so this evaluated to a boolean (true/false) instead of an
  // array. That made `merged.admin.pendingSignups.some(...)` below throw
  // "not a function", crashing the server on any update carrying admin
  // signup data. Now Array.isArray() only checks the local value, and the
  // ternary picks the array (or falls back to []).
  const localPendingSignups = localState.admin && localState.admin.pendingSignups;
  merged.admin = { pendingSignups: Array.isArray(localPendingSignups) ? localPendingSignups : [] };

  const incomingPendingSignups = incomingState.admin && incomingState.admin.pendingSignups;
  if (Array.isArray(incomingPendingSignups)) {
    incomingPendingSignups.forEach((item) => {
      if (!merged.admin.pendingSignups.some((r) => r.id === item.id)) merged.admin.pendingSignups.push(item);
    });
  }

  merged.admin.adminId = (incomingState.admin && incomingState.admin.adminId) ? incomingState.admin.adminId : (localState.admin && localState.admin.adminId);
  merged.weather = (incomingState.weather && incomingState.weather.setAt > (localState.weather && localState.weather.setAt ? localState.weather.setAt : 0)) ? incomingState.weather : localState.weather;
  return merged;
}

function sendToClient(ws, data) {
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(data));
  }
}

function broadcastGameState(gameId) {
  const state = ensureGameState(gameId);
  wss.clients.forEach((client) => {
    if (client.readyState !== WebSocket.OPEN) return;
    const clientGameId = clientByGame.get(client);
    if (clientGameId === gameId) {
      sendToClient(client, { type: 'state', gameId, state });
    }
  });
}

wss.on('connection', (ws) => {
  let subscribedGameId = null;

  ws.on('message', (message) => {
    try {
      let data;
      try { data = JSON.parse(message); } catch (_) { return; }
      if (!data || typeof data !== 'object') return;
      if (data.type === 'hello' && data.gameId) {
        subscribedGameId = data.gameId;
        clientByGame.set(ws, subscribedGameId);
        const gameState = ensureGameState(subscribedGameId);
        sendToClient(ws, { type: 'state', gameId: subscribedGameId, state: gameState });
      } else if (data.type === 'update' && data.gameId) {
        const gameId = data.gameId;
        const gameState = ensureGameState(gameId);
        sharedState[gameId] = mergeState(gameState, data.state || {});
        persistSharedState();
        broadcastGameState(gameId);
      }
    } catch (err) {
      // FIX: mergeState (and anything else in this handler) was previously
      // unguarded. A synchronous throw in a ws 'message' handler is an
      // uncaught exception that kills the whole Node process, taking down
      // every connected player, not just the one that sent the bad message.
      console.error('Error handling message:', err);
    }
  });

  ws.on('close', () => {
    clientByGame.delete(ws);
  });

  ws.on('error', (err) => {
    console.error('WebSocket client error:', err);
  });
});

// FIX: without these, any uncaught error/rejection anywhere in the process
// (not just inside a ws handler) crashes the server outright and disconnects
// every player in every game room.
process.on('uncaughtException', (err) => {
  console.error('Uncaught exception:', err);
});
process.on('unhandledRejection', (err) => {
  console.error('Unhandled rejection:', err);
});

server.listen(PORT, "0.0.0.0", () => {
  console.log(`Server running on port ${PORT}`);
});