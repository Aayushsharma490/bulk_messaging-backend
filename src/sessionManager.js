const { Client, LocalAuth } = require('whatsapp-web.js');
const QRCode = require('qrcode');
const path = require('path');
const fs = require('fs').promises;
const db = require('./db');

const clients = {};
let socketIo = null;

const AUTH_PATH = path.join(__dirname, '..', '.wwebjs_auth');

function setSocketIo(io) {
  socketIo = io;
}

// Broadcast helper
function emitToSocket(event, data) {
  if (socketIo) {
    socketIo.emit(event, data);
  }
}

// Get client by session ID
function getClient(sessionId) {
  return clients[sessionId];
}

// Check if a client is ready
function isClientReady(sessionId) {
  const client = clients[sessionId];
  return client && client.info && client.info.wid;
}

// Initialize all saved sessions from DB on startup
// Initialize all saved sessions from DB on startup (only restore active CONNECTED ones to save memory)
async function initSessions() {
  const sessions = await db.getSessions();
  for (const session of sessions) {
    if (session.status === 'CONNECTED') {
      console.log(`Restoring active session: ${session.name} (${session.id})`);
      await startSession(session.id);
    } else {
      console.log(`Skipping restore for offline session: ${session.name} (${session.id})`);
      // Reset any stuck states (like CONNECTING or QR_READY) to DISCONNECTED
      if (session.status !== 'DISCONNECTED') {
        session.status = 'DISCONNECTED';
        session.qrCode = null;
        session.phoneNumber = '';
        await db.saveSession(session);
      }
    }
  }
}

// Start a WhatsApp client instance for a session
async function startSession(sessionId) {
  if (clients[sessionId]) {
    console.log(`Session ${sessionId} is already running.`);
    return clients[sessionId];
  }

  let sessionData = await db.getSession(sessionId);
  if (!sessionData) {
    sessionData = {
      id: sessionId,
      name: `Session ${sessionId}`,
      status: 'DISCONNECTED',
      phoneNumber: ''
    };
    await db.saveSession(sessionData);
  }

  // Update status to CONNECTING
  sessionData.status = 'CONNECTING';
  sessionData.qrCode = null;
  await db.saveSession(sessionData);
  emitToSocket('session_update', sessionData);

  const client = new Client({
    authStrategy: new LocalAuth({
      clientId: sessionId,
      dataPath: AUTH_PATH
    }),
    webVersionCache: {
      type: 'remote',
      remotePath: 'https://raw.githubusercontent.com/wppconnect-team/wa-version/main/html/2.3000.1040199739-alpha.html'
    },
    puppeteer: {
      headless: true,
      executablePath: process.env.PUPPETEER_EXECUTABLE_PATH || undefined,
      protocolTimeout: 180000, // 3 minutes timeout to prevent CDP connection crashes under CPU load
      // Optimized flags for headless environments, prevents throttling and resource freezes
      args: [
        '--no-sandbox',
        '--disable-setuid-sandbox',
        '--disable-dev-shm-usage',
        '--disable-accelerated-2d-canvas',
        '--no-first-run',
        '--disable-gpu',
        '--disable-extensions',
        '--disable-backgrounding-occluded-windows',
        '--disable-renderer-backgrounding',
        '--disable-background-timer-throttling',
        '--blink-settings=imagesEnabled=false',
        '--js-flags=--max-old-space-size=512'
      ]
    }
  });

  clients[sessionId] = client;

  client.on('qr', async (qr) => {
    try {
      console.log(`QR received for session ${sessionId}`);
      const qrImageUrl = await QRCode.toDataURL(qr);
      
      sessionData.status = 'QR_READY';
      sessionData.qrCode = qrImageUrl;
      await db.saveSession(sessionData);
      
      emitToSocket('session_update', sessionData);
    } catch (err) {
      console.error('Error generating QR code image:', err);
    }
  });

  client.on('ready', async () => {
    console.log(`Client is ready for session ${sessionId}`);
    const phoneNumber = client.info.wid.user;
    
    sessionData.status = 'CONNECTED';
    sessionData.qrCode = null;
    sessionData.phoneNumber = phoneNumber;
    await db.saveSession(sessionData);
    
    emitToSocket('session_update', sessionData);
  });

  client.on('authenticated', () => {
    console.log(`Session ${sessionId} authenticated successfully.`);
  });

  client.on('auth_failure', async (msg) => {
    console.error(`Auth failure for session ${sessionId}:`, msg);
    
    sessionData.status = 'DISCONNECTED';
    sessionData.qrCode = null;
    await db.saveSession(sessionData);
    
    emitToSocket('session_update', sessionData);
    destroyClient(sessionId);
  });

  client.on('disconnected', async (reason) => {
    console.log(`Session ${sessionId} was disconnected:`, reason);
    
    sessionData.status = 'DISCONNECTED';
    sessionData.qrCode = null;
    sessionData.phoneNumber = '';
    await db.saveSession(sessionData);
    
    emitToSocket('session_update', sessionData);
    destroyClient(sessionId);
  });

  // Handle initialization errors cleanly
  client.initialize().catch(async (err) => {
    console.error(`Error initializing client for ${sessionId}:`, err);
    sessionData.status = 'DISCONNECTED';
    await db.saveSession(sessionData);
    emitToSocket('session_update', sessionData);
    destroyClient(sessionId);
  });

  return client;
}

// Clean up references and memory
async function destroyClient(sessionId) {
  const client = clients[sessionId];
  if (client) {
    try {
      await client.destroy();
    } catch (e) {
      console.error('Error destroying client:', e);
    }
    delete clients[sessionId];
  }
}

// Disconnect and remove all credentials
async function removeSession(sessionId) {
  await destroyClient(sessionId);
  await db.deleteSession(sessionId);
  emitToSocket('session_deleted', { id: sessionId });

  // Delete session files on disk
  const sessionAuthPath = path.join(AUTH_PATH, `session-${sessionId}`);
  try {
    await fs.rm(sessionAuthPath, { recursive: true, force: true });
    console.log(`Deleted credentials folder for session: ${sessionId}`);
  } catch (err) {
    console.error(`Error deleting credentials folder for session ${sessionId}:`, err);
  }
}

module.exports = {
  setSocketIo,
  initSessions,
  startSession,
  removeSession,
  getClient,
  isClientReady,
  clients
};
