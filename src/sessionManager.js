const makeWASocket = require('@whiskeysockets/baileys').default;
const { useMultiFileAuthState, DisconnectReason } = require('@whiskeysockets/baileys');
const pino = require('pino');
const QRCode = require('qrcode');
const path = require('path');
const fs = require('fs').promises;
const db = require('./db');

const clients = {};
let socketIo = null;

const AUTH_PATH = path.join(__dirname, '..', 'data', 'sessions_baileys');

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
  const sock = clients[sessionId];
  return sock && sock.user && sock.user.id;
}

// Initialize all saved sessions from DB on startup
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

  const sessionAuthPath = path.join(AUTH_PATH, `session-${sessionId}`);
  
  try {
    await fs.mkdir(AUTH_PATH, { recursive: true });
    const { state, saveCreds } = await useMultiFileAuthState(sessionAuthPath);

    const sock = makeWASocket({
      auth: state,
      logger: pino({ level: 'silent' }),
      printQRInTerminal: false,
      defaultQueryTimeoutMs: 60000,
      connectTimeoutMs: 60000
    });

    clients[sessionId] = sock;

    // Save credentials whenever they are updated
    sock.ev.on('creds.update', saveCreds);

    // Connection updates (including QR code emission and login status)
    sock.ev.on('connection.update', async (update) => {
      const { connection, lastDisconnect, qr } = update;
      
      // Reload session state from DB to avoid overwriting concurrent changes
      sessionData = await db.getSession(sessionId) || sessionData;

      if (qr) {
        try {
          console.log(`QR received for session ${sessionId}`);
          const qrImageUrl = await QRCode.toDataURL(qr);
          
          sessionData.status = 'QR_READY';
          sessionData.qrCode = qrImageUrl;
          await db.saveSession(sessionData);
          
          emitToSocket('session_update', sessionData);
        } catch (err) {
          console.error(`Error generating QR code for session ${sessionId}:`, err);
        }
      }

      if (connection === 'open') {
        console.log(`Client is ready for session ${sessionId}`);
        
        // Extract phone number from sock.user.id (e.g. "917727038430:2@s.whatsapp.net" or "917727038430@s.whatsapp.net")
        const phoneNumber = sock.user.id.split(':')[0].split('@')[0];
        
        sessionData.status = 'CONNECTED';
        sessionData.qrCode = null;
        sessionData.phoneNumber = phoneNumber;
        await db.saveSession(sessionData);
        
        emitToSocket('session_update', sessionData);
      }

      if (connection === 'close') {
        const statusCode = lastDisconnect?.error?.output?.statusCode;
        const shouldReconnect = statusCode !== DisconnectReason.loggedOut;
        
        console.log(`Session ${sessionId} connection closed. Code: ${statusCode}. Reconnecting: ${shouldReconnect}`);

        if (shouldReconnect) {
          // Restart session connection
          delete clients[sessionId];
          setTimeout(() => {
            startSession(sessionId).catch(err => {
              console.error(`Error reconnecting session ${sessionId}:`, err);
            });
          }, 3000);
        } else {
          // Explicit logout: clean credentials and mark disconnected
          console.log(`Session ${sessionId} was logged out.`);
          
          sessionData.status = 'DISCONNECTED';
          sessionData.qrCode = null;
          sessionData.phoneNumber = '';
          await db.saveSession(sessionData);
          
          emitToSocket('session_update', sessionData);
          
          await destroyClient(sessionId);
          
          // Delete auth directory
          try {
            await fs.rm(sessionAuthPath, { recursive: true, force: true });
            console.log(`Deleted credentials folder for session ${sessionId} after logout`);
          } catch (e) {
            console.error(`Error deleting credentials directory for logged out session ${sessionId}:`, e);
          }
        }
      }
    });

    return sock;
  } catch (err) {
    console.error(`Error starting Baileys connection for session ${sessionId}:`, err);
    sessionData.status = 'DISCONNECTED';
    await db.saveSession(sessionData);
    emitToSocket('session_update', sessionData);
    delete clients[sessionId];
    throw err;
  }
}

// Clean up references and memory
async function destroyClient(sessionId) {
  const sock = clients[sessionId];
  if (sock) {
    try {
      sock.end(undefined); // Close connection cleanly without logging out
    } catch (e) {
      console.error(`Error ending connection for session ${sessionId}:`, e);
    }
    delete clients[sessionId];
  }
}

// Disconnect and remove all credentials
async function removeSession(sessionId) {
  const sock = clients[sessionId];
  if (sock) {
    try {
      await sock.logout();
    } catch (e) {
      console.error(`Error logging out session ${sessionId}:`, e);
      try {
        sock.end(undefined);
      } catch (_) {}
    }
    delete clients[sessionId];
  }
  
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
