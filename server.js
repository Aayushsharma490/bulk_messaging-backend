require('dotenv').config();
const express = require('express');
const http = require('http');
const socketIo = require('socket.io');
const cors = require('cors');
const path = require('path');
const db = require('./src/db');
const routes = require('./src/routes');
const sessionManager = require('./src/sessionManager');
const queueManager = require('./src/queueManager');

const app = express();
const server = http.createServer(app);

// ─── Rate Limiter (simple in-memory, no external deps needed) ──────────────
const rateLimitMap = new Map();
const RATE_LIMIT_WINDOW_MS = 60 * 1000; // 1 minute window
const RATE_LIMIT_MAX_REQUESTS = 120;     // max 120 requests per minute per IP

function rateLimiter(req, res, next) {
  const ip = req.ip || req.connection.remoteAddress || 'unknown';
  const now = Date.now();

  if (!rateLimitMap.has(ip)) {
    rateLimitMap.set(ip, { count: 1, resetAt: now + RATE_LIMIT_WINDOW_MS });
    return next();
  }

  const record = rateLimitMap.get(ip);

  if (now > record.resetAt) {
    // Window expired — reset counter
    record.count = 1;
    record.resetAt = now + RATE_LIMIT_WINDOW_MS;
    return next();
  }

  record.count++;
  if (record.count > RATE_LIMIT_MAX_REQUESTS) {
    return res.status(429).json({
      error: 'Too many requests. Please slow down and try again in a minute.'
    });
  }

  next();
}

// Clean up stale rate limit entries every 5 minutes to prevent memory leak
setInterval(() => {
  const now = Date.now();
  for (const [ip, record] of rateLimitMap.entries()) {
    if (now > record.resetAt) {
      rateLimitMap.delete(ip);
    }
  }
}, 5 * 60 * 1000);

// ─── Socket.io ────────────────────────────────────────────────────────────
const io = socketIo(server, {
  cors: {
    origin: '*',
    methods: ['GET', 'POST', 'DELETE']
  }
});

const PORT = process.env.PORT || 5000;

// ─── Middleware ───────────────────────────────────────────────────────────
app.use(cors({
  origin: '*',
  methods: ['GET', 'POST', 'DELETE', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'x-api-key', 'Authorization']
}));
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(rateLimiter); // Apply rate limiting to all routes

// Serve uploaded files
app.use('/uploads', express.static(path.join(__dirname, 'uploads')));

// Routes
app.use('/api', routes);

// Health check
app.get('/health', (req, res) => {
  res.json({ status: 'ok', time: new Date() });
});

// Setup socket io connections
io.on('connection', (socket) => {
  console.log(`Socket client connected: ${socket.id}`);
  
  socket.on('disconnect', () => {
    console.log(`Socket client disconnected: ${socket.id}`);
  });
});

// Main bootstrap function
async function bootstrap() {
  try {
    // 1. Initialize Database
    console.log('Initializing database...');
    await db.init();
    
    // 2. Wire Socket.io to Managers
    sessionManager.setSocketIo(io);
    queueManager.setSocketIo(io);

    // 3. Restore Sessions
    console.log('Restoring WhatsApp sessions...');
    await sessionManager.initSessions();

    // 4. Start Queue Worker
    console.log('Starting campaign queue worker...');
    queueManager.startWorker();

    // 5. Start Server
    server.listen(PORT, () => {
      console.log(`==================================================`);
      console.log(`  WhatsApp Bulk Messaging Backend Server started  `);
      console.log(`  Running on http://localhost:${PORT}             `);
      console.log(`  Admin API Key is set: ${!!process.env.ADMIN_API_KEY}`);
      console.log(`==================================================`);
    });
  } catch (err) {
    console.error('Critical boot error:', err);
    process.exit(1);
  }
}

// Graceful shutdown
process.on('SIGTERM', () => {
  console.log('SIGTERM signal received. Shutting down worker...');
  queueManager.stopWorker();
  server.close(() => {
    process.exit(0);
  });
});

bootstrap();
