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

// Configure Socket.io with CORS
const io = socketIo(server, {
  cors: {
    origin: '*', // Allow all in local/development environment
    methods: ['GET', 'POST', 'DELETE']
  }
});

const PORT = process.env.PORT || 5000;

// Middleware
app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// Serve uploaded files
app.use('/uploads', express.static(path.join(__dirname, 'uploads')));

// Routes
app.use('/api', routes);

// Base route
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
