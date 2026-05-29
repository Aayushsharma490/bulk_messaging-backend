const express = require('express');
const router = express.Router();
const multer = require('multer');
const path = require('path');
const fs = require('fs').promises;
const db = require('./db');
const sessionManager = require('./sessionManager');
const queueManager = require('./queueManager');

// ─── Configure Multer for file uploads ──────────────────────────────────────
const UPLOADS_DIR = path.join(__dirname, '..', 'uploads');

const storage = multer.diskStorage({
  destination: async (req, file, cb) => {
    try {
      await fs.mkdir(UPLOADS_DIR, { recursive: true });
      cb(null, UPLOADS_DIR);
    } catch (err) {
      cb(err);
    }
  },
  filename: (req, file, cb) => {
    const uniqueSuffix = Date.now() + '-' + Math.round(Math.random() * 1e9);
    cb(null, uniqueSuffix + path.extname(file.originalname));
  }
});

const upload = multer({ 
  storage,
  limits: { fileSize: 15 * 1024 * 1024 } // 15MB limit
});

// ─── ANTI-BAN SAFETY LIMITS (Hardcoded — DO NOT LOWER THESE) ────────────────
// These limits protect the client's WhatsApp number from getting banned.
// WhatsApp's spam detection triggers on fast bulk senders.
const SAFE_LIMITS = {
  MIN_DELAY_SECONDS: 20,          // Minimum 20 seconds between messages
  MAX_DELAY_SECONDS: 45,          // Maximum 45 seconds between messages
  MAX_BATCH_SIZE: 50,             // Max 50 messages per batch (then cooldown kicks in)
  MIN_BATCH_COOLDOWN_SECONDS: 15 * 60, // Minimum 15 minutes between batches
};

// ─── Admin Auth Middleware ───────────────────────────────────────────────────
// Protects destructive admin routes with an API key.
function requireAdminKey(req, res, next) {
  const adminKey = process.env.ADMIN_API_KEY;
  
  // If no admin key is configured on the server, allow (dev mode)
  if (!adminKey) {
    return next();
  }

  const providedKey = req.headers['x-api-key'] || req.query.apiKey;
  
  if (!providedKey || providedKey !== adminKey) {
    return res.status(403).json({
      error: 'Forbidden: Valid admin API key required. Provide it in the x-api-key header.'
    });
  }
  
  next();
}

// ─── SESSIONS ────────────────────────────────────────────────────────────────

// Get all sessions
router.get('/sessions', async (req, res) => {
  try {
    const sessions = await db.getSessions();
    res.json(sessions);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Create/Start a session
router.post('/sessions', async (req, res) => {
  try {
    const { name } = req.body;
    if (!name) {
      return res.status(400).json({ error: 'Session name is required' });
    }

    const sessionId = 'session_' + Math.random().toString(36).substring(2, 9);
    await sessionManager.startSession(sessionId);
    
    // Update name — guard against rare race condition where session isn't saved yet
    const session = await db.getSession(sessionId);
    if (!session) {
      return res.status(500).json({ error: 'Session was created but could not be retrieved. Please refresh.' });
    }
    session.name = name;
    await db.saveSession(session);

    res.json(session);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Restart a disconnected session
router.post('/sessions/:id/restart', async (req, res) => {
  try {
    const sessionId = req.params.id;
    const session = await db.getSession(sessionId);
    if (!session) {
      return res.status(404).json({ error: 'Session not found' });
    }
    await sessionManager.startSession(sessionId);
    res.json({ message: 'Session initialization started' });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Delete session
router.delete('/sessions/:id', async (req, res) => {
  try {
    const sessionId = req.params.id;
    await sessionManager.removeSession(sessionId);
    res.json({ success: true, message: 'Session deleted successfully' });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ─── CAMPAIGNS ───────────────────────────────────────────────────────────────

// Get all campaigns with statistics loaded
router.get('/campaigns', async (req, res) => {
  try {
    const campaigns = await db.getCampaigns();
    const allMessages = await db.getMessages();

    // Map stats
    const campaignsWithStats = campaigns.map(campaign => {
      const campMessages = allMessages.filter(m => m.campaignId === campaign.id);
      const total = campMessages.length;
      const sent = campMessages.filter(m => m.status === 'sent').length;
      const failed = campMessages.filter(m => m.status === 'failed').length;
      const pending = campMessages.filter(m => m.status === 'pending').length;

      return {
        ...campaign,
        stats: { total, sent, failed, pending }
      };
    });

    // Sort by created date descending
    campaignsWithStats.sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));

    res.json(campaignsWithStats);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Get campaign by ID
router.get('/campaigns/:id', async (req, res) => {
  try {
    const campaign = await db.getCampaign(req.params.id);
    if (!campaign) {
      return res.status(404).json({ error: 'Campaign not found' });
    }

    const messages = await db.getMessages(campaign.id);
    const total = messages.length;
    const sent = messages.filter(m => m.status === 'sent').length;
    const failed = messages.filter(m => m.status === 'failed').length;
    const pending = messages.filter(m => m.status === 'pending').length;

    campaign.stats = { total, sent, failed, pending };

    res.json(campaign);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Create campaign
router.post('/campaigns', upload.single('mediaFile'), async (req, res) => {
  try {
    console.log('Incoming campaign request body:', req.body);
    console.log('Uploaded file details:', req.file);

    const { 
      name, 
      sessionId, 
      messageMode, 
      templatesJson,
      scheduledAt,
      contactsJson
    } = req.body;

    if (!name || !sessionId || !messageMode || !templatesJson || !contactsJson) {
      const missing = [];
      if (!name) missing.push('name');
      if (!sessionId) missing.push('sessionId');
      if (!messageMode) missing.push('messageMode');
      if (!templatesJson) missing.push('templatesJson');
      if (!contactsJson) missing.push('contactsJson');
      return res.status(400).json({ error: `Missing required fields: ${missing.join(', ')}` });
    }

    let templates, rawContacts;
    try {
      templates = JSON.parse(templatesJson);
    } catch (e) {
      return res.status(400).json({ error: 'templatesJson is not valid JSON' });
    }

    try {
      rawContacts = JSON.parse(contactsJson);
    } catch (e) {
      return res.status(400).json({ error: 'contactsJson is not valid JSON' });
    }

    if (!Array.isArray(templates) || templates.length === 0) {
      return res.status(400).json({ error: 'Templates must be a non-empty array' });
    }

    if (!Array.isArray(rawContacts) || rawContacts.length === 0) {
      return res.status(400).json({ error: 'Contacts must be a non-empty array' });
    }

    // 1. Process contacts: Remove duplicates and filter invalid numbers
    const processedContacts = [];
    const numbersSeen = new Set();

    for (const contact of rawContacts) {
      if (!contact.phone) continue;
      
      let phoneStr = contact.phone.toString().replace(/[\s\-\(\)]+/g, '');
      const match = phoneStr.match(/^\+?[0-9]+/);
      let cleanPhone = match ? match[0].replace(/[^0-9]/g, '') : phoneStr.replace(/[^0-9]/g, '');
      if (cleanPhone.length < 8) continue;

      if (numbersSeen.has(cleanPhone)) continue;
      numbersSeen.add(cleanPhone);

      processedContacts.push({
        name: contact.name || '',
        phone: cleanPhone,
        customFields: contact.customFields || {}
      });
    }

    if (processedContacts.length === 0) {
      return res.status(400).json({ error: 'No valid phone numbers found (numbers must be at least 8 digits)' });
    }

    // 2. Prepare media info
    let media = null;
    if (req.file) {
      media = {
        filename: req.file.filename,
        originalName: req.file.originalname,
        mimeType: fileMimeType(req.file.originalname) || req.file.mimetype
      };
    }

    const campaignId = 'camp_' + Math.random().toString(36).substring(2, 9);
    const isScheduled = !!scheduledAt;

    // 3. Create campaign object
    // ⚠️  ANTI-BAN: All safety limits are ENFORCED on the server side.
    //     The frontend values are ignored — safe limits are hardcoded here.
    const campaign = {
      id: campaignId,
      name,
      sessionId,
      messageMode,
      templates,
      status: isScheduled ? 'scheduled' : 'running',
      scheduledAt: isScheduled ? scheduledAt : null,
      batchSize: SAFE_LIMITS.MAX_BATCH_SIZE,               // Max 50 per batch
      batchCooldown: SAFE_LIMITS.MIN_BATCH_COOLDOWN_SECONDS, // 15 min cooldown
      minDelay: SAFE_LIMITS.MIN_DELAY_SECONDS,             // 20 seconds min
      maxDelay: SAFE_LIMITS.MAX_DELAY_SECONDS,             // 45 seconds max
      media,
      totalContacts: processedContacts.length,
      batchSentCount: 0,
      createdAt: new Date().toISOString()
    };

    // 4. Generate messages to queue
    const queuedMessages = processedContacts.map((contact, index) => {
      let messageContent = '';
      if (templates.length === 1) {
        messageContent = templates[0];
      } else {
        const randomIndex = Math.floor(Math.random() * templates.length);
        messageContent = templates[randomIndex];
      }

      return {
        id: `msg_${campaignId}_${index}_` + Math.random().toString(36).substring(2, 6),
        campaignId,
        sessionId,
        phoneNumber: contact.phone,
        name: contact.name,
        customFields: contact.customFields,
        messageContent,
        status: 'pending'
      };
    });

    // 5. Save to database
    await db.saveCampaign(campaign);
    await db.saveMessages(queuedMessages);
    
    // Add logs
    await db.addLog(campaignId, 'info', `Campaign created with ${processedContacts.length} unique contacts. Duplicate/invalid numbers filtered: ${rawContacts.length - processedContacts.length}.`);
    await db.addLog(campaignId, 'info', `🛡️ Anti-ban limits active: ${SAFE_LIMITS.MIN_DELAY_SECONDS}–${SAFE_LIMITS.MAX_DELAY_SECONDS}s delay, batch of ${SAFE_LIMITS.MAX_BATCH_SIZE}, ${SAFE_LIMITS.MIN_BATCH_COOLDOWN_SECONDS / 60}min cooldown between batches.`);
    
    if (isScheduled) {
      await db.addLog(campaignId, 'info', `Campaign scheduled to start at ${new Date(scheduledAt).toLocaleString()}.`);
    } else {
      await db.addLog(campaignId, 'info', `Campaign started running.`);
    }

    res.status(201).json({ campaign, totalContacts: processedContacts.length });
  } catch (err) {
    console.error('Error creating campaign:', err);
    res.status(500).json({ error: err.message });
  }
});

// Pause Campaign
router.post('/campaigns/:id/pause', async (req, res) => {
  try {
    await queueManager.pauseCampaign(req.params.id);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Resume Campaign
router.post('/campaigns/:id/resume', async (req, res) => {
  try {
    await queueManager.resumeCampaign(req.params.id);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Stop Campaign
router.post('/campaigns/:id/stop', async (req, res) => {
  try {
    await queueManager.stopCampaign(req.params.id);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Delete Campaign
router.delete('/campaigns/:id', async (req, res) => {
  try {
    await db.deleteCampaign(req.params.id);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Get Messages for a campaign
router.get('/campaigns/:id/messages', async (req, res) => {
  try {
    const messages = await db.getMessages(req.params.id);
    res.json(messages);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Get Logs for a campaign
router.get('/campaigns/:id/logs', async (req, res) => {
  try {
    const logs = await db.getLogs(req.params.id);
    logs.sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));
    res.json(logs);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ─── Helper: File extension → MIME type ─────────────────────────────────────
function fileMimeType(filename) {
  const ext = path.extname(filename).toLowerCase();
  const mimeTypes = {
    '.png': 'image/png',
    '.jpg': 'image/jpeg',
    '.jpeg': 'image/jpeg',
    '.gif': 'image/gif',
    '.mp4': 'video/mp4',
    '.pdf': 'application/pdf',
    '.doc': 'application/msword',
    '.docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
    '.xls': 'application/vnd.ms-excel',
    '.xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    '.txt': 'text/plain',
    '.csv': 'text/csv'
  };
  return mimeTypes[ext];
}

// ─── ADMIN RESET ROUTE (PROTECTED) ──────────────────────────────────────────
// Requires x-api-key header matching ADMIN_API_KEY in .env
router.get('/admin/reset', requireAdminKey, async (req, res) => {
  try {
    await db.resetDb();
    res.send('<h1>Database Reset Successful</h1><p>All campaigns, messages, and logs have been cleared. Connected sessions remain active.</p>');
  } catch (err) {
    res.status(500).send(`<h1>Database Reset Failed</h1><p>${err.message}</p>`);
  }
});

router.post('/admin/reset', requireAdminKey, async (req, res) => {
  try {
    await db.resetDb();
    res.json({ success: true, message: 'Database reset successfully (campaigns, messages, and logs cleared).' });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Get current anti-ban limits (for frontend display)
router.get('/config/limits', (req, res) => {
  res.json({
    minDelay: SAFE_LIMITS.MIN_DELAY_SECONDS,
    maxDelay: SAFE_LIMITS.MAX_DELAY_SECONDS,
    batchSize: SAFE_LIMITS.MAX_BATCH_SIZE,
    batchCooldownMinutes: SAFE_LIMITS.MIN_BATCH_COOLDOWN_SECONDS / 60
  });
});

module.exports = router;
