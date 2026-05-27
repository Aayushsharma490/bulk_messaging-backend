const fs = require('fs').promises;
const path = require('path');

const DB_DIR = path.join(__dirname, '..', 'data');
const DB_FILE = path.join(DB_DIR, 'database.json');

let dbCache = null;
let isWriting = false;
let pendingWriteData = null;

// Initialize database with default empty structure if it doesn't exist
async function initDb() {
  try {
    await fs.mkdir(DB_DIR, { recursive: true });
    try {
      await fs.access(DB_FILE);
      // Load initial cache from file
      const data = await fs.readFile(DB_FILE, 'utf8');
      dbCache = JSON.parse(data);
    } catch {
      dbCache = {
        sessions: [],
        campaigns: [],
        messages: [],
        logs: []
      };
      await writeDb(dbCache);
    }
  } catch (err) {
    console.error('Error initializing database:', err);
  }
}

// Queue-based non-blocking background write to avoid corruption & event loop lag
async function writeDb(data) {
  dbCache = data; // Update cache immediately in memory
  pendingWriteData = data;

  if (isWriting) {
    return; // Already writing, the active loop will pick up the updated data
  }

  isWriting = true;

  // Let the file write run in the background (asynchronously)
  setImmediate(async () => {
    try {
      while (pendingWriteData !== null) {
        const dataToWrite = pendingWriteData;
        pendingWriteData = null; // Clear before starting write so we can detect new updates

        const tempPath = `${DB_FILE}.tmp`;
        await fs.writeFile(tempPath, JSON.stringify(dataToWrite, null, 2), 'utf8');
        await fs.rename(tempPath, DB_FILE);
      }
    } catch (err) {
      console.error('Failed to write database atomically in background:', err);
    } finally {
      isWriting = false;
      if (pendingWriteData !== null) {
        writeDb(pendingWriteData);
      }
    }
  });
}

async function readDb() {
  if (dbCache) {
    return dbCache;
  }
  try {
    const data = await fs.readFile(DB_FILE, 'utf8');
    dbCache = JSON.parse(data);
    return dbCache;
  } catch (err) {
    console.error('Failed to read database:', err);
    dbCache = { sessions: [], campaigns: [], messages: [], logs: [] };
    return dbCache;
  }
}

const db = {
  init: initDb,

  // --- SESSIONS ---
  async getSessions() {
    const data = await readDb();
    return data.sessions || [];
  },

  async getSession(id) {
    const sessions = await this.getSessions();
    return sessions.find(s => s.id === id);
  },

  async saveSession(session) {
    const data = await readDb();
    const index = data.sessions.findIndex(s => s.id === session.id);
    if (index !== -1) {
      data.sessions[index] = { ...data.sessions[index], ...session };
    } else {
      data.sessions.push(session);
    }
    await writeDb(data);
    return session;
  },

  async deleteSession(id) {
    const data = await readDb();
    data.sessions = data.sessions.filter(s => s.id !== id);
    await writeDb(data);
  },

  // --- CAMPAIGNS ---
  async getCampaigns() {
    const data = await readDb();
    return data.campaigns || [];
  },

  async getCampaign(id) {
    const campaigns = await this.getCampaigns();
    return campaigns.find(c => c.id === id);
  },

  async saveCampaign(campaign) {
    const data = await readDb();
    const index = data.campaigns.findIndex(c => c.id === campaign.id);
    if (index !== -1) {
      data.campaigns[index] = { ...data.campaigns[index], ...campaign };
    } else {
      data.campaigns.push(campaign);
    }
    await writeDb(data);
    return campaign;
  },

  async deleteCampaign(id) {
    const data = await readDb();
    data.campaigns = data.campaigns.filter(c => c.id !== id);
    data.messages = data.messages.filter(m => m.campaignId !== id);
    data.logs = data.logs.filter(l => l.campaignId !== id);
    await writeDb(data);
  },

  // --- MESSAGES ---
  async getMessages(campaignId) {
    const data = await readDb();
    if (campaignId) {
      return (data.messages || []).filter(m => m.campaignId === campaignId);
    }
    return data.messages || [];
  },

  async getPendingMessages() {
    const data = await readDb();
    return (data.messages || []).filter(m => m.status === 'pending');
  },

  async saveMessages(newMessages) {
    const data = await readDb();
    data.messages = data.messages || [];
    data.messages.push(...newMessages);
    await writeDb(data);
  },

  async updateMessageStatus(id, status, error = null) {
    const data = await readDb();
    const msg = data.messages.find(m => m.id === id);
    if (msg) {
      msg.status = status;
      if (error) msg.error = error;
      msg.sentAt = new Date().toISOString();
      await writeDb(data);
    }
    return msg;
  },

  async deleteCampaignMessages(campaignId) {
    const data = await readDb();
    data.messages = data.messages.filter(m => m.campaignId !== campaignId);
    await writeDb(data);
  },

  // --- LOGS ---
  async getLogs(campaignId) {
    const data = await readDb();
    if (campaignId) {
      return (data.logs || []).filter(l => l.campaignId === campaignId);
    }
    return data.logs || [];
  },

  async addLog(campaignId, level, message) {
    const data = await readDb();
    data.logs = data.logs || [];
    const newLog = {
      id: Math.random().toString(36).substring(2, 9),
      timestamp: new Date().toISOString(),
      campaignId,
      level, // 'info' | 'success' | 'error'
      message
    };
    data.logs.push(newLog);

    // Cap logs at 5000 entries total to keep file size reasonable
    if (data.logs.length > 5000) {
      data.logs.shift();
    }

    await writeDb(data);
    return newLog;
  },

  async resetDb() {
    const data = await readDb();
    data.campaigns = [];
    data.messages = [];
    data.logs = [];
    await writeDb(data);
  }
};

module.exports = db;
