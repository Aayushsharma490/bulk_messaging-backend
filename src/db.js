const fs = require('fs').promises;
const path = require('path');

const DB_DIR = path.join(__dirname, '..', 'data');
const DB_FILE = path.join(DB_DIR, 'database.json');

// Initialize database with default empty structure if it doesn't exist
async function initDb() {
  try {
    await fs.mkdir(DB_DIR, { recursive: true });
    try {
      await fs.access(DB_FILE);
    } catch {
      const defaultDb = {
        sessions: [],
        campaigns: [],
        messages: [],
        logs: []
      };
      await writeDb(defaultDb);
    }
  } catch (err) {
    console.error('Error initializing database:', err);
  }
}

// Atomic file write to avoid corruption
async function writeDb(data) {
  const tempPath = `${DB_FILE}.tmp`;
  try {
    await fs.writeFile(tempPath, JSON.stringify(data, null, 2), 'utf8');
    await fs.rename(tempPath, DB_FILE);
  } catch (err) {
    console.error('Failed to write database atomically:', err);
    throw err;
  }
}

async function readDb() {
  try {
    const data = await fs.readFile(DB_FILE, 'utf8');
    return JSON.parse(data);
  } catch (err) {
    console.error('Failed to read database:', err);
    return { sessions: [], campaigns: [], messages: [], logs: [] };
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
  }
};

module.exports = db;
