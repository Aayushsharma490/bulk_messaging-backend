const db = require('./db');
const sessionManager = require('./sessionManager');
const fs = require('fs').promises;
const path = require('path');

// ─── Helper: Wrap promise with a timeout ─────────────────────────────────────
function withTimeout(promise, ms, errorMsg = 'Operation timed out') {
  let timeoutId;
  const timeoutPromise = new Promise((_, reject) => {
    timeoutId = setTimeout(() => {
      reject(new Error(errorMsg));
    }, ms);
  });
  return Promise.race([
    promise.then((res) => {
      clearTimeout(timeoutId);
      return res;
    }),
    timeoutPromise
  ]);
}

// ─── Anti-ban safety floors — must match routes.js SAFE_LIMITS ──────────────
// Even if the DB has bad values (old campaigns), these floors protect the number.
const ABSOLUTE_MIN_DELAY_SECONDS = 20;
const ABSOLUTE_MAX_DELAY_SECONDS = 45;
const ABSOLUTE_MAX_BATCH_SIZE = 50;
const ABSOLUTE_MIN_BATCH_COOLDOWN_SECONDS = 15 * 60; // 15 minutes

let socketIo = null;
let workerInterval = null;
let isProcessing = false;

// Track active timers for quick interrupts
const activeCampaignSleeps = {};

// Track reconnect retry count per running campaign
const campaignReconnectRetries = {};

function setSocketIo(io) {
  socketIo = io;
}

function emitToSocket(event, data) {
  if (socketIo) {
    socketIo.emit(event, data);
  }
}

// Start the background campaign processing worker
function startWorker() {
  if (workerInterval) return;
  
  // Check campaigns every 5 seconds
  workerInterval = setInterval(async () => {
    if (isProcessing) return;
    isProcessing = true;
    try {
      await processQueue();
    } catch (err) {
      console.error('Error in queue worker:', err);
    } finally {
      isProcessing = false;
    }
  }, 5000);
  
  console.log('Campaign queue worker started.');
}

// Stop the worker
function stopWorker() {
  if (workerInterval) {
    clearInterval(workerInterval);
    workerInterval = null;
  }
}

// Process the campaigns in the queue
async function processQueue() {
  const campaigns = await db.getCampaigns();
  const now = new Date();

  // 1. Handle Scheduled Campaigns
  for (const campaign of campaigns) {
    if (campaign.status === 'scheduled' && campaign.scheduledAt) {
      const scheduleTime = new Date(campaign.scheduledAt);
      if (scheduleTime <= now) {
        console.log(`Starting scheduled campaign: ${campaign.name} (${campaign.id})`);
        campaign.status = 'running';
        campaign.batchSentCount = 0;
        await db.saveCampaign(campaign);
        await db.addLog(campaign.id, 'info', `Scheduled campaign started automatically.`);
        emitToSocket('campaign_update', campaign);
      }
    }
  }

  // Find running campaigns
  const runningCampaigns = campaigns.filter(c => c.status === 'running');
  for (const campaign of runningCampaigns) {

    // ── Cooldown check ──────────────────────────────────────────────────────
    if (campaign.cooldownUntil) {
      const cooldownTime = new Date(campaign.cooldownUntil);
      if (cooldownTime > now) {
        continue; // Still cooling down
      } else {
        delete campaign.cooldownUntil;
        campaign.batchSentCount = 0;
        await db.saveCampaign(campaign);
        await db.addLog(campaign.id, 'info', `Batch cooldown ended. Resuming sending...`);
        emitToSocket('campaign_update', campaign);
      }
    }

    // ── Check client session ────────────────────────────────────────────────
    const sessionId = campaign.sessionId;
    const client = sessionManager.getClient(sessionId);
    const isReady = sessionManager.isClientReady(sessionId);

    if (!client || !isReady) {
      const session = await db.getSession(sessionId);
      const isExplicitlyDisconnected = !session || session.status === 'DISCONNECTED';
      
      if (isExplicitlyDisconnected) {
        console.log(`Session ${sessionId} is disconnected. Pausing campaign ${campaign.id}`);
        campaign.status = 'paused';
        await db.saveCampaign(campaign);
        await db.addLog(campaign.id, 'error', `Campaign paused: WhatsApp session "${session?.name || sessionId}" is disconnected.`);
        emitToSocket('campaign_update', campaign);
        continue;
      }
      
      // Temporary reconnect grace period (10 loops = 50 seconds max)
      const retries = campaignReconnectRetries[campaign.id] || 0;
      if (retries < 10) {
        campaignReconnectRetries[campaign.id] = retries + 1;
        console.log(`Session ${sessionId} socket temporarily down. Grace retry ${retries + 1}/10 for campaign ${campaign.id}`);
        if (retries === 0) {
          await db.addLog(campaign.id, 'info', `WhatsApp connection dropped. Attempting to auto-reconnect...`);
        }
        continue;
      } else {
        delete campaignReconnectRetries[campaign.id];
        campaign.status = 'paused';
        await db.saveCampaign(campaign);
        await db.addLog(campaign.id, 'error', `Campaign paused: WhatsApp session "${session?.name || sessionId}" failed to reconnect within 50 seconds.`);
        emitToSocket('campaign_update', campaign);
        continue;
      }
    }
    
    // Connection successful, reset retry counter
    delete campaignReconnectRetries[campaign.id];

    // ── Get pending messages ────────────────────────────────────────────────
    const messages = await db.getMessages(campaign.id);
    const pendingMessages = messages.filter(m => m.status === 'pending');

    if (pendingMessages.length === 0) {
      campaign.status = 'completed';
      await db.saveCampaign(campaign);
      await db.addLog(campaign.id, 'success', `✅ Campaign completed successfully! All messages processed.`);
      emitToSocket('campaign_update', campaign);
      continue;
    }

    // ── Enforce Anti-ban Batch Limits ───────────────────────────────────────
    // Use server safe values — floor any bad values from old campaigns
    const batchSize = Math.min(
      campaign.batchSize || ABSOLUTE_MAX_BATCH_SIZE,
      ABSOLUTE_MAX_BATCH_SIZE
    );
    const batchCooldown = Math.max(
      campaign.batchCooldown || ABSOLUTE_MIN_BATCH_COOLDOWN_SECONDS,
      ABSOLUTE_MIN_BATCH_COOLDOWN_SECONDS
    );
    const sentInCurrentBatch = campaign.batchSentCount || 0;

    if (sentInCurrentBatch >= batchSize) {
      const cooldownUntil = new Date(Date.now() + batchCooldown * 1000).toISOString();
      campaign.cooldownUntil = cooldownUntil;
      await db.saveCampaign(campaign);
      
      const minutes = Math.round(batchCooldown / 60);
      await db.addLog(campaign.id, 'info', `🛡️ Batch limit of ${batchSize} reached. Cooling down for ${minutes} minutes to protect account from ban...`);
      emitToSocket('campaign_update', campaign);
      continue;
    }

    // ── Send next message ───────────────────────────────────────────────────
    const message = pendingMessages[0];
    await sendSingleMessage(campaign, message, client);
    break; // One message per worker tick
  }
}

// ─── Send a single message with safe delays ──────────────────────────────────
async function sendSingleMessage(campaign, message, client) {
  const campaignId = campaign.id;

  // ⚠️ Enforce safe delay floors — never send faster than ABSOLUTE_MIN_DELAY_SECONDS
  // Clamp min to floor, clamp max to ceiling, ensure max >= min
  const minDelay = Math.max(campaign.minDelay || ABSOLUTE_MIN_DELAY_SECONDS, ABSOLUTE_MIN_DELAY_SECONDS);
  const maxDelay = Math.max(
    Math.max(campaign.maxDelay || ABSOLUTE_MAX_DELAY_SECONDS, ABSOLUTE_MAX_DELAY_SECONDS),
    minDelay  // Ensure max is never less than min
  );

  const delaySeconds = Math.floor(Math.random() * (maxDelay - minDelay + 1)) + minDelay;

  console.log(`Sending message ${message.id} to ${message.phoneNumber} in ${delaySeconds} seconds.`);
  await db.addLog(campaignId, 'info', `⏳ Preparing to send to ${message.name || message.phoneNumber} in ${delaySeconds}s (anti-ban delay)...`);

  // ── Interruptible sleep loop ────────────────────────────────────────────
  let interrupted = false;
  activeCampaignSleeps[campaignId] = true;
  
  for (let s = 0; s < delaySeconds; s++) {
    const currentCampaign = await db.getCampaign(campaignId);
    if (!currentCampaign || currentCampaign.status !== 'running') {
      interrupted = true;
      break;
    }
    await new Promise(resolve => setTimeout(resolve, 1000));
  }
  
  delete activeCampaignSleeps[campaignId];

  if (interrupted) {
    console.log(`Sending to ${message.phoneNumber} was interrupted (campaign paused/stopped).`);
    return;
  }

  // ── Re-check session is still alive before sending ──────────────────────
  const isStillReady = sessionManager.isClientReady(campaign.sessionId);
  if (!isStillReady) {
    console.log(`Session dropped during sleep for message ${message.id}. Will retry next tick.`);
    await db.addLog(campaignId, 'error', `Connection lost during delay. Message to ${message.name || message.phoneNumber} will be retried...`);
    return;
  }

  try {
    // ── 1. Sanitize phone number ─────────────────────────────────────────
    let cleanNumber = message.phoneNumber.replace(/[^0-9]/g, '');
    let targetJid = `${cleanNumber}@s.whatsapp.net`;

    // ── 2. Personalize message content ───────────────────────────────────
    let formattedText = message.messageContent;
    formattedText = formattedText.replace(/{name}/gi, message.name || '');
    
    if (message.customFields) {
      for (const [key, val] of Object.entries(message.customFields)) {
        const regex = new RegExp(`{${key}}`, 'gi');
        formattedText = formattedText.replace(regex, val || '');
      }
    }

    // ── 3. Send message (with or without media) ──────────────────────────
    let sendResult;
    
    if (campaign.media && campaign.media.filename) {
      const mediaPath = path.join(__dirname, '..', 'uploads', campaign.media.filename);
      try {
        const fileBuffer = await fs.readFile(mediaPath);
        
        let mediaContent;
        const mimeType = campaign.media.mimeType || '';
        
        if (mimeType.startsWith('image/')) {
          mediaContent = { image: fileBuffer, caption: formattedText };
        } else if (mimeType.startsWith('video/')) {
          mediaContent = { video: fileBuffer, caption: formattedText };
        } else if (mimeType.startsWith('audio/')) {
          mediaContent = { audio: fileBuffer, caption: formattedText };
        } else {
          mediaContent = { 
            document: fileBuffer, 
            mimetype: mimeType, 
            fileName: campaign.media.originalName || campaign.media.filename, 
            caption: formattedText 
          };
        }

        sendResult = await withTimeout(
          client.sendMessage(targetJid, mediaContent),
          60000,
          'Media message send timed out'
        );
      } catch (err) {
        console.error('Error reading/sending media:', err);
        throw new Error(`Media send failed: ${err.message}`);
      }
    } else {
      sendResult = await withTimeout(
        client.sendMessage(targetJid, { text: formattedText }),
        30000,
        'Message send timed out'
      );
    }

    // ── 4. Verify send result — only mark "sent" if Baileys confirmed it ──
    // Baileys returns a proto message object on success; null/undefined means failure
    if (!sendResult || !sendResult.key) {
      throw new Error('Message was not acknowledged by WhatsApp (no message key returned). The number may not be on WhatsApp.');
    }

    // ── 5. Success: update DB ─────────────────────────────────────────────
    await db.updateMessageStatus(message.id, 'sent');
    await db.addLog(campaignId, 'success', `✅ Message sent to ${message.name || message.phoneNumber} (${message.phoneNumber})`);
    
    campaign.batchSentCount = (campaign.batchSentCount || 0) + 1;
    await db.saveCampaign(campaign);
    
    emitToSocket('message_status', { messageId: message.id, status: 'sent', campaignId });

  } catch (err) {
    console.error(`Failed to send message ${message.id}:`, err);
    
    const errMsg = (err.message || '').toLowerCase();
    
    // ── Temporary connection issue — keep as pending to retry ────────────
    const isTempConnectionIssue = 
      errMsg.includes('protocol error') || 
      errMsg.includes('context was destroyed') || 
      errMsg.includes('session closed') || 
      errMsg.includes('browser has already been closed') || 
      errMsg.includes('target closed') || 
      errMsg.includes('network.enable') || 
      errMsg.includes('timed out') || 
      errMsg.includes('timeout') || 
      errMsg.includes('websocket') || 
      errMsg.includes('stream error') ||
      errMsg.includes('econnreset') ||
      errMsg.includes('socket hang up') ||
      errMsg.includes('disconnected') ||
      errMsg.includes('connection reset') ||
      errMsg.includes('write after end');

    if (isTempConnectionIssue) {
      console.log(`Temporary connection issue, keeping message ${message.id} as pending to retry...`);
      await db.addLog(campaignId, 'error', `⚠️ Connection hiccup when sending to ${message.name || message.phoneNumber}. Will retry shortly...`);
      return; // Don't mark as failed — keep as pending for retry
    }

    // ── Permanent failure — number not on WhatsApp or blocked ────────────
    await db.updateMessageStatus(message.id, 'failed', err.message || 'Unknown sending error');
    await db.addLog(campaignId, 'error', `❌ Failed to send to ${message.name || message.phoneNumber}: ${err.message}`);
    
    campaign.batchSentCount = (campaign.batchSentCount || 0) + 1;
    await db.saveCampaign(campaign);
    
    emitToSocket('message_status', { messageId: message.id, status: 'failed', campaignId });
  }
}

// ─── Campaign control functions ──────────────────────────────────────────────

async function pauseCampaign(campaignId) {
  const campaign = await db.getCampaign(campaignId);
  if (campaign && campaign.status === 'running') {
    campaign.status = 'paused';
    await db.saveCampaign(campaign);
    await db.addLog(campaignId, 'info', `Campaign paused by user.`);
    emitToSocket('campaign_update', campaign);
  }
}

async function resumeCampaign(campaignId) {
  const campaign = await db.getCampaign(campaignId);
  if (campaign && (campaign.status === 'paused' || campaign.status === 'scheduled')) {
    campaign.status = 'running';
    delete campaign.cooldownUntil;
    campaign.batchSentCount = 0;
    await db.saveCampaign(campaign);
    await db.addLog(campaignId, 'info', `Campaign resumed by user.`);
    emitToSocket('campaign_update', campaign);
  }
}

async function stopCampaign(campaignId) {
  const campaign = await db.getCampaign(campaignId);
  if (campaign) {
    campaign.status = 'stopped';
    delete campaign.cooldownUntil;
    
    const messages = await db.getMessages(campaignId);
    for (const msg of messages) {
      if (msg.status === 'pending') {
        await db.updateMessageStatus(msg.id, 'failed', 'Campaign stopped by user');
      }
    }
    
    await db.saveCampaign(campaign);
    await db.addLog(campaignId, 'info', `Campaign stopped by user. All remaining pending messages cancelled.`);
    emitToSocket('campaign_update', campaign);
  }
}

module.exports = {
  setSocketIo,
  startWorker,
  stopWorker,
  pauseCampaign,
  resumeCampaign,
  stopCampaign
};
