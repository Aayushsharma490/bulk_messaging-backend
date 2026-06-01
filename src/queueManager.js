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

// Map of Baileys message key → {campaignId, messageId}
// Used by sessionManager's messages.update listener to track real delivery ACK
const sentKeyMap = {};

// Called by sessionManager when WhatsApp ACKs a message
// ack: 1 = sent to WA server (1 tick), 2 = delivered to device (2 ticks), 3 = read (blue ticks)
async function handleMessageAck(baileysKey, ack) {
  const entry = sentKeyMap[baileysKey];
  if (!entry) return;

  const { campaignId, messageId } = entry;

  if (ack >= 2) {
    // Actually delivered to the recipient's phone
    await db.updateMessageStatus(messageId, 'sent');
    await db.addLog(campaignId, 'success', `✅ Delivered (confirmed by WhatsApp) to message ID ${messageId}`);
    emitToSocket('message_ack', { messageId, ack, campaignId, status: 'delivered' });
    delete sentKeyMap[baileysKey]; // cleanup
  } else if (ack === 1) {
    // Reached WA server but not yet to device (transient, we wait for ack 2)
    emitToSocket('message_ack', { messageId, ack, campaignId, status: 'server_ack' });
  } else if (ack < 0) {
    // Failed at server level
    await db.updateMessageStatus(messageId, 'failed', 'WhatsApp server rejected the message (server-level failure)');
    await db.addLog(campaignId, 'error', `❌ WhatsApp SERVER rejected message to ${messageId}. This may indicate spam filtering.`);
    emitToSocket('message_ack', { messageId, ack, campaignId, status: 'failed' });
    delete sentKeyMap[baileysKey];
  }
}

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

// ─── Human-Like Delay Generator ──────────────────────────────────────────────
// Uses weighted random tiers to mimic real human sending patterns.
// Humans are NOT consistent — they sometimes reply fast, sometimes slow.
// A bot with fixed delays is trivially detectable by WhatsApp's AI.
function generateHumanDelay(hasMedia) {
  const rand = Math.random();
  let delaySeconds;

  if (hasMedia) {
    // Media takes longer to "look at" — shift all tiers up
    if (rand < 0.55)      delaySeconds = 20 + Math.floor(Math.random() * 15); // 20–35s (55%)
    else if (rand < 0.85) delaySeconds = 35 + Math.floor(Math.random() * 20); // 35–55s (30%)
    else                  delaySeconds = 70 + Math.floor(Math.random() * 60); // 70–130s (15%)
  } else {
    // Text-only messages
    if (rand < 0.60)      delaySeconds = 12 + Math.floor(Math.random() * 14); // 12–26s (60%)
    else if (rand < 0.90) delaySeconds = 26 + Math.floor(Math.random() * 20); // 26–46s (30%)
    else                  delaySeconds = 60 + Math.floor(Math.random() * 70); // 60–130s (10%)
  }

  // Absolute floor: never go below 12 seconds no matter what
  return Math.max(delaySeconds, 12);
}

// ─── Send a single message with human-like behavior ──────────────────────────
async function sendSingleMessage(campaign, message, client) {
  const campaignId = campaign.id;
  const hasMedia = !!(campaign.media && campaign.media.filename);

  // ── Generate human-like delay ────────────────────────────────────────────
  const delaySeconds = generateHumanDelay(hasMedia);

  console.log(`Sending message ${message.id} to ${message.phoneNumber} in ${delaySeconds}s.`);
  await db.addLog(campaignId, 'info', `⏳ Next message to ${message.name || message.phoneNumber} in ${delaySeconds}s...`);

  // ── Night-hour warning (11 PM – 7 AM) ────────────────────────────────────
  const hour = new Date().getHours();
  if (hour >= 23 || hour < 7) {
    await db.addLog(campaignId, 'info', `⚠️ Sending at night (${hour}:00). Higher ban risk — consider pausing and resuming in morning.`);
  }

  // ── Interruptible sleep loop ──────────────────────────────────────────────
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

  // ── Re-check session is still alive after sleep ───────────────────────────
  const isStillReady = sessionManager.isClientReady(campaign.sessionId);
  if (!isStillReady) {
    console.log(`Session dropped during sleep for message ${message.id}. Will retry next tick.`);
    await db.addLog(campaignId, 'error', `Connection lost during delay. Will retry...`);
    return;
  }

  try {
    // ── 1. Sanitize phone number ──────────────────────────────────────────
    const cleanNumber = message.phoneNumber.replace(/[^0-9]/g, '');
    let targetJid = `${cleanNumber}@s.whatsapp.net`;

    // ── 2. Pre-check: verify this number is registered on WhatsApp ────────
    // Baileys sendMessage() returns a local key even for non-WA numbers.
    // Without this check, we get fake 'sent' logs for numbers not on WhatsApp.
    try {
      const waCheckResults = await withTimeout(
        client.onWhatsApp(cleanNumber),
        10000,
        'WhatsApp registration check timed out'
      );
      const waResult = Array.isArray(waCheckResults) ? waCheckResults[0] : waCheckResults;

      if (!waResult || !waResult.exists) {
        throw new Error('Number is not registered on WhatsApp');
      }

      // Use the canonical JID returned by WhatsApp if available
      if (waResult.jid) {
        targetJid = waResult.jid;
      }
    } catch (checkErr) {
      if (checkErr.message === 'Number is not registered on WhatsApp') {
        // Permanent failure — number genuinely not on WA
        throw checkErr;
      }
      // Network/timeout error on the check itself — proceed and try sending anyway
      console.warn(`WA registration check failed (network issue), proceeding with send:`, checkErr.message);
      await db.addLog(campaignId, 'info', `⚠️ Could not pre-verify number ${message.phoneNumber} (network). Attempting send anyway...`);
    }

    // ── 3. Personalize message ────────────────────────────────────────────
    let formattedText = message.messageContent;
    formattedText = formattedText.replace(/{name}/gi, message.name || '');
    if (message.customFields) {
      for (const [key, val] of Object.entries(message.customFields)) {
        const regex = new RegExp(`{${key}}`, 'gi');
        formattedText = formattedText.replace(regex, val || '');
      }
    }

    // ── 3. Simulate typing indicator (HUMAN BEHAVIOR) ─────────────────────
    // This shows the recipient a "typing..." bubble before the message arrives.
    // Real humans type before sending — bots don't. This reduces spam detection.
    try {
      await client.sendPresenceUpdate('composing', targetJid);
      // Typing duration: proportional to message length, capped at 6 seconds
      const typingMs = Math.min(2000 + Math.floor(formattedText.length * 30), 6000);
      await new Promise(resolve => setTimeout(resolve, typingMs));
      await client.sendPresenceUpdate('paused', targetJid);
    } catch (presenceErr) {
      // Non-fatal — continue even if presence update fails
      console.warn(`Presence update failed for ${targetJid} (non-fatal):`, presenceErr.message);
    }

    // ── 4. Send message (text or media) ──────────────────────────────────
    let sendResult;

    if (hasMedia) {
      const mediaPath = path.join(__dirname, '..', 'uploads', campaign.media.filename);
      try {
        const fileBuffer = await fs.readFile(mediaPath);
        const mimeType = campaign.media.mimeType || '';
        let mediaContent;

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

    // ── 5. Verify Baileys confirmation ────────────────────────────────────
    if (!sendResult || !sendResult.key) {
      throw new Error('No delivery receipt from WhatsApp. Number may not be on WhatsApp.');
    }

    // ── 6. Register key for ACK tracking ─────────────────────────────────
    // Store the Baileys message key so sessionManager's messages.update
    // listener can update the status when WhatsApp actually delivers the message.
    const baileysKey = JSON.stringify(sendResult.key);
    sentKeyMap[baileysKey] = { campaignId, messageId: message.id };

    // Clean up stale keys after 5 minutes (in case ACK never arrives)
    setTimeout(() => {
      delete sentKeyMap[baileysKey];
    }, 5 * 60 * 1000);

    // ── 7. Success ────────────────────────────────────────────────────────
    await db.updateMessageStatus(message.id, 'sent');
    await db.addLog(campaignId, 'success', `✅ Sent to ${message.name || message.phoneNumber} (${message.phoneNumber}) — waiting for delivery confirmation...`);

    campaign.batchSentCount = (campaign.batchSentCount || 0) + 1;
    await db.saveCampaign(campaign);
    emitToSocket('message_status', { messageId: message.id, status: 'sent', campaignId });

  } catch (err) {
    console.error(`Failed to send message ${message.id}:`, err);
    const errMsg = (err.message || '').toLowerCase();

    // Temporary — keep as pending and retry
    const isTemp =
      errMsg.includes('protocol error') ||
      errMsg.includes('context was destroyed') ||
      errMsg.includes('session closed') ||
      errMsg.includes('timed out') ||
      errMsg.includes('timeout') ||
      errMsg.includes('websocket') ||
      errMsg.includes('stream error') ||
      errMsg.includes('econnreset') ||
      errMsg.includes('socket hang up') ||
      errMsg.includes('disconnected') ||
      errMsg.includes('connection reset') ||
      errMsg.includes('write after end') ||
      errMsg.includes('network');

    if (isTemp) {
      await db.addLog(campaignId, 'error', `⚠️ Connection hiccup sending to ${message.name || message.phoneNumber}. Will retry shortly...`);
      return;
    }

    // Permanent failure
    await db.updateMessageStatus(message.id, 'failed', err.message || 'Unknown error');
    await db.addLog(campaignId, 'error', `❌ Failed: ${message.name || message.phoneNumber} — ${err.message}`);
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
  stopCampaign,
  handleMessageAck,  // called by sessionManager for WhatsApp delivery ACKs
  sentKeyMap         // exposed for reference / debugging
};
