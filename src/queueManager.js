const db = require('./db');
const sessionManager = require('./sessionManager');
const { MessageMedia } = require('whatsapp-web.js');
const fs = require('fs').promises;
const path = require('path');

// Helper to wrap promises with a timeout
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

let socketIo = null;
let workerInterval = null;
let isProcessing = false;

// Track active timers for quick interrupts
const activeCampaignSleeps = {};

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
  
  // Check campaigns and process messages every 3 seconds
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
  }, 3000);
  
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
    // Check if campaign is in cooldown
    if (campaign.cooldownUntil) {
      const cooldownTime = new Date(campaign.cooldownUntil);
      if (cooldownTime > now) {
        // Still cooling down, skip
        continue;
      } else {
        // Cooldown finished, remove flag and continue
        delete campaign.cooldownUntil;
        campaign.batchSentCount = 0;
        await db.saveCampaign(campaign);
        await db.addLog(campaign.id, 'info', `Batch cooldown ended. Resuming sending...`);
        emitToSocket('campaign_update', campaign);
      }
    }

    // Get client session
    const sessionId = campaign.sessionId;
    const client = sessionManager.getClient(sessionId);
    const isReady = sessionManager.isClientReady(sessionId);

    if (!client || !isReady) {
      console.log(`Session ${sessionId} is not ready for campaign ${campaign.id}`);
      campaign.status = 'paused';
      await db.saveCampaign(campaign);
      await db.addLog(campaign.id, 'error', `Campaign paused: WhatsApp session "${sessionId}" is disconnected.`);
      emitToSocket('campaign_update', campaign);
      continue;
    }

    // Get pending messages for this campaign
    const messages = await db.getMessages(campaign.id);
    const pendingMessages = messages.filter(m => m.status === 'pending');

    if (pendingMessages.length === 0) {
      // All messages processed
      campaign.status = 'completed';
      await db.saveCampaign(campaign);
      await db.addLog(campaign.id, 'success', `Campaign completed successfully! All messages processed.`);
      emitToSocket('campaign_update', campaign);
      continue;
    }

    // Check Batching logic
    const batchSize = campaign.batchSize || 200;
    const batchCooldown = campaign.batchCooldown || 300; // default 5 mins
    const sentInCurrentBatch = campaign.batchSentCount || 0;

    if (sentInCurrentBatch >= batchSize) {
      const cooldownUntil = new Date(Date.now() + batchCooldown * 1000).toISOString();
      campaign.cooldownUntil = cooldownUntil;
      await db.saveCampaign(campaign);
      
      const minutes = Math.round(batchCooldown / 60);
      await db.addLog(campaign.id, 'info', `Batch limit of ${batchSize} reached. Cooling down for ${minutes} minutes to protect account...`);
      emitToSocket('campaign_update', campaign);
      continue;
    }

    // We take the next message to send
    const message = pendingMessages[0];
    
    // We start the sending process (runs asynchronously, but we only send ONE message per worker tick to respect delays)
    // Wait, since we want to sleep between messages, let's run the send process for this message
    await sendSingleMessage(campaign, message, client);
    break; // Only process one message per worker tick to prevent race conditions and overlapping loops
  }
}

// Send a single message with individual delays and safety checks
async function sendSingleMessage(campaign, message, client) {
  const campaignId = campaign.id;
  
  // Set random delay: 5-10 seconds for faster sending
  const minDelay = campaign.minDelay || 5;
  const maxDelay = campaign.maxDelay || 10;
  const delaySeconds = Math.floor(Math.random() * (maxDelay - minDelay + 1)) + minDelay;

  console.log(`Sending message ${message.id} to ${message.phoneNumber} in ${delaySeconds} seconds.`);
  await db.addLog(campaignId, 'info', `Preparing to send to ${message.name} (${message.phoneNumber}) in ${delaySeconds}s...`);

  // Interruptible sleep loop
  let interrupted = false;
  activeCampaignSleeps[campaignId] = true;
  
  for (let s = 0; s < delaySeconds; s++) {
    // Check if campaign was paused or stopped
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

  try {
    // 1. Sanitize and format the phone number
    let cleanNumber = message.phoneNumber.replace(/[^0-9]/g, '');
    
    // Auto-prefix logic: if number doesn't have country code (e.g. length is 10), we can assume +91 or warn.
    // However, we assume the user imports clean numbers with country codes.
    // Let's ensure it has @c.us at the end
    if (!cleanNumber.endsWith('@c.us')) {
      cleanNumber = `${cleanNumber}@c.us`;
    }

    // 2. Validate WhatsApp registration (Wrapped in 15s timeout to prevent hanging)
    const isRegistered = await withTimeout(
      client.isRegisteredUser(cleanNumber), 
      15000, 
      'WhatsApp check timed out'
    );
    
    if (!isRegistered) {
      await db.updateMessageStatus(message.id, 'failed', 'Invalid number (not registered on WhatsApp)');
      await db.addLog(campaignId, 'error', `Failed to send to ${message.name}: Number is not registered on WhatsApp.`);
      
      // Update campaign counters
      campaign.batchSentCount = (campaign.batchSentCount || 0) + 1;
      await db.saveCampaign(campaign);
      
      emitToSocket('message_status', { messageId: message.id, status: 'failed', campaignId });
      return;
    }

    // 3. Format message content
    // Personalized variables: Replace {name} or any custom field
    let formattedText = message.messageContent;
    formattedText = formattedText.replace(/{name}/gi, message.name || '');
    
    if (message.customFields) {
      for (const [key, val] of Object.entries(message.customFields)) {
        const regex = new RegExp(`{${key}}`, 'gi');
        formattedText = formattedText.replace(regex, val || '');
      }
    }

    // 4. Send Message (with or without media) - Wrapped in 25s/30s timeout to prevent hanging
    if (campaign.media && campaign.media.filename) {
      // Media path
      const mediaPath = path.join(__dirname, '..', 'uploads', campaign.media.filename);
      try {
        const fileBuffer = await fs.readFile(mediaPath);
        const base64Data = fileBuffer.toString('base64');
        const media = new MessageMedia(campaign.media.mimeType, base64Data, campaign.media.filename);
        
        await withTimeout(
          client.sendMessage(cleanNumber, media, { caption: formattedText }),
          30000,
          'Media message send timed out'
        );
      } catch (err) {
        console.error('Error reading/sending media:', err);
        throw new Error(`Media send failed: ${err.message}`);
      }
    } else {
      await withTimeout(
        client.sendMessage(cleanNumber, formattedText),
        20000,
        'Message send timed out'
      );
    }

    // 5. Success Logging & Updates
    await db.updateMessageStatus(message.id, 'sent');
    await db.addLog(campaignId, 'success', `Message sent to ${message.name} (${message.phoneNumber})`);
    
    // Update batch counter
    campaign.batchSentCount = (campaign.batchSentCount || 0) + 1;
    await db.saveCampaign(campaign);
    
    emitToSocket('message_status', { messageId: message.id, status: 'sent', campaignId });
  } catch (err) {
    console.error(`Failed to send message ${message.id}:`, err);
    
    // Check for temporary browser context reloads or CDP disconnects
    const errMsg = err.message || '';
    const isTempBrowserIssue = 
      errMsg.includes('Protocol error') || 
      errMsg.includes('context was destroyed') || 
      errMsg.includes('Session closed') ||
      errMsg.includes('browser has already been closed') ||
      errMsg.includes('Target closed') ||
      errMsg.includes('Network.enable');

    if (isTempBrowserIssue) {
      console.log(`Temporary browser issue detected, keeping message ${message.id} as pending to retry...`);
      await db.addLog(campaignId, 'error', `Connection hiccup: Browser context was reset. Retrying send to ${message.name || message.phoneNumber} shortly...`);
      // Return without updating DB status to failed, preserving it as pending
      return;
    }

    await db.updateMessageStatus(message.id, 'failed', err.message || 'Unknown sending error');
    await db.addLog(campaignId, 'error', `Failed to send to ${message.name}: ${err.message}`);
    
    campaign.batchSentCount = (campaign.batchSentCount || 0) + 1;
    await db.saveCampaign(campaign);
    
    emitToSocket('message_status', { messageId: message.id, status: 'failed', campaignId });
  }
}

// Pause campaign
async function pauseCampaign(campaignId) {
  const campaign = await db.getCampaign(campaignId);
  if (campaign && campaign.status === 'running') {
    campaign.status = 'paused';
    await db.saveCampaign(campaign);
    await db.addLog(campaignId, 'info', `Campaign paused by user.`);
    emitToSocket('campaign_update', campaign);
  }
}

// Resume campaign
async function resumeCampaign(campaignId) {
  const campaign = await db.getCampaign(campaignId);
  if (campaign && (campaign.status === 'paused' || campaign.status === 'scheduled')) {
    campaign.status = 'running';
    // Clear cooldown if resuming manually, resets batch count
    delete campaign.cooldownUntil;
    campaign.batchSentCount = 0;
    
    await db.saveCampaign(campaign);
    await db.addLog(campaignId, 'info', `Campaign resumed by user.`);
    emitToSocket('campaign_update', campaign);
  }
}

// Stop campaign
async function stopCampaign(campaignId) {
  const campaign = await db.getCampaign(campaignId);
  if (campaign) {
    campaign.status = 'stopped';
    delete campaign.cooldownUntil;
    
    // Update all pending messages in this campaign to failed/cancelled
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
