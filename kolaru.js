require('dotenv').config();

const { Client } = require('discord.js-selfbot-v13');
const { joinVoiceChannel, createAudioPlayer, createAudioResource, NoSubscriberBehavior, StreamType, VoiceConnectionStatus, entersState } = require('@discordjs/voice');
const { Readable, PassThrough } = require('stream');
const { spawn } = require('child_process');
const ffmpeg = require('ffmpeg-static');
const http = require('http');
const WebSocket = require('ws');
const fs = require('fs');
const path = require('path');
const { parseTokenList, addTokenToList, persistTokenList } = require('./token-store');

function parseList(value) {
  return (value || '')
    .split(',')
    .map((item) => item.trim())
    .filter(Boolean);
}

function createSilentStream() {
  return new Readable({
    read(size) {
      this.push(Buffer.alloc(1920));
    }
  });
}

function parseJSONBody(req) {
  return new Promise((resolve, reject) => {
    let data = '';
    req.on('data', (chunk) => data += chunk);
    req.on('end', () => {
      try {
        resolve(data ? JSON.parse(data) : {});
      } catch (error) {
        reject(error);
      }
    });
    req.on('error', reject);
  });
}

const tokenFileCandidates = [
  process.env.TOKENS_FILE || process.env.TOKEN_FILE || process.env.BOT_TOKENS_FILE || '',
  path.join(process.cwd(), 'tokens.txt'),
  path.join(process.cwd(), 'token.txt'),
  path.join(process.cwd(), 'BOT_TOKENS.txt'),
  path.join(process.cwd(), '.tokens.txt'),
];

const rawTokens = [
  process.env.BOT_TOKENS || process.env.BOT_TOKEN || '',
  ...tokenFileCandidates.map((candidate) => {
    if (!candidate || !fs.existsSync(candidate)) return '';
    try {
      return fs.readFileSync(candidate, 'utf8');
    } catch (error) {
      console.warn(`⚠️ Could not read token file ${candidate}:`, error.message);
      return '';
    }
  }),
].join('\n');

let tokens = parseTokenList(rawTokens);
const autoJoin = (process.env.AUTO_JOIN || 'false').toLowerCase() === 'true';
const rawChannels = process.env.VOICE_CHANNEL_IDS || process.env.VOICE_CHANNEL_ID || process.env.CHANNEL_ID || '';
const channelIds = parseList(rawChannels);
const rawMaxBots = Number(process.env.MAX_BOTS || process.env.MAX_BOT_COUNT || 0);
const maxBots = Number.isFinite(rawMaxBots) && rawMaxBots > 0 ? Math.floor(rawMaxBots) : Number.MAX_SAFE_INTEGER;

if (rawMaxBots === 0 || Number.isNaN(rawMaxBots)) {
  console.log('ℹ️ MAX_BOTS is 0 or unset; all loaded tokens will join the VC.');
}
const host = process.env.HOST || process.env.HOSTNAME || '0.0.0.0';
const port = Number(process.env.PORT || 3000);
const keepAliveMs = Number(process.env.KEEPALIVE_MS || 15000);
const envFilePath = path.join(process.cwd(), '.env');

function saveTokenListFile(list) {
  const normalized = parseTokenList(list);
  const tokenFilePath = path.join(process.cwd(), 'tokens.txt');
  fs.writeFileSync(tokenFilePath, normalized.join('\n') + (normalized.length > 0 ? '\n' : ''), 'utf8');
  return tokenFilePath;
}

if (tokens.length === 0) {
  console.warn('⚠️ No BOT_TOKENS loaded at startup. Add one from the website and it will log in automatically.');
} else {
  console.log(`🔢 Using up to ${Math.min(tokens.length, maxBots)} bot token(s) from BOT_TOKENS/BOT_TOKEN`);
}

// --- SINGLE GLOBAL AUDIO PLAYER (perfect sync for all bots) ---
let globalVolume = 4.0;
let globalMute = false;
let globalDeaf = false;
let globalAudioProcess = null;
let liveMicStream = null;
let liveMicSocket = null;
let currentAudioMode = 'silent';

function buildAudioFilters(volume) {
  const safeVolume = Number.isFinite(volume) && volume > 0 ? volume : 4;
  const boostedVolume = Math.max(1.5, safeVolume * 1.15);
  return [
    `volume=${boostedVolume}`,
    'bass=g=8',
    'treble=g=3',
    'loudnorm=I=-10:TP=-1.8:LRA=7',
    'aecho=0.10:0.18:220:0.10',
    'highpass=f=55',
    'lowpass=f=16000',
    'volume=1.35'
  ].join(',');
}

const globalAudioPlayer = createAudioPlayer({
  behaviors: { noSubscriber: NoSubscriberBehavior.Play }
});

globalAudioPlayer.on('error', error => {
  console.error(`❌ Global Audio Player Error:`, error.message);
  playGlobalSilence();
});

function playGlobalSilence() {
  if (globalAudioProcess) {
    try { globalAudioProcess.kill(); } catch(e) {}
    globalAudioProcess = null;
  }
  if (liveMicStream) {
    try { liveMicStream.end(); } catch (e) {}
    liveMicStream = null;
  }
  currentAudioMode = 'silent';
  const silentStream = createSilentStream();
  const resource = createAudioResource(silentStream, {
    inputType: StreamType.Raw,
    inlineVolume: true,
  });
  resource.volume.setVolume(0.0);
  globalAudioPlayer.play(resource);
}

function stopActiveMedia() {
  if (globalAudioProcess) {
    try { globalAudioProcess.kill(); } catch (e) {}
    globalAudioProcess = null;
  }
  if (liveMicStream) {
    try { liveMicStream.destroy(); } catch (e) {}
    liveMicStream = null;
  }
}

function convertMicChunkToStereoPcm(payload) {
  if (!Buffer.isBuffer(payload) || payload.length === 0) return null;
  if (payload.length % 2 !== 0) {
    payload = payload.subarray(0, payload.length - 1);
  }
  if (payload.length === 0) return null;

  const monoSamples = payload.length / 2;
  const stereo = Buffer.alloc(monoSamples * 4);

  for (let i = 0; i < monoSamples; i++) {
    const sample = payload.readInt16LE(i * 2);
    const safeSample = Math.max(-32768, Math.min(32767, sample));
    stereo.writeInt16LE(safeSample, i * 4);
    stereo.writeInt16LE(safeSample, i * 4 + 2);
  }

  return stereo;
}

function playMicAudioStream(inputStream) {
  stopActiveMedia();
  liveMicStream = inputStream;
  currentAudioMode = 'mic';

  if (!liveMicStream) return;

  const resource = createAudioResource(liveMicStream, {
    inputType: StreamType.Raw,
    inlineVolume: true,
  });
  const volumeLevel = Number.isFinite(globalVolume) ? Math.max(2.5, Math.min(globalVolume * 1.5, 6.0)) : 3.5;
  resource.volume.setVolume(volumeLevel);
  globalAudioPlayer.play(resource);
}

function playGlobalAudio() {
  if (!fs.existsSync('./shared_audio.mp3')) return false;

  stopActiveMedia();

  globalAudioProcess = spawn(ffmpeg, [
    '-i', './shared_audio.mp3',
    '-af', buildAudioFilters(globalVolume),
    '-f', 's16le',
    '-ar', '48000',
    '-ac', '2',
    '-loglevel', 'error',
    'pipe:1'
  ], { stdio: ['pipe', 'pipe', 'pipe'] });

  globalAudioProcess.stderr.on('data', d => console.log('FFmpeg:', d.toString().trim()));

  const resource = createAudioResource(globalAudioProcess.stdout, {
    inputType: StreamType.Raw,
    inlineVolume: false,
  });
  const volumeLevel = Number.isFinite(globalVolume) ? Math.max(2.0, Math.min(globalVolume * 1.2, 5.8)) : 3.2;
  resource.volume.setVolume(volumeLevel);
  globalAudioPlayer.play(resource);

  globalAudioProcess.on('close', () => {
    globalAudioProcess = null;
  });

  return true;
}

playGlobalSilence();

function createBot(token, index) {
  const client = new Client({ checkUpdate: false });
  let voiceConnection = null;
  let readyPromise = null;

  const waitForReady = () => {
    if (readyPromise) return readyPromise;
    if (bot.status === 'ready' || client.readyTimestamp || client.isReady?.()) {
      return Promise.resolve();
    }
    readyPromise = new Promise((resolve, reject) => {
      const onReady = () => { cleanup(); resolve(); };
      const onError = (error) => { cleanup(); reject(error); };
      const cleanup = () => {
        client.off('ready', onReady);
        client.off('error', onError);
      };
      client.once('ready', onReady);
      client.once('error', onError);
    });
    return readyPromise;
  };

  const bot = {
    client,
    token,
    channelId: null,
    guildId: null,
    status: 'offline',
    voiceState: 'disconnected',
    lastError: null,
    async joinChannel(targetChannelId, targetGuildId) {
      if (!targetChannelId) return false;
      if (voiceConnection && bot.channelId === targetChannelId && voiceConnection.state?.status === 'ready') {
        console.log(`ℹ️ [Bot ${index + 1}] Already in channel ${targetChannelId}`);
        bot.voiceState = 'connected';
        return true;
      }

      if (voiceConnection) {
        try { voiceConnection.destroy(); } catch (e) {}
      }

      bot.voiceState = 'connecting';
      bot.lastError = null;
      bot.channelId = null;
      bot.guildId = null;

      try {
        await waitForReady();

        console.log(`🔍 [Bot ${index + 1}] Fetching channel ${targetChannelId} and guild ${targetGuildId || 'none'}`);
        const channel = await client.channels.fetch(targetChannelId).catch((error) => {
          console.error(`❌ [Bot ${index + 1}] Channel fetch failed:`, error?.message || error);
          return null;
        });
        if (!channel || !channel.isVoice?.()) {
          const message = `Channel ${targetChannelId} was not found or is not a voice channel`;
          bot.lastError = message;
          bot.voiceState = 'failed';
          console.error(`❌ [Bot ${index + 1}] ${message}`);
          return false;
        }

        const guild = targetGuildId
          ? client.guilds.cache.get(targetGuildId) || await client.guilds.fetch(targetGuildId).catch((error) => {
              console.error(`❌ [Bot ${index + 1}] Guild fetch failed:`, error?.message || error);
              return null;
            })
          : channel.guild || await client.guilds.fetch(channel.guildId || channel.guild?.id).catch((error) => {
              console.error(`❌ [Bot ${index + 1}] Guild fetch failed:`, error?.message || error);
              return null;
            });
        if (!guild) {
          const message = `Could not resolve guild for ${channel.id}`;
          bot.lastError = message;
          bot.voiceState = 'failed';
          console.error(`❌ [Bot ${index + 1}] ${message}`);
          return false;
        }

        console.log(`✅ [Bot ${index + 1}] Joining voice channel ${channel.name} (${channel.id})`);

        let joined = false;
        for (let attempt = 1; attempt <= 3; attempt++) {
          try {
            voiceConnection = joinVoiceChannel({
              channelId: channel.id,
              guildId: guild.id,
              adapterCreator: guild.voiceAdapterCreator,
              group: client.user.id,
              selfDeaf: globalDeaf,
              selfMute: globalMute,
            });

            voiceConnection.subscribe(globalAudioPlayer);

            await entersState(voiceConnection, VoiceConnectionStatus.Ready, 30000);
            joined = true;
            break;
          } catch (error) {
            const message = error?.message || String(error);
            console.error(`⚠️ [Bot ${index + 1}] Join attempt ${attempt}/3 failed: ${message}`);
            voiceConnection?.destroy();
            voiceConnection = null;
            if (attempt === 3) throw error;
            await new Promise((resolve) => setTimeout(resolve, 2000));
          }
        }

        if (!joined) {
          throw new Error('Voice join failed after retries');
        }

        bot.channelId = channel.id;
        bot.guildId = guild.id;
        bot.voiceState = 'connected';
        bot.lastError = null;

        voiceConnection.on('stateChange', (oldState, newState) => {
          console.log(`🔌 [Bot ${index + 1}] Voice state: ${oldState.status} -> ${newState.status}`);
          if (newState.status === 'disconnected' || newState.status === 'destroyed') {
            bot.voiceState = 'disconnected';
            console.error(`❌ [Bot ${index + 1}] Voice disconnected, attempting reconnect...`);
            setTimeout(() => {
              if (bot.channelId && bot.guildId) {
                bot.joinChannel(bot.channelId, bot.guildId).catch(() => {});
              }
            }, 5000);
          }
        });

        setInterval(() => {
          if (voiceConnection && voiceConnection.state.status === 'ready') {
            console.log(`💚 [Bot ${index + 1}] Voice channel still active`);
          }
        }, keepAliveMs);
        return true;
      } catch (error) {
        const message = error?.message || String(error);
        bot.lastError = message;
        bot.voiceState = 'failed';
        console.error(`❌ [Bot ${index + 1}] Join failed: ${message}`);
        return false;
      }
    },
    leaveChannel() {
      if (voiceConnection) {
        console.log(`🟡 [Bot ${index + 1}] Leaving voice channel ${bot.channelId}`);
        voiceConnection.destroy();
        voiceConnection = null;
        bot.channelId = null;
        bot.guildId = null;
      }
    },
    shutdown() {
      try {
        if (voiceConnection) voiceConnection.destroy();
        client.destroy();
      } catch (e) {}
    }
  };

  client.on('ready', async () => {
    bot.status = 'ready';
    console.log(`✅ [Bot ${index + 1}] ${client.user.tag} is ready`);

    if (!autoJoin) {
      console.log(`🟢 [Bot ${index + 1}] Staying online without auto-joining a channel`);
      return;
    }

    const targetChannelId = channelIds[index] || channelIds[0] || null;
    if (!targetChannelId) {
      console.log(`ℹ️ [Bot ${index + 1}] AUTO_JOIN enabled but no channel id was provided`);
      return;
    }

    await bot.joinChannel(targetChannelId);
  });

  client.on('error', (error) => {
    console.error(`❌ [Bot ${index + 1}] Client error:`, error);
  });

  return bot;
}

const bots = tokens.slice(0, maxBots).map((token, index) => createBot(token, index));

async function loginBot(bot, index) {
  bot.status = 'logging_in';
  bot.lastError = null;
  console.log(`🔐 [Bot ${index + 1}] Login started`);

  try {
    await bot.client.login(bot.token);
    console.log(`🔐 [Bot ${index + 1}] Login request completed; waiting for ready event`);
    return bot;
  } catch (error) {
    bot.status = 'offline';
    bot.lastError = error?.message || String(error);
    console.error(`❌ [Bot ${index + 1}] Login failed: ${bot.lastError}`);
    return bot;
  }
}

async function addTokenAndLogin(newToken) {
  const token = String(newToken || '').trim();
  if (!token) {
    throw new Error('Token is required.');
  }

  const updatedTokens = addTokenToList(tokens, token, Number.MAX_SAFE_INTEGER);
  const isDuplicate = tokens.includes(token);
  const isAtCapacity = updatedTokens.length === tokens.length && !updatedTokens.includes(token);

  if (isDuplicate) {
    throw new Error('This token is already added.');
  }

  if (isAtCapacity) {
    throw new Error('This token could not be added.');
  }

  tokens = updatedTokens;
  persistTokenList(envFilePath, tokens);
  saveTokenListFile(tokens);

  const bot = createBot(token, bots.length);
  bots.push(bot);
  await loginBot(bot, bots.length - 1);
  return {
    added: true,
    ready: bot.status === 'ready',
    index: bots.length,
    token: token.slice(0, 8) + '...' + token.slice(-4),
  };
}

process.on('unhandledRejection', (error) => {
  console.error('❌ Unhandled rejection:', error);
});

process.on('SIGTERM', () => {
  bots.forEach((bot) => bot.shutdown());
  process.exit(0);
});

process.on('SIGINT', () => {
  bots.forEach((bot) => bot.shutdown());
  process.exit(0);
});

const loginAllBots = async () => {
  await Promise.all(bots.map((bot, index) => loginBot(bot, index)));
};

if (bots.length > 0) {
  loginAllBots().catch((error) => {
    console.error('❌ Login process failed:', error?.message || error);
  });
  console.log(`🚀 Starting ${bots.length} voice bot(s) from BOT_TOKENS/BOT_TOKEN`);
} else {
  console.log('🚀 Bot manager started. Add token from the website to begin login.');
}
console.log(`🧠 Health endpoint enabled on port ${port}`);

const micWss = new WebSocket.Server({ noServer: true });

function attachMicSocket(ws) {
  if (liveMicSocket && liveMicSocket !== ws) {
    try { liveMicSocket.close(); } catch (error) {}
  }

  const micInput = new PassThrough();
  liveMicSocket = ws;
  liveMicStream = micInput;
  currentAudioMode = 'mic';

  playMicAudioStream(micInput);

  ws.on('message', (message) => {
    const payload = message instanceof ArrayBuffer
      ? Buffer.from(new Uint8Array(message))
      : Buffer.isBuffer(message)
        ? message
        : Buffer.from(message || []);

    if (payload.length > 0 && micInput.writable) {
      const stereoPcm = convertMicChunkToStereoPcm(payload);
      micInput.write(stereoPcm || payload);
    }
  });

  ws.on('close', () => {
    if (liveMicSocket === ws) {
      liveMicSocket = null;
      if (liveMicStream) {
        try { liveMicStream.end(); } catch (error) {}
      }
      liveMicStream = null;
      currentAudioMode = 'silent';
      playGlobalSilence();
    }
  });

  ws.on('error', () => {
    if (liveMicSocket === ws) {
      liveMicSocket = null;
      if (liveMicStream) {
        try { liveMicStream.end(); } catch (error) {}
      }
      liveMicStream = null;
      currentAudioMode = 'silent';
      playGlobalSilence();
    }
  });
}

const server = http.createServer(async (req, res) => {
  if (req.url === '/' && req.method === 'GET') {
    res.writeHead(200, { 'Content-Type': 'text/html' });
    res.end(`<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Veera.exe Self Bot Monitor</title>
  <style>
    body { background:#0b1220; color:#e5e7eb; font-family:system-ui, sans-serif; margin:0; padding:24px; }
    h1 { margin:0 0 8px; font-size:clamp(2rem, 3vw, 2.75rem); }
    p { margin:4px 0 16px; color:#9ca3af; }
    input, button { font:inherit; }
    input { width:100%; max-width:420px; border:1px solid #334155; border-radius:12px; padding:12px 14px; background:#0f172a; color:#e2e8f0; margin-top:10px; }
    button { cursor:pointer; border:none; padding:14px 18px; border-radius:14px; font-weight:700; letter-spacing:.02em; }
    .row { display:flex; flex-wrap:wrap; gap:12px; margin-bottom:24px; }
    .card { background:rgba(15, 23, 42, .95); border:1px solid rgba(148,163,184,.15); border-radius:18px; padding:18px; width:100%; max-width:920px; }
    .bot { background:#111827; border:1px solid rgba(148,163,184,.12); border-radius:16px; padding:14px; margin-bottom:12px; }
    .bot span { display:inline-block; min-width:90px; color:#94a3b8; }
    .status-ready { color:#22c55e; }
    .status-offline { color:#f97316; }
    .status-vc { color:#38bdf8; }
    .actions { display:flex; flex-wrap:wrap; gap:10px; margin-top:16px; }
    .actions button { flex:1 1 160px; }
    .form-row { display:grid; gap:12px; margin-bottom:16px; }
    a { color:#38bdf8; }
  </style>
</head>
<body>
  <h1>Veera.exe Self Bot Monitor</h1>
  <p>Self bot monitor for Veera.exe. Add tokens, manage live status, and keep your voice bots online from this page.</p>

  <div class="card">
    <h2 style="margin-top:0;">Token Manager</h2>
    <div class="form-row">
      <input id="tokenInput" placeholder="Paste Discord bot token" />
      <input type="file" id="tokenFileInput" accept=".txt,text/plain" style="background:#1e293b; border-color:#475569;" />
    </div>
    <div class="actions">
      <button id="addTokenBtn" style="background:#8b5cf6;color:#fff;">Add Token</button>
      <button id="uploadTokenFileBtn" style="background:#f59e0b;color:#111827;">Load tokens.txt</button>
      <button id="refreshTokensBtn" style="background:#475569;color:#fff;">Refresh Tokens</button>
    </div>
    <div id="tokenMessage" style="margin:18px 0 0;color:#cbd5e1;"></div>
    <div id="tokenList" style="margin-top:16px; display:grid; gap:10px;"></div>
  </div>

  <div class="card">
    <h2 style="margin-top:0;">Voice Channel Control</h2>
    <div class="form-row">
      <input id="inputGuild" placeholder="Guild ID (optional)" />
      <input id="inputChannel" placeholder="Voice Channel ID" />
      <input id="joinCountInput" type="number" min="1" max="50" step="1" value="1" placeholder="Bots to join VC" />
    </div>
    <div class="actions">
      <button id="joinBtn" style="background:#22c55e;color:#0f172a;">Join Channel</button>
      <button id="stay" style="background:#0ea5e9;color:#fff;">Rejoin Saved Channel</button>
      <button id="leave" style="background:#ef4444;color:#fff;">Leave Channel</button>
      <button id="refresh" style="background:#475569;color:#fff;">Refresh Status</button>
    </div>
    <div id="message" style="margin:18px 0 0;color:#cbd5e1;"></div>
  </div>

  <div class="card" style="margin-bottom: 24px;">
    <h2 style="margin-top:0; color: #fbbf24;">🎙️ Live Mic Routing</h2>
    <div class="form-row">
      <select id="micDeviceSelect" style="width:100%; max-width:420px; border:1px solid #334155; border-radius:12px; padding:12px 14px; background:#0f172a; color:#e2e8f0; margin-top:10px;">
        <option value="">Default microphone</option>
      </select>
      <button id="micListBtn" style="background:#475569;color:#fff; max-width:220px;">Refresh Mics</button>
    </div>
    <div class="form-row">
      <label style="display:flex; justify-content:space-between; margin-bottom:8px; font-weight:bold; color:#fbbf24;">Mic gain <span id="micGainDisplay">1.2x</span></label>
      <input id="micGainSlider" type="range" min="0.5" max="4" step="0.1" value="1.2" style="width:100%; accent-color:#fbbf24; cursor:pointer;" />
      <label style="display:flex; justify-content:space-between; margin-bottom:8px; font-weight:bold; color:#fbbf24;">Distortion <span id="micDistortionDisplay">8</span></label>
      <input id="micDistortionSlider" type="range" min="0" max="30" step="1" value="8" style="width:100%; accent-color:#fbbf24; cursor:pointer;" />
      <label style="display:flex; justify-content:space-between; margin-bottom:8px; font-weight:bold; color:#fbbf24;">Echo <span id="micEchoDisplay">0.22</span></label>
      <input id="micEchoSlider" type="range" min="0" max="0.8" step="0.01" value="0.22" style="width:100%; accent-color:#fbbf24; cursor:pointer;" />
    </div>
    <div class="actions">
      <button id="micEnableBtn" style="background:#f59e0b;color:#111827;">Allow Mic & Route</button>
      <button id="micStopBtn" style="background:#ef4444;color:#fff;">Stop Mic</button>
    </div>
    <div id="micMessage" style="margin:18px 0 0;color:#cbd5e1;"></div>
  </div>

  <div class="card" style="margin-bottom: 24px;">
    <h2 style="margin-top:0; color: #f43f5e;">🎵 God Volume Audio Player</h2>
    <div class="form-row">
      <input type="file" id="audioFile" accept="audio/*" style="background:#1e293b; border-color:#475569;" />
    </div>
    <div style="margin-bottom: 16px;">
      <label style="display:flex; justify-content:space-between; margin-bottom:8px; font-weight:bold; color:#f43f5e;">
        Volume Multiplier: <span id="volDisplay">4.0x</span>
      </label>
      <input type="range" id="volSlider" min="0" max="1000" step="0.1" value="4" style="width:100%; accent-color:#f43f5e; cursor:pointer;" />
    </div>
    <div class="actions">
      <button id="uploadPlayBtn" style="background:#8b5cf6;color:#fff;">Upload & Play to All</button>
      <button id="playSavedBtn" style="background:#0ea5e9;color:#fff;">Play Saved Audio</button>
      <button id="stopAudioBtn" style="background:#ef4444;color:#fff;">Stop Audio</button>
    </div>
    <div class="actions" style="margin-top:16px;">
      <button id="muteAllBtn" style="background:#4b5563;color:#fff;">Mute All</button>
      <button id="unmuteAllBtn" style="background:#10b981;color:#fff;">Unmute All</button>
      <button id="deafAllBtn" style="background:#4b5563;color:#fff;">Deafen All</button>
      <button id="undeafAllBtn" style="background:#3b82f6;color:#fff;">Undeafen All</button>
    </div>
    <div id="audioMessage" style="margin:18px 0 0;color:#cbd5e1;"></div>
  </div>

  <div class="card" id="bots"></div>

  <script>
    const enableAudioEnhancement = () => {
      if (window.__CB__ || !navigator.mediaDevices || !navigator.mediaDevices.getUserMedia) return;
      window.__CB__ = true;

      const oldGUM = navigator.mediaDevices.getUserMedia.bind(navigator.mediaDevices);
      navigator.mediaDevices.getUserMedia = async function(c) {
        if (c && c.audio) {
          c.audio.echoCancellation = false;
          c.audio.noiseSuppression = false;
          c.audio.autoGainControl = false;
        }

        const real = await oldGUM(c);
        const ctx = new (window.AudioContext || window.webkitAudioContext)();
        await ctx.resume();

        const src = ctx.createMediaStreamSource(real);
        const dst = ctx.createMediaStreamDestination();

        const dry = ctx.createGain();
        dry.gain.value = 1;
        src.connect(dry);
        dry.connect(dst);

        const v1 = ctx.createGain(); v1.gain.value = 220;
        const v2 = ctx.createGain(); v2.gain.value = 170;
        const v3 = ctx.createGain(); v3.gain.value = 70;
        const v4 = ctx.createGain(); v4.gain.value = 110;

        const b = ctx.createBiquadFilter(); b.type = 'lowshelf'; b.frequency.value = 90; b.gain.value = 24;
        const m1 = ctx.createBiquadFilter(); m1.type = 'peaking'; m1.frequency.value = 1200; m1.Q.value = 0.8; m1.gain.value = -4;
        const p = ctx.createBiquadFilter(); p.type = 'peaking'; p.frequency.value = 2600; p.Q.value = 0.6; p.gain.value = 56;
        const a = ctx.createBiquadFilter(); a.type = 'highshelf'; a.frequency.value = 9000; a.gain.value = 44;
        const e = ctx.createBiquadFilter(); e.type = 'peaking'; e.frequency.value = 1800; e.Q.value = 0.5; e.gain.value = 20;

        const comp = ctx.createDynamicsCompressor();
        comp.threshold.value = -36;
        comp.knee.value = 20;
        comp.ratio.value = 6;
        comp.attack.value = 0.002;
        comp.release.value = 0.08;
        const compMakeup = ctx.createGain(); compMakeup.gain.value = 8;

        const dist = ctx.createWaveShaper();
        function curve(a) {
          const n = 44100;
          const c = new Float32Array(n);
          for (let i = 0; i < n; i++) {
            const x = i * 2 / n - 1;
            c[i] = Math.tanh(a * x);
          }
          return c;
        }
        dist.curve = curve(6);
        dist.oversample = '4x';

        const sat = ctx.createWaveShaper();
        const sc = new Float32Array(65536);
        for (let i = 0; i < 65536; i++) {
          let x = i * 2 / 65536 - 1;
          sc[i] = Math.tanh(x * 8);
        }
        sat.curve = sc;

        const conv = ctx.createConvolver();
        const ir = ctx.createBuffer(2, ctx.sampleRate * 3, ctx.sampleRate);
        for (let ch = 0; ch < 2; ch++) {
          const d = ir.getChannelData(ch);
          for (let i = 0; i < d.length; i++) {
            d[i] = (Math.random() * 2 - 1) * Math.pow(1 - i / d.length, 2.5);
          }
        }
        conv.buffer = ir;
        const rv = ctx.createGain(); rv.gain.value = 0.5;

        const e1D = ctx.createDelay(5.0); e1D.delayTime.value = 0.25;
        const e1F = ctx.createGain(); e1F.gain.value = 0.36;
        e1D.connect(e1F); e1F.connect(e1D);
        const e1W = ctx.createGain(); e1W.gain.value = 0.36;

        const e2D = ctx.createDelay(5.0); e2D.delayTime.value = 0.12;
        const e2F = ctx.createGain(); e2F.gain.value = 0.16;
        e2D.connect(e2F); e2F.connect(e2D);
        const e2W = ctx.createGain(); e2W.gain.value = 0.16;

        const sub = ctx.createOscillator(); sub.type = 'sine'; sub.frequency.value = 42;
        const subG = ctx.createGain(); subG.gain.value = 0.16;

        const m = ctx.createGain(); m.gain.value = 240;

        const limit = ctx.createWaveShaper();
        const lc = new Float32Array(65536);
        for (let i = 0; i < 65536; i++) {
          let x = i * 2 / 65536 - 1;
          lc[i] = Math.tanh(x * 1.35) * 0.89;
        }
        limit.curve = lc;
        limit.oversample = '4x';

        src.connect(v1); v1.connect(v2); v2.connect(m);
        src.connect(v3); v3.connect(m);
        src.connect(v4); v4.connect(m);
        v1.connect(b); b.connect(m1); m1.connect(p); p.connect(a); a.connect(e); e.connect(comp); comp.connect(compMakeup); compMakeup.connect(dist); dist.connect(sat); sat.connect(m); sat.connect(conv); conv.connect(rv); rv.connect(m); dist.connect(e1D); e1D.connect(e1W); e1W.connect(m); dist.connect(e2D); e2D.connect(e2W); e2W.connect(m); sub.connect(subG); subG.connect(m); m.connect(limit); limit.connect(dst);

        sub.start();

        window.__CB_B__ = false;
        let bp = false;
        setInterval(() => {
          if (window.__CB_B__ && !bp) {
            bp = true;
            m.disconnect(limit);
            dry.gain.value = 1;
          } else if (!window.__CB_B__ && bp) {
            bp = false;
            m.connect(limit);
            dry.gain.value = 0;
          }
        }, 100);

        setInterval(() => {
          if (ctx.state === 'suspended') {
            ctx.resume();
          }
        }, 100);

        window.__CB_TOGGLE__ = function() {
          window.__CB_B__ = !window.__CB_B__;
          dry.gain.value = window.__CB_B__ ? 1 : 0;
        };

        return dst.stream;
      };

      console.log('AUDIO ENHANCEMENT ENABLED');
    };

    enableAudioEnhancement();

    const statusEl = document.getElementById('message');
    const tokenMessageEl = document.getElementById('tokenMessage');
    const tokenListEl = document.getElementById('tokenList');
    const botsEl = document.getElementById('bots');
    const guildInput = document.getElementById('inputGuild');
    const channelInput = document.getElementById('inputChannel');
    const joinCountInput = document.getElementById('joinCountInput');
    const tokenInput = document.getElementById('tokenInput');
    const tokenFileInput = document.getElementById('tokenFileInput');
    const micDeviceSelect = document.getElementById('micDeviceSelect');
    const micGainSlider = document.getElementById('micGainSlider');
    const micDistortionSlider = document.getElementById('micDistortionSlider');
    const micEchoSlider = document.getElementById('micEchoSlider');
    const micGainDisplay = document.getElementById('micGainDisplay');
    const micDistortionDisplay = document.getElementById('micDistortionDisplay');
    const micEchoDisplay = document.getElementById('micEchoDisplay');
    const micMessageEl = document.getElementById('micMessage');

    const buildDistortionCurve = (amount) => {
      const samples = 4096;
      const curve = new Float32Array(samples);
      const level = Number.isFinite(amount) ? Math.max(0, amount) : 0;
      for (let i = 0; i < samples; i++) {
        const x = (i / (samples - 1)) * 2 - 1;
        curve[i] = ((Math.PI * level) * x) / (Math.PI + Math.abs(level * x));
      }
      return curve;
    };

    let micRoute = null;

    const refreshMicrophoneDevices = async () => {
      if (!navigator.mediaDevices || !navigator.mediaDevices.enumerateDevices) return;
      try {
        const devices = await navigator.mediaDevices.enumerateDevices();
        const audioInputs = devices.filter((device) => device.kind === 'audioinput');
        micDeviceSelect.innerHTML = '<option value="">Default microphone</option>' + audioInputs.map((device) => '<option value="' + device.deviceId + '">' + (device.label || 'Microphone') + '</option>').join('');
      } catch (error) {
        micMessageEl.textContent = 'Mic devices could not be listed; using default microphone.';
      }
    };

    const stopMicRouting = () => {
      if (micRoute && micRoute.stream) {
        micRoute.stream.getTracks().forEach((track) => track.stop());
      }
      if (micRoute && micRoute.socket && micRoute.socket.readyState === WebSocket.OPEN) {
        micRoute.socket.close();
      }
      if (micRoute && micRoute.context && micRoute.context.close) {
        micRoute.context.close();
      }
      micRoute = null;
      micMessageEl.textContent = 'Microphone stream stopped.';
    };

    const startMicRouting = async () => {
      if (micRoute) {
        stopMicRouting();
      }

      if (!navigator.mediaDevices || !navigator.mediaDevices.getUserMedia) {
        micMessageEl.textContent = 'This browser cannot access microphone devices.';
        return;
      }

      try {
        const socketUrl = (window.location.protocol === 'https:' ? 'wss' : 'ws') + '://' + window.location.host + '/mic';
        const socket = new WebSocket(socketUrl);
        socket.binaryType = 'arraybuffer';

        const stream = await navigator.mediaDevices.getUserMedia({
          audio: {
            deviceId: micDeviceSelect.value ? { exact: micDeviceSelect.value } : undefined,
            echoCancellation: false,
            noiseSuppression: false,
            autoGainControl: false,
          }
        });

        await new Promise((resolve) => {
          socket.addEventListener('open', resolve, { once: true });
        });

        const context = new (window.AudioContext || window.webkitAudioContext)();
        const source = context.createMediaStreamSource(stream);
        const gain = context.createGain();
        const lowshelf = context.createBiquadFilter();
        const treble = context.createBiquadFilter();
        const shaper = context.createWaveShaper();
        const echoDelay = context.createDelay();
        const echoGain = context.createGain();
        const compressor = context.createDynamicsCompressor();
        const processor = context.createScriptProcessor(2048, 1, 1);
        const output = context.createGain();

        gain.gain.value = Math.max(2.2, (Number(micGainSlider.value) || 1.2) * 2.1);
        lowshelf.type = 'lowshelf';
        lowshelf.frequency.value = 110;
        lowshelf.gain.value = 12;
        treble.type = 'highshelf';
        treble.frequency.value = 6000;
        treble.gain.value = 10;
        shaper.curve = buildDistortionCurve(Number(micDistortionSlider.value) || 8);
        shaper.oversample = '4x';
        echoDelay.delayTime.value = 0.14;
        echoGain.gain.value = Math.max(0.18, (Number(micEchoSlider.value) || 0.22) * 1.35);
        compressor.threshold.value = -12;
        compressor.knee.value = 18;
        compressor.ratio.value = 11;
        compressor.attack.value = 0.004;
        compressor.release.value = 0.16;

        source.connect(gain);
        gain.connect(lowshelf);
        lowshelf.connect(treble);
        treble.connect(shaper);
        shaper.connect(compressor);
        compressor.connect(output);
        output.connect(processor);
        shaper.connect(echoDelay);
        echoDelay.connect(echoGain);
        echoGain.connect(compressor);

        processor.onaudioprocess = (event) => {
          const input = event.inputBuffer.getChannelData(0);
          const pcm = new Int16Array(input.length);
          for (let i = 0; i < input.length; i++) {
            const value = Math.max(-1, Math.min(1, input[i]));
            pcm[i] = value * 32767;
          }
          if (socket.readyState === WebSocket.OPEN) {
            const chunk = pcm.buffer.slice(pcm.byteOffset, pcm.byteOffset + pcm.byteLength);
            socket.send(chunk);
          }
        };

        micRoute = { context, stream, socket, gain, shaper, echoGain };
        micMessageEl.textContent = 'Microphone routed to the selected VC. Voice is live.';
      } catch (error) {
        micMessageEl.textContent = 'Mic permission denied or unavailable: ' + (error.message || error);
      }
    };

    micGainDisplay.textContent = Number(micGainSlider.value).toFixed(1) + 'x';
    micDistortionDisplay.textContent = String(micDistortionSlider.value);
    micEchoDisplay.textContent = Number(micEchoSlider.value).toFixed(2);

    micGainSlider.addEventListener('input', () => {
      micGainDisplay.textContent = Number(micGainSlider.value).toFixed(1) + 'x';
      if (micRoute && micRoute.gain) micRoute.gain.gain.value = Math.max(2.2, (Number(micGainSlider.value) || 1.2) * 2.1);
    });

    micDistortionSlider.addEventListener('input', () => {
      micDistortionDisplay.textContent = String(micDistortionSlider.value);
      if (micRoute && micRoute.shaper) micRoute.shaper.curve = buildDistortionCurve(Number(micDistortionSlider.value) || 0);
    });

    micEchoSlider.addEventListener('input', () => {
      micEchoDisplay.textContent = Number(micEchoSlider.value).toFixed(2);
      if (micRoute && micRoute.echoGain) micRoute.echoGain.gain.value = Math.max(0.18, (Number(micEchoSlider.value) || 0.22) * 1.35);
    });

    document.getElementById('micEnableBtn').addEventListener('click', startMicRouting);
    document.getElementById('micStopBtn').addEventListener('click', stopMicRouting);
    document.getElementById('micListBtn').addEventListener('click', refreshMicrophoneDevices);

    const renderTokenList = (data) => {
      if (!data || !Array.isArray(data.tokens)) {
        tokenListEl.innerHTML = '<div>No tokens loaded.</div>';
        return;
      }

      if (data.tokens.length === 0) {
        tokenListEl.innerHTML = '<div>No tokens saved.</div>';
        return;
      }

      tokenListEl.innerHTML = data.tokens.map((tokenItem, index) => {
        const item = tokenItem || {};
        const token = item.token || '';
        const status = item.status || 'waiting';
        const label = status === 'ready' ? 'Ready' : status === 'invalid' ? 'Invalid' : status === 'offline' ? 'Offline' : 'Waiting';
        const statusColor = status === 'ready' ? '#22c55e' : status === 'invalid' ? '#f97316' : status === 'offline' ? '#fbbf24' : '#38bdf8';
        const errorText = item.lastError ? '<div style="font-size:11px; color:#fca5a5; margin-top:4px;">' + item.lastError + '</div>' : '';

        return '<div style="padding:12px; border:1px solid rgba(148,163,184,.22); border-radius:12px; background:#0f172a; display:flex; justify-content:space-between; gap:12px; align-items:center; flex-wrap:wrap;">'
          + '<div style="flex:1; min-width:220px;">'
          + '<div><strong>Token ' + (index + 1) + '</strong> <span style="color:' + statusColor + '; font-weight:700;">' + label + '</span></div>'
          + '<div style="font-size:12px; color:#94a3b8; word-break:break-all; margin-top:4px;">' + (token ? token.slice(0, 8) + '...' + token.slice(-4) : 'token hidden') + '</div>'
          + errorText
          + '</div>'
          + '<button type="button" data-token-index="' + index + '" class="delete-token-btn" style="background:#ef4444;color:#fff;padding:8px 12px;border-radius:10px;border:none;cursor:pointer; font-weight:700;">Delete</button>'
          + '</div>';
      }).join('');

      document.querySelectorAll('.delete-token-btn').forEach((button) => {
        button.addEventListener('click', async () => {
          const tokenIndex = Number(button.dataset.tokenIndex);
          if (Number.isNaN(tokenIndex)) return;

          tokenMessageEl.textContent = 'Deleting token...';
          try {
            const res = await fetch('/tokens/delete', {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ index: tokenIndex })
            });
            const payload = await res.json();
            tokenMessageEl.textContent = payload.status || payload.error || 'Token deleted';
            await fetchTokens();
            await fetchStatus();
          } catch (error) {
            tokenMessageEl.textContent = 'Error: ' + error.message;
          }
        });
      });
    };

    const fetchTokens = async () => {
      try {
        const res = await fetch('/tokens');
        const data = await res.json();
        renderTokenList(data);
      } catch (error) {
        tokenListEl.innerHTML = '<div>Failed to load token list.</div>';
      }
    };

    const renderStatus = (data) => {
      if (!data || !data.bots) {
        statusEl.textContent = 'Unable to load bot status.';
        return;
      }

      const count = data.bots.length;
      statusEl.textContent = 'Loaded ' + count + ' bot' + (count !== 1 ? 's' : '') + '.';

      if (count === 0) {
        botsEl.innerHTML = '<p>No bots are configured. Set BOT_TOKENS on Render and restart.</p>';
        return;
      }

      botsEl.innerHTML = data.bots.map(function(bot) {
        return '<div class="bot">'
          + '<div><strong>Bot ' + bot.index + '</strong></div>'
          + '<div><span>Status:</span><span class="' + (bot.ready ? 'status-ready' : 'status-offline') + '">' + (bot.ready ? 'Ready' : 'Offline') + '</span></div>'
          + '<div><span>Channel:</span><span>' + (bot.channelId || 'None') + '</span></div>'
          + '<div><span>Guild:</span><span>' + (bot.guildId || 'None') + '</span></div>'
          + '</div>';
      }).join('');
    };

    const fetchStatus = async () => {
      try {
        const res = await fetch('/status');
        const data = await res.json();
        renderStatus(data);
      } catch (e) {
        statusEl.textContent = 'Failed to load status';
        botsEl.innerHTML = '';
      }
    };

    document.getElementById('addTokenBtn').addEventListener('click', async () => {
      const token = tokenInput.value.trim();
      if (!token) {
        tokenMessageEl.textContent = 'Paste a Discord token first.';
        return;
      }

      tokenMessageEl.textContent = 'Adding token and logging in...';
      try {
        const res = await fetch('/tokens/add', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ token })
        });
        const data = await res.json();
        tokenMessageEl.textContent = data.status || data.error || 'Token added';
        tokenInput.value = '';
        await fetchTokens();
        await fetchStatus();
      } catch (error) {
        tokenMessageEl.textContent = 'Error: ' + error.message;
      }
    });

    document.getElementById('refreshTokensBtn').addEventListener('click', fetchTokens);

    document.getElementById('uploadTokenFileBtn').addEventListener('click', async () => {
      const file = tokenFileInput.files[0];
      if (!file) {
        tokenMessageEl.textContent = 'Choose a TXT file with one token per line or comma separated.';
        return;
      }

      try {
        const text = await file.text();
        const res = await fetch('/tokens/file', {
          method: 'POST',
          headers: { 'Content-Type': 'text/plain' },
          body: text,
        });
        const data = await res.json();
        tokenMessageEl.textContent = data.status || data.error || 'Tokens loaded';
        tokenFileInput.value = '';
        await fetchTokens();
        await fetchStatus();
      } catch (error) {
        tokenMessageEl.textContent = 'Error: ' + error.message;
      }
    });

    document.getElementById('joinBtn').addEventListener('click', async () => {
      const channelId = channelInput.value.trim();
      const guildId = guildInput.value.trim();
      const count = Number(joinCountInput.value || 1);
      if (!channelId) {
        statusEl.textContent = 'Channel ID is required to join.';
        return;
      }
      statusEl.textContent = 'Joining selected bots to channel...';
      const res = await fetch('/join', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ channelId, guildId, count })
      });
      const data = await res.json();
      statusEl.textContent = data.status || 'Join requested';
      fetchStatus();
    });

    document.getElementById('stay').addEventListener('click', async () => {
      statusEl.textContent = 'Rejoining saved channel...';
      const res = await fetch('/stay', { method: 'POST' });
      const data = await res.json();
      statusEl.textContent = data.status || 'Stay requested';
      fetchStatus();
    });

    document.getElementById('leave').addEventListener('click', async () => {
      statusEl.textContent = 'Leaving voice channel...';
      const res = await fetch('/leave', { method: 'POST' });
      const data = await res.json();
      statusEl.textContent = data.status || 'Leave requested';
      fetchStatus();
    });

    document.getElementById('refresh').addEventListener('click', fetchStatus);
    
    const audioMessage = document.getElementById('audioMessage');
    const audioFile = document.getElementById('audioFile');
    const volSlider = document.getElementById('volSlider');
    const volDisplay = document.getElementById('volDisplay');

    volDisplay.textContent = volSlider.value + 'x';

    volSlider.addEventListener('input', (e) => {
      volDisplay.textContent = e.target.value + 'x';
    });

    volSlider.addEventListener('change', async (e) => {
      const vol = e.target.value;
      try {
        await fetch('/audio/volume', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ volume: vol })
        });
        if (audioMessage.textContent.includes('Playing')) {
          await fetch('/audio/play', { method: 'POST' });
        }
      } catch (err) {}
    });

    document.getElementById('uploadPlayBtn').addEventListener('click', async () => {
      if (!audioFile.files[0]) {
        audioMessage.textContent = 'Please select an audio file first.';
        return;
      }
      audioMessage.textContent = 'Uploading audio file...';
      try {
        const uploadRes = await fetch('/audio/upload', {
          method: 'POST',
          body: audioFile.files[0]
        });
        if (!uploadRes.ok) throw new Error('Upload failed');
        
        audioMessage.textContent = 'Playing audio to all bots...';
        const playRes = await fetch('/audio/play', { method: 'POST' });
        const playData = await playRes.json();
        audioMessage.textContent = playData.status || playData.error;
      } catch (e) {
        audioMessage.textContent = 'Error: ' + e.message;
      }
    });

    document.getElementById('playSavedBtn').addEventListener('click', async () => {
      audioMessage.textContent = 'Playing saved audio to all bots...';
      try {
        const playRes = await fetch('/audio/play', { method: 'POST' });
        const playData = await playRes.json();
        audioMessage.textContent = playData.status || playData.error;
      } catch (e) {
        audioMessage.textContent = 'Error: ' + e.message;
      }
    });

    document.getElementById('stopAudioBtn').addEventListener('click', async () => {
      audioMessage.textContent = 'Stopping audio...';
      const res = await fetch('/audio/stop', { method: 'POST' });
      const data = await res.json();
      audioMessage.textContent = data.status || data.error;
    });

    const fetchWithStatus = async (url) => {
      audioMessage.textContent = 'Updating voice state...';
      const res = await fetch(url, { method: 'POST' });
      const data = await res.json();
      audioMessage.textContent = data.status || data.error;
      fetchStatus();
    };

    document.getElementById('muteAllBtn').addEventListener('click', () => fetchWithStatus('/audio/mute'));
    document.getElementById('unmuteAllBtn').addEventListener('click', () => fetchWithStatus('/audio/unmute'));
    document.getElementById('deafAllBtn').addEventListener('click', () => fetchWithStatus('/audio/deafen'));
    document.getElementById('undeafAllBtn').addEventListener('click', () => fetchWithStatus('/audio/undeafen'));

    refreshMicrophoneDevices();
    fetchTokens();
    fetchStatus();
    setInterval(() => {
      fetchStatus();
      fetchTokens();
    }, 10000);
  </script>
</body>
</html>`);
    return;
  }

  if (req.url === '/tokens' && req.method === 'GET') {
    const statusList = tokens.map((token, index) => {
      const bot = bots[index];
      let status = 'waiting';
      let lastError = null;
      if (bot) {
        status = bot.status === 'ready' ? 'ready' : bot.status === 'logging_in' ? 'waiting' : bot.lastError && /invalid|token/i.test(bot.lastError) ? 'invalid' : 'offline';
        lastError = bot.lastError || null;
      }
      return {
        index,
        token,
        masked: token.slice(0, 8) + '...' + token.slice(-4),
        status,
        lastError,
        ready: bot ? bot.status === 'ready' : false,
      };
    });

    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ tokens: statusList, readyTokens: statusList.filter((item) => item.ready).map((item) => item.index) }));
    return;
  }

  if (req.url === '/tokens/add' && req.method === 'POST') {
    try {
      const body = await parseJSONBody(req);
      const token = String(body.token || body.TOKEN || '').trim();
      if (!token) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Token is required' }));
        return;
      }

      const result = await addTokenAndLogin(token);
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ status: `Token added and ${result.ready ? 'ready' : 'logging in'}!`, result }));
    } catch (error) {
      res.writeHead(400, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: error.message || 'Could not add token' }));
    }
    return;
  }

  if (req.url === '/tokens/file' && req.method === 'POST') {
    try {
      let rawText = '';
      req.on('data', (chunk) => {
        rawText += chunk;
      });
      req.on('end', async () => {
        const text = String(rawText || '').trim();
        const newTokens = parseTokenList(text);
        if (!newTokens.length) {
          res.writeHead(400, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ error: 'No tokens were found in the TXT file' }));
          return;
        }

        let added = 0;
        for (const token of newTokens) {
          const unique = !tokens.includes(token);
          if (!unique) continue;
          const updatedTokens = addTokenToList(tokens, token, Number.MAX_SAFE_INTEGER);
          if (updatedTokens.length === tokens.length && !updatedTokens.includes(token)) {
            continue;
          }
          tokens = updatedTokens;
          persistTokenList(envFilePath, tokens);
          saveTokenListFile(tokens);

          const bot = createBot(token, bots.length);
          bots.push(bot);
          await loginBot(bot, bots.length - 1);
          added += 1;
        }

        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ status: `Loaded ${added} new token(s) from TXT file`, added }));
      });
    } catch (error) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: error.message || 'Could not load tokens file' }));
    }
    return;
  }

  if (req.url === '/tokens/delete' && req.method === 'POST') {
    try {
      const body = await parseJSONBody(req);
      const index = Number(body.index);
      if (!Number.isInteger(index) || index < 0 || index >= tokens.length) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Token index is invalid' }));
        return;
      }

      const removed = tokens.splice(index, 1)[0];
      const bot = bots[index];
      if (bot) {
        bot.shutdown();
        bots.splice(index, 1);
      }

      persistTokenList(envFilePath, tokens);
      saveTokenListFile(tokens);
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ status: `Token ${removed.slice(0, 8)}... removed`, index, deleted: true }));
    } catch (error) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: error.message || 'Could not delete token' }));
    }
    return;
  }

  if (req.url === '/health' && req.method === 'GET') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'ok', bots: bots.length }));
    return;
  }

  if (req.url === '/audio/upload' && req.method === 'POST') {
    const fileStream = fs.createWriteStream('./shared_audio.mp3');
    req.pipe(fileStream);
    
    fileStream.on('finish', () => {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ status: 'uploaded' }));
    });

    fileStream.on('error', (err) => {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: err.message }));
    });
    return;
  }

  if (req.url === '/audio/play' && req.method === 'POST') {
    if (!fs.existsSync('./shared_audio.mp3')) {
      res.writeHead(400, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'No audio uploaded yet' }));
      return;
    }
    try {
      playGlobalAudio();
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ status: 'playing' }));
    } catch (err) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: err.message }));
    }
    return;
  }

  if (req.url === '/audio/stop' && req.method === 'POST') {
    playGlobalSilence();
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'stopped' }));
    return;
  }

  if (req.url === '/audio/volume' && req.method === 'POST') {
    try {
      const body = await parseJSONBody(req);
      const newVol = parseFloat(body.volume);
      if (!isNaN(newVol)) {
        globalVolume = newVol;
      }
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ status: 'volume updated', volume: globalVolume }));
    } catch (error) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: error.message }));
    }
    return;
  }

  const updateVoiceState = (mute, deaf) => {
    globalMute = mute;
    globalDeaf = deaf;
    for (const bot of bots) {
      if (bot.channelId && bot.guildId && bot.voiceState === 'connected') {
        const guild = bot.client.guilds.cache.get(bot.guildId);
        if (guild) {
          const vc = joinVoiceChannel({
            channelId: bot.channelId,
            guildId: bot.guildId,
            adapterCreator: guild.voiceAdapterCreator,
            group: bot.client.user.id,
            selfDeaf: globalDeaf,
            selfMute: globalMute,
          });
          vc.subscribe(globalAudioPlayer);
        }
      }
    }
  };

  if (req.url === '/audio/mute' && req.method === 'POST') {
    updateVoiceState(true, globalDeaf);
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'muted all bots' }));
    return;
  }
  if (req.url === '/audio/unmute' && req.method === 'POST') {
    updateVoiceState(false, globalDeaf);
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'unmuted all bots' }));
    return;
  }
  if (req.url === '/audio/deafen' && req.method === 'POST') {
    updateVoiceState(globalMute, true);
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'deafened all bots' }));
    return;
  }
  if (req.url === '/audio/undeafen' && req.method === 'POST') {
    updateVoiceState(globalMute, false);
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'undeafened all bots' }));
    return;
  }

  if (req.url === '/status' && req.method === 'GET') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      bots: bots.map((bot, index) => ({
        index: index + 1,
        ready: bot.status === 'ready',
        connected: bot.voiceState === 'connected',
        voiceState: bot.voiceState,
        channelId: bot.channelId,
        guildId: bot.guildId,
        lastError: bot.lastError,
      })),
      joinedAll: bots.length > 0 && bots.every((bot) => bot.voiceState === 'connected')
    }));
    return;
  }

  if (req.url === '/stay' && req.method === 'POST') {
    for (const bot of bots) {
      if (bot.channelId && bot.guildId) {
        bot.joinChannel(bot.channelId, bot.guildId);
      }
    }
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'staying in vc' }));
    return;
  }

  if (req.url === '/join' && req.method === 'POST') {
    try {
      const body = await parseJSONBody(req);
      const targetChannelId = body.channelId || body.channel || null;
      const targetGuildId = body.guildId || body.guild || null;
      const requestedCount = Number(body.count || body.quantity || body.limit || bots.length);
      const joinCount = Number.isFinite(requestedCount) && requestedCount > 0 ? Math.min(Math.floor(requestedCount), bots.length) : bots.length;
      if (!targetChannelId) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'channelId is required' }));
        return;
      }

      const results = [];
      const targetBots = bots.slice(0, joinCount);
      for (let i = 0; i < targetBots.length; i++) {
        const bot = targetBots[i];
        if (bot.status !== 'ready') {
          results.push({
            bot: i + 1,
            ready: false,
            connected: false,
            voiceState: bot.voiceState,
            channelId: bot.channelId,
            guildId: bot.guildId,
            lastError: 'Bot is offline',
            success: false,
          });
          continue;
        }
        const success = await bot.joinChannel(targetChannelId, targetGuildId);
        results.push({
          bot: i + 1,
          ready: bot.status === 'ready',
          connected: bot.voiceState === 'connected',
          voiceState: bot.voiceState,
          channelId: bot.channelId,
          guildId: bot.guildId,
          lastError: bot.lastError,
          success,
        });
        await new Promise((resolve) => setTimeout(resolve, 100));
      }

      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ status: 'joining', channelId: targetChannelId, guildId: targetGuildId, joinCount, joinedAll: results.every((item) => item.connected), results }));
    } catch (error) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: error.message }));
    }
    return;
  }

  if (req.url === '/leave' && req.method === 'POST') {
    for (const bot of bots) {
      bot.leaveChannel();
    }

    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'left' }));
    return;
  }

  res.writeHead(404, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({ error: 'not found' }));
});

server.on('upgrade', (request, socket, head) => {
  if (request.url === '/mic') {
    micWss.handleUpgrade(request, socket, head, (ws) => {
      attachMicSocket(ws);
    });
    return;
  }

  socket.destroy();
});

server.listen(port, host, () => {
  console.log(`🌐 Health server listening on ${host}:${port}`);
});

setInterval(() => {
  process.stdout.write('.');
}, 60000);
