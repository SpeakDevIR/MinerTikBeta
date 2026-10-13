/**
 * RVN WebSocket Proxy Server
 * جایگزین websocket-stratum-server.com
 * پروتکل رو عیناً مثل earnify شبیه‌سازی می‌کنه
 */
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const net = require('net');
const path = require('path');

const PORT = process.env.PORT || 3000;

// ─── HTTP + Express (برای سرو فایل‌های استاتیک) ───
const app = express();
app.use(express.static(path.join(__dirname, 'public')));

const server = http.createServer(app);

// ─── Socket.io با CORS آزاد ───
const io = new Server(server, {
  cors: { origin: '*', methods: ['GET', 'POST'] },
  transports: ['websocket']
});

// ─── آمار کلی ───
const STATS = {
  clients: 0,
  totalShares: 0,
  startTime: Date.now()
};

// ─── مدیریت هر کلاینت ───
io.on('connection', (socket) => {
  STATS.clients++;
  console.log(`[+] Client connected: ${socket.id} | Total: ${STATS.clients}`);

  let stratum = null;
  let buffer = '';
  let pingInterval = null;

  const state = {
    extranonce1: '',
    extranonce2_size: 0,
    difficulty: 1,
    worker: '',
    password: 'c=RVN',
    authorized: false,
    connected: false
  };

  // ═══════════════════════════════════════════
  // توابع کمکی
  // ═══════════════════════════════════════════

  function sendStratum(obj) {
    if (!stratum || stratum.destroyed) return;
    try {
      stratum.write(JSON.stringify(obj) + '\n');
    } catch (e) {
      console.error('[!] sendStratum error:', e.message);
    }
  }

  function cleanup() {
    if (pingInterval) {
      clearInterval(pingInterval);
      pingInterval = null;
    }
    if (stratum) {
      try { stratum.end(); } catch (_) {}
      try { stratum.destroy(); } catch (_) {}
      stratum = null;
    }
  }

  // ═══════════════════════════════════════════
  // پردازش پیام‌های Stratum (از استخر)
  // ═══════════════════════════════════════════

  function handleStratumLine(line) {
    if (!line.trim()) return;

    let msg;
    try {
      msg = JSON.parse(line);
    } catch (e) {
      console.error('[!] Parse error:', e.message, '| Line:', line.slice(0, 200));
      return;
    }

    // ─── پاسخ subscribe ───
    if (msg.id === 1 && msg.result) {
      state.extranonce1 = msg.result[1];
      state.extranonce2_size = msg.result[2];
      console.log(`[→] Subscribed | extranonce1=${state.extranonce1} size=${state.extranonce2_size}`);

      // حالا authorize بفرست
      sendStratum({
        id: 2,
        method: 'mining.authorize',
        params: [state.worker, state.password]
      });
      return;
    }

    // ─── پاسخ authorize ───
    if (msg.id === 2) {
      if (msg.result === true) {
        state.authorized = true;
        state.connected = true;
        console.log(`[✓] Authorized: ${state.worker}`);
        socket.emit('can start');
      } else {
        console.error('[✗] Authorization failed:', msg.error);
        socket.emit('error', 'Authorization failed: ' + JSON.stringify(msg.error));
      }
      return;
    }

    // ─── تنظیم سختی ───
    if (msg.method === 'mining.set_difficulty') {
      state.difficulty = msg.params[0];
      console.log(`[i] Difficulty: ${state.difficulty}`);
      return;
    }

    // ─── کار جدید ───
    if (msg.method === 'mining.notify') {
      const [
        job_id, prevhash, coinb1, coinb2,
        merkle_branch, version, nbits, ntime, clean_jobs
      ] = msg.params;

      console.log(`[⛏] New job: ${job_id}`);

      socket.emit('work', {
        job_id,
        jobId: job_id,
        prevhash,
        coinb1,
        coinb2,
        merkle_branch,
        version,
        nbits,
        ntime,
        clean_jobs,
        extraNonce1: state.extranonce1,
        extraNonce2Size: state.extranonce2_size,
        miningDiff: state.difficulty
      });
      return;
    }

    // ─── پاسخ submit ───
    if (typeof msg.id === 'number' && msg.id >= 1000) {
      if (msg.result === true) {
        STATS.totalShares++;
        console.log(`[✓] Share ACCEPTED | Total: ${STATS.totalShares}`);
        socket.emit('submit_result', { accepted: true });
      } else {
        console.log(`[✗] Share REJECTED:`, msg.error);
        socket.emit('submit failed', JSON.stringify(msg.error));
      }
      return;
    }

    // ─── ping از سمت استخر ───
    if (msg.method === 'mining.ping') {
      sendStratum({
        id: msg.id,
        method: 'mining.pong',
        params: msg.params
      });
      return;
    }

    // ─── خطای سراسری از استخر ───
    if (msg.method === 'client.reconnect' || msg.method === 'client.show_message') {
      console.log('[Stratum Notice]', JSON.stringify(msg));
      socket.emit('error', JSON.stringify(msg));
    }
  }

  // ═══════════════════════════════════════════
  // اتصال به استخر
  // ═══════════════════════════════════════════

  function connectStratum(opts) {
    cleanup();
    buffer = '';

    console.log(`[...] Connecting to ${opts.server}:${opts.port}`);

    try {
      stratum = net.connect({ host: opts.server, port: opts.port });
    } catch (e) {
      console.error('[✗] Failed to create socket:', e.message);
      socket.emit('error', 'Cannot connect to pool');
      return;
    }

    stratum.setEncoding('utf8');
    stratum.setNoDelay(true);

    // تایم‌اوت اتصال
    const connectTimeout = setTimeout(() => {
      if (!state.connected) {
        console.error('[✗] Connection timeout');
        socket.emit('error', 'Pool connection timeout');
        cleanup();
      }
    }, 20000);

    stratum.on('connect', () => {
      clearTimeout(connectTimeout);
      console.log(`[✓] Stratum TCP connected`);

      sendStratum({
        id: 1,
        method: 'mining.subscribe',
        params: ['rvn-ws-proxy/1.0']
      });

      // ping نگه‌دارنده
      pingInterval = setInterval(() => {
        sendStratum({ id: 0, method: 'mining.ping', params: [] });
      }, 30000);
    });

    stratum.on('data', (chunk) => {
      buffer += chunk;
      let idx;
      while ((idx = buffer.indexOf('\n')) !== -1) {
        const line = buffer.slice(0, idx).trim();
        buffer = buffer.slice(idx + 1);
        if (line) handleStratumLine(line);
      }
      // محدودیت حافظه
      if (buffer.length > 1024 * 1024) buffer = '';
    });

    stratum.on('error', (err) => {
      clearTimeout(connectTimeout);
      console.error('[✗] Stratum error:', err.message);
      socket.emit('error', 'Pool error: ' + err.message);
    });

    stratum.on('close', () => {
      clearTimeout(connectTimeout);
      console.log('[i] Stratum connection closed');
      cleanup();
      if (socket.connected) {
        socket.emit('error', 'Pool disconnected');
      }
    });
  }

  // ═══════════════════════════════════════════
  // رویدادهای Socket.io (از کلاینت مرورگر)
  // ═══════════════════════════════════════════

  socket.on('start', (opts) => {
    console.log(`[→] start | algo=${opts.algo} worker=${opts.stratum?.worker}`);
    state.worker = opts.stratum?.worker || '';
    state.password = opts.stratum?.password || 'c=RVN';
    connectStratum({
      server: opts.stratum?.server,
      port: opts.stratum?.port
    });
  });

  socket.on('submit', (data) => {
    if (!stratum || !state.authorized) {
      console.log('[!] submit dropped (not authorized)');
      return;
    }
    const id = Math.floor(Math.random() * 100000) + 1000;
    console.log(`[⛏→] submit | job=${data.job_id} nonce=${data.nonce}`);

    sendStratum({
      id,
      method: 'mining.submit',
      params: [
        state.worker,
        data.job_id,
        data.extranonce2,
        data.ntime,
        data.nonce
      ]
    });
  });

  socket.on('hashrate', (data) => {
    // فقط برای مانیتور (اختیاری)
  });

  socket.on('disconnect', (reason) => {
    STATS.clients--;
    console.log(`[-] Client disconnected: ${socket.id} | ${reason} | Total: ${STATS.clients}`);
    cleanup();
  });

  socket.on('error', (err) => {
    console.error(`[!] Socket error from ${socket.id}:`, err);
  });
});

// ─── راه‌اندازی سرور ───
server.listen(PORT, () => {
  console.log('');
  console.log('═══════════════════════════════════════════');
  console.log(`  🚀 RVN WebSocket Proxy Server`);
  console.log(`  ─────────────────────────────────────`);
  console.log(`  Listening on: http://0.0.0.0:${PORT}`);
  console.log(`  WebSocket endpoint: ws://<your-domain>:${PORT}/socket.io/`);
  console.log(`  Serving HTML from: ./public/`);
  console.log('═══════════════════════════════════════════');
  console.log('');
});

// ─── مدیریت graceful shutdown ───
process.on('SIGINT', () => {
  console.log('\n[i] Shutting down...');
  io.close(() => {
    server.close(() => process.exit(0));
  });
});
