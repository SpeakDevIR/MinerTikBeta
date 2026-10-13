const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const net = require('net');

const PORT = process.env.PORT || 3000;
const app = express();
const server = http.createServer(app);

const io = new Server(server, {
  cors: { origin: '*', methods: ['GET', 'POST'] },
  transports: ['websocket'],
  pingTimeout: 60000,
  pingInterval: 25000
});

// ─── Health check ───
app.get('/', (req, res) => {
  res.json({ status: 'ok', uptime: process.uptime(), clients: io.engine.clientsCount });
});
app.get('/health', (req, res) => res.send('OK'));

// ─── Test pool connectivity ───
app.get('/api/test-pool', (req, res) => {
  const { host, port } = req.query;
  if (!host || !port) return res.status(400).json({ error: 'host and port required' });

  const socket = net.connect({ host, port: parseInt(port) });
  const timeout = setTimeout(() => {
    socket.destroy();
    res.json({ host, port, reachable: false, reason: 'timeout' });
  }, 5000);

  socket.on('connect', () => {
    clearTimeout(timeout);
    socket.destroy();
    res.json({ host, port, reachable: true });
  });

  socket.on('error', (err) => {
    clearTimeout(timeout);
    res.json({ host, port, reachable: false, reason: err.message });
  });
});

// ─── Socket.io ───
const STATS = { totalShares: 0, startTime: Date.now() };

io.on('connection', (socket) => {
  console.log(`[+] ${socket.id} | total: ${io.engine.clientsCount}`);

  let stratum = null;
  let buffer = '';
  let pingInterval = null;

  const state = {
    extranonce1: '',
    extranonce2_size: 4,
    difficulty: 1,
    worker: '',
    password: 'c=RVN',
    authorized: false
  };

  function sendStratum(obj) {
    if (!stratum || stratum.destroyed) return;
    try { stratum.write(JSON.stringify(obj) + '\n'); } catch (_) {}
  }

  function cleanup() {
    if (pingInterval) { clearInterval(pingInterval); pingInterval = null; }
    if (stratum) {
      try { stratum.destroy(); } catch (_) {}
      stratum = null;
    }
  }

  function handleLine(line) {
    if (!line.trim()) return;
    let msg;
    try { msg = JSON.parse(line); } catch { return; }

    // subscribe ok
    if (msg.id === 1 && msg.result) {
      state.extranonce1 = msg.result[1];
      state.extranonce2_size = msg.result[2] || 4;
      console.log(`[→] sub ok en1=${state.extranonce1}`);
      sendStratum({ id: 2, method: 'mining.authorize', params: [state.worker, state.password] });
      return;
    }

    // authorize ok
    if (msg.id === 2) {
      if (msg.result === true) {
        state.authorized = true;
        console.log(`[✓] auth ${state.worker}`);
        socket.emit('can start');
      } else {
        socket.emit('error', 'Auth failed');
      }
      return;
    }

    // difficulty
    if (msg.method === 'mining.set_difficulty') {
      state.difficulty = msg.params[0];
      return;
    }

    // job
    if (msg.method === 'mining.notify') {
      const [job_id, prevhash, coinb1, coinb2, merkle_branch, version, nbits, ntime, clean_jobs] = msg.params;
      socket.emit('work', {
        job_id, jobId: job_id, prevhash, coinb1, coinb2,
        merkle_branch, version, nbits, ntime, clean_jobs,
        extraNonce1: state.extranonce1,
        extraNonce2Size: state.extranonce2_size,
        miningDiff: state.difficulty
      });
      return;
    }

    // submit result
    if (typeof msg.id === 'number' && msg.id >= 1000) {
      if (msg.result === true) {
        STATS.totalShares++;
        socket.emit('submit_result', { accepted: true });
      } else {
        socket.emit('submit failed', JSON.stringify(msg.error));
      }
      return;
    }

    // pool ping
    if (msg.method === 'mining.ping') {
      sendStratum({ id: msg.id, method: 'mining.pong', params: msg.params });
    }
  }

  function connectPool(opts) {
    cleanup();
    buffer = '';
    console.log(`[...] ${opts.server}:${opts.port}`);

    stratum = net.connect({ host: opts.server, port: opts.port });
    stratum.setEncoding('utf8');
    stratum.setNoDelay(true);

    const timeout = setTimeout(() => {
      if (!state.authorized) {
        socket.emit('error', 'Pool timeout');
        cleanup();
      }
    }, 20000);

    stratum.on('connect', () => {
      clearTimeout(timeout);
      sendStratum({ id: 1, method: 'mining.subscribe', params: ['rvn-proxy/1.0'] });
      pingInterval = setInterval(() => {
        sendStratum({ id: 0, method: 'mining.ping', params: [] });
      }, 30000);
    });

    stratum.on('data', (chunk) => {
      buffer += chunk;
      let i;
      while ((i = buffer.indexOf('\n')) !== -1) {
        handleLine(buffer.slice(0, i).trim());
        buffer = buffer.slice(i + 1);
      }
      if (buffer.length > 1024 * 1024) buffer = '';
    });

    stratum.on('error', (e) => {
      clearTimeout(timeout);
      socket.emit('error', 'Pool error: ' + e.message);
    });

    stratum.on('close', () => {
      clearTimeout(timeout);
      cleanup();
      if (socket.connected) socket.emit('error', 'Pool closed');
    });
  }

  socket.on('start', (opts) => {
    state.worker = opts.stratum?.worker || '';
    state.password = opts.stratum?.password || 'c=RVN';
    connectPool({ server: opts.stratum?.server, port: opts.stratum?.port });
  });

  socket.on('submit', (data) => {
    if (!stratum || !state.authorized) return;
    const id = Math.floor(Math.random() * 100000) + 1000;
    sendStratum({
      id, method: 'mining.submit',
      params: [state.worker, data.job_id, data.extranonce2, data.ntime, data.nonce]
    });
  });

  socket.on('disconnect', () => { cleanup(); });
  socket.on('error', () => {});
});

server.listen(PORT, () => {
  console.log(`🚀 RVN Proxy listening on :${PORT}`);
});

process.on('SIGTERM', () => {
  io.close(() => server.close(() => process.exit(0)));
});
