// Direct canvas K-line chart (no chart.js, more reliable)
const fs = require('fs');
const path = require('path');
const https = require('https');
const { createCanvas } = require('canvas');

const argv = process.argv.slice(2);
const instId = argv[0] || 'ETH-USDT-SWAP';
const bar = argv[1] || '4H';
const limit = parseInt(argv[2] || '120', 10);
const outDir = path.join(__dirname, 'output');
if (!fs.existsSync(outDir)) fs.mkdirSync(outDir, { recursive: true });

function fetchCandles(instId, bar, limit) {
  return new Promise((resolve, reject) => {
    const url = `https://www.okx.com/api/v5/market/candles?instId=${instId}&bar=${bar}&limit=${limit}`;
    let agent;
    try {
      const HttpsProxyAgent = require('https-proxy-agent').HttpsProxyAgent;
      agent = new HttpsProxyAgent(process.env.HTTPS_PROXY || 'http://127.0.0.1:7897');
    } catch {}
    https.get(url, agent ? { agent } : {}, (res) => {
      let body = '';
      res.on('data', c => body += c);
      res.on('end', () => {
        try {
          const json = JSON.parse(body);
          if (json.code !== '0') return reject(new Error(json.msg || 'OKX'));
          const data = json.data.map(r => ({
            t: parseInt(r[0]), o: parseFloat(r[1]), h: parseFloat(r[2]),
            l: parseFloat(r[3]), c: parseFloat(r[4]), v: parseFloat(r[5]),
          })).reverse();
          resolve(data);
        } catch (e) { reject(e); }
      });
    }).on('error', reject);
  });
}

function ema(arr, period) {
  const k = 2 / (period + 1), out = []; let prev;
  for (let i = 0; i < arr.length; i++) {
    if (i < period - 1) { out.push(null); continue; }
    if (i === period - 1) { let s = 0; for (let j = 0; j < period; j++) s += arr[j]; prev = s / period; out.push(prev); continue; }
    prev = arr[i] * k + prev * (1 - k); out.push(prev);
  }
  return out;
}

function bb(arr, p = 20, sd = 2) {
  const m = [], u = [], l = [];
  for (let i = 0; i < arr.length; i++) {
    if (i < p - 1) { m.push(null); u.push(null); l.push(null); continue; }
    let s = 0; for (let j = i - p + 1; j <= i; j++) s += arr[j];
    const mean = s / p;
    let v = 0; for (let j = i - p + 1; j <= i; j++) v += (arr[j] - mean) ** 2;
    const std = Math.sqrt(v / p);
    m.push(mean); u.push(mean + sd * std); l.push(mean - sd * std);
  }
  return { m, u, l };
}

(async () => {
  const candles = await fetchCandles(instId, bar, limit);
  const closes = candles.map(c => c.c);
  const highs = candles.map(c => c.h);
  const lows = candles.map(c => c.l);
  const vols = candles.map(c => c.v);
  const ema21 = ema(closes, 21);
  const ema55 = ema(closes, 55);
  const ema144 = ema(closes, 144);
  const bbands = bb(closes, 20, 2);

  const W = 1400, H = 800;
  const padL = 70, padR = 90, padT = 40, padB = 40;
  const volH = 120;
  const priceH = H - padT - padB - volH - 20;
  const chartW = W - padL - padR;

  const canvas = createCanvas(W, H);
  const ctx = canvas.getContext('2d');

  // bg
  ctx.fillStyle = '#0d1117';
  ctx.fillRect(0, 0, W, H);

  const allHighs = [...highs, ...bbands.u.filter(x => x != null)];
  const allLows = [...lows, ...bbands.l.filter(x => x != null)];
  const yMax = Math.max(...allHighs);
  const yMin = Math.min(...allLows);
  const yPad = (yMax - yMin) * 0.04;
  const yHi = yMax + yPad, yLo = yMin - yPad;

  const xOf = i => padL + (i + 0.5) * (chartW / candles.length);
  const yOf = p => padT + (yHi - p) / (yHi - yLo) * priceH;
  const cw = Math.max(1, (chartW / candles.length) * 0.7);

  // grid + price labels
  ctx.strokeStyle = '#21262d';
  ctx.fillStyle = '#8b949e';
  ctx.font = '11px sans-serif';
  ctx.lineWidth = 0.5;
  for (let i = 0; i <= 6; i++) {
    const p = yLo + (yHi - yLo) * i / 6;
    const y = yOf(p);
    ctx.beginPath(); ctx.moveTo(padL, y); ctx.lineTo(W - padR, y); ctx.stroke();
    ctx.fillText(p.toFixed(p < 1 ? 4 : p < 100 ? 2 : 0), W - padR + 5, y + 4);
  }

  // BB shading
  ctx.fillStyle = 'rgba(66, 165, 245, 0.06)';
  ctx.beginPath();
  let started = false;
  for (let i = 0; i < candles.length; i++) {
    if (bbands.u[i] == null) continue;
    if (!started) { ctx.moveTo(xOf(i), yOf(bbands.u[i])); started = true; }
    else ctx.lineTo(xOf(i), yOf(bbands.u[i]));
  }
  for (let i = candles.length - 1; i >= 0; i--) {
    if (bbands.l[i] == null) continue;
    ctx.lineTo(xOf(i), yOf(bbands.l[i]));
  }
  ctx.closePath(); ctx.fill();

  // BB lines
  function drawLine(arr, color, dashed = false, w = 1) {
    ctx.strokeStyle = color; ctx.lineWidth = w;
    ctx.setLineDash(dashed ? [4, 4] : []);
    ctx.beginPath();
    let started = false;
    for (let i = 0; i < candles.length; i++) {
      if (arr[i] == null) { started = false; continue; }
      const x = xOf(i), y = yOf(arr[i]);
      if (!started) { ctx.moveTo(x, y); started = true; } else ctx.lineTo(x, y);
    }
    ctx.stroke();
  }
  drawLine(bbands.u, '#42a5f5', true, 0.8);
  drawLine(bbands.m, '#42a5f5', false, 0.8);
  drawLine(bbands.l, '#42a5f5', true, 0.8);

  // Candles
  for (let i = 0; i < candles.length; i++) {
    const c = candles[i];
    const up = c.c >= c.o;
    const x = xOf(i);
    ctx.strokeStyle = up ? '#26a69a' : '#ef5350';
    ctx.fillStyle = up ? '#26a69a' : '#ef5350';
    // wick
    ctx.lineWidth = 1;
    ctx.beginPath(); ctx.moveTo(x, yOf(c.h)); ctx.lineTo(x, yOf(c.l)); ctx.stroke();
    // body
    const bodyTop = yOf(Math.max(c.o, c.c));
    const bodyBot = yOf(Math.min(c.o, c.c));
    const bodyH = Math.max(1, bodyBot - bodyTop);
    ctx.fillRect(x - cw / 2, bodyTop, cw, bodyH);
  }

  // EMAs
  ctx.setLineDash([]);
  drawLine(ema21, '#ffeb3b', false, 1.4);
  drawLine(ema55, '#ff9800', false, 1.4);
  drawLine(ema144, '#ab47bc', false, 1.4);

  // Volume
  const volTop = padT + priceH + 20;
  const volMax = Math.max(...vols);
  ctx.fillStyle = '#21262d';
  ctx.fillRect(padL, volTop, chartW, volH);
  for (let i = 0; i < candles.length; i++) {
    const c = candles[i];
    const up = c.c >= c.o;
    const x = xOf(i);
    const h = (vols[i] / volMax) * volH;
    ctx.fillStyle = up ? 'rgba(38, 166, 154, 0.6)' : 'rgba(239, 83, 80, 0.6)';
    ctx.fillRect(x - cw / 2, volTop + volH - h, cw, h);
  }

  // Title
  const last = candles[candles.length - 1];
  const high24 = Math.max(...highs);
  const low24 = Math.min(...lows);
  const change = ((last.c - candles[0].c) / candles[0].c * 100).toFixed(2);
  ctx.fillStyle = '#fff';
  ctx.font = 'bold 16px sans-serif';
  ctx.fillText(`${instId}  ${bar}  ${limit} 根`, padL, 25);
  ctx.font = '12px sans-serif';
  ctx.fillStyle = '#8b949e';
  ctx.fillText(`last=${last.c}  ${limit}-bar high=${high24}  low=${low24}  ${limit}-bar Δ=${change}%`, padL + 240, 25);

  // Legend
  const lx = W - padR - 280, ly = 25;
  const legends = [
    ['EMA21', '#ffeb3b'], ['EMA55', '#ff9800'], ['EMA144', '#ab47bc'], ['BB(20,2)', '#42a5f5'],
  ];
  let lxOff = 0;
  for (const [n, c] of legends) {
    ctx.fillStyle = c; ctx.fillRect(lx + lxOff, ly - 8, 10, 10);
    ctx.fillStyle = '#ddd'; ctx.fillText(n, lx + lxOff + 14, ly + 1);
    lxOff += 70;
  }

  // last price line
  ctx.strokeStyle = last.c >= last.o ? '#26a69a' : '#ef5350';
  ctx.setLineDash([2, 3]);
  const ly2 = yOf(last.c);
  ctx.beginPath(); ctx.moveTo(padL, ly2); ctx.lineTo(W - padR, ly2); ctx.stroke();
  ctx.setLineDash([]);
  ctx.fillStyle = last.c >= last.o ? '#26a69a' : '#ef5350';
  ctx.fillRect(W - padR + 2, ly2 - 9, 60, 18);
  ctx.fillStyle = '#fff';
  ctx.font = 'bold 11px sans-serif';
  ctx.fillText(last.c.toFixed(last.c < 1 ? 4 : 2), W - padR + 5, ly2 + 4);

  const safeId = instId.replace(/[^a-zA-Z0-9-]/g, '_');
  const out = path.join(outDir, `${safeId}_${bar}.png`);
  fs.writeFileSync(out, canvas.toBuffer('image/png'));
  console.log(out);
})().catch(e => { console.error(e); process.exit(1); });
