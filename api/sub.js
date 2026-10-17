// تسليم المشاريع: ملفات الطلاب مشفّرة في المتصفح بمفتاح الطالب، والخادم يخزّن قطعاً مشفّرة فقط
const URL_ = () => process.env.KV_REST_API_URL || process.env.UPSTASH_REDIS_REST_URL;
const TOK_ = () => process.env.KV_REST_API_TOKEN || process.env.UPSTASH_REDIS_REST_TOKEN;
async function pipe(cmds) {
  const r = await fetch(URL_() + '/pipeline', { method: 'POST', headers: { Authorization: 'Bearer ' + TOK_(), 'Content-Type': 'application/json' }, body: JSON.stringify(cmds) });
  if (!r.ok) throw new Error('kv ' + r.status);
  return r.json();
}
function send(res, code, obj) { res.statusCode = code; res.setHeader('Content-Type', 'application/json; charset=utf-8'); res.setHeader('Cache-Control', 'no-store'); res.end(JSON.stringify(obj)); }
function readBody(req) {
  return new Promise((ok, bad) => {
    if (req.body && typeof req.body === 'object') return ok(req.body);
    if (typeof req.body === 'string') { try { return ok(JSON.parse(req.body)); } catch (e) { return bad(e); } }
    let s = ''; req.on('data', (c) => { s += c; if (s.length > 1.2e6) { bad(new Error('big')); req.destroy(); } });
    req.on('end', () => { try { ok(JSON.parse(s || '{}')); } catch (e) { bad(e); } });
    req.on('error', bad);
  });
}
const RIDRE = /^[0-9a-f]{32}$/, SIDRE = /^[a-z0-9]{6,24}$/, PIDRE = /^[A-Za-z0-9_-]{1,40}$/, B64RE = /^[A-Za-z0-9+/=]*$/;
const MIMES = ['application/pdf', 'image/jpeg', 'image/png', 'image/webp', 'image/gif'];
const MAXN = 6, CH = 700000, MAXLEN = 4200000, QUOTA = 150e6, MAXRAW = 3200000;
const DEF = [1, 2, 3, 4, 5, 6, 7, 8].map((i) => ({ id: 'w' + i, n: 'ورقة عمل ' + i }));
async function projs() {
  try {
    const c = JSON.parse((await pipe([['GET', 'ofuq:cfg']]))[0].result);
    if (c && Array.isArray(c.proj)) return c.proj.filter((p) => p && PIDRE.test(p.id) && typeof p.n === 'string').map((p) => ({ id: p.id, n: p.n.slice(0, 60) }));
  } catch (e) { }
  return DEF;
}
module.exports = async function (req, res) {
  try {
    if (!URL_() || !TOK_()) return send(res, 503, { error: 'not-configured' });
    const ip = String(req.headers['x-forwarded-for'] || (req.socket && req.socket.remoteAddress) || '').split(',')[0].trim() || 'x';
    const rk = 'ofuq:rls:' + ip;
    const c = await pipe([['INCR', rk], ['EXPIRE', rk, 60]]);
    if (Number(c[0].result) > 150) return send(res, 429, { error: 'too-many' });
    if (req.method === 'GET') {
      const q = new URLSearchParams(String(req.url || '').split('?')[1] || '');
      if (q.get('types')) return send(res, 200, { proj: await projs() });
      const rid = q.get('rid') || '';
      if (!RIDRE.test(rid)) return send(res, 400, { error: 'rid' });
      const pid = q.get('pid');
      if (pid) {
        if (!PIDRE.test(pid)) return send(res, 400, { error: 'pid' });
        const h = (await pipe([['HGET', 'ofuq:sub:' + rid, pid]]))[0].result;
        if (!h) return send(res, 404, { error: 'none' });
        let m; try { m = JSON.parse(h); } catch (e) { return send(res, 500, { error: 'bad' }); }
        const i = parseInt(q.get('c'), 10);
        if (!(i >= 0 && i < m.n)) return send(res, 400, { error: 'c' });
        const d = (await pipe([['GET', 'ofuq:sc:' + rid + ':' + pid + ':' + m.sid + ':' + i]]))[0].result;
        if (!d) return send(res, 404, { error: 'none' });
        return send(res, 200, { d });
      }
      const a = (await pipe([['HGETALL', 'ofuq:sub:' + rid]]))[0].result || [];
      const subs = {};
      for (let j = 0; j < a.length; j += 2) { try { subs[a[j]] = JSON.parse(a[j + 1]); } catch (e) { } }
      return send(res, 200, { subs });
    }
    if (req.method !== 'POST') return send(res, 405, { error: 'method' });
    const b = await readBody(req);
    const rid = String(b.rid || ''), pid = String(b.pid || ''), sid = String(b.sid || '');
    if (!RIDRE.test(rid) || !PIDRE.test(pid) || !SIDRE.test(sid)) return send(res, 400, { error: 'args' });
    const ex = (await pipe([['EXISTS', 'ofuq:rep:' + rid]]))[0].result;
    if (Number(ex) !== 1) return send(res, 404, { error: 'none' });
    const list = await projs();
    if (!list.some((p) => p.id === pid)) return send(res, 400, { error: 'type' });
    const base = 'ofuq:sc:' + rid + ':' + pid + ':' + sid + ':';
    const n = parseInt(b.n, 10);
    if (!(n >= 1 && n <= MAXN)) return send(res, 400, { error: 'n' });
    if (b.a === 'chunk') {
      const i = parseInt(b.i, 10), d = b.d;
      if (!(i >= 0 && i < n) || typeof d !== 'string' || !d.length || d.length > CH || !B64RE.test(d)) return send(res, 400, { error: 'chunk' });
      await pipe([['SET', base + i, d, 'EX', 86400]]);
      return send(res, 200, { ok: true });
    }
    if (b.a === 'commit') {
      const mime = String(b.mime || '');
      const size = Number(b.size);
      if (MIMES.indexOf(mime) < 0 || !(size > 0 && size <= MAXRAW)) return send(res, 400, { error: 'file' });
      const name = String(b.name || 'file').replace(/[\u0000-\u001f<>"\\/]/g, '').slice(0, 80) || 'file';
      const keys = []; for (let i = 0; i < n; i++) keys.push(base + i);
      const lens = (await pipe(keys.map((k) => ['STRLEN', k]))).map((x) => Number(x.result) || 0);
      if (lens.some((l) => l === 0)) return send(res, 400, { error: 'missing' });
      const total = lens.reduce((s, l) => s + l, 0);
      if (total > MAXLEN) { await pipe(keys.map((k) => ['DEL', k])); return send(res, 413, { error: 'big' }); }
      const r = await pipe([['GET', 'ofuq:subbytes'], ['HGET', 'ofuq:sub:' + rid, pid]]);
      let old = null; try { old = JSON.parse(r[1].result); } catch (e) { }
      const cur = Number(r[0].result || 0), oldLen = old && old.len ? old.len : 0;
      if (cur + total - oldLen > QUOTA) { await pipe(keys.map((k) => ['DEL', k])); return send(res, 507, { error: 'full' }); }
      const meta = { sid, n, len: total, name, mime, size, upd: new Date().toISOString() };
      const cmds = keys.map((k) => ['PERSIST', k]);
      cmds.push(['HSET', 'ofuq:sub:' + rid, pid, JSON.stringify(meta)], ['SADD', 'ofuq:subrids', rid], ['INCRBY', 'ofuq:subbytes', total - oldLen]);
      if (old && old.sid && old.sid !== sid) for (let i = 0; i < (old.n || 0) && i < 10; i++) cmds.push(['DEL', 'ofuq:sc:' + rid + ':' + pid + ':' + old.sid + ':' + i]);
      await pipe(cmds);
      return send(res, 200, { ok: true, meta });
    }
    return send(res, 400, { error: 'a' });
  } catch (e) { return send(res, 500, { error: 'server' }); }
};
