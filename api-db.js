// واجهة بيانات المعلم: تحتاج المتغير TEACHER_KEY (كلمة سر المعلم) + قاعدة Upstash
const crypto = require('crypto');
const URL_ = () => process.env.KV_REST_API_URL || process.env.UPSTASH_REDIS_REST_URL;
const TOK_ = () => process.env.KV_REST_API_TOKEN || process.env.UPSTASH_REDIS_REST_TOKEN;
async function pipe(cmds) {
  const r = await fetch(URL_() + '/pipeline', { method: 'POST', headers: { Authorization: 'Bearer ' + TOK_(), 'Content-Type': 'application/json' }, body: JSON.stringify(cmds) });
  if (!r.ok) throw new Error('kv ' + r.status);
  return r.json();
}
const sha = (s) => crypto.createHash('sha256').update(String(s)).digest();
function send(res, code, obj) { res.statusCode = code; res.setHeader('Content-Type', 'application/json; charset=utf-8'); res.setHeader('Cache-Control', 'no-store'); res.end(JSON.stringify(obj)); }
function readBody(req) {
  return new Promise((ok, bad) => {
    if (req.body && typeof req.body === 'object') return ok(req.body);
    if (typeof req.body === 'string') { try { return ok(JSON.parse(req.body)); } catch (e) { return bad(e); } }
    let s = ''; req.on('data', (c) => { s += c; if (s.length > 3e6) { bad(new Error('big')); req.destroy(); } });
    req.on('end', () => { try { ok(JSON.parse(s || '{}')); } catch (e) { bad(e); } });
    req.on('error', bad);
  });
}
const IDRE = /^[A-Za-z0-9_-]{1,40}$/, RIDRE = /^[0-9a-f]{32}$/;
async function delSub(rid, pid) {
  const h = (await pipe([['HGET', 'ofuq:sub:' + rid, pid]]))[0].result; if (!h) return;
  let m = null; try { m = JSON.parse(h); } catch (e) { }
  const c = [['HDEL', 'ofuq:sub:' + rid, pid]];
  if (m && m.sid) { for (let i = 0; i < (m.n || 0) && i < 10; i++) c.push(['DEL', 'ofuq:sc:' + rid + ':' + pid + ':' + m.sid + ':' + i]); if (m.len) c.push(['DECRBY', 'ofuq:subbytes', m.len]); }
  await pipe(c);
}
async function delAllSub(rid) {
  const ks = (await pipe([['HKEYS', 'ofuq:sub:' + rid]]))[0].result || [];
  for (const k of ks) await delSub(rid, k);
  await pipe([['DEL', 'ofuq:sub:' + rid], ['SREM', 'ofuq:subrids', rid]]);
}
module.exports = async function (req, res) {
  try {
    const key = process.env.TEACHER_KEY;
    if (!key || !URL_() || !TOK_()) return send(res, 503, { error: 'not-configured' });
    const ip = String(req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim() || 'x';
    const fk = 'ofuq:fl:' + ip;
    const given = req.headers['x-key'] || '';
    const ok = crypto.timingSafeEqual(sha(given), sha(key));
    if (!ok) {
      const fl = await pipe([['GET', fk]]);
      if (Number(fl[0].result || 0) >= 10) return send(res, 429, { error: 'too-many' });
      await pipe([['SET', fk, 0, 'EX', 600, 'NX'], ['INCR', fk]]);
      return send(res, 401, { error: 'bad-key' });
    }
    if (req.method === 'GET' && /(?:^|[?&])subs=1(?:&|$)/.test(String(req.url || ''))) {
      const rids = (await pipe([['SMEMBERS', 'ofuq:subrids']]))[0].result || [];
      const out = rids.length ? await pipe(rids.map((r) => ['HGETALL', 'ofuq:sub:' + r])) : [];
      const subs = {};
      rids.forEach((r, k) => { const a = out[k].result || [], m = {}; for (let j = 0; j < a.length; j += 2) { try { m[a[j]] = JSON.parse(a[j + 1]); } catch (e) { } } if (Object.keys(m).length) subs[r] = m; });
      const bt = (await pipe([['GET', 'ofuq:subbytes']]))[0].result;
      return send(res, 200, { subs, bytes: Number(bt || 0) });
    }
    if (req.method === 'GET') {
      const ids = (await pipe([['SMEMBERS', 'ofuq:stids']]))[0].result || [];
      const cmds = [['GET', 'ofuq:cfg']].concat(ids.map((i) => ['GET', 'ofuq:st:' + i]));
      const out = await pipe(cmds);
      const students = [];
      ids.forEach((id, k) => { try { const d = JSON.parse(out[k + 1].result); if (d) students.push(Object.assign({ id }, d)); } catch (e) { } });
      let cfg = null; try { cfg = JSON.parse(out[0].result); } catch (e) { }
      return send(res, 200, { students, cfg });
    }
    if (req.method !== 'POST') return send(res, 405, { error: 'method' });
    const body = await readBody(req);
    const ops = Array.isArray(body.ops) ? body.ops : [];
    if (!ops.length || ops.length > 300) return send(res, 400, { error: 'ops' });
    const cmds = [];
    for (const o of ops) {
      if (o.o === 'sset' && IDRE.test(o.id) && o.d && typeof o.d === 'object') {
        const d = Object.assign({}, o.d); delete d.id;
        const j = JSON.stringify(d); if (j.length > 600000) return send(res, 413, { error: 'big' });
        cmds.push(['SET', 'ofuq:st:' + o.id, j], ['SADD', 'ofuq:stids', o.id]);
      } else if (o.o === 'sdel' && IDRE.test(o.id)) {
        cmds.push(['DEL', 'ofuq:st:' + o.id], ['SREM', 'ofuq:stids', o.id]);
      } else if (o.o === 'cset' && o.d && typeof o.d === 'object') {
        const j = JSON.stringify(o.d); if (j.length > 200000) return send(res, 413, { error: 'big' });
        cmds.push(['SET', 'ofuq:cfg', j]);
      } else if (o.o === 'rset' && RIDRE.test(o.id) && o.v && typeof o.v.iv === 'string' && typeof o.v.ct === 'string') {
        const j = JSON.stringify({ iv: o.v.iv, ct: o.v.ct, upd: String(o.v.upd || '').slice(0, 10) }); if (j.length > 200000) return send(res, 413, { error: 'big' });
        cmds.push(['SET', 'ofuq:rep:' + o.id, j]);
      } else if (o.o === 'subdel' && RIDRE.test(o.rid) && IDRE.test(o.pid)) {
        await delSub(o.rid, o.pid);
      } else if (o.o === 'suball' && RIDRE.test(o.rid)) {
        await delAllSub(o.rid);
      } else if (o.o === 'subpurge' && IDRE.test(o.pid)) {
        const rids = (await pipe([['SMEMBERS', 'ofuq:subrids']]))[0].result || [];
        for (const r of rids) await delSub(r, o.pid);
      } else if (o.o === 'aset') {
        if (o.v === null) cmds.push(['DEL', 'ofuq:ach']);
        else if (o.v && typeof o.v === 'object') { const j = JSON.stringify(o.v); if (j.length > 60000) return send(res, 413, { error: 'big' }); cmds.push(['SET', 'ofuq:ach', j]); }
        else return send(res, 400, { error: 'bad-op' });
      } else if (o.o === 'rdel' && RIDRE.test(o.id)) {
        cmds.push(['DEL', 'ofuq:rep:' + o.id]);
      } else return send(res, 400, { error: 'bad-op' });
    }
    if (cmds.length) await pipe(cmds);
    return send(res, 200, { ok: true, n: ops.length });
  } catch (e) { return send(res, 500, { error: 'server' }); }
};
