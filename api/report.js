// تقرير الطالب المشفّر: يُطلب بمعرّف مشتق من رمز الطالب (لا يكشف شيئاً بدون الرمز)
const URL_ = () => process.env.KV_REST_API_URL || process.env.UPSTASH_REDIS_REST_URL;
const TOK_ = () => process.env.KV_REST_API_TOKEN || process.env.UPSTASH_REDIS_REST_TOKEN;
async function pipe(cmds) {
  const r = await fetch(URL_() + '/pipeline', { method: 'POST', headers: { Authorization: 'Bearer ' + TOK_(), 'Content-Type': 'application/json' }, body: JSON.stringify(cmds) });
  if (!r.ok) throw new Error('kv ' + r.status);
  return r.json();
}
function send(res, code, obj) { res.statusCode = code; res.setHeader('Content-Type', 'application/json; charset=utf-8'); res.setHeader('Cache-Control', 'no-store'); res.end(JSON.stringify(obj)); }
module.exports = async function (req, res) {
  try {
    if (req.method !== 'GET') return send(res, 405, { error: 'method' });
    if (!URL_() || !TOK_()) return send(res, 503, { error: 'not-configured' });
    const q = String(req.url || '').split('?')[1] || '';
    const m = /(?:^|&)id=([0-9a-f]{32})(?:&|$)/.exec(q);
    if (!m) return send(res, 400, { error: 'id' });
    const ip = String(req.headers['x-forwarded-for'] || (req.socket && req.socket.remoteAddress) || '').split(',')[0].trim() || 'x';
    const rk = 'ofuq:rl:' + ip;
    const c = await pipe([['SET', rk, 0, 'EX', 60, 'NX'], ['INCR', rk]]);
    if (Number(c[1].result) > 40) return send(res, 429, { error: 'too-many' });
    const r = await pipe([['GET', 'ofuq:rep:' + m[1]]]);
    if (!r[0].result) return send(res, 404, { error: 'none' });
    let j; try { j = JSON.parse(r[0].result); } catch (e) { return send(res, 500, { error: 'bad' }); }
    return send(res, 200, j);
  } catch (e) { return send(res, 500, { error: 'server' }); }
};
