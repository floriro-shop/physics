// عدّاد الزيارات: يخزّن الرقم في Upstash Redis المربوط بمشروع Vercel
module.exports = async function (req, res) {
  const url = process.env.KV_REST_API_URL || process.env.UPSTASH_REDIS_REST_URL;
  const token = process.env.KV_REST_API_TOKEN || process.env.UPSTASH_REDIS_REST_TOKEN;
  res.setHeader('Cache-Control', 'no-store');
  res.setHeader('Content-Type', 'application/json; charset=utf-8');
  if (!url || !token) { res.statusCode = 200; res.end(JSON.stringify({ count: null })); return; }
  try {
    const hit = req.url && req.url.indexOf('hit=1') !== -1;
    const r = await fetch(url + (hit ? '/incr/visits' : '/get/visits'), { headers: { Authorization: 'Bearer ' + token } });
    const j = await r.json();
    res.statusCode = 200;
    res.end(JSON.stringify({ count: Number(j.result) || 0 }));
  } catch (e) {
    res.statusCode = 200;
    res.end(JSON.stringify({ count: null }));
  }
};
