// إنجازات الطلاب المنشورة في الصفحة الرئيسية (قراءة عامة فقط)
module.exports = async function (req, res) {
  const url = process.env.KV_REST_API_URL || process.env.UPSTASH_REDIS_REST_URL;
  const token = process.env.KV_REST_API_TOKEN || process.env.UPSTASH_REDIS_REST_TOKEN;
  res.setHeader('Cache-Control', 'no-store');
  res.setHeader('Content-Type', 'application/json; charset=utf-8');
  if (!url || !token) { res.statusCode = 200; res.end('{"ach":null}'); return; }
  try {
    const r = await fetch(url + '/get/ofuq:ach', { headers: { Authorization: 'Bearer ' + token } });
    const j = await r.json();
    let a = null; try { a = j.result ? JSON.parse(j.result) : null; } catch (e) { }
    res.statusCode = 200;
    res.end(JSON.stringify({ ach: a }));
  } catch (e) {
    res.statusCode = 200;
    res.end('{"ach":null}');
  }
};
