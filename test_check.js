const http = require('http');
const dns = require('dns');

// Test 1 : résolution DNS de localhost
dns.lookup('localhost', (err, addr, family) => {
  console.log('localhost resout en:', addr, '(IPv' + family + ')');
});

// Test 2 : connexion avec localhost
function check(url, label) {
  return new Promise(res => {
    const r = http.get(url, resp => {
      console.log(label, '-> status:', resp.statusCode);
      resp.destroy();
      res(true);
    });
    r.on('error', e => { console.log(label, '-> ERREUR:', e.message); res(false); });
    r.setTimeout(3000, () => { r.destroy(); console.log(label, '-> TIMEOUT'); res(false); });
  });
}

(async () => {
  await check('http://localhost:8000/api/stats', 'localhost');
  await check('http://127.0.0.1:8000/api/stats', '127.0.0.1');
  await check('http://[::1]:8000/api/stats', '::1 (IPv6)');
})();
