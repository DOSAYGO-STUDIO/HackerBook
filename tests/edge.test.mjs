import test from 'node:test';
import assert from 'node:assert/strict';
import { createEdge } from './edge-fixture.mjs';
import { CONFIG_KEY } from '../promotion/catalog.mjs';

test('real Workers runtime rewrites original HTML and isolates geography from cache', { timeout: 60000 }, async () => {
  const mf = await createEdge();
  try {
    for (const [country, ua, allowed] of [['US', 'Googlebot', true], ['CN', 'Googlebot', false], ['HK', 'Mozilla/5.0', false],
      ['MO', 'Mozilla/5.0', false], ['XX', 'Googlebot', false], ['US', 'Baiduspider', false], ['US', 'Mozilla/5.0', true]]) {
      const response = await mf.dispatchFetch('https://hackerbook.dosaygo.com/', { cf: { country }, headers: { 'user-agent': ua, 'cf-ipcountry': 'US' } });
      const html = await response.text();
      assert.equal(response.status, 200);
      assert.equal(html.includes('https://browserbox.io/'), allowed, `${country} ${ua}`);
      assert.match(html, /data-campaign="1"/);
      assert.match(response.headers.get('cache-control'), /no-store/);
      assert.equal(response.headers.get('etag'), null);
    }
    const wasm = await mf.dispatchFetch('https://hackerbook.dosaygo.com/jswasm/sqlite3.wasm');
    assert.equal(wasm.headers.get('etag'), 'fixture');
    await wasm.body.cancel();
    const kv = await mf.getKVNamespace('PROMO_CONFIG');
    await kv.put(CONFIG_KEY, '{}');
    const fallback = await mf.dispatchFetch('https://hackerbook.dosaygo.com/mobile');
    const html = await fallback.text();
    assert.match(html, /https:\/\/gripui.com/);
    assert.doesNotMatch(html, /href="https:\/\/browserbox.io/);
  } finally { await mf.dispose(); }
});
