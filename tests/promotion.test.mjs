import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { browserBoxEligible, candidateBrowser, initialConfig, validateConfig, renderPromotion } from '../promotion/catalog.mjs';
import { handleRequest } from '../promotion/worker.mjs';
import { review } from '../promotion/review.mjs';
import { readEventBody } from '../promotion/body.mjs';

const browser = 'Mozilla/5.0 Chrome/130.0.0.0 Safari/537.36';
function request(country, ua = browser, extra = {}) {
  const r = new Request('https://hackerbook.dosaygo.com/', { headers: { 'user-agent': ua, ...extra } });
  Object.defineProperty(r, 'cf', { value: { country } });
  return r;
}

test('BrowserBox geography cannot be bypassed by Google identity or country headers', () => {
  for (const country of ['CN', 'HK', 'MO', 'XX', 'T1', undefined, '', 'USA']) {
    for (const ua of [browser, 'Googlebot']) assert.equal(browserBoxEligible(request(country, ua, { 'cf-ipcountry': 'US' })), false);
  }
  for (const country of ['US', 'GB', 'DE', 'TW']) {
    assert.equal(browserBoxEligible(request(country)), true);
    assert.equal(browserBoxEligible(request(country, 'Googlebot')), true);
    for (const ua of ['Baiduspider', 'Sogou web spider', 'Bytespider', 'PetalBot', '360Spider', 'YisouSpider', 'QuarkSpider']) {
      assert.equal(browserBoxEligible(request(country, ua)), false);
    }
  }
});

test('crawler discovery links exist in initial HTML; BrowserBox never in offline fallback', async () => {
  for (const entry of ['index', 'static', 'mobile']) {
    const html = await readFile(new URL(`../docs/${entry}.html`, import.meta.url), 'utf8');
    assert.equal((html.match(/id="house-promotion"/g) ?? []).length, 1);
    assert.match(html, /<a href="https:\/\/trees\.dosaygo\.com\//);
    assert.match(html, /<a href="https:\/\/gripui\.com\//);
    assert.doesNotMatch(html, /href="https:\/\/browserbox\.io/);
    assert.ok(html.indexOf('id="house-promotion"') < html.indexOf('id="content-table"'));
  }
});

test('rendered links are direct, attributed and eligible; config cannot inject markup', () => {
  const config = initialConfig();
  const us = renderPromotion(request('US'), config, () => 0.2);
  const cn = renderPromotion(request('CN'), config, () => 0.2);
  assert.match(us, /https:\/\/browserbox.io\//);
  assert.doesNotMatch(cn, /browserbox/i);
  assert.match(cn, /utm_medium=house_promo/);
  assert.match(cn, /agentic web browsing/);
  assert.equal(renderPromotion(request('US'), { ...config, enabled: false }), '');
  assert.throws(() => validateConfig({ ...config, weights: { ...config.weights, grip: [100, 0, 0] } }));
  assert.throws(() => validateConfig({ ...config, weights: { ...config.weights, other: [50, 50, 0] } }));
});

test('automation and previously suspicious cohorts never count as candidate browsers', () => {
  for (const ua of ['Googlebot', 'Mozilla/5.0 Baiduspider', 'curl/8.7', 'node', 'Mozilla/5.0 Chrome/99.0.4844.51', 'Mozilla/5.0 HeadlessChrome/130']) {
    assert.equal(candidateBrowser(request('US', ua)), false);
  }
  assert.equal(candidateBrowser(request('US')), true);
});

test('telemetry validates origin, product, geography, method and binding', async () => {
  const points = [];
  const env = { PROMO_EVENTS: { writeDataPoint: point => points.push(point) } };
  const event = async (country, product, headers = {}, value = {}) => {
    const r = new Request('https://hackerbook.dosaygo.com/_promotion/event', { method: 'POST',
      headers: { origin: 'https://hackerbook.dosaygo.com', 'content-type': 'application/json', 'user-agent': browser, ...headers },
      body: JSON.stringify({ event: 'click', product, variant: 0, revision: 1, ...value }) });
    Object.defineProperty(r, 'cf', { value: { country } });
    return handleRequest(r, env);
  };
  assert.equal((await event('US', 'grip')).status, 204);
  assert.equal((await event('CN', 'browserbox')).status, 400);
  assert.equal((await event('US', 'grip', { origin: 'https://unrelated.example' })).status, 403);
  assert.equal((await event('US', 'grip', { 'user-agent': 'Baiduspider' })).status, 403);
  assert.equal((await event('US', 'grip', {}, { variant: 3 })).status, 400);
  assert.equal(points.length, 1);
  assert.equal(points[0].blobs[0], 'grip');
});

test('archive assets bypass promotion code and preserve cache validators', async () => {
  const asset = new Response('binary', { headers: { etag: 'immutable', 'content-type': 'application/wasm' } });
  const result = await handleRequest(new Request('https://hackerbook.dosaygo.com/jswasm/sqlite3.wasm'), { ASSETS: { fetch: async () => asset } });
  assert.equal(result, asset);
});

test('review holds sparse data, detects a winner, preserves exploration and respects off switch', () => {
  const config = initialConfig(new Date('2026-09-01'));
  const now = new Date('2026-10-01');
  assert.equal(review(config, [], now).changed, false);
  const rows = [{ product: 'grip', variant: 0, exposures: 1000, clicks: 120 }, { product: 'grip', variant: 1, exposures: 1000, clicks: 10 }];
  const result = review(config, rows, now);
  assert.equal(result.changed, true);
  assert.deepEqual(result.config.weights.grip, [80, 10, 10]);
  assert.deepEqual(result.config.weights.tdf, [50, 50, 0]);
  assert.equal(review({ ...config, enabled: false }, rows, now).changed, false);
  assert.equal(review(config, rows.map(r => ({ ...r, clicks: 1100 })), now).changed, false);
  assert.equal(review(config, rows, new Date('2026-09-03')).changed, false);
});

test('adequately exposed nonperforming copy gets an approved alternative, not a rebake', () => {
  const config = initialConfig(new Date('2026-09-01'));
  const rows = [0, 1].map(variant => ({ product: 'tdf', variant, exposures: 300, clicks: 0 }));
  const result = review(config, rows, new Date('2026-10-01'));
  assert.equal(result.changed, true);
  assert.deepEqual(result.config.weights.tdf, [10, 10, 80]);
  assert.equal(result.config.revision, 2);
});

test('low volume broadens exploration after six weeks without inventing a winner', () => {
  const result = review(initialConfig(new Date('2026-09-01')), [], new Date('2026-10-20'));
  assert.equal(result.changed, true);
  assert.deepEqual(result.config.weights.grip, [34, 33, 33]);
  assert.match(result.decisions[0].reason, /no winner claimed/);
});

test('telemetry body is bounded even without a Content-Length header', async () => {
  const oversized = new Request('https://example.test', { method: 'POST', body: 'a'.repeat(1025) });
  assert.equal((await readEventBody(oversized)).status, 413);
  const malformed = new Request('https://example.test', { method: 'POST', body: '{' });
  assert.equal((await readEventBody(malformed)).status, 400);
});
