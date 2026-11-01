import { readFile, writeFile, appendFile } from 'node:fs/promises';
import { cloudflareClient } from './cloudflare-api.mjs';
import { CONFIG_KEY, validateConfig } from '../promotion/catalog.mjs';
import { review } from '../promotion/review.mjs';

const { request } = cloudflareClient();
const settings = JSON.parse(await readFile(new URL('../wrangler.json', import.meta.url), 'utf8'));
const namespace = settings.kv_namespaces.find(n => n.binding === 'PROMO_CONFIG')?.id;
if (!/^[a-f0-9]{32}$/.test(namespace ?? '')) throw new Error('PROMO_CONFIG namespace missing');
const keyPath = `storage/kv/namespaces/${namespace}/values/${CONFIG_KEY}`;
const config = validateConfig(JSON.parse(await request(keyPath, { raw: true })));
const since = new Date(Math.max(Date.parse(config.lastChangedAt), Date.now() - 28 * 86400000)).toISOString().slice(0,19).replace('T', ' ');
const query = `SELECT blob1 AS product, blob2 AS variant,
 SUM(IF(blob3 = 'exposure', _sample_interval, 0)) AS exposures,
 SUM(IF(blob3 = 'click', _sample_interval, 0)) AS clicks,
 MAX(_sample_interval) AS sampling
 FROM hackerbook_promotions WHERE timestamp >= toDateTime('${since}')
 AND blob3 IN ('exposure', 'click')
 AND blob4 IN ('hackerbook.dosaygo.com', 'staticnews.dosaygo.com', 'hnbackuptape.dosaygo.com')
 GROUP BY product, variant FORMAT JSON`;
// A dataset starts on its first event. Distinguish that state from zero traffic;
// authorization failures and malformed responses still fail loudly.
const tables = await request('analytics_engine/sql', { method: 'POST', body: 'SHOW TABLES FORMAT JSON', contentType: 'text/plain' });
if (!Array.isArray(tables.data)) throw new Error('Analytics Engine returned no dataset list');
const initialized = tables.data.some(row => row.dataset === 'hackerbook_promotions');
const metrics = initialized ? await request('analytics_engine/sql', { method: 'POST', body: query, contentType: 'text/plain' }) : { data: [] };
if (!Array.isArray(metrics.data)) throw new Error('Analytics Engine returned no data array');
const sampled = metrics.data.some(row => Number(row.sampling) !== 1);
const rows = metrics.data.map(row => ({ product: row.product, variant: Number(row.variant),
  exposures: Number(row.exposures), clicks: Number(row.clicks) }));
const result = !initialized ? { config, changed: false, decisions: [], reason: 'telemetry dataset not initialized; awaiting first event, no automatic change' }
  : sampled ? { config, changed: false, decisions: [], reason: 'sampled data; no automatic change' } : review(config, rows);
if (result.changed && process.argv.includes('--apply')) {
  const latest = JSON.parse(await request(keyPath, { raw: true }));
  if (JSON.stringify(latest) !== JSON.stringify(config)) throw new Error('Campaign changed during review; refusing overwrite');
  await request(`storage/kv/namespaces/${namespace}/values/history-${config.revision}`, { method: 'PUT', body: JSON.stringify(config) });
  await request(keyPath, { method: 'PUT', body: JSON.stringify(result.config) });
}
const report = { reviewedAt: new Date().toISOString(), applied: result.changed && process.argv.includes('--apply'),
  source: 'interaction-qualified browser events, not verified people or sales', since, rows, ...result };
await writeFile('promotion-review.json', JSON.stringify(report, null, 2));
const summary = ['# HackerBook promotion review', '', report.source, '',
  `Applied configuration change: ${report.applied}. Revision: ${result.config.revision}.`, '',
  '| Product | Decision | Weights |', '|---|---|---|',
  ...result.decisions.map(d => `| ${d.product} | ${d.reason} | ${d.weights.join('/')} |`),
  '', result.reason ?? '', '', 'Geography and destination URLs are fixed in reviewed code. No archive rebuild was dispatched.'].join('\n');
await writeFile('promotion-review.md', summary);
if (process.env.GITHUB_STEP_SUMMARY) await appendFile(process.env.GITHUB_STEP_SUMMARY, summary);
console.log(summary);
