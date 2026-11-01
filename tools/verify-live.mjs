import { writeFile } from 'node:fs/promises';
import { gunzipSync } from 'node:zlib';

const hosts = ['hackerbook.dosaygo.com', 'staticnews.dosaygo.com', 'hnbackuptape.dosaygo.com'];
const evidence = [];
for (const host of hosts) {
  for (const path of ['/', '/mobile']) {
    const response = await fetch(`https://${host}${path}`, { signal: AbortSignal.timeout(30000) });
    const html = await response.text();
    if (!response.ok || !html.includes('id="house-promotion"') || !html.includes('https://trees.dosaygo.com/')
      || !html.includes('https://gripui.com/')) throw new Error(`Live promotion missing: ${host}${path}`);
    if (!response.headers.get('x-promotion-revision')) throw new Error(`Promotion worker/config unavailable: ${host}${path}`);
    if (!response.headers.get('cache-control')?.includes('no-store')) throw new Error(`Unsafe personalized caching: ${host}${path}`);
    evidence.push({ host, path, status: response.status, revision: response.headers.get('x-promotion-revision'),
      browserbox: html.includes('https://browserbox.io/'), checkedAt: new Date().toISOString() });
  }
}
const manifestResponse = await fetch(`https://${hosts[0]}/static-manifest.json.gz`, { signal: AbortSignal.timeout(30000) });
if (!manifestResponse.ok) throw new Error('Published archive manifest unavailable');
const bytes = Buffer.from(await manifestResponse.arrayBuffer());
const manifest = JSON.parse((bytes[0] === 0x1f ? gunzipSync(bytes) : bytes).toString());
if (!manifest.shards?.length) throw new Error('Published archive manifest contains no shards');
for (const shard of [manifest.shards[0], manifest.shards.at(-1)]) {
  if (!/^[-a-zA-Z0-9_.]+\.gz$/.test(shard.file)) throw new Error('Unsafe shard filename');
  const response = await fetch(`https://${hosts[0]}/static-shards/${shard.file}`, { method: 'HEAD', signal: AbortSignal.timeout(30000) });
  if (!response.ok) throw new Error(`Published shard missing: ${shard.file}`);
  evidence.push({ shard: shard.file, status: response.status });
}
await writeFile('live-verification.json', JSON.stringify(evidence, null, 2));
console.log(JSON.stringify({ event: 'live_verification_ok', checks: evidence.length, shards: manifest.shards.length }));
