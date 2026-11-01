import { build } from 'esbuild';
import { Miniflare, convertV4MiniflareOptions } from 'miniflare';
import { readFile } from 'node:fs/promises';
import { resolve, extname } from 'node:path';
import { initialConfig, CONFIG_KEY } from '../promotion/catalog.mjs';

export async function createEdge() {
  const bundle = await build({ entryPoints: ['docs/_worker.js'], bundle: true, write: false, format: 'esm', platform: 'browser' });
  const root = resolve('docs');
  const types = { '.html': 'text/html', '.css': 'text/css', '.js': 'application/javascript', '.wasm': 'application/wasm' };
  const mf = new Miniflare(convertV4MiniflareOptions({ modules: true, script: bundle.outputFiles[0].text,
    compatibilityDate: '2026-10-01', cf: { country: 'US' }, kvNamespaces: ['PROMO_CONFIG'],
    analyticsEngineDatasets: { PROMO_EVENTS: { dataset: 'hackerbook_promotions' } },
    serviceBindings: { ASSETS: async request => {
      let pathname = new URL(request.url).pathname;
      if (pathname === '/') pathname = '/index.html';
      if (['/index', '/mobile', '/static'].includes(pathname)) pathname += '.html';
      const file = resolve(root, '.' + pathname);
      if (!file.startsWith(root + '/')) return new Response(null, { status: 403 });
      try {
        const body = await readFile(file);
        return new Response(body, { headers: { 'content-type': types[extname(file)] ?? 'application/octet-stream', etag: 'fixture', 'cache-control': 'public, max-age=86400' } });
      } catch (error) {
        if (error.code !== 'ENOENT') throw error;
        return new Response(null, { status: 404 });
      }
    } },
  }));
  try {
    const kv = await mf.getKVNamespace('PROMO_CONFIG');
    await kv.put(CONFIG_KEY, JSON.stringify(initialConfig()));
    return mf;
  } catch (error) {
    await mf.dispose();
    throw error;
  }
}
