import { CONFIG_KEY, PRODUCTS, browserBoxEligible, candidateBrowser, renderPromotion, validateConfig } from './catalog.mjs';
import { readEventBody } from './body.mjs';

const ENTRY_PATHS = new Set(['/', '/index', '/index.html', '/static', '/static.html', '/mobile', '/mobile.html']);
const noStore = { 'Cache-Control': 'private, no-store', 'CDN-Cache-Control': 'no-store' };

async function receiveEvent(request, env) {
  const url = new URL(request.url);
  if (request.method !== 'POST') return new Response(null, { status: 405, headers: { ...noStore, Allow: 'POST' } });
  if (request.headers.get('origin') !== url.origin || !candidateBrowser(request)) return new Response(null, { status: 403, headers: noStore });
  if (!request.headers.get('content-type')?.startsWith('application/json')) return new Response(null, { status: 415, headers: noStore });
  const { value, status } = await readEventBody(request);
  if (status) return new Response(null, { status, headers: noStore });
  const product = Object.hasOwn(PRODUCTS, value?.product) ? PRODUCTS[value.product] : null;
  if (!product || !['exposure', 'click'].includes(value.event)
    || !Number.isInteger(value.variant) || value.variant < 0 || value.variant >= product.copy.length
    || !Number.isSafeInteger(value.revision) || value.revision < 1
    || (value.product === 'browserbox' && !browserBoxEligible(request))) {
    return new Response(null, { status: 400, headers: noStore });
  }
  if (!env.PROMO_EVENTS) throw new Error('Promotion telemetry: PROMO_EVENTS binding missing');
  env.PROMO_EVENTS.writeDataPoint({ indexes: [url.hostname],
    blobs: [value.product, String(value.variant), value.event, url.hostname, request.cf?.country ?? 'XX', String(value.revision)],
    doubles: [1] });
  return new Response(null, { status: 204, headers: noStore });
}

export async function handleRequest(request, env) {
  const pathname = new URL(request.url).pathname;
  if (pathname === '/_promotion/event') {
    try { return await receiveEvent(request, env); }
    catch (error) {
      console.error(JSON.stringify({ event: 'promotion_telemetry_failed', message: error.message }));
      return new Response(null, { status: 503, headers: noStore });
    }
  }
  let assetRequest = request;
  if (ENTRY_PATHS.has(pathname) && request.method === 'GET') {
    const assetHeaders = new Headers(request.headers);
    assetHeaders.delete('if-none-match');
    assetHeaders.delete('if-modified-since');
    assetRequest = new Request(request, { headers: assetHeaders });
  }
  const response = await env.ASSETS.fetch(assetRequest);
  if (!ENTRY_PATHS.has(pathname) || !response.ok || request.method !== 'GET'
    || !response.headers.get('content-type')?.includes('text/html')) return response;
  const headers = new Headers(response.headers);
  headers.set('Content-Type', 'text/html; charset=utf-8');
  for (const [key, value] of Object.entries(noStore)) headers.set(key, value);
  headers.delete('etag');
  headers.delete('last-modified');
  headers.delete('content-length');
  const html = new Response(response.body, { status: response.status, headers });
  try {
    if (!env.PROMO_CONFIG) throw new Error('PROMO_CONFIG binding missing');
    const config = validateConfig(await env.PROMO_CONFIG.get(CONFIG_KEY, { type: 'json', cacheTtl: 300 }));
    headers.set('X-Promotion-Revision', String(config.revision));
    const content = renderPromotion(request, config);
    return new HTMLRewriter().on('#house-promotion', {
      element(element) { element.setInnerContent(content, { html: true }); },
    }).transform(new Response(html.body, { status: html.status, headers }));
  } catch (error) {
    // Explicit degraded mode: preserve static TDF/Grip links, never expose BrowserBox.
    console.error(JSON.stringify({ event: 'promotion_static_fallback', message: error.message }));
    return html;
  }
}

export default { fetch: handleRequest };
