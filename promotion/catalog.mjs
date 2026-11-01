export const PRODUCTS = Object.freeze({
  tdf: {
    label: 'TDF / Tree — web archiver', url: 'https://trees.dosaygo.com/',
    copy: ['Keep the page before it disappears.', 'Build a web collection you can open offline.', 'Save web pages. Reopen them in Tree Player.'],
  },
  grip: {
    label: 'Grip — agentic web browsing', url: 'https://gripui.com/',
    copy: ['Give your agent a browser that is already signed in.', 'Let your agent browse. Take over when you need to.', 'Use your existing browser profiles from an agent CLI.'],
  },
  browserbox: {
    label: 'BrowserBox — remote browser isolation', url: 'https://browserbox.io/',
    copy: ['Run the browser remotely. Keep web content off the endpoint.', 'Bring secure remote browsing to your team.', 'Evaluate a browser you can host on your own infrastructure.'],
  },
});

export const PRODUCT_IDS = Object.freeze(Object.keys(PRODUCTS));
export const CONFIG_KEY = 'campaign-v1';
export const CHINA_CRAWLERS = /baidu|sogou|sosospider|360spider|haosouspider|bytespider|petalbot|yisouspider|shenmaspider|toutiaospider|quarkspider/i;
export const AUTOMATION = /bot|spider|crawl|curl|wget|python|^node$|headless|httpclient|http-client|scanner|facebookexternalhit|httpx|axios|aiohttp|scrapy|archive\.org|authorized-ai-gateway/i;

export function browserBoxEligible(request) {
  const country = request.cf?.country;
  return /^[A-Z]{2}$/.test(country ?? '') && !['CN', 'HK', 'MO', 'XX', 'T1'].includes(country)
    && !CHINA_CRAWLERS.test(request.headers.get('user-agent') ?? '');
}

export function candidateBrowser(request) {
  const ua = request.headers.get('user-agent') ?? '';
  return /Mozilla\//.test(ua) && !AUTOMATION.test(ua) && !CHINA_CRAWLERS.test(ua)
    && !/Chrome\/(99\.0\.4844\.51|48\.0\.2564\.116)/.test(ua);
}

export function initialConfig(now = new Date()) {
  return { version: 1, enabled: true, revision: 1, startedAt: now.toISOString(),
    lastChangedAt: now.toISOString(), weights: Object.fromEntries(PRODUCT_IDS.map(id => [id, [50, 50, 0]])) };
}

export function validateConfig(value) {
  if (!value || value.version !== 1 || typeof value.enabled !== 'boolean'
    || !Number.isSafeInteger(value.revision) || value.revision < 1
    || !Number.isFinite(Date.parse(value.startedAt)) || !Number.isFinite(Date.parse(value.lastChangedAt))) {
    throw new Error('Promotion config: invalid version, enabled, revision or dates');
  }
  if (!value.weights || Object.keys(value.weights).sort().join() !== [...PRODUCT_IDS].sort().join()) {
    throw new Error('Promotion config: weights must contain exactly the supported products');
  }
  for (const id of PRODUCT_IDS) {
    const weights = value.weights[id];
    if (!Array.isArray(weights) || weights.length !== PRODUCTS[id].copy.length
      || weights.some(n => !Number.isInteger(n) || n < 0 || n > 80)
      || weights.reduce((a, b) => a + b, 0) !== 100) {
      throw new Error(`Promotion config: invalid weights for ${id}`);
    }
  }
  return value;
}

export function chooseVariant(weights, fraction) {
  let cumulative = 0;
  for (let i = 0; i < weights.length; i++) {
    cumulative += weights[i];
    if (fraction * 100 < cumulative) return i;
  }
  throw new Error('Promotion variant: random fraction must be in [0, 1)');
}

export function escapeHtml(value) {
  return String(value).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[c]);
}

export function renderPromotion(request, config, random = Math.random) {
  if (!config.enabled) return '';
  const ids = PRODUCT_IDS.filter(id => id !== 'browserbox' || browserBoxEligible(request));
  const source = new URL(request.url).hostname;
  const featured = ids[Math.floor(random() * ids.length)];
  const cards = ids.map(id => {
    const product = PRODUCTS[id];
    const variant = chooseVariant(config.weights[id], random());
    const destination = new URL(product.url);
    destination.search = new URLSearchParams({ utm_source: source, utm_medium: 'house_promo',
      utm_campaign: 'hackerbook_tools_v1', utm_content: `${id}-${variant}` }).toString();
    return `<div class="house-product${id === featured ? ' house-featured' : ''}" data-product="${id}" data-variant="${variant}"><a href="${escapeHtml(destination)}">${escapeHtml(product.label)}</a><span>${escapeHtml(product.copy[variant])}</span></div>`;
  });
  return `<div class="house-label">Tools from DOSAYGO</div><div class="house-products" data-campaign="${config.revision}">${cards.join('')}</div>`;
}
