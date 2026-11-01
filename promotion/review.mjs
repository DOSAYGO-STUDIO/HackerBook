import { PRODUCT_IDS, validateConfig } from './catalog.mjs';

export function wilson(clicks, exposures) {
  if (exposures === 0) return [0, 1];
  const z = 1.96, p = clicks / exposures, denominator = 1 + z * z / exposures;
  const center = (p + z * z / (2 * exposures)) / denominator;
  const margin = z * Math.sqrt(p * (1 - p) / exposures + z * z / (4 * exposures * exposures)) / denominator;
  return [center - margin, center + margin];
}

function productDecision(id, weights, rows, ageDays) {
  const stats = weights.map((weight, variant) => ({ variant, weight, exposures: 0, clicks: 0 }));
  for (const row of rows.filter(r => r.product === id)) {
    if (!Number.isInteger(row.variant) || !stats[row.variant]
      || !Number.isSafeInteger(row.exposures) || row.exposures < 0
      || !Number.isSafeInteger(row.clicks) || row.clicks < 0) throw new Error(`Invalid promotion metrics for ${id}`);
    stats[row.variant].exposures += row.exposures;
    stats[row.variant].clicks += row.clicks;
  }
  const result = { product: id, stats, weights, reason: 'insufficient evidence; retain current experiment' };
  if (stats.some(s => s.clicks > s.exposures || (s.exposures > 100 && s.clicks / s.exposures > 0.5))) {
    return { ...result, reason: 'anomalous click counts; no automatic change' };
  }
  const active = stats.filter(s => s.weight > 0);
  if (ageDays >= 42 && active.some(s => s.exposures < 200) && active.length === 2) {
    return { ...result, weights: [34, 33, 33],
      reason: 'six weeks of low volume; broaden approved-copy exploration, no winner claimed' };
  }
  if (ageDays < 14 || active.some(s => s.exposures < 200)) return result;
  const ranked = [...active].sort((a, b) => b.clicks / b.exposures - a.clicks / a.exposures);
  if (ranked[0].clicks >= 20 && wilson(ranked[0].clicks, ranked[0].exposures)[0]
    > Math.max(...ranked.slice(1).map(s => wilson(s.clicks, s.exposures)[1]))) {
    return { ...result, weights: weights.map((_, i) => i === ranked[0].variant ? 80 : 10),
      reason: `variant ${ranked[0].variant} has separated 95% intervals; 80% winner, 20% exploration` };
  }
  if (ageDays >= 28 && active.every(s => s.clicks === 0)) {
    const untested = stats.find(s => s.weight === 0);
    if (untested) return { ...result, weights: weights.map((_, i) => i === untested.variant ? 80 : 10),
      reason: `no clicks after adequate exposure; test approved alternative ${untested.variant}` };
  }
  return { ...result, reason: 'no clear winner; retain current experiment' };
}

export function review(config, rows, now = new Date()) {
  validateConfig(config);
  if (!Array.isArray(rows) || rows.some(r => !PRODUCT_IDS.includes(r.product))) throw new Error('Invalid promotion report rows');
  if (!config.enabled) return { config, changed: false, decisions: [], reason: 'campaign disabled; no changes' };
  const ageDays = (now - new Date(config.lastChangedAt)) / 86400000;
  const decisions = PRODUCT_IDS.map(id => productDecision(id, config.weights[id], rows, ageDays));
  const weights = Object.fromEntries(decisions.map(d => [d.product, d.weights]));
  const changed = JSON.stringify(weights) !== JSON.stringify(config.weights);
  const next = changed ? { ...config, weights, revision: config.revision + 1, lastChangedAt: now.toISOString() } : config;
  return { config: validateConfig(next), changed, decisions };
}
