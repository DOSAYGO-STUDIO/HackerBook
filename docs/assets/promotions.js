// No cookies, user IDs, timers, navigation interception or page-view inflation.
const promotion = document.querySelector('#house-promotion');
const campaign = promotion?.querySelector('[data-campaign]');
if (campaign && !navigator.webdriver && navigator.doNotTrack !== '1') {
  const state = { exposed: new Set(), clicked: new Set(), pending: new Set() };
  function visible(card) {
    const rect = card.getBoundingClientRect();
    return document.visibilityState === 'visible' && rect.top < innerHeight && rect.bottom > 0;
  }
  function record(card, event) {
    const set = event === 'click' ? state.clicked : state.exposed;
    if (set.has(card.dataset.product)) return;
    set.add(card.dataset.product);
    const operation = fetch('/_promotion/event', { method: 'POST', keepalive: true,
      headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ event,
        product: card.dataset.product, variant: Number(card.dataset.variant), revision: Number(campaign.dataset.campaign) })
    }).then(response => {
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
    }).catch(error => {
      console.warn('Promotion measurement unavailable:', error.message);
    }).finally(() => state.pending.delete(operation));
    state.pending.add(operation);
  }
  function expose(event) {
    if (!event.isTrusted) return;
    for (const card of campaign.querySelectorAll('[data-product]')) if (visible(card)) record(card, 'exposure');
  }
  function click(event) {
    if (!event.isTrusted || (event.type === 'auxclick' && event.button !== 1)) return;
    const card = event.target.closest('[data-product]');
    if (!card || !event.target.closest('a') || !visible(card)) return;
    record(card, 'exposure');
    record(card, 'click');
  }
  const listeners = new AbortController();
  for (const type of ['pointerdown', 'keydown', 'wheel', 'touchstart']) {
    document.addEventListener(type, expose, { passive: true, signal: listeners.signal });
  }
  promotion.addEventListener('click', click, { signal: listeners.signal });
  promotion.addEventListener('auxclick', click, { signal: listeners.signal });
  // Keep listeners through bfcache restores; document destruction owns their lifetime.
}
