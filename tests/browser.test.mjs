import test from 'node:test';
import assert from 'node:assert/strict';
import puppeteer from 'puppeteer';
import { createEdge } from './edge-fixture.mjs';

test('headless desktop/mobile: readable links, no overflow and no dependence on archive loading', { timeout: 90000 }, async () => {
  const mf = await createEdge();
  let browser;
  try {
    browser = await puppeteer.launch({ headless: true, args: ['--no-sandbox'] });
    const base = String(await mf.ready);
    const page = await browser.newPage();
    await page.setUserAgent('Mozilla/5.0 Chrome/130.0.0.0 Safari/537.36');
    await page.evaluateOnNewDocument(() => Object.defineProperty(navigator, 'webdriver', { get: () => false }));
    for (const [path, width, height] of [['/', 1440, 1000], ['/mobile', 390, 844]]) {
      await page.setViewport({ width, height });
      await page.goto(new URL(path, base).href, { waitUntil: 'domcontentloaded' });
      await page.waitForSelector('#house-promotion [data-product="browserbox"]');
      const layout = await page.$eval('#house-promotion', node => ({
        text: node.textContent, top: node.getBoundingClientRect().top,
        width: node.getBoundingClientRect().width, scroll: node.scrollWidth,
        links: [...node.querySelectorAll('a')].map(a => ({ href: a.href, width: a.getBoundingClientRect().width })),
      }));
      assert.match(layout.text, /TDF \/ Tree/);
      assert.match(layout.text, /Grip/);
      assert.match(layout.text, /BrowserBox/);
      assert.match(layout.text, /Grip — agentic web browsing/);
      assert.ok(layout.top < height / 2);
      assert.ok(layout.scroll <= layout.width + 1, `promotion overflow at ${width}px`);
      assert.equal(layout.links.length, 3);
      assert.ok(layout.links.every(link => link.width > 0 && link.href.includes('utm_medium=house_promo')));
      await page.screenshot({ path: `/tmp/hackerbook-promotion-${width}.png`, fullPage: false });
    }
    const measured = page.waitForResponse(response => response.url().endsWith('/_promotion/event') && response.status() === 204);
    await page.click('.house-label');
    await measured;
    await page.setJavaScriptEnabled(false);
    await page.goto(base, { waitUntil: 'domcontentloaded' });
    assert.equal(await page.$$eval('#house-promotion a', links => links.length), 3);
  } finally {
    if (browser) await browser.close();
    await mf.dispose();
  }
});
