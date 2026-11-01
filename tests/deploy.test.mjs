import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { cloudflareClient } from '../tools/cloudflare-api.mjs';

test('deployment errors preserve API context and missing credentials fail before networking', async () => {
  assert.throws(() => cloudflareClient({}), /ACCOUNT_ID/);
  assert.throws(() => cloudflareClient({ CLOUDFLARE_ACCOUNT_ID: 'a'.repeat(32) }), /API_TOKEN/);
  const { request } = cloudflareClient({ CLOUDFLARE_ACCOUNT_ID: 'a'.repeat(32), CLOUDFLARE_API_TOKEN: 'test-token' },
    async () => new Response('{"errors":[{"code":10000,"message":"Authentication error"}]}', { status: 403 }));
  await assert.rejects(request('pages/projects/static-news'), /pages\/projects\/static-news: HTTP 403.*Authentication error/);
});

test('full rebuild is dispatch-only, preflights before expensive work, and has no yes pipe', async () => {
  const workflow = await readFile(new URL('../.github/workflows/daily-rebuild.yml', import.meta.url), 'utf8');
  assert.doesNotMatch(workflow, /schedule:|cron:|yes y \|/);
  assert.match(workflow, /workflow_dispatch:/);
  assert.ok(workflow.indexOf('run: npm run deploy-preflight') < workflow.indexOf('run: bash .\/toool\/s\/predeploy-checks.sh'));
  const script = await readFile(new URL('../toool/s/predeploy-checks.sh', import.meta.url), 'utf8');
  assert.ok(script.indexOf('archive build was not started') < script.indexOf('step "Checking raw data"'));
});

test('weekly review cannot dispatch a full build', async () => {
  const workflow = await readFile(new URL('../.github/workflows/promotion-review.yml', import.meta.url), 'utf8');
  assert.match(workflow, /schedule:/);
  assert.match(workflow, /review-promotions.mjs --apply/);
  assert.doesNotMatch(workflow, /actions: write|predeploy-checks.sh|gh workflow run/);
});
