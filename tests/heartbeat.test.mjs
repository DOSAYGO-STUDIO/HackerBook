import test from 'node:test';
import assert from 'node:assert/strict';
import { nextHeartbeat } from '../tools/promotion-heartbeat.mjs';

test('scheduler activity refreshes monthly, rather than committing every weekly run', () => {
  const first = nextHeartbeat(null, new Date('2026-11-01T13:17:00Z'));
  assert.equal(first.recordedAt, '2026-11-01T13:17:00.000Z');
  assert.equal(nextHeartbeat(first, new Date('2026-11-08T13:17:00Z')), null);
  assert.equal(nextHeartbeat(first, new Date('2026-11-28T13:17:00Z')), null);
  assert.equal(nextHeartbeat(first, new Date('2026-11-29T13:17:00Z')).recordedAt, '2026-11-29T13:17:00.000Z');
});

test('bad or future heartbeat state fails visibly instead of suppressing activity indefinitely', () => {
  assert.throws(() => nextHeartbeat({ version: 1, recordedAt: 'invalid' }), /Invalid/);
  assert.throws(() => nextHeartbeat({ version: 1, recordedAt: '2099-01-01' }), /Invalid/);
});
