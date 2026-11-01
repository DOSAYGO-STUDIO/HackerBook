import { readFile, writeFile } from 'node:fs/promises';
import { pathToFileURL } from 'node:url';

// Repository activity keeps GitHub's scheduled workflow eligible beyond 60 days.
// This timestamp is not a success signal and contains no traffic or sales data.
export function nextHeartbeat(previous, now = new Date()) {
  if (previous !== null) {
    const recorded = Date.parse(previous?.recordedAt);
    if (previous?.version !== 1 || !Number.isFinite(recorded) || recorded > now.getTime()) {
      throw new Error('Invalid promotion review heartbeat');
    }
    if (now.getTime() - recorded < 28 * 86400000) return null;
  }
  return { version: 1, recordedAt: now.toISOString() };
}

async function main() {
  const file = '.github/promotion-review-heartbeat.json';
  let previous;
  try { previous = JSON.parse(await readFile(file, 'utf8')); }
  catch (error) {
    if (error.code !== 'ENOENT') throw error;
    previous = null;
  }
  const next = nextHeartbeat(previous);
  if (next) await writeFile(file, JSON.stringify(next, null, 2) + '\n');
  console.log(next ? 'Recorded monthly scheduler activity timestamp' : 'Scheduler activity timestamp is recent; no commit needed');
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) await main();
