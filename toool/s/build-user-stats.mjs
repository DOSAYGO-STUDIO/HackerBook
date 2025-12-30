#!/usr/bin/env node
/*
 * Build user stats shards from item shards.
 * Output: docs/static-user-stats-shards/user_<sid>.sqlite(.gz)
 * Manifest: docs/static-user-stats-manifest.json
 */

import fs from 'fs';
import fsp from 'fs/promises';
import path from 'path';
import os from 'os';
import zlib from 'zlib';
import Database from 'better-sqlite3';

const DEFAULT_MANIFEST = 'docs/static-manifest.json';
const DEFAULT_SHARDS_DIR = 'docs/static-shards';
const DEFAULT_OUT_DIR = 'docs/static-user-stats-shards';
const DEFAULT_OUT_MANIFEST = 'docs/static-user-stats-manifest.json';

const ALPHABET = '0123456789abcdefghijklmnopqrstuvwxyz_';
const BUCKETS = ALPHABET.length;

function usage() {
  const msg = `Usage:
  toool/s/build-user-stats.mjs [--manifest PATH] [--shards-dir PATH]
                               [--out-dir PATH] [--out-manifest PATH]
                               [--gzip] [--keep-sqlite]

Examples:
  toool/s/build-user-stats.mjs --gzip
`;
  process.stdout.write(msg);
}

function parseArgs(argv) {
  const out = {
    manifest: DEFAULT_MANIFEST,
    shardsDir: DEFAULT_SHARDS_DIR,
    outDir: DEFAULT_OUT_DIR,
    outManifest: DEFAULT_OUT_MANIFEST,
    gzip: false,
    keepSqlite: false
  };

  for (let i = 2; i < argv.length; i += 1) {
    const a = argv[i];
    if (a === '--help' || a === '-h') {
      usage();
      process.exit(0);
    }
    if (!a.startsWith('--')) continue;
    const key = a.slice(2);
    const next = argv[i + 1];
    if (!next || next.startsWith('--')) {
      out[key] = true;
      continue;
    }
    out[key] = next;
    i += 1;
  }

  return out;
}

function readJson(p) {
  return JSON.parse(fs.readFileSync(p, 'utf8'));
}

function charBucket(name) {
  if (!name) return ALPHABET.indexOf('_');
  const c = String(name).trim().toLowerCase()[0] || '_';
  const idx = ALPHABET.indexOf(c);
  return idx >= 0 ? idx : ALPHABET.indexOf('_');
}

function gzipFileSync(srcPath, dstPath) {
  const data = fs.readFileSync(srcPath);
  const gz = zlib.gzipSync(data, { level: 9 });
  const tmpPath = `${dstPath}.tmp`;
  fs.writeFileSync(tmpPath, gz);
  fs.renameSync(tmpPath, dstPath);
  return gz.length;
}

function validateGzipFileSync(gzPath) {
  // throws on failure
  zlib.gunzipSync(fs.readFileSync(gzPath));
}

async function gunzipToTemp(srcPath, tmpRoot) {
  const dstPath = path.join(tmpRoot, path.basename(srcPath, '.gz'));
  await new Promise((resolve, reject) => {
    const src = fs.createReadStream(srcPath);
    const gunzip = zlib.createGunzip();
    const dst = fs.createWriteStream(dstPath);
    src.on('error', reject);
    gunzip.on('error', reject);
    dst.on('error', reject);
    dst.on('finish', resolve);
    src.pipe(gunzip).pipe(dst);
  });
  return dstPath;
}

function openShardDb(shardPath, tmpRoot, tempFiles) {
  if (!shardPath.endsWith('.gz')) return { path: shardPath, cleanup: false };
  const tmpPath = path.join(tmpRoot, path.basename(shardPath, '.gz'));
  if (fs.existsSync(tmpPath)) return { path: tmpPath, cleanup: false };
  const data = zlib.gunzipSync(fs.readFileSync(shardPath));
  fs.writeFileSync(tmpPath, data);
  tempFiles.add(tmpPath);
  return { path: tmpPath, cleanup: true };
}

function initUserDb(dbPath) {
  if (fs.existsSync(dbPath)) fs.unlinkSync(dbPath);
  const db = new Database(dbPath);
  db.pragma('journal_mode = OFF');
  db.pragma('synchronous = OFF');
  db.exec(`
    CREATE TABLE users (
      username TEXT PRIMARY KEY,
      first_time INTEGER,
      last_time INTEGER,
      items INTEGER,
      comments INTEGER,
      stories INTEGER,
      ask INTEGER,
      show INTEGER,
      launch INTEGER,
      jobs INTEGER,
      polls INTEGER,
      avg_score REAL,
      sum_score INTEGER,
      max_score INTEGER,
      min_score INTEGER,
      max_score_id INTEGER,
      max_score_title TEXT
    );

    CREATE TABLE user_domains (
      username TEXT NOT NULL,
      domain TEXT NOT NULL,
      count INTEGER NOT NULL,
      PRIMARY KEY(username, domain)
    );

    CREATE TABLE user_months (
      username TEXT NOT NULL,
      month TEXT NOT NULL,
      count INTEGER NOT NULL,
      PRIMARY KEY(username, month)
    );
  `);
  return db;
}

function monthKey(ts) {
  if (!ts) return null;
  const d = new Date(ts * 1000);
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, '0');
  return `${y}-${m}`;
}

function domainFromUrl(url) {
  try {
    const u = new URL(url);
    return u.host.replace(/^www\./, '');
  } catch {
    return null;
  }
}

async function main() {
  const args = parseArgs(process.argv);
  const manifestPath = path.resolve(args.manifest);
  const shardsDir = path.resolve(args.shardsDir);
  const outDir = path.resolve(args.outDir);
  const outManifest = path.resolve(args.outManifest);
  const gzipOut = !!args.gzip;
  const keepSqlite = !!args['keep-sqlite'];

  if (!fs.existsSync(manifestPath)) {
    console.error(`Manifest not found: ${manifestPath}`);
    process.exit(1);
  }

  const manifest = readJson(manifestPath);
  const shards = (manifest.shards || []).slice().sort((a, b) => a.sid - b.sid);
  if (!shards.length) {
    console.error('No shards found in manifest.');
    process.exit(1);
  }

  await fsp.mkdir(outDir, { recursive: true });
  await fsp.mkdir(path.dirname(outManifest), { recursive: true });

  const tmpRoot = await fsp.mkdtemp(path.join(os.tmpdir(), 'static-news-user-'));
  const tempFiles = new Set();

  const shardDbs = [];
  for (let i = 0; i < BUCKETS; i += 1) {
    const dbPath = path.join(outDir, `user_${i}.sqlite`);
    const db = initUserDb(dbPath);
    shardDbs.push({ db, path: dbPath, sid: i, char: ALPHABET[i] });
  }

  const upsertUser = shardDbs.map(({ db }) => db.prepare(`
    INSERT INTO users (username, first_time, last_time, items, comments, stories, ask, show, launch, jobs, polls, avg_score, sum_score, max_score, min_score, max_score_id, max_score_title)
    VALUES (@username, @first_time, @last_time, 1, @comments, @stories, @ask, @show, @launch, @jobs, @polls, @avg_score, @sum_score, @max_score, @min_score, @max_score_id, @max_score_title)
    ON CONFLICT(username) DO UPDATE SET
      first_time = MIN(first_time, excluded.first_time),
      last_time = MAX(last_time, excluded.last_time),
      items = users.items + 1,
      comments = users.comments + excluded.comments,
      stories = users.stories + excluded.stories,
      ask = users.ask + excluded.ask,
      show = users.show + excluded.show,
      launch = users.launch + excluded.launch,
      jobs = users.jobs + excluded.jobs,
      polls = users.polls + excluded.polls,
      sum_score = users.sum_score + excluded.sum_score,
      max_score = MAX(users.max_score, excluded.max_score),
      min_score = MIN(users.min_score, excluded.min_score),
      max_score_id = CASE WHEN excluded.max_score > users.max_score THEN excluded.max_score_id ELSE users.max_score_id END,
      max_score_title = CASE WHEN excluded.max_score > users.max_score THEN excluded.max_score_title ELSE users.max_score_title END
  `));

  const upsertDomain = shardDbs.map(({ db }) => db.prepare(`
    INSERT INTO user_domains (username, domain, count)
    VALUES (?, ?, 1)
    ON CONFLICT(username, domain) DO UPDATE SET count = count + 1
  `));

  const upsertMonth = shardDbs.map(({ db }) => db.prepare(`
    INSERT INTO user_months (username, month, count)
    VALUES (?, ?, 1)
    ON CONFLICT(username, month) DO UPDATE SET count = count + 1
  `));

  const txUser = shardDbs.map(({ db }, idx) => db.transaction((rows) => {
    for (const r of rows) {
      upsertUser[idx].run(r);
      if (r.domain) upsertDomain[idx].run(r.username, r.domain);
      if (r.month) upsertMonth[idx].run(r.username, r.month);
    }
  }));

  let totalItems = 0;
  let totalUsers = 0;
  const growthCounts = new Map();
  let shardIndex = 0;

  try {
    for (const shard of shards) {
      shardIndex += 1;
      const shardPath = path.join(shardsDir, shard.file);
      if (!fs.existsSync(shardPath)) {
        console.warn(`Missing shard file: ${shardPath}`);
        continue;
      }

      process.stdout.write(`\r[users] shard ${shardIndex}/${shards.length} sid ${shard.sid}... `);
      let dbPath = shardPath;
      if (shardPath.endsWith('.gz')) {
        try {
          dbPath = await gunzipToTemp(shardPath, tmpRoot);
          tempFiles.add(dbPath);
        } catch (err) {
          console.warn(`Failed to gunzip shard ${shard.sid}: ${err.code || err.message}`);
          continue;
        }
      }

      const db = new Database(dbPath, { readonly: true });
      const iter = db.prepare('SELECT id, type, time, by, title, url, score FROM items WHERE by IS NOT NULL').iterate();

      const buffers = Array.from({ length: BUCKETS }, () => []);
      const seenUsers = new Set();

      for (const row of iter) {
        const username = String(row.by);
        if (!seenUsers.has(username)) {
          seenUsers.add(username);
          totalUsers += 1;
        }
        const bucket = charBucket(username);
        const isComment = row.type === 'comment' ? 1 : 0;
        const isStory = row.type === 'story' ? 1 : 0;
        const isJob = row.type === 'job' ? 1 : 0;
        const isPoll = row.type === 'poll' ? 1 : 0;
        const title = row.title || '';
        const isAsk = isStory && /^Ask HN:/i.test(title) ? 1 : 0;
        const isShow = isStory && /^Show HN:/i.test(title) ? 1 : 0;
        const isLaunch = isStory && /^Launch HN:/i.test(title) ? 1 : 0;
        const score = Number.isFinite(row.score) ? row.score : 0;

        buffers[bucket].push({
          username,
          first_time: row.time || null,
          last_time: row.time || null,
          comments: isComment,
          stories: isStory,
          ask: isAsk,
          show: isShow,
          launch: isLaunch,
          jobs: isJob,
          polls: isPoll,
          avg_score: score,
          sum_score: score,
          max_score: score,
          min_score: score,
          max_score_id: row.id || null,
          max_score_title: row.title || null,
          domain: row.url ? domainFromUrl(row.url) : null,
          month: row.time ? monthKey(row.time) : null
        });

        totalItems += 1;
        if (totalItems % 200000 === 0) {
          process.stdout.write(`\r[users] shard ${shardIndex}/${shards.length} sid ${shard.sid} | items ${totalItems.toLocaleString('en-US')} | users ${totalUsers.toLocaleString('en-US')}`);
        }
      }
      db.close();

      for (let i = 0; i < BUCKETS; i += 1) {
        if (buffers[i].length) txUser[i](buffers[i]);
      }
      process.stdout.write(`\r[users] shard ${shardIndex}/${shards.length} sid ${shard.sid} | items ${totalItems.toLocaleString('en-US')} | users ${totalUsers.toLocaleString('en-US')} ok`);
    }

    process.stdout.write(`\r[users] items ${totalItems.toLocaleString('en-US')} | users ${totalUsers.toLocaleString('en-US')}\n`);

    const shardMeta = [];
    let uniqueUsers = 0;
    for (const shardDb of shardDbs) {
      shardDb.db.exec('UPDATE users SET avg_score = CAST(sum_score AS REAL) / NULLIF(items, 0)');
      shardDb.db.exec('CREATE INDEX IF NOT EXISTS idx_users_last_time ON users(last_time)');
      shardDb.db.exec('CREATE INDEX IF NOT EXISTS idx_users_items ON users(items)');
      shardDb.db.exec('CREATE INDEX IF NOT EXISTS idx_user_domains ON user_domains(username)');
      shardDb.db.exec('CREATE INDEX IF NOT EXISTS idx_user_months ON user_months(username)');

      const userRows = shardDb.db.prepare('SELECT first_time FROM users WHERE first_time IS NOT NULL').iterate();
      for (const row of userRows) {
        const m = monthKey(row.first_time);
        if (!m) continue;
        growthCounts.set(m, (growthCounts.get(m) || 0) + 1);
        uniqueUsers += 1;
      }

      shardDb.db.close();

      let finalPath = shardDb.path;
      let bytes = fs.statSync(finalPath).size;
      if (gzipOut) {
        const gzPath = `${finalPath}.gz`;
        const gzBytes = gzipFileSync(finalPath, gzPath);
        try {
          validateGzipFileSync(gzPath);
        } catch (err) {
          console.error(`\n[user] gzip validation failed for shard ${shardDb.sid}: ${err && err.message ? err.message : err}`);
          process.exit(1);
        }
        bytes = gzBytes;
        finalPath = gzPath;
        if (!keepSqlite) fs.unlinkSync(shardDb.path);
      }

      shardMeta.push({
        sid: shardDb.sid,
        char: shardDb.char,
        file: path.basename(finalPath),
        bytes
      });
    }

    const out = {
      version: 1,
      created_at: new Date().toISOString(),
      shards: shardMeta,
      alphabet: ALPHABET,
      totals: {
        users: uniqueUsers
      }
    };

    const growthMonths = Array.from(growthCounts.entries())
      .sort((a, b) => a[0].localeCompare(b[0]));
    let cumulative = 0;
    out.user_growth = growthMonths.map(([month, count]) => {
      cumulative += count;
      return { month, new_users: count, total_users: cumulative };
    });

    fs.writeFileSync(outManifest, JSON.stringify(out, null, 2));
    console.log(`Wrote ${outManifest}`);
  } finally {
    for (const p of tempFiles) {
      try { await fsp.unlink(p); } catch {}
    }
    try { await fsp.rmdir(tmpRoot); } catch {}
  }
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
