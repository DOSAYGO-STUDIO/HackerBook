const fs = require('fs');
const path = require('path');
const Database = require('better-sqlite3');

const manifestPath = path.join('docs', 'static-manifest.json');
const shardsDir = path.join('docs', 'static-shards');
const outPath = path.join('docs', 'archive-index.json');

const manifest = JSON.parse(fs.readFileSync(manifestPath, 'utf8'));
const shards = manifest.shards || [];

const out = {
  generated_at: new Date().toISOString(),
  snapshot_time: manifest.snapshot_time || null,
  totals: {
    items: 0,
    posts: 0,
    comments: 0,
    bytes: 0,
    shards: shards.length
  },
  manifests: [],
  shards: []
};

const manifestFiles = [
  { file: 'static-manifest.json', note: 'Shard metadata, ranges, and snapshot time.' },
  { file: 'filter-manifest.json', note: 'Prime filter data for the main view.' }
];

for (const entry of manifestFiles) {
  const full = path.join('docs', entry.file);
  if (!fs.existsSync(full)) continue;
  const stat = fs.statSync(full);
  out.manifests.push({
    file: entry.file,
    bytes: stat.size,
    note: entry.note
  });
}

for (const shard of shards) {
  const file = shard.file;
  const fullPath = path.join(shardsDir, file);
  const stat = fs.statSync(fullPath);
  const db = new Database(fullPath, { readonly: true });
  const timeCountRow = db.prepare(`SELECT COUNT(*) as c FROM items WHERE time IS NOT NULL`).get();
  const timeCount = timeCountRow.c || 0;
  let tminEff = shard.tmin || null;
  let tmaxEff = shard.tmax || null;
  let tnull = 0;
  if (timeCount > 0) {
    const p1 = Math.floor((timeCount - 1) * 0.01);
    const p99 = Math.floor((timeCount - 1) * 0.99);
    const rowMin = db.prepare(`SELECT time as t FROM items WHERE time IS NOT NULL ORDER BY time LIMIT 1 OFFSET ?`).get(p1);
    const rowMax = db.prepare(`SELECT time as t FROM items WHERE time IS NOT NULL ORDER BY time LIMIT 1 OFFSET ?`).get(p99);
    tminEff = rowMin ? rowMin.t : tminEff;
    tmaxEff = rowMax ? rowMax.t : tmaxEff;
  }
  const nullRow = db.prepare(`SELECT COUNT(*) as c FROM items WHERE time IS NULL`).get();
  tnull = nullRow.c || 0;
  const row = db.prepare(`
    SELECT
      COUNT(*) as items,
      SUM(CASE WHEN type='comment' THEN 1 ELSE 0 END) as comments,
      SUM(CASE WHEN type!='comment' THEN 1 ELSE 0 END) as posts
    FROM items
  `).get();
  db.close();

  const items = row.items || 0;
  const comments = row.comments || 0;
  const posts = row.posts || 0;

  out.totals.items += items;
  out.totals.comments += comments;
  out.totals.posts += posts;
  out.totals.bytes += stat.size;

  out.shards.push({
    sid: shard.sid,
    file,
    tmin: shard.tmin || null,
    tmax: shard.tmax || null,
    tmin_eff: tminEff,
    tmax_eff: tmaxEff,
    time_null: tnull,
    id_lo: shard.id_lo || null,
    id_hi: shard.id_hi || null,
    count: items,
    posts,
    comments,
    bytes: stat.size
  });
}

fs.writeFileSync(outPath, JSON.stringify(out, null, 2));
console.log(`Wrote ${outPath}`);
