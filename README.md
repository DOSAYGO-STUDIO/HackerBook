# Hacker Book — Community, All the HN Belong to You! 2006 - 2025 FOREVER

Static, offline-friendly Hacker News archive shipped as plain files. Everything runs client-side in your browser via SQLite WASM; the browser only downloads the shards it needs.

- Demo: https://hackerbook.dosaygo.com
- Code: https://github.com/DOSAYGO-STUDIO/static-news

## Quick start (browse the archive)
- Clone or download this repo.
- Serve the `docs/` folder locally (any static server works):
  - `npx serve docs` **or** `python3 -m http.server 8000 --directory docs`
- Open http://localhost:8000 (or the port your server reports).
- Time-warp with the date picker; all queries run locally in your browser.

## What’s inside
- `docs/static-shards/`: gzipped SQLite shards of HN items and comments.
- `docs/static-user-stats-shards/`: gzipped SQLite shards with per-user stats and monthly activity.
- `docs/static-manifest.json.gz`, `docs/archive-index.json.gz`, `docs/cross-shard-index.bin.gz`: indexes the app fetches and gunzips on load.
- All assets are static; no backend required.

## Rebuild the user stats shards (optional)
If you’re regenerating from the shipped content (or after re-running ETL):
1) Install deps: `npm install`
2) Build user stats shards + manifest (gzip on):  
   `node toool/s/build-user-stats.mjs --gzip --target-mb 15`
3) Serve `docs/` as above.

## Regenerate everything from BigQuery (advanced)
This repo assumes you already exported the full HN dataset to `docs/static-shards/`. To redo ETL from BigQuery, adapt `etl-hn.js` / `etl-hn.sh` (not documented here) to produce new shards, then run the user stats step above.

## Notes
- Works best on modern browsers (Chrome, Firefox, Safari) with `DecompressionStream`; falls back to pako gzip when needed.
- Mobile: layout is locked to the viewport, and everything runs offline once the needed shards are cached.
- The code for the viewer and ETL pipeline is released under the MIT License.
- The content (Hacker News data) is property of Y Combinator and the respective comment authors.
