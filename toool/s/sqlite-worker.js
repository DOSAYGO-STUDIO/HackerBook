
const { parentPort, workerData } = require('worker_threads');
const Database = require('better-sqlite3');

const { action, dbPath, query, params } = workerData;

if (action === 'query') {
  try {
    const db = new Database(dbPath, { readonly: true });
    const stmt = db.prepare(query);
    const result = stmt.get(...(params || []));
    db.close();
    parentPort.postMessage({ result });
  } catch (err) {
    parentPort.postMessage({ error: err.message });
  }
}
