
const { parentPort, workerData } = require('worker_threads');
const Database = require('better-sqlite3');

const { action, dbPath, query, params } = workerData;

(async () => {
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
  } else if (action === 'iterate') {
    try {
      const db = new Database(dbPath, { readonly: true });
      // Set cache size for the worker connection too
      db.pragma('cache_size = -500000');
      
      const stmt = db.prepare(query);
      const iter = stmt.iterate(...(params || []));
      
      let batch = [];
      const BATCH_SIZE = 1000;
      
      // Backpressure mechanism
      let ackResolver = null;
      const waitForAck = () => new Promise(resolve => { ackResolver = resolve; });
      
      parentPort.on('message', (msg) => {
        if (msg.type === 'ack' && ackResolver) {
          const resolve = ackResolver;
          ackResolver = null;
          resolve();
        }
      });
      
      // This loop will block during the first .next() call (the sort),
      // but since we are in a worker, the main thread stays free.
      for (const row of iter) {
        batch.push(row);
        if (batch.length >= BATCH_SIZE) {
          parentPort.postMessage({ type: 'batch', data: batch });
          batch = [];
          await waitForAck();
        }
      }
      
      if (batch.length > 0) {
        parentPort.postMessage({ type: 'batch', data: batch });
        await waitForAck();
      }
      
      db.close();
      parentPort.postMessage({ type: 'done' });
    } catch (err) {
      parentPort.postMessage({ error: err.message });
    }
  } else if (action === 'exec') {
    try {
      const db = new Database(dbPath);
      db.exec(query);
      db.close();
      parentPort.postMessage({ result: true });
    } catch (err) {
      parentPort.postMessage({ error: err.message });
    }
  }
})();
