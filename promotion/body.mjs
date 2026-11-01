// Read a small event without buffering an arbitrary request body or waiting forever.
export async function readEventBody(request) {
  if (Number(request.headers.get('content-length')) > 1024) return { status: 413 };
  if (!request.body) return { status: 400 };
  const reader = request.body.getReader();
  const chunks = [];
  let size = 0;
  let timer;
  try {
    const timeout = new Promise((_, reject) => { timer = setTimeout(() => reject(new Error('Event body timeout')), 5000); });
    while (true) {
      const { value, done } = await Promise.race([reader.read(), timeout]);
      if (done) break;
      size += value.byteLength;
      if (size > 1024) return { status: 413 };
      chunks.push(value);
    }
    const bytes = new Uint8Array(size);
    let offset = 0;
    for (const chunk of chunks) { bytes.set(chunk, offset); offset += chunk.byteLength; }
    try { return { value: JSON.parse(new TextDecoder().decode(bytes)) }; }
    catch { return { status: 400 }; }
  } finally {
    clearTimeout(timer);
    await reader.cancel();
    reader.releaseLock();
  }
}
