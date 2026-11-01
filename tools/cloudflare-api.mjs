export function cloudflareClient(env = process.env, fetcher = fetch) {
  const account = env.CLOUDFLARE_ACCOUNT_ID;
  const token = env.CLOUDFLARE_API_TOKEN;
  if (!/^[a-f0-9]{32}$/.test(account ?? '')) throw new Error('CLOUDFLARE_ACCOUNT_ID must be a 32-character account ID');
  if (!token || /\s/.test(token)) throw new Error('CLOUDFLARE_API_TOKEN is missing or contains whitespace');
  async function request(path, { method = 'GET', body, raw = false, contentType = 'application/json' } = {}) {
    const response = await fetcher(`https://api.cloudflare.com/client/v4/accounts/${account}/${path}`, {
      method, headers: { Authorization: `Bearer ${token}`, 'Content-Type': contentType },
      body, signal: AbortSignal.timeout(30000),
    });
    const text = await response.text();
    if (!response.ok) throw new Error(`Cloudflare ${method} ${path}: HTTP ${response.status}; ${text.slice(0,500)}`);
    if (raw) return text;
    const result = JSON.parse(text);
    if (result.success === false) throw new Error(`Cloudflare ${method} ${path}: ${JSON.stringify(result.errors)}`);
    return result;
  }
  return { account, request };
}
