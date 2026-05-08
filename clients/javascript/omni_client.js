/**
 * OmniKV Node.js Client SDK
 * Zero dependencies — uses native fetch (Node 18+).
 */
export class OmniClient {
  constructor(baseUrl, options = {}) {
    this.baseUrl = baseUrl.replace(/\/+$/, '');
    this.token = options.token || null;
    this.timeout = options.timeout || 30000;
  }

  async get(key) {
    const r = await this._req('GET', `/kv/${encodeURIComponent(key)}`);
    return r.success && r.data ? r.data.value : null;
  }

  async set(key, value, ttl = null) {
    const body = { key, value };
    if (ttl !== null) body.expiry = ttl;
    const r = await this._req('POST', '/kv', body);
    return r.data?.seq || 0;
  }

  async delete(key) {
    await this._req('DELETE', `/kv/${encodeURIComponent(key)}`);
  }

  async scan(start, end, limit = null) {
    let p = `/scan?start=${encodeURIComponent(start)}&end=${encodeURIComponent(end)}`;
    if (limit) p += `&limit=${limit}`;
    const r = await this._req('GET', p);
    return r.data || [];
  }

  async batch(operations) {
    const r = await this._req('POST', '/batch', { operations });
    return r.data?.seq || 0;
  }

  async health() {
    const r = await this._req('GET', '/health');
    return r.data;
  }

  async metrics() {
    const r = await fetch(`${this.baseUrl}/metrics`, { headers: this._h() });
    return await r.text();
  }

  async query(sql) {
    return await this._req('POST', '/query', { query: sql });
  }

  _h() {
    const h = { 'Content-Type': 'application/json' };
    if (this.token) h['Authorization'] = `Bearer ${this.token}`;
    return h;
  }

  async _req(method, path, body = null) {
    const opts = { method, headers: this._h() };
    if (body) opts.body = JSON.stringify(body);
    const resp = await fetch(`${this.baseUrl}${path}`, opts);
    const text = await resp.text();
    if (!text) return { success: true, data: null };
    const json = JSON.parse(text);
    if (!json.success && json.error) throw new Error(`OmniKV ${resp.status}: ${json.error}`);
    return json;
  }
}

export default OmniClient;
