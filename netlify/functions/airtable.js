const BASE_URL = 'https://api.airtable.com/v0';
const JOBS_TABLE = 'tblfphTqRzN24qmZS';
const INSPECT_TABLE = 'tblTbd2bfab4Ce8n9';

function parseCookies(header = '') {
  return Object.fromEntries(
    header.split(';').map(part => part.trim()).filter(Boolean).map(part => {
      const idx = part.indexOf('=');
      return [part.slice(0, idx), decodeURIComponent(part.slice(idx + 1))];
    })
  );
}

const crypto = require('crypto');
function sign(payload, secret) {
  return crypto.createHmac('sha256', secret).update(payload).digest('hex');
}
function decodeSession(cookieValue, secret) {
  if (!cookieValue || !cookieValue.includes('.')) return null;
  const [base, sig] = cookieValue.split('.');
  if (sign(base, secret) !== sig) return null;
  try {
    const payload = JSON.parse(Buffer.from(base, 'base64url').toString('utf8'));
    if (!payload.exp || payload.exp < Date.now()) return null;
    return payload;
  } catch {
    return null;
  }
}

function json(statusCode, body) {
  return {
    statusCode,
    headers: { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' },
    body: JSON.stringify(body),
  };
}

function getRole(event) {
  const secret = process.env.SESSION_SECRET;
  if (!secret) throw new Error('Missing SESSION_SECRET');
  const cookies = parseCookies(event.headers.cookie || event.headers.Cookie || '');
  const session = decodeSession(cookies.ne_session, secret);
  return session?.role || null;
}

function getAllowed(role, method, tableId) {
  if (role === 'admin') return true;
  if (role !== 'employee') return false;
  if (method === 'GET') return true;
  return tableId === INSPECT_TABLE;
}

async function airtableFetch(path, options = {}) {
  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!apiKey || !baseId) throw new Error('Missing Airtable environment variables');
  const res = await fetch(`${BASE_URL}/${baseId}/${path}`, {
    ...options,
    headers: {
      Authorization: `Bearer ${apiKey}`,
      'Content-Type': 'application/json',
      ...(options.headers || {}),
    },
  });
  const text = await res.text();
  let data = {};
  try { data = text ? JSON.parse(text) : {}; } catch { data = { raw: text }; }
  if (!res.ok) {
    const msg = data?.error?.message || data?.error?.type || `Airtable error ${res.status}`;
    throw new Error(msg);
  }
  return data;
}

exports.handler = async (event) => {
  try {
    const role = getRole(event);
    if (!role) return json(401, { error: 'Unauthorized' });

    let tableId, recordId, fields;
    if (event.httpMethod === 'GET') {
      tableId = event.queryStringParameters?.tableId;
      recordId = event.queryStringParameters?.recordId;
      fields = event.multiValueQueryStringParameters?.['fields[]'] || [];
    } else {
      const body = JSON.parse(event.body || '{}');
      tableId = body.tableId;
      recordId = body.recordId;
      fields = body.fields;
    }

    if (!tableId) return json(400, { error: 'Missing tableId' });
    if (!getAllowed(role, event.httpMethod, tableId)) return json(403, { error: 'Forbidden for this role' });

    if (event.httpMethod === 'GET') {
      const params = new URLSearchParams();
      const query = event.queryStringParameters || {};
      if (fields && fields.length) fields.forEach(f => params.append('fields[]', f));
      if (query.filterByFormula) params.set('filterByFormula', query.filterByFormula);
      if (query.pageSize) params.set('pageSize', query.pageSize);
      const path = recordId ? `${tableId}/${recordId}` : `${tableId}?${params.toString()}`;
      const data = await airtableFetch(path, { method: 'GET' });
      return json(200, data);
    }

    if (event.httpMethod === 'POST') {
      const data = await airtableFetch(tableId, {
        method: 'POST',
        body: JSON.stringify({ fields: fields || {} }),
      });
      return json(200, data);
    }

    if (event.httpMethod === 'PATCH') {
      if (!recordId) return json(400, { error: 'Missing recordId' });
      const data = await airtableFetch(`${tableId}/${recordId}`, {
        method: 'PATCH',
        body: JSON.stringify({ fields: fields || {} }),
      });
      return json(200, data);
    }

    return json(405, { error: 'Method not allowed' });
  } catch (error) {
    return json(500, { error: error.message || 'Server error' });
  }
};
