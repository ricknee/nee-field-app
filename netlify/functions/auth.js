const crypto = require('crypto');

function getEnv(name) {
  const value = process.env[name];
  if (!value) throw new Error(`Missing env var: ${name}`);
  return value;
}

function sign(payload, secret) {
  return crypto.createHmac('sha256', secret).update(payload).digest('hex');
}

function encodeSession(role, secret) {
  const payload = JSON.stringify({ role, exp: Date.now() + 1000 * 60 * 60 * 12 });
  const base = Buffer.from(payload).toString('base64url');
  const sig = sign(base, secret);
  return `${base}.${sig}`;
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

function parseCookies(header = '') {
  return Object.fromEntries(
    header.split(';').map(part => part.trim()).filter(Boolean).map(part => {
      const idx = part.indexOf('=');
      return [part.slice(0, idx), decodeURIComponent(part.slice(idx + 1))];
    })
  );
}

function json(statusCode, body, headers = {}) {
  return {
    statusCode,
    headers: {
      'Content-Type': 'application/json',
      'Cache-Control': 'no-store',
      ...headers,
    },
    body: JSON.stringify(body),
  };
}

exports.handler = async (event) => {
  const secret = getEnv('SESSION_SECRET');
  const employeePin = getEnv('EMPLOYEE_PIN');
  const adminPin = getEnv('ADMIN_PIN');
  const cookies = parseCookies(event.headers.cookie || event.headers.Cookie || '');
  const session = decodeSession(cookies.ne_session, secret);

  if (event.httpMethod === 'GET') {
    if (!session) return json(200, { authenticated: false });
    return json(200, { authenticated: true, role: session.role });
  }

  if (event.httpMethod === 'DELETE') {
    return json(200, { ok: true }, {
      'Set-Cookie': 'ne_session=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0; Secure'
    });
  }

  if (event.httpMethod !== 'POST') return json(405, { error: 'Method not allowed' });

  let body = {};
  try { body = JSON.parse(event.body || '{}'); } catch {}
  const pin = String(body.pin || '').trim();

  let role = null;
  if (pin === adminPin) role = 'admin';
  else if (pin === employeePin) role = 'employee';
  else return json(401, { error: 'Invalid PIN' });

  const token = encodeSession(role, secret);
  return json(200, { ok: true, role }, {
    'Set-Cookie': `ne_session=${token}; Path=/; HttpOnly; SameSite=Lax; Max-Age=43200; Secure`
  });
};
