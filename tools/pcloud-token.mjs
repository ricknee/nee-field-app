#!/usr/bin/env node
// pCloud token WITHOUT app registration — produces PCLOUD_AUTH_TOKEN + PCLOUD_API_HOST.
// See docs/PLAN-job-photos.md §8.
// ---------------------------------------------------------------------------
// Use this when pCloud's app-registration page is down ("temporarily
// unavailable"). It uses pCloud's native login: userinfo?getauth=1 returns a
// long-lived token tied to the account.
//
//   node tools/pcloud-token.mjs
//
// Prompts for email and password — deliberately NOT argv, which would land in
// shell history. The password is sent to pCloud over HTTPS once, never stored.
//
// The token goes in PCLOUD_AUTH_TOKEN — a DIFFERENT env var from the OAuth
// path's PCLOUD_ACCESS_TOKEN, because pCloud sends the two under different
// parameter names (?auth= vs ?access_token=). _pcloud.js accepts either.
//
// KNOWN LIMITATION: accounts with two-step verification return 1022
// ("Please provide 'code'") and pCloud does not document which parameter
// carries the second factor. This script prints the FULL response body on any
// unexpected result so the missing parameter can be identified instead of
// guessed at. If you hit that wall, use OAuth (tools/pcloud-oauth.mjs) — the
// browser flow handles any second factor natively.

import readline from "node:readline";

const INACTIVE_EXPIRE_SECONDS = 31536000;   // 1 year idle; every use resets it
const HOSTS = ["api.pcloud.com", "eapi.pcloud.com"];

// ONE readline interface for the whole run. Creating and closing one per
// prompt crashes Node on Windows with
//   Assertion failed: !(handle->flags & UV_HANDLE_CLOSING), src\win\async.c
// because stdin gets torn down and re-attached between questions.
const rl = readline.createInterface({ input: process.stdin, output: process.stdout, terminal: true });
rl._writeToOutput = function (s) {
  if (rl.stdoutMuted) rl.output.write("*");
  else rl.output.write(s);
};

function ask(question, hidden = false) {
  return new Promise((resolve) => {
    rl.question(question, (answer) => {
      rl.stdoutMuted = false;
      if (hidden) process.stdout.write("\n");
      resolve(answer.trim());
    });
    rl.stdoutMuted = !!hidden;
  });
}

function finish(code) { rl.close(); process.exit(code); }

// Dumps everything pCloud sent back. These are error payloads, and the whole
// point is to see which parameter it is actually asking for.
function dump(label, d) {
  console.log(`\n--- full pCloud response (${label}) ---`);
  console.log(JSON.stringify(d, null, 2));
  console.log(`--- end ---\n`);
}

async function login(host, username, password, extra = {}) {
  const body = new URLSearchParams({
    username,
    password,
    getauth: "1",
    logout: "0",
    authinactiveexpire: String(INACTIVE_EXPIRE_SECONDS),
    ...extra,
  });
  // POST so the password sits in the body, not in a URL that lands in logs.
  const res = await fetch(`https://${host}/userinfo`, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body,
  });
  return await res.json();   // pCloud answers 200 even on failure; `result` is the truth
}

console.log(`
pCloud token (no app registration needed)
-----------------------------------------
Sign in as the NEE pCloud account. The password is used once and not stored.
`);

const username = await ask("pCloud email: ");
if (!username) { console.error("No email given."); finish(1); }
const password = await ask("pCloud password: ", true);
if (!password) { console.error("No password given."); finish(1); }

let token = null, host = null, lastErr = null;

for (const h of HOSTS) {
  process.stdout.write(`Trying ${h} ... `);
  let d;
  try {
    d = await login(h, username, password);
  } catch (e) {
    console.log("network error");
    lastErr = String(e?.message || e);
    continue;
  }

  if (d.result === 0 && d.auth) {
    console.log("success");
    token = d.auth; host = h;
    console.log(`\nSigned in as: ${d.email || username}`);
    break;
  }

  console.log(`rejected (result ${d.result})`);
  lastErr = d.error || `result ${d.result}`;

  // 2000 on one region usually just means "account isn't here" — keep going.
  if (d.result === 2000) continue;

  // Anything else is informative: the password was accepted and pCloud wants
  // something more. Show the whole body rather than guessing at param names.
  dump(h, d);

  if (d.result === 1022) {
    console.log(`${h} accepted the password but requires a second factor.

pCloud does not document which parameter carries it, and the obvious guess
('code') is ignored — the same 1022 comes back. The response above is the
evidence; look for a field naming what it wants (e.g. a token to echo back).

You can try a value against a named parameter:`);
    const param = await ask("\n  parameter name (blank to give up): ");
    if (!param) {
      console.log(`
Giving up on the native login. Use OAuth instead — the browser flow handles
two-step verification natively:

  1. Register an app at https://docs.pcloud.com/my_apps/
     (redirect URI: http://localhost:65432/, then log out and back in)
  2. node tools/pcloud-oauth.mjs --id <CLIENT_ID> --secret <CLIENT_SECRET>
`);
      finish(1);
    }
    const value = await ask(`  value for ${param}: `);
    process.stdout.write(`Retrying ${h} with ${param} ... `);
    let d2;
    try {
      d2 = await login(h, username, password, { [param]: value });
    } catch (e) {
      console.log("network error");
      lastErr = String(e?.message || e);
      continue;
    }
    if (d2.result === 0 && d2.auth) {
      console.log("success");
      token = d2.auth; host = h;
      console.log(`\nSigned in as: ${d2.email || username}`);
      break;
    }
    console.log(`rejected (result ${d2.result})`);
    dump(`${h} retry`, d2);
    lastErr = d2.error || `result ${d2.result}`;
  }
}

if (!token) {
  console.error(`
Login failed. Last error: ${lastErr}

Reading the result codes:
  2000  wrong password — OR the wrong region, which also returns 2000. If the
        other region returned something different, that other one is yours and
        the password is fine.
  1022  password accepted, a second factor is required
  1000  no credentials reached the server

If 1022 keeps coming back, stop here and use OAuth (tools/pcloud-oauth.mjs).
The browser login handles two-step verification without any of this.
`);
  finish(1);
}

const check = await fetch(`https://${host}/userinfo?auth=${encodeURIComponent(token)}`);
const cd = await check.json();
if (cd.result !== 0) {
  console.error(`Token was issued but immediately rejected (result ${cd.result}). Not usable.`);
  finish(1);
}

console.log(`Token verified against ${host}.\n`);
console.log("Paste these into Netlify -> Site configuration -> Environment variables:\n");
console.log(`  PCLOUD_AUTH_TOKEN=${token}`);
console.log(`  PCLOUD_API_HOST=${host}\n`);
console.log("Add the same two lines to your local .env for `netlify dev`.");
console.log("Treat this like a password — it can read and write the entire pCloud account.");
console.log("To revoke: change the pCloud password, or call pCloud's `logout` method with it.\n");
finish(0);
