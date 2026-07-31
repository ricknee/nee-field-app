#!/usr/bin/env node
// One-time pCloud OAuth helper — produces PCLOUD_ACCESS_TOKEN + PCLOUD_API_HOST.
// See docs/PLAN-job-photos.md §8.
// ---------------------------------------------------------------------------
// Run this ONCE on your own machine. It opens a local listener, sends you to
// pCloud to approve the app, catches the redirect, exchanges the code for a
// token, verifies the token works, and prints the two env vars to paste into
// Netlify. Nothing is written to disk and nothing is sent anywhere except
// pCloud.
//
//   node tools/pcloud-oauth.mjs --id <CLIENT_ID> --secret <CLIENT_SECRET>
//
// Prerequisite: register an app at https://docs.pcloud.com/my_apps/ and add
//   http://localhost:65432/
// as a redirect URI. pCloud only shows a newly-added redirect URI after you log
// out and back in — if the authorize page rejects the redirect, that's why.
//
// WHY A SCRIPT: the token pCloud hands back is a full read/write credential for
// the whole account. Doing the exchange here keeps it off any third-party
// "OAuth playground" site and out of your shell history.

import { createServer } from "node:http";

const PORT = 65432;
const REDIRECT = `http://localhost:${PORT}/`;

function arg(name) {
  const i = process.argv.indexOf(`--${name}`);
  return i > -1 ? process.argv[i + 1] : process.env[`PCLOUD_${name.toUpperCase()}`];
}

const CLIENT_ID     = arg("id");
const CLIENT_SECRET = arg("secret");

if (!CLIENT_ID || !CLIENT_SECRET) {
  console.error(`
Missing credentials.

  node tools/pcloud-oauth.mjs --id <CLIENT_ID> --secret <CLIENT_SECRET>

Get both from https://docs.pcloud.com/my_apps/ after registering an app,
and add ${REDIRECT} as a redirect URI there first.
`);
  process.exit(1);
}

const authorizeUrl =
  `https://my.pcloud.com/oauth2/authorize?client_id=${encodeURIComponent(CLIENT_ID)}` +
  `&response_type=code&redirect_uri=${encodeURIComponent(REDIRECT)}`;

console.log(`
pCloud OAuth — step 1 of 2
--------------------------
Open this URL, sign in as the NEE pCloud account, and click Allow:

${authorizeUrl}

Waiting for the redirect on ${REDIRECT} ...
(Ctrl+C to abort)
`);

const done = (server, code) => { server.close(); process.exit(code); };

const server = createServer(async (req, res) => {
  const url = new URL(req.url, REDIRECT);
  const code = url.searchParams.get("code");

  // pCloud tells us which region this account lives in. Using the wrong host
  // for the token exchange (and for every later API call) returns "invalid
  // access token", which reads exactly like a bad credential.
  const hostname = url.searchParams.get("hostname")
    || (url.searchParams.get("locationid") === "2" ? "eapi.pcloud.com" : "api.pcloud.com");

  if (!code) {
    res.writeHead(400, { "Content-Type": "text/plain" });
    res.end("No code in redirect. Check the redirect URI registered on the app.");
    console.error("\nRedirect arrived with no ?code=. Confirm the app's redirect URI is exactly " + REDIRECT);
    return done(server, 1);
  }

  res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
  res.end("<h2>Got it — you can close this tab and return to the terminal.</h2>");

  try {
    console.log("Step 2 of 2 — exchanging the code for a token ...\n");
    const tokenUrl = new URL(`https://${hostname}/oauth2_token`);
    tokenUrl.searchParams.set("client_id", CLIENT_ID);
    tokenUrl.searchParams.set("client_secret", CLIENT_SECRET);
    tokenUrl.searchParams.set("code", code);

    const tr = await fetch(tokenUrl);
    const td = await tr.json();
    // pCloud answers HTTP 200 even on failure; the body's `result` is the truth.
    if (td.result !== 0 || !td.access_token) {
      console.error(`Token exchange FAILED (result ${td.result}): ${td.error || "no access_token returned"}`);
      return done(server, 1);
    }

    // Prove the token actually works before telling anyone to paste it into
    // production — a token that 401s in Netlify at 6am is a bad surprise.
    const ui = await fetch(`https://${hostname}/userinfo?access_token=${encodeURIComponent(td.access_token)}`);
    const uid = await ui.json();
    if (uid.result !== 0) {
      console.error(`Token was issued but userinfo rejected it (result ${uid.result}): ${uid.error || ""}`);
      return done(server, 1);
    }

    console.log("Verified against pCloud account: " + (uid.email || `uid ${td.uid}`));
    console.log(`Region: ${hostname}\n`);
    console.log("Paste these into Netlify -> Site configuration -> Environment variables:\n");
    console.log(`  PCLOUD_ACCESS_TOKEN=${td.access_token}`);
    console.log(`  PCLOUD_API_HOST=${hostname}\n`);
    console.log("Add the same two lines to your local .env for `netlify dev`.");
    console.log("Treat the token like a password — it can read and write the entire pCloud account.\n");
    done(server, 0);
  } catch (e) {
    console.error("Token exchange threw: " + (e?.message || e));
    done(server, 1);
  }
});

server.on("error", (e) => {
  if (e.code === "EADDRINUSE") {
    console.error(`Port ${PORT} is already in use. Close whatever is using it and rerun.`);
  } else {
    console.error(String(e?.message || e));
  }
  process.exit(1);
});

server.listen(PORT);
