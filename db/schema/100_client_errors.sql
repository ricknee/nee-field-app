-- 100: browser errors from the field app, saved so they can be seen.
--
-- Until now an error on a crew member's phone lived only in that phone's console,
-- so nobody knew it happened. The app now posts uncaught errors and failed
-- promises here (POST logClientError). The browser dedupes and caps what it sends;
-- the server truncates every field. Read-only for everything else: nothing in the
-- app reads this table except the admin `clientErrors` read.
CREATE TABLE IF NOT EXISTS client_errors (
  id          bigserial PRIMARY KEY,
  created_at  timestamptz NOT NULL DEFAULT now(),
  employee_id text,          -- token id (legacy rec id or uuid), as login returns it
  role        text,
  app         text,          -- 'field' | 'inventory'
  kind        text,          -- 'error' | 'rejection'
  message     text,
  source      text,          -- file:line:col
  stack       text,
  url         text,
  user_agent  text,
  app_build   text
);
CREATE INDEX IF NOT EXISTS client_errors_created_idx ON client_errors (created_at DESC);
