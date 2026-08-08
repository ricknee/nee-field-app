# Plan: clock in/out from a phone shortcut

**Status:** **NOT BUILT — planned only.** Owner's idea 2026-08-08, while first trying the time
clock: *"can i make a shortcut on my phone to clock in out and somehow?? just another thing to plan
for dont build."*

**One-line:** Punch in or out without opening the app, hunting for ⏱ My Hours, and tapping a button
— because the whole value of a clock is that using it is faster than not bothering.

Prerequisite: the time clock itself (`docs/PLAN-time-clock.md`), which is built and live behind
`TIME_CLOCK`.

---

## 1. Why it's worth doing

The clock currently costs four interactions: unlock, open app, open My Hours, tap Clock In. That is
fine once a day and annoying twice a day. QuickBooks Time's actual advantage over us was never its
UI — it was auto-punching on arrival, which meant *zero* interactions.

We can't reproduce that in the browser (see §4), but we can get to one tap, or to a voice command,
and on iOS we can get surprisingly close to arrival-based punching.

## 2. The constraint that shapes everything

**There is no `manifest.json` in this repo.** The PWA is service-worker-only (`sw.js`). That means:

- No web-app manifest, so **no `shortcuts` array**, so no long-press-the-icon jump targets today.
- Adding a minimal manifest is cheap and is a prerequisite for the Android half.
- **iOS Safari ignores manifest `shortcuts` entirely** regardless — so the iOS answer is a
  different mechanism, not the same one.

The second constraint is auth. The session token lives in `localStorage` and is attached by
`apiGet`/`apiPost`. Anything that runs *outside* the page — an iOS Shortcut, a widget — has no
access to it and cannot perform a login flow.

## 3. Layer 1 — deep links + Android home-screen shortcuts (~1-2 h)

The cheap layer, and it uses the existing session with **no new auth**.

- Handle `?clock=in` / `?clock=out` on load: punch, then land on ⏱ My Hours with the result
  visible. Reuses `clockIn()` / `clockOut()` exactly as they are, including the offline queue.
- Add a minimal `manifest.json` with a `shortcuts` array so a long-press on the Android home-screen
  icon offers **Clock In** / **Clock Out** directly.
- On iOS, "Add to Home Screen" on those URLs gives two icons that each do one thing. Not elegant,
  but it is one tap and it costs nothing extra.

⚠ **A deep link must never punch silently on a cold load.** If the app opens to a login screen, or
the session has expired, the URL has to survive the login and run afterwards — otherwise someone
taps "Clock In", sees a login page, logs in, and walks away believing they are on the clock.
Confirm visibly on success, and never assume.

⚠ **Double-fire.** A shortcut tapped twice, or a page restored from bfcache, must not open two
shifts. The `clientPunchId` cycle key already makes the server side idempotent — the link handler
must mint the id **once per navigation**, not once per render.

## 4. Layer 2 — iOS Shortcuts (~2-3 h, most of it auth)

iOS Safari ignores manifest shortcuts, so the real iOS answer is the **Shortcuts app** POSTing
straight at the function. This is the layer that actually changes the ergonomics:

- **"Hey Siri, clock in"**
- **Back Tap** — double-tap the back of the phone
- **Lock Screen / Action Button**
- **Location automation** — and this is the one worth noting: `PLAN-time-clock.md` §8 says
  auto-punch-on-arrival is not achievable, and that is true **inside a PWA** (no background
  geolocation on iOS Safari, no reliable wake-up). It is *not* true of iOS Shortcuts, which can run
  an automation on arriving at a location. So "you've arrived at the shop — clock in?" is
  achievable this way. Not silent auto-punching, but close, and honest about it.

### The auth problem, which is most of the work

A Shortcut cannot log in. It needs a credential it can hold:

- A **long-lived, narrowly-scoped token** — clock actions only, never the rest of the API. If this
  leaks, the blast radius must be "someone can punch this one person's clock", not "someone can
  read payroll".
- `_auth.js` already has **`signScope` / `verifyScope`**, which is the right building block —
  they exist for exactly this shape of problem.
- Needs a small admin screen to **mint and revoke** one per person, and revocation must actually
  work (the `token_valid_from` mechanism from the employee-admin slice is the precedent).
- ⚠ **Do not reuse the 30-day session token.** It expires, taking the Shortcut with it silently,
  and it carries full API authority.

## 5. Recommended order

1. **Layer 1 first.** Cheap, no new auth, covers Android properly and gives iOS a one-tap icon.
2. **Layer 2 only if wanted.** Voice, Back Tap and arrival prompts are genuinely nice, but the
   token model is most of the cost and adds a credential to look after.

## 6. Not in scope

An actual native app; Apple Watch; background auto-punching without confirmation (see §4 — the
honest version always asks); NFC tags at the shop door (possible via the same Shortcut, but it is a
hardware decision, not a software one).

## 7. Rough size

| Piece | Size |
|---|---|
| `?clock=in/out` deep-link handling + login-survival + double-fire guard | ~1 h |
| Minimal `manifest.json` + `shortcuts` array | ~0.5-1 h |
| Scoped clock-only token: mint, verify, revoke, admin UI | ~2 h |
| Documented iOS Shortcut recipe (+ location automation) | ~0.5-1 h |

**Layer 1 ~1-2 h. Layer 2 ~2-3 h on top.**
