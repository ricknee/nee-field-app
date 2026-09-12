// ── The city-tax option list, in ONE place ─────────────────────────────────
//
// ⚠⚠ THESE STRINGS MUST MATCH QUICKBOOKS TIME VERBATIM, TYPOS INCLUDED.
// "Massilon", "New Philadephia" and "Hayesville" are misspelled in QuickBooks
// custom field 65840, and this list reproduces them exactly. DO NOT CORRECT
// THEM. The value arrives from the hourly pull as whatever QuickBooks sends and
// is matched by string equality — a "fixed" spelling here does not fix the data,
// it stops the data matching anything, and a city tax that matches nothing reads
// as "no tax" rather than as an error.
//
// That is not hypothetical. Someone corrected "Carrolton City Tax" (QuickBooks'
// spelling, one L) to "Carrollton City Tax" at some point. 30 timesheets carry
// the QuickBooks spelling and the app has not recognised any of them since.
// Both spellings are listed below for exactly that reason — see the block at
// the bottom.
//
// Shared rather than duplicated because airtable.js imports _integrity.js, so
// _integrity.js cannot import airtable.js back without a cycle.
//
// ⚠ index.html carries its own copy (`PR_CITY_TAXES`) because it is a static
// page with no bundler and cannot import this. `tests/handlers.test.mjs` asserts
// the two lists are identical — that assertion is the only thing keeping them in
// step, so do not delete it when one of them changes.

export const CITY_TAX_OPTS = [
  "A No Tax", "Alliance Tax", "Amherst Tax", "Ashland City Tax", "Austintown Tax",
  "Canton Tax", "Carrollton City Tax", "Cleveland Tax", "Columbiana Tax",
  "Cuyahoga Falls Tax", "Dennison City Tax", "Grafton Tax", "Green Tax",
  "Hartville Tax", "Hayesville", "Madison City Tax", "Massilon Tax", "Medina Tax",
  "Millersburg City Tax", "Minerva Tax", "N Canton", "New Philadephia",
  "Orrville City Tax", "Rita Tax", "Salem Tax", "Sebring Tax", "Steubenville Tax",
  "Streetsboro Tax", "Strongsville Tax", "Utica Tax", "Wadsworth Tax", "Akron Tax",
  "Other",

  // ── Added 2026-09-12, after asking QuickBooks what it ACTUALLY offers ─────
  // Comparing this list against custom field 65840's live options turned up
  // three values QuickBooks can send that the app did not recognise. Each one
  // fails silently: the app rejects it on its own writes, the pull stores it
  // anyway, and it matches no option in either dropdown.
  //
  // "Carrolton City Tax" — QuickBooks' spelling, one L. 30 timesheets / 130.5 h
  //   already carry it. Kept ALONGSIDE the two-L version rather than replacing
  //   it, because accepting a value the payroll system really produces is the
  //   safe direction; refusing one is the harm.
  "Carrolton City Tax",
  // Offered by QuickBooks, absent here, never yet picked. Present so the first
  // person to pick one is not silently mis-taxed.
  "Lisbon Tax", "Loudenville Tax",
];

// Single-quote escaped for inlining into a SQL literal list. The values are a
// hardcoded constant, never user input, but escaping costs nothing and means a
// future option containing an apostrophe cannot break the integrity check.
export const CITY_TAX_SQL_LIST =
  CITY_TAX_OPTS.map(v => `'${v.replace(/'/g, "''")}'`).join(", ");
