Northeastern Electric Field App - Phase 1 Secure Netlify Build

This package uses your uploaded single HTML file as the base version.
It keeps the current one-page app structure for now, but secures these two things first:

1. Airtable API key is removed from the frontend.
2. Employee/Admin PIN checks move to Netlify Functions.

Files:
- index.html
- netlify.toml
- netlify/functions/auth.js
- netlify/functions/airtable.js

Netlify environment variables required:
- AIRTABLE_API_KEY=your new Airtable PAT
- AIRTABLE_BASE_ID=appiqWg6SvKcGfMAu
- EMPLOYEE_PIN=1991
- ADMIN_PIN=1184
- SESSION_SECRET=make this a long random string

Deploy steps:
1. Create a new folder on your computer.
2. Put all files from this package into that folder.
3. In Netlify, deploy that folder or drag/drop the zip contents.
4. Add the environment variables above.
5. Redeploy.

Notes:
- This is Phase 1, not the full architecture rebuild yet.
- It uses your current uploaded HTML as the working base.
- Next phase should split employee/admin into separate pages and smaller JS modules.
- Rotate the old Airtable PAT if you have not already.
- Change both PINs after first successful deploy.
