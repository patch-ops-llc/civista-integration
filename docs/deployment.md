# Deployment & Environment

## Platform

Hosted on **Railway**. The app is a Node.js Express server that runs continuously (handles the cron schedule internally).

---

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `DATABASE_URL` | Yes | PostgreSQL connection string (Railway provides this automatically when you attach a Postgres plugin) |
| `HUBSPOT_API_KEY` | Yes | HubSpot private app bearer token |
| `PORT` | No | Defaults to 3000. Railway sets this automatically. |
| `INCOMING_DIR` | No | Where CSVs land. Defaults to `./incoming/` |
| `ARCHIVE_DIR` | No | Where processed CSVs go. Defaults to `./archive/` |

---

## First-Time Setup

### 1. Deploy to Railway

The `railway.toml` is already configured:
- Build: `npm install`
- Start: `npm start`
- Health check: `GET /health`

### 2. Initialize the Database

After the Postgres plugin is attached:
```bash
node db/init.js
```
This creates all staging tables, the sync log, and the ID mapping table.

### 3. Configure HubSpot Custom Object Unique Identifiers

**This is critical. Skip this and you get duplicates.**

Before the first sync runs, you must set unique identifier properties on each custom object in HubSpot. Use the HubSpot Schema API:

| Object | Property to Make Unique |
|--------|------------------------|
| Contacts | `cif_number` |
| Companies | `cif_number` |
| Deposits (2-60107989) | `account_key` |
| Loans (2-60108336) | `account_key` |
| Time Deposits (2-60108759) | `account_key` |
| Debit Cards (2-60107457) | `composite_key` |

Without these unique identifiers, the upsert API creates new records instead of updating existing ones.

### 4. Verify SFTP Access

Ensure Civista can reach the SFTP endpoint and that files land in the configured `INCOMING_DIR`.

---

## Redeployment

Railway auto-deploys on push to `main`. No manual steps needed for code changes.

For database schema changes: run `node db/init.js` after deploy. **Note:** this drops and recreates all staging tables. Sync log history is preserved separately if you modify the script, but the current version drops everything.

---

## Troubleshooting Deployment

| Problem | Fix |
|---------|-----|
| App crashes on start | Check `DATABASE_URL` is set. The DB connection is made at import time. |
| Health returns "unhealthy" | Usually means no syncs have run yet or the last sync had errors. |
| "relation stg_cif does not exist" | Run `node db/init.js` to create tables. |
| HubSpot 401 errors | `HUBSPOT_API_KEY` is wrong or the private app was deactivated. |
