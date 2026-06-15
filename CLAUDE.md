# Civista Bank — HubSpot Integration

## Overview

Nightly data sync pipeline for Civista Bank (PatchOps end-client). Their core banking system (Silver Lake / Jack Henry) exports 5 CSV files nightly. This Node.js middleware on Railway ingests them into PostgreSQL staging tables, transforms the data, and batch-upserts into HubSpot CRM.

## Quick Commands

```bash
npm start              # Start Express server
node db/init.js        # Initialize database tables
```

## Architecture

```
Silver Lake CSVs → SFTP/Upload → /incoming/
  → CSV Parser → PostgreSQL staging tables
    → Classification (CIF → contact or company)
    → Normalization (types, dates, booleans)
    → Diff Engine (row hash comparison)
    → Circuit Breaker (30% drop threshold)
    → HubSpot Batch Upsert (100/batch, exponential backoff)
    → Archive to /archive/{date}/
```

## Source Files (5 CSVs)

| File | Records (prod) | HubSpot Target | CIF Column | Primary Key |
|------|----------------|----------------|------------|-------------|
| CIF | ~261k | Contacts + Companies | CIFNum | CIFNum |
| DDA | ~250k | Custom: 2-60107989 | CIF# | account_key (branch\|type\|last4); collapses owner rows |
| Loans | ~92k | Custom: 2-60108336 | CIFNum | account_key (branch\|type\|last4); collapses owner rows |
| CDs | ~39k | Custom: 2-60108759 | CIFNum | account_key (branch\|type\|last4); collapses owner rows |
| Debit Cards | ~76k | Custom: 2-60107457 | CIF# | Composite (generated) |

## CIF Classification Rules

- TaxIdType = "T" AND FirstName empty AND LastName empty → Company
- TaxIdType = "T" AND FirstName+LastName populated → Contact
- TaxIdType != "T" → Contact

## Critical Data Edge Cases

1. CIF column named `CIF#` in DDA and Debit Cards, `CIFNum` elsewhere — normalize on ingest
2. CIF numbers have `\` and `@` prefixes — preserve exactly
3. `relationship` values have trailing whitespace — trim on ingest
4. Email `"none"` = null
5. Deceased `" "` (space) = alive/false
6. acctstatus has values 0-9 — store raw, don't enum
7. Interest rates are decimal strings (`.03680000`) — convert to number
8. Balances are string numbers — convert to number
9. Debit Cards have NO pre-hashed key — generate composite: CIF# + Acctlast4 + AcctType + Last4DebitCard + Expiredate

## HubSpot Custom Object IDs

- Deposits (DDA): `2-60107989`
- Loans: `2-60108336`
- Time Deposits (CDs): `2-60108759`
- Debit Cards: `2-60107457`

## Build Order

1. Database schema (replace db/init.js with full staging tables)
2. CSV parser and staging loader
3. CIF classification engine
4. Data normalization
5. Circuit breaker (30% row count drop = skip)
6. Diff engine (row hash comparison)
7. HubSpot sync engine (batch upsert, 100/batch, exponential backoff)
8. Sync orchestrator and cron
9. Health and monitoring endpoint
10. SFTP receiver

## Account model & associations (built)

Deposits, Loans, and Time Deposits use a **deduplicated account model**: the source has one
row per owner for a given physical account, so rows are collapsed onto one HubSpot record
keyed by `account_key` (`branch_number | account_type | last_4_account_digits` — see
`buildAccountKey` in `src/transform/hubspot-mapping.js`; provisional pending mapping-doc
sign-off). All owner rows are kept in `stg_*_owners` and become labeled owner associations
(`src/sync/associations.js`), using Ivan's relationship-code legend
(`src/transform/relationship-map.js`) resolved to portal type IDs
(`src/sync/association-labels.js`). Debit Cards link to their single owner unlabeled.
Idempotency via `shipped_associations`. See `docs/operations-runbook.md`.

## NOT in scope

- Enum mappings for acctstatus, CardStatus, relationship
- Frontend dashboard
- Civista-side infrastructure

## Environment Variables

- `DATABASE_URL` — PostgreSQL connection string
- `HUBSPOT_API_KEY` — HubSpot bearer token
- `PORT` — Express port (default 3000)

## Key Files

- `index.js` — Express app, routes
- `db/init.js` — Database schema initialization
- `railway.toml` — Railway deployment config

## IMPORTANT: Before First Sync

`cif_number` must be set as a unique identifier property on all four custom objects in HubSpot via the schema API. Same for `primary_key` properties. Without this, every sync creates duplicates.
