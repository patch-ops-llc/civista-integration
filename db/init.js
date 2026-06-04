const { Pool } = require('pg');

// Pool timeouts: a hung Postgres can lock up /health forever. Railway's
// healthcheck has a strict timeout — pool.connect() must fail fast when
// the DB is unreachable so we can return 503 instead of timing out.
// Ref: Railway message of 2026-04-28 about /health hanging.
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  connectionTimeoutMillis: 2000,
  idleTimeoutMillis: 30000,
  // Per-statement ceiling. This MUST be large enough for the nightly bulk
  // workload: the diff-engine anti-join + row pull runs over the full staging
  // table (232k+ contacts, 252k+ deposits at production scale). The previous
  // 5s value was sized for the 50-row UAT dataset and silently killed the
  // first real run — contacts timed out mid-diff and the whole CIF file was
  // quarantined. 10 minutes leaves generous headroom for a slow/IO-bound DB
  // while still bounding a genuinely wedged query. /health does NOT rely on
  // this cap to stay responsive — it has its own 2s Promise.race in index.js.
  statement_timeout: 600000,
});

/**
 * Schema. Staging tables mirror HubSpot property names 1:1 — no transposition.
 * If HubSpot adds/renames a property, this file and hubspot-mapping.js
 * are the two places to update.
 *
 * All staging columns are TEXT at the DB level. Type coercion (number, bool, date)
 * happens at send time using the type hints in hubspot-mapping.js. This keeps
 * the ingest path forgiving (we can always store whatever the CSV has)
 * and puts type validation at the HubSpot boundary.
 */
async function initDb() {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    await client.query(`
      CREATE TABLE IF NOT EXISTS sync_log (
        id SERIAL PRIMARY KEY,
        table_name VARCHAR(50),
        started_at TIMESTAMPTZ,
        completed_at TIMESTAMPTZ,
        records_attempted INTEGER DEFAULT 0,
        records_created INTEGER DEFAULT 0,
        records_updated INTEGER DEFAULT 0,
        records_failed INTEGER DEFAULT 0,
        records_skipped INTEGER DEFAULT 0,
        error_details JSONB,
        file_hash VARCHAR(64),
        row_count INTEGER
      );
    `);

    // --- Contacts (from CIF CSV, classified as individual) ---
    await client.query(`
      CREATE TABLE IF NOT EXISTS stg_contacts (
        id SERIAL PRIMARY KEY,
        -- HubSpot standard properties
        firstname TEXT,
        lastname TEXT,
        email TEXT,
        phone TEXT,
        address TEXT,
        city TEXT,
        state TEXT,
        zip TEXT,
        date_of_birth TEXT,
        -- HubSpot custom properties (names match HubSpot exactly)
        cif_number TEXT,
        street_address_2 TEXT,
        hashed_ssn TEXT,
        private_banking_flag_yn TEXT,
        dnc_flag_yn TEXT,
        minor_flag_yn TEXT,
        insider_code TEXT,
        insufficient_address_yn TEXT,
        clf_flag_yn TEXT,
        civistawork_yn TEXT,
        orginal_customer_date TEXT,
        deceased_flag_yn TEXT,
        class_code TEXT,
        tax_id_type TEXT,
        customer_relationship_number TEXT,
        digital_banking_flag_yn TEXT,
        atmdebit_card_yn TEXT,
        q2_user_id TEXT,
        last_login TEXT,
        recurring_transactions_yn TEXT,
        stmt_type TEXT,
        enrollment_date TEXT,
        central_group_id TEXT,
        text_opt_in TEXT,
        estatement_disclosure_acceptance_date TEXT,
        -- Internal
        row_hash VARCHAR(64),
        loaded_at TIMESTAMPTZ DEFAULT NOW()
      );
    `);

    // --- Companies (from CIF CSV, classified as business) ---
    await client.query(`
      CREATE TABLE IF NOT EXISTS stg_companies (
        id SERIAL PRIMARY KEY,
        -- HubSpot standard
        name TEXT,
        email TEXT,
        phone TEXT,
        address TEXT,
        city TEXT,
        state TEXT,
        zip TEXT,
        -- Custom
        cif_number TEXT,
        naics_code TEXT,
        treasury_management_flag_yn TEXT,
        hashed_ssn TEXT,
        dnc_flag_yn TEXT,
        insider_code TEXT,
        insufficient_address_yn TEXT,
        clf_flag_yn TEXT,
        civistawork_yn TEXT,
        orginal_customer_date TEXT,
        deceased_flag_yn TEXT,
        class_code TEXT,
        tax_id_type TEXT,
        customer_relationship_number TEXT,
        digital_banking_flag_yn TEXT,
        atmdebit_card_yn TEXT,
        q2_user_id TEXT,
        last_login TEXT,
        recurring_transactions_yn TEXT,
        stmt_type TEXT,
        enrollment_date TEXT,
        central_group_id TEXT,
        text_opt_in TEXT,
        estatement_disclosure_acceptance_date TEXT,
        -- Internal
        row_hash VARCHAR(64),
        loaded_at TIMESTAMPTZ DEFAULT NOW()
      );
    `);

    // --- Deposits (DDA CSV → custom object 2-60442978) ---
    await client.query(`
      CREATE TABLE IF NOT EXISTS stg_deposits (
        id SERIAL PRIMARY KEY,
        primary_key TEXT,
        cif_number TEXT,
        last_4_account_digits TEXT,
        interest_rate TEXT,
        account_type TEXT,
        account_description TEXT,
        date_opened TEXT,
        date_closed TEXT,
        sales_associate TEXT,
        date_last_active TEXT,
        current_balance TEXT,
        yesterdays_balance TEXT,
        branch_number TEXT,
        officer_name TEXT,
        deposit_account_status TEXT,
        promo_code TEXT,
        opened_online_yn TEXT,
        row_hash VARCHAR(64),
        loaded_at TIMESTAMPTZ DEFAULT NOW()
      );
    `);

    // --- Loans (custom object 2-60442977) ---
    await client.query(`
      CREATE TABLE IF NOT EXISTS stg_loans (
        id SERIAL PRIMARY KEY,
        primary_key TEXT,
        cif_number TEXT,
        last_4_account_digits TEXT,
        interest_rate TEXT,
        account_type TEXT,
        loan_type TEXT,
        orgination_date TEXT,
        maturity_date TEXT,
        sales_associate TEXT,
        date_last_active TEXT,
        current_balance TEXT,
        branch_number TEXT,
        officer_name TEXT,
        loan_status TEXT,
        original_balance TEXT,
        row_hash VARCHAR(64),
        loaded_at TIMESTAMPTZ DEFAULT NOW()
      );
    `);

    // --- Time Deposits / CDs (custom object 2-60442980) ---
    await client.query(`
      CREATE TABLE IF NOT EXISTS stg_time_deposits (
        id SERIAL PRIMARY KEY,
        primary_key TEXT,
        cif_number TEXT,
        last_4_account_digits TEXT,
        interest_rate TEXT,
        account_type TEXT,
        account_description TEXT,
        issue_date TEXT,
        maturity_date TEXT,
        sales_associate TEXT,
        current_balance TEXT,
        branch_number TEXT,
        officer_name TEXT,
        time_deposit_status TEXT,
        opened_online_yn TEXT,
        row_hash VARCHAR(64),
        loaded_at TIMESTAMPTZ DEFAULT NOW()
      );
    `);

    // --- Debit Cards (custom object 2-60442979) ---
    await client.query(`
      CREATE TABLE IF NOT EXISTS stg_debit_cards (
        id SERIAL PRIMARY KEY,
        composite_key TEXT,
        cif_number TEXT,
        last_4_of_associated_account TEXT,
        associated_account_type TEXT,
        last_4_of_debit_card_digits TEXT,
        card_status TEXT,
        expiration_date TEXT,
        last_used TEXT,
        pos_trans_count__last_30_day TEXT,
        active_pos TEXT,
        row_hash VARCHAR(64),
        loaded_at TIMESTAMPTZ DEFAULT NOW()
      );
    `);

    await client.query(`
      CREATE TABLE IF NOT EXISTS hubspot_id_map (
        id SERIAL PRIMARY KEY,
        source_table VARCHAR(50) NOT NULL,
        source_key VARCHAR(255) NOT NULL,
        hubspot_id VARCHAR(50) NOT NULL,
        object_type VARCHAR(50) NOT NULL,
        updated_at TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE(source_table, source_key)
      );
    `);

    // Audit ledger of what has been successfully shipped to HubSpot.
    await client.query(`
      CREATE TABLE IF NOT EXISTS shipped_records (
        source_table VARCHAR(50) NOT NULL,
        source_key VARCHAR(255) NOT NULL,
        row_hash VARCHAR(64) NOT NULL,
        hubspot_id VARCHAR(50),
        shipped_at TIMESTAMPTZ DEFAULT NOW(),
        PRIMARY KEY (source_table, source_key)
      );
    `);

    // sync_errors: every failure, quarantine, OR loud warning is a first-class row here.
    // severity: 'error' (data blocked/quarantined) | 'warning' (operator attention) | 'info' (notable event)
    await client.query(`
      CREATE TABLE IF NOT EXISTS sync_errors (
        id SERIAL PRIMARY KEY,
        run_id INTEGER,
        source_table VARCHAR(50),
        source_key VARCHAR(255),
        error_type VARCHAR(50) NOT NULL,
        severity VARCHAR(10) NOT NULL DEFAULT 'error',
        error_message TEXT,
        record_snapshot JSONB,
        created_at TIMESTAMPTZ DEFAULT NOW()
      );
    `);
    // Ensure severity column exists on any pre-existing deployments.
    await client.query(`
      ALTER TABLE sync_errors ADD COLUMN IF NOT EXISTS severity VARCHAR(10) NOT NULL DEFAULT 'error'
    `);

    await client.query(`CREATE INDEX IF NOT EXISTS idx_sync_errors_run ON sync_errors(run_id)`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_sync_errors_created ON sync_errors(created_at DESC)`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_sync_errors_severity ON sync_errors(severity)`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_shipped_hash ON shipped_records(source_table, source_key, row_hash)`);

    // Three-stage hash verification + lossless preservation columns.
    // row_hash already exists on every staging table (HASH A — sha256 of canonical
    // CSV row). Add the rest. ALTER...IF NOT EXISTS so this is idempotent for both
    // fresh installs and existing deployments.
    const STAGING_TABLES = [
      'stg_contacts', 'stg_companies', 'stg_deposits',
      'stg_loans', 'stg_time_deposits', 'stg_debit_cards',
    ];
    for (const t of STAGING_TABLES) {
      await client.query(`ALTER TABLE ${t} ADD COLUMN IF NOT EXISTS raw_csv JSONB`);
      await client.query(`ALTER TABLE ${t} ADD COLUMN IF NOT EXISTS db_persist_hash VARCHAR(64)`);
      await client.query(`ALTER TABLE ${t} ADD COLUMN IF NOT EXISTS db_verified_at TIMESTAMPTZ`);
      await client.query(`ALTER TABLE ${t} ADD COLUMN IF NOT EXISTS hubspot_persist_hash VARCHAR(64)`);
      await client.query(`ALTER TABLE ${t} ADD COLUMN IF NOT EXISTS hubspot_verified_at TIMESTAMPTZ`);
      await client.query(`ALTER TABLE ${t} ADD COLUMN IF NOT EXISTS hubspot_verify_diff JSONB`);
      await client.query(`ALTER TABLE ${t} ADD COLUMN IF NOT EXISTS needs_review BOOLEAN DEFAULT false`);
      // Coercion audit per memory rule 3 — every transformation recorded.
      await client.query(`ALTER TABLE ${t} ADD COLUMN IF NOT EXISTS coercions JSONB DEFAULT '[]'::jsonb`);
      await client.query(`CREATE INDEX IF NOT EXISTS idx_${t}_needs_review ON ${t}(needs_review) WHERE needs_review = true`);
    }

    // Index the per-table unique key used for all WHERE <key> = $1 lookups
    // (coercion-audit UPDATE in syncRows, HASH C verify UPDATE, and the
    // diff-engine join). Without these, every per-row UPDATE on a 232k+ row
    // staging table is a sequential scan, making the pre-batch transform
    // O(n^2) — the first full-scale run sat in that loop for hours and never
    // reached the HubSpot batch upsert. These turn each lookup into a single
    // index probe.
    const STAGING_KEY_COLUMNS = {
      stg_contacts: 'cif_number',
      stg_companies: 'cif_number',
      stg_deposits: 'primary_key',
      stg_loans: 'primary_key',
      stg_time_deposits: 'primary_key',
      stg_debit_cards: 'composite_key',
    };
    for (const [t, col] of Object.entries(STAGING_KEY_COLUMNS)) {
      await client.query(`CREATE INDEX IF NOT EXISTS idx_${t}_key ON ${t}(${col})`);
    }

    // Account custom objects (DDA, Loans, Time Deposits) now sync the
    // CSV `relationship` column (P/C/M/B/X) to a HubSpot enumeration of
    // the same name. The picklist is created by scripts/repair_demo_schema.js.
    // Closes UAT "Verification of Segments" — Civista needs to filter
    // accounts by Primary vs Co-Owner without going through the
    // (deferred) association engine.
    for (const t of ['stg_deposits', 'stg_loans', 'stg_time_deposits']) {
      await client.query(`ALTER TABLE ${t} ADD COLUMN IF NOT EXISTS relationship VARCHAR(8)`);
    }

    // Account-level dedup model (Option B): a single physical account appears
    // as one source row per owner. We collapse owner rows onto one record per
    // account_key (see buildAccountKey in hubspot-mapping.js) and use
    // account_key as the HubSpot upsert identity for the three account objects.
    // The companion *_owners tables retain every owner row (account_key +
    // owner CIF + relationship code) to feed the association engine.
    for (const t of ['stg_deposits', 'stg_loans', 'stg_time_deposits']) {
      await client.query(`ALTER TABLE ${t} ADD COLUMN IF NOT EXISTS account_key TEXT`);
      await client.query(`CREATE INDEX IF NOT EXISTS idx_${t}_account_key ON ${t}(account_key)`);
    }

    const OWNER_TABLES = ['stg_deposit_owners', 'stg_loan_owners', 'stg_time_deposit_owners'];
    for (const t of OWNER_TABLES) {
      await client.query(`
        CREATE TABLE IF NOT EXISTS ${t} (
          id SERIAL PRIMARY KEY,
          account_key TEXT,
          cif_number TEXT,
          relationship VARCHAR(8),
          primary_key TEXT,
          row_hash VARCHAR(64),
          raw_csv JSONB,
          loaded_at TIMESTAMPTZ DEFAULT NOW()
        );
      `);
      await client.query(`CREATE INDEX IF NOT EXISTS idx_${t}_account_key ON ${t}(account_key)`);
      await client.query(`CREATE INDEX IF NOT EXISTS idx_${t}_cif ON ${t}(cif_number)`);
    }

    // Idempotency ledger for created associations. Keyed on the full directed
    // labeled edge so a nightly run never recreates an existing link.
    await client.query(`
      CREATE TABLE IF NOT EXISTS shipped_associations (
        from_object VARCHAR(20) NOT NULL,
        from_id     VARCHAR(50) NOT NULL,
        to_object   VARCHAR(20) NOT NULL,
        to_id       VARCHAR(50) NOT NULL,
        type_id     INTEGER     NOT NULL,
        created_at  TIMESTAMPTZ DEFAULT NOW(),
        PRIMARY KEY (from_object, from_id, to_object, to_id, type_id)
      );
    `);

    // Meta table for sandbox<->prod portal cutover safeguard. Stores the
    // last HubSpot portal id we ran against so the boot check can refuse
    // to start when HUBSPOT_API_KEY flips between sandbox and prod
    // (sandbox-portal hubspot_ids in shipped_records would be invalid in
    // prod). Cutover script (scripts/cutover-portal.js) clears the
    // affected tables and updates this row.
    await client.query(`
      CREATE TABLE IF NOT EXISTS meta (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL,
        updated_at TIMESTAMPTZ DEFAULT NOW()
      );
    `);

    // Persistent record of CSV->HubSpot mapping mismatches so the UI can
    // display them across runs. Populated at startup (one row per send:false
    // mapping) and at run time (date_coercion warnings increment counts).
    await client.query(`
      CREATE TABLE IF NOT EXISTS mapping_issues (
        id SERIAL PRIMARY KEY,
        source_csv TEXT NOT NULL,
        source_column TEXT NOT NULL,
        hs_object TEXT NOT NULL,
        hs_property TEXT NOT NULL,
        hs_type TEXT NOT NULL,
        problem TEXT NOT NULL,
        sample_value TEXT,
        rows_affected INTEGER DEFAULT 0,
        first_seen TIMESTAMPTZ DEFAULT NOW(),
        last_seen TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE(source_csv, source_column, hs_property)
      );
    `);

    await client.query('COMMIT');
    console.log('Database initialized: all tables created');
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
}

module.exports = { initDb, pool };
