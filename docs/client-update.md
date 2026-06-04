# Civista — Sandbox QA Update (send-ready)

Use after the sandbox re-sync + QA pass. Answers the client's three questions and
confirms the associations work.

---

**1. Is the Sandbox a full copy or a select set?**
The integration is built to sync your complete dataset — no filtering or sampling. The
Sandbox now holds the full, deduplicated set after our validation pass.

**2. Will we see everything destined for production?**
Yes. The object types (Contacts, Companies, Deposits, Loans, Time Deposits, Debit Cards)
are final and are exactly what production will use. The two items that were still in
progress are now complete:
- **Account-to-customer associations** are built. Each Deposit, Loan, and Time Deposit is
  linked back to its owner Contact(s)/Company(ies) with the correct role label (Primary
  Owner, Co-Owner, Beneficiary, Trustee, etc., per Civista's relationship codes). Debit
  Cards link to their owner.
- **Account de-duplication** is in place. Previously a joint account showed up as several
  records (one per owner). It is now a single account record with each owner attached via
  the appropriate role — matching how the account exists in your core system.

**3. What differences should we expect between Sandbox and Production?**
- Sandbox HubSpot scale limits mean record counts won't always match production 1:1.
- Go-live is a deliberate, validated cutover (not a live mirror).
- Otherwise the structure, fields, associations, and de-duplication logic are identical to
  what production will receive.

---

### What we'd like Civista to QA in the Sandbox
- Open a few multi-owner accounts and confirm the owners + role labels look right.
- Confirm the account counts look reasonable now that duplicate per-owner records are
  collapsed.
- Flag any relationship label that doesn't read the way you'd expect.

Once you're happy, we'll schedule the production cutover.
