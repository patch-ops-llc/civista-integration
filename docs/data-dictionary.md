# Data Dictionary

This document maps every field from Civista's source files to what it becomes in HubSpot. Use this when reading CRM data or debugging "where did this value come from?"

---

## CIF → HubSpot Contacts

People records. Each person has a unique CIF number that links them to their accounts.

| HubSpot Property | Source Column | Notes |
|-----------------|---------------|-------|
| `cif_number` | CIFNum | Unique identifier. May start with `\` or `@` — that's intentional. |
| `firstname` | FirstName | |
| `lastname` | LastName | |
| `fullname` | FullName | |
| `email` | Email | Source value `"none"` → null (no email on file) |
| `phone` | PhoneNumber | |
| `address` | Address1 | |
| `address2` | Address2 | |
| `city` | City | |
| `state` | State | |
| `zip` | ZipCode | |
| `date_of_birth` | Birthday | |
| `became_customer_date` | OrigCustDate | When they first became a Civista customer |
| `hash_ssn` | HashSSN | One-way hash of SSN (not reversible) |
| `digital_banking` | DigBank | Whether enrolled in digital banking |
| `private_banking` | PrivBanking | Private banking customer flag |
| `deceased` | Deceased | Source value `" "` (single space) = alive/false |
| `minor` | Minor | Under-18 flag |
| `do_not_call` | DoNotCall | |
| `q2_user_id` | Q2UserID | Digital banking platform user ID |
| `last_login` | LastLogin | Last digital banking login date |

---

## CIF → HubSpot Companies

Business records. Same source file as contacts — classified by Tax ID Type.

| HubSpot Property | Source Column | Notes |
|-----------------|---------------|-------|
| `cif_number` | CIFNum | Unique identifier |
| `name` | FullName | Business name |
| `phone` | PhoneNumber | |
| `address` | Address1 | |
| `address2` | Address2 | |
| `city` | City | |
| `state` | State | |
| `zip` | ZipCode | |
| `became_customer_date` | OrigCustDate | |
| `naics_code` | NAICSCode | Industry classification code |

---

## DDA → HubSpot Custom Object: Deposits

Checking and savings accounts. HubSpot Object ID: `2-60107989`

| HubSpot Property | Source Column | Notes |
|-----------------|---------------|-------|
| `primary_key` | PrimaryKey | Pre-hashed unique ID from source |
| `cif_number` | CIF# | Links to the contact/company |
| `acctlast4` | Acctlast4 | Last 4 digits of account number |
| `interest_rate` | InterestRate | Converted from string (`.03680000`) to number |
| `accttype` | AcctType | Account type code |
| `acctdesc` | AcctDesc | Human-readable account description |
| `open_date` | OpenDate | |
| `close_date` | CloseDate | Null if still open |
| `slsassoc` | SlsAssoc | Sales associate who opened it |
| `dt_last_active` | DtLastActive | Last transaction date |
| `current_bal` | CurrentBal | Today's balance (converted to number) |
| `yest_bal` | YestBal | Yesterday's balance |
| `branch_num` | BranchNum | Branch that owns the account |
| `officr_code` | OfficrCode | Officer code |
| `acct_status` | AcctStatus | Values 0-9, stored raw |
| `relationship` | Relationship | Trimmed (source has trailing whitespace) |
| `promo_code` | PromoCode | |
| `open_online` | OpenOnline | Whether opened via digital channel |

---

## Loans → HubSpot Custom Object: Loans

All loan products. HubSpot Object ID: `2-60108336`

| HubSpot Property | Source Column | Notes |
|-----------------|---------------|-------|
| `primary_key` | PrimaryKey | Pre-hashed unique ID |
| `cif_number` | CIFNum | Links to contact/company |
| `acctlast4` | Acctlast4 | |
| `interest_rate` | InterestRate | Converted to number |
| `accttype` | AcctType | |
| `loan_type` | LoanType | |
| `orig_date` | OrigDate | Origination date |
| `maturity_date` | MaturityDate | |
| `slsassoc` | SlsAssoc | |
| `last_active_date` | LastActiveDate | |
| `curr_bal` | CurrBal | Current balance (converted to number) |
| `branch_num` | BranchNum | |
| `officr_code` | OfficrCode | |
| `acct_status` | AcctStatus | Values 0-9, stored raw |
| `relationship` | Relationship | Trimmed |
| `orig_bal` | OrigBal | Original loan amount |

---

## CDs → HubSpot Custom Object: Time Deposits

Certificates of Deposit. HubSpot Object ID: `2-60108759`

| HubSpot Property | Source Column | Notes |
|-----------------|---------------|-------|
| `primary_key` | PrimaryKey | Pre-hashed unique ID |
| `cif_number` | CIFNum | Links to contact/company |
| `acctlast4` | Acctlast4 | |
| `interest_rate` | InterestRate | Converted to number |
| `accttype` | AcctType | |
| `acctdesc` | AcctDesc | |
| `issue_date` | IssueDate | |
| `maturity_date` | MaturityDate | |
| `slsassoc` | SlsAssoc | |
| `curr_bal` | CurrBal | Current balance (converted to number) |
| `branch_num` | BranchNum | |
| `officr_code` | OfficrCode | |
| `acct_status` | AcctStatus | Values 0-9, stored raw |
| `relationship` | Relationship | Trimmed |
| `open_online` | OpenOnline | |

---

## Debit Cards → HubSpot Custom Object: Debit Cards

HubSpot Object ID: `2-60107457`

| HubSpot Property | Source Column | Notes |
|-----------------|---------------|-------|
| `composite_key` | *(generated)* | `CIF# + Acctlast4 + AcctType + Last4DebitCard + Expiredate` |
| `cif_number` | CIF# | Links to contact/company |
| `acctlast4` | Acctlast4 | |
| `acct_type` | AcctType | |
| `last4_debit_card` | Last4DebitCard | |
| `card_status` | CardStatus | Stored raw (no enum mapping) |
| `expire_date` | Expiredate | |
| `last_used_dt` | LastUsedDt | |
| `pos_last_30_days` | POSLast30Days | Number of POS transactions in last 30 days |
| `active_pos` | ActivePOS | |

---

## How Records Link Together

Every account record (DDA, Loan, CD, Debit Card) has a `cif_number` field that references the Contact or Company it belongs to. This is the glue that connects a person to their products.

> **Note:** Formal HubSpot associations (the visual links between records) are not yet implemented. The `cif_number` field exists on all objects and can be used to cross-reference.
