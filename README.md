# Snowflake Secure File Sharing — Multi-Tenancy

Secure file sharing with per-tenant access control using a single shared stage and Row Access Policies.

---

## Architecture

```
    LOCAL ACCESS (same account)           CROSS-ACCOUNT ACCESS (via listing)
    ============================          =================================

+------------------+                      +------------------+
|  BANK1 User      |                      | Consumer Account |
|  (bank1_user)    |                      | (e.g. GK68488)   |
+--------+---------+                      +--------+---------+
         |                                         |
         v                                         v
+--------+---------+                      +--------+---------+
|   BANK1_ROLE     |                      |   ACCOUNTADMIN   |
+--------+---------+                      +--------+---------+
         |                                         |
         |  CURRENT_ROLE() = TENANT_ID             |  CURRENT_ROLE() IS NULL
         |                                         |  (Snowflake hides consumer role
         |                                         |   inside shared secure views)
         |                                         |
         +--------------------+--------------------+
                              |
           +------------------+-------------------+
           | ECOS_RAW_SV_STAGE_FILES_DOWNLOAD_MT  |
           |           (SECURE VIEW)              |
           |     shared via listing/share         |
           +------------------+-------------------+
                              |
           +------------------+-------------------+
           |    ECOS_RAW_PL_STAGE_FILES_MT        |
           |       (ROW ACCESS POLICY)            |
           |                                      |
           |  1. CURRENT_ROLE() = 'ACCOUNTADMIN'  |
           |  2. CURRENT_ROLE() = TENANT_ID       |
           |  3. CURRENT_ROLE() IS NULL           |
           |     AND CURRENT_ACCOUNT() matches    |
           |     ECOS_RAW_TB_CONSUMER_ROLE_MAPPING|
           +------------------+-------------------+
                              |
           +------------------+-------------------+
           |    ECOS_RAW_TB_STAGE_FILES_MT        |
           |  TENANT_ID | FILE_NAME | FILE_PATH   |
           +------------------+-------------------+
                              ^
                              | ECOS_RAW_SP_REGISTER_FILE()
                              | (called on every upload)
                              |
           +------------------+-------------------+
           |         @RAW_ST_DOC_MT               |
           |     (single shared stage)            |
           +--------------------------------------+

    ECOS_RAW_TB_CONSUMER_ROLE_MAPPING (cross-account access)
    +------------------+---------------+-----------+
    | CONSUMER_ACCOUNT | CONSUMER_ROLE | TENANT_ID |
    +------------------+---------------+-----------+
    | GK68488          | BANK1_ROLE    | BANK1     |
    | GK68488          | BANK2_ROLE    | BANK2     |
    +------------------+---------------+-----------+
    Note: Use account LOCATOR (e.g. GK68488), not org.account format.
    CURRENT_ROLE() is NULL in shared views — only CURRENT_ACCOUNT() is used.
```

## Components

| Object | Type | Description |
|--------|------|-------------|
| `RAW_ST_DOC_MT` | Stage | Single shared stage for all tenants |
| `ECOS_RAW_TB_STAGE_FILES_MT` | Table | File metadata with TENANT_ID column |
| `ECOS_RAW_TB_CONSUMER_ROLE_MAPPING` | Table | Maps consumer account locators to tenant IDs for cross-account access |
| `ECOS_RAW_SV_STAGE_FILES_DOWNLOAD_MT` | Secure View | Per-tenant file list with download URLs |
| `ECOS_RAW_PL_STAGE_FILES_MT` | Row Access Policy | Local: `CURRENT_ROLE() = TENANT_ID`; Cross-account: account locator mapping |
| `ECOS_RAW_SP_REGISTER_FILE` | Stored Procedure | Upserts file metadata on upload (MERGE) |

## Cross-Account Sharing

When the secure view is shared via a Snowflake listing:

- `CURRENT_ROLE()` returns **NULL** inside shared secure views (Snowflake privacy restriction)
- `CURRENT_ACCOUNT()` returns the consumer's **account locator** (e.g. `GK68488`)
- The RAP falls through to `ECOS_RAW_TB_CONSUMER_ROLE_MAPPING`

To onboard a new consumer account:

```sql
INSERT INTO ECOS_RAW_TB_CONSUMER_ROLE_MAPPING (CONSUMER_ACCOUNT, CONSUMER_ROLE, TENANT_ID)
VALUES ('GK68488', 'BANK1_ROLE', 'BANK1');
```

---

## Requirements

```bash
pip install snowflake-connector-python
```

## Connection Setup

Add to `~/.snowflake/connections.toml`:

```toml
[bank1]
account = "<YOUR_ACCOUNT>"
user = "bank1_user"
role = "BANK1_ROLE"
database = "MD_TEST"
schema = "DOC_AI"
warehouse = "MD_TEST_WH"
authenticator = "programmatic_access_token"
token = "<PAT_TOKEN>"

[bank2]
account = "<YOUR_ACCOUNT>"
user = "bank2_user"
role = "BANK2_ROLE"
database = "MD_TEST"
schema = "DOC_AI"
warehouse = "MD_TEST_WH"
authenticator = "programmatic_access_token"
token = "<PAT_TOKEN>"
```

> Generate PAT tokens in Snowsight under **Admin → Security → Programmatic Access Tokens**.

## Usage

### Upload files
```bash
python upload_mt.py --connection <admin_connection> --tenant-id BANK1 --files invoice.pdf report.pdf
```

### List available files
```bash
python download_files.py --connection bank1 --list-only
```

### Download files
```bash
python download_files.py --connection bank1 --output-dir ./downloads
```

## Files

| File | Description |
|------|-------------|
| `deploy_all.sql` | Deployment script (configurable via session variables) |
| `download_files.py` | Download files for the current role |
| `upload_mt.py` | Upload files with tenant registration |
