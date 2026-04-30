# Snowflake Secure File Sharing -- Multi-Tenancy

Secure file sharing with per-tenant access control using a single shared stage and Row Access Policies.  
Managed as a DCM (Database Change Management) project following [DataOpsBackbone](https://github.com/zBrainiac/DataOpsBackbone) naming conventions.

---

## Naming Convention

All Snowflake object names follow the [DataOpsBackbone naming standard](https://github.com/zBrainiac/DataOpsBackbone#naming-convention):

| Level | Pattern | This Project |
|-------|---------|--------------|
| Database | `{DOMAIN}_{ENV}` | `ECO_DEV` |
| Schema | `{DOMAIN}_{MATURITY}_v{NNN}` | `ECOS_RAW_V001` |
| Objects | `{DOMAIN}{COMP}_{MATURITY}_{TYPE}_{TEXT}` | `ECOS_RAW_TB_STAGE_FILES_MT` |

- **Domain**: `ECO` (Ecosystem)
- **Component**: `S` (Sharing)
- **Maturity**: `RAW`

---

## Project Structure

```
sharing_any_objects/
├── manifest.yml                    # DCM project manifest
├── pre_deploy.sql                  # Database, schema, DCM project creation
├── post_deploy.sql                 # Secure view, RAP, stored procedure
├── post_deployment_grants.sql      # Role grants and contacts (run with ACCOUNTADMIN)
├── .github/
│   └── workflows/
│       └── update-local-repo.yml   # CI/CD pipeline
├── sources/
│   └── definitions/
│       ├── infrastructure.sql      # Stage definition (DEFINE)
│       └── tables.sql              # Table definitions (DEFINE)
├── sqlunit/
│   └── tests.sqltest               # SQL validation tests
├── app/
│   └── app.py                      # Streamlit file download portal
├── upload_mt.py                    # Upload files with tenant registration
├── download_files.py               # Download files for the current role
├── connections.toml.example        # Snowflake connection template
└── README.md
```

---

## Architecture

```
    LOCAL ACCESS (same account)           CROSS-ACCOUNT ACCESS (via listing)
    ============================          =================================

+------------------+                      +------------------+
|  BANK1_USER      |                      | Consumer Account |
|  (BANK1_ROLE)    |                      | (e.g. GK68488)   |
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
           | ECOS_RAW_VW_STAGE_FILES_DOWNLOAD_MT  |
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
           |      @ECOS_RAW_ST_DOC_MT             |
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
    CURRENT_ROLE() is NULL in shared views -- only CURRENT_ACCOUNT() is used.
```

## Components

| Object | Type | Managed By | Description |
|--------|------|------------|-------------|
| `ECOS_RAW_ST_DOC_MT` | Internal Stage | DCM DEFINE | Single shared stage for all tenants |
| `ECOS_RAW_TB_STAGE_FILES_MT` | Table | DCM DEFINE | File metadata with TENANT_ID column |
| `ECOS_RAW_TB_CONSUMER_ROLE_MAPPING` | Table | DCM DEFINE | Maps consumer account locators to tenant IDs |
| `ECOS_RAW_VW_STAGE_FILES_DOWNLOAD_MT` | Secure View | post_deploy.sql | Per-tenant file list with download URLs |
| `ECOS_RAW_PL_STAGE_FILES_MT` | Row Access Policy | post_deploy.sql | Local + cross-account tenant isolation |
| `ECOS_RAW_SP_REGISTER_FILE` | Stored Procedure | post_deploy.sql | Upserts file metadata on upload (MERGE) |

## DCM Deployment

### Prerequisites

- Snowflake CLI >= 3.16 (`snow --version`)
- A Snowflake connection configured (`~/.snowflake/connections.toml`)
- Account: `zs28104.eu-central-1`

### First-Time Setup

```bash
# 1. Create database, schema, and DCM project
snow sql -f pre_deploy.sql -c <connection> --role ACCOUNTADMIN

# 2. Analyze definitions
snow dcm raw-analyze ECO_DEV.ECOS_RAW_V001.SHARING_OBJECTS -c <connection> --target DEV

# 3. Plan changes
snow dcm plan ECO_DEV.ECOS_RAW_V001.SHARING_OBJECTS -c <connection> --target DEV --save-output

# 4. Deploy
snow dcm deploy ECO_DEV.ECOS_RAW_V001.SHARING_OBJECTS -c <connection> --target DEV --alias "initial"

# 5. Run post-deployment (secure view, RAP, stored procedure)
snow sql -f post_deploy.sql -c <connection> --role ACCOUNTADMIN

# 6. Apply grants and contacts (requires ACCOUNTADMIN)
snow sql -f post_deployment_grants.sql -c <connection> --role ACCOUNTADMIN
```

### CI/CD Pipeline

The `.github/workflows/update-local-repo.yml` workflow automates:

1. SonarQube SQL linting
2. DCM analyze + plan + deploy
3. Schema clone for regression testing
4. SQLUnit validation tests
5. GitHub Release creation

## Cross-Account Sharing

When the secure view is shared via a Snowflake listing:

- `CURRENT_ROLE()` returns **NULL** inside shared secure views
- `CURRENT_ACCOUNT()` returns the consumer's **account locator** (e.g. `GK68488`)
- The RAP falls through to `ECOS_RAW_TB_CONSUMER_ROLE_MAPPING`

To onboard a new consumer account:

```sql
INSERT INTO ECO_DEV.ECOS_RAW_V001.ECOS_RAW_TB_CONSUMER_ROLE_MAPPING
    (CONSUMER_ACCOUNT, CONSUMER_ROLE, TENANT_ID)
VALUES ('GK68488', 'BANK1_ROLE', 'BANK1');
```

---

## Requirements

```bash
pip install snowflake-connector-python streamlit
```

## Connection Setup

Copy `connections.toml.example` to `~/.snowflake/connections.toml` and fill in your PAT tokens:

```toml
[bank1]
account = "zs28104.eu-central-1"
user = "BANK1_USER"
role = "BANK1_ROLE"
database = "ECO_DEV"
schema = "ECOS_RAW_V001"
warehouse = "MD_TEST_WH"
authenticator = "programmatic_access_token"
token = "<PAT_TOKEN>"
```

> Generate PAT tokens in Snowsight under **Admin > Security > Programmatic Access Tokens**.

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

### Streamlit app

```bash
streamlit run app/app.py
```

Open http://localhost:8501 in your browser. Select a role (`BANK1_ROLE` or `BANK2_ROLE`) to list and download files.

## SQL Validation Tests

Tests are in `sqlunit/tests.sqltest` and validate:

- Table existence (`ECOS_RAW_TB_STAGE_FILES_MT`, `ECOS_RAW_TB_CONSUMER_ROLE_MAPPING`)
- Stage existence (`ECOS_RAW_ST_DOC_MT`)
- View existence (`ECOS_RAW_VW_STAGE_FILES_DOWNLOAD_MT`)
- Column presence and count for each table

## Files

| File | Description |
|------|-------------|
| `manifest.yml` | DCM project manifest with DEV target |
| `pre_deploy.sql` | Creates database, schema, DCM project |
| `post_deploy.sql` | Secure view, RAP, stored procedure |
| `post_deployment_grants.sql` | Role grants, warehouse access, contacts (ACCOUNTADMIN) |
| `sources/definitions/infrastructure.sql` | Stage definition (DCM DEFINE) |
| `sources/definitions/tables.sql` | Table definitions (DCM DEFINE) |
| `sqlunit/tests.sqltest` | SQL validation tests |
| `upload_mt.py` | Upload files with tenant registration |
| `download_files.py` | Download files for the current role |
| `app/app.py` | Streamlit file download portal |
| `connections.toml.example` | Snowflake connection template |
