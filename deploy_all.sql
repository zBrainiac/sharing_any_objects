-- Deploy All MT: Multi-Tenancy Stage File Sharing
-- Single shared stage with TENANT_ID-based Row Access Policy isolation
-- Prerequisite: Execute as ACCOUNTADMIN or role with appropriate privileges
--
-- CONFIGURATION: Set these variables before running
-- ==============================================================================
SET V_DATABASE  = 'MD_TEST';
SET V_SCHEMA    = 'DOC_AI';
SET V_WAREHOUSE = 'MD_TEST_WH';
SET V_SCHEMA_FQ = $V_DATABASE || '.' || $V_SCHEMA;

-- ==============================================================================
-- SETUP
-- ==============================================================================
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE IDENTIFIER($V_WAREHOUSE);
USE DATABASE IDENTIFIER($V_DATABASE);
USE SCHEMA IDENTIFIER($V_SCHEMA);

-- ==============================================================================
-- 1. CREATE STAGE
-- ==============================================================================
CREATE STAGE IF NOT EXISTS RAW_ST_DOC_MT
    DIRECTORY = (ENABLE = TRUE);

-- ==============================================================================
-- 2. CREATE TABLE
-- ==============================================================================
CREATE OR ALTER TABLE ECOS_RAW_TB_STAGE_FILES_MT (
    TENANT_ID          VARCHAR NOT NULL    COMMENT 'Tenant identifier that owns this file (e.g. BANK1)',
    FILE_NAME          VARCHAR NOT NULL    COMMENT 'Display name of the uploaded file',
    FILE_PATH          VARCHAR NOT NULL    COMMENT 'Relative path within the shared stage RAW_ST_DOC_MT',
    FILE_SIZE          NUMBER              COMMENT 'File size in bytes',
    LAST_MODIFIED      TIMESTAMP_TZ        COMMENT 'Timestamp of last file modification on stage',
    REFRESHED_AT       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'Timestamp when this metadata row was last inserted or updated',
    ASSIGNED_CONTACTS  VARCHAR             COMMENT 'Comma-separated list of contact emails responsible for this file',
    PRIMARY KEY (TENANT_ID, FILE_PATH)
)
COMMENT = 'Multi-tenancy file registry. Each row represents a file uploaded to the shared stage RAW_ST_DOC_MT, tagged with a tenant identifier for row-level access control.';

-- ==============================================================================
-- 3. CREATE STORED PROCEDURE (upsert file metadata on upload)
-- ==============================================================================
CREATE OR REPLACE PROCEDURE ECOS_RAW_SP_REGISTER_FILE(
    TENANT_ID  VARCHAR,
    FILE_PATH  VARCHAR,
    FILE_NAME  VARCHAR,
    FILE_SIZE  FLOAT
)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
'var q = String.fromCharCode(39);
var db = snowflake.createStatement({sqlText: "SELECT CURRENT_DATABASE()"}).execute();
db.next();
var schema = snowflake.createStatement({sqlText: "SELECT CURRENT_SCHEMA()"}).execute();
schema.next();
var fullSchema = db.getColumnValue(1) + "." + schema.getColumnValue(1);

var mergeSql =
    "MERGE INTO " + fullSchema + ".ECOS_RAW_TB_STAGE_FILES_MT t " +
    "USING (SELECT " + q + TENANT_ID + q + " AS TENANT_ID, " + q + FILE_PATH + q + " AS FILE_PATH) s " +
    "ON t.TENANT_ID = s.TENANT_ID AND t.FILE_PATH = s.FILE_PATH " +
    "WHEN MATCHED THEN UPDATE SET " +
    "    t.FILE_SIZE = " + FILE_SIZE + ", " +
    "    t.FILE_NAME = " + q + FILE_NAME + q + ", " +
    "    t.REFRESHED_AT = CURRENT_TIMESTAMP() " +
    "WHEN NOT MATCHED THEN INSERT (TENANT_ID, FILE_NAME, FILE_PATH, FILE_SIZE) " +
    "VALUES (" + q + TENANT_ID + q + ", " + q + FILE_NAME + q + ", " + q + FILE_PATH + q + ", " + FILE_SIZE + ")";

snowflake.execute({sqlText: mergeSql});
return "File registered: " + FILE_PATH + " for tenant: " + TENANT_ID;';

-- ==============================================================================
-- 4. CREATE SECURE VIEW
-- ==============================================================================
CREATE OR REPLACE SECURE VIEW ECOS_RAW_SV_STAGE_FILES_DOWNLOAD_MT AS
SELECT
    TENANT_ID,
    FILE_NAME,
    FILE_PATH,
    FILE_SIZE,
    LAST_MODIFIED,
    REFRESHED_AT,
    BUILD_STAGE_FILE_URL('@MD_TEST.DOC_AI.RAW_ST_DOC_MT', FILE_PATH) AS DOWNLOAD_URL
FROM ECOS_RAW_TB_STAGE_FILES_MT;

-- ==============================================================================
-- 5. CONSUMER ACCOUNT MAPPING TABLE (for cross-account listing consumers)
-- Note: CURRENT_ROLE() is NULL inside shared secure views — only CURRENT_ACCOUNT()
--       is available. Map consumer account locator → tenant_id for listing access.
-- ==============================================================================
CREATE TABLE IF NOT EXISTS ECOS_RAW_TB_CONSUMER_ROLE_MAPPING (
    CONSUMER_ACCOUNT  VARCHAR NOT NULL,
    CONSUMER_ROLE     VARCHAR NOT NULL,
    TENANT_ID         VARCHAR NOT NULL,
    CONSTRAINT PK_CONSUMER_ROLE_MAPPING PRIMARY KEY (CONSUMER_ACCOUNT, CONSUMER_ROLE)
);

-- ==============================================================================
-- 6. CREATE ROW ACCESS POLICY (local role + cross-account mapping)
--    - Local:         CURRENT_ROLE() = TENANT_ID || '_ROLE'  (e.g. BANK1_ROLE for BANK1)
--    - Cross-account: CURRENT_ROLE() IS NULL → fall through to account locator mapping
-- ==============================================================================
CREATE OR REPLACE ROW ACCESS POLICY ECOS_RAW_PL_STAGE_FILES_MT
AS (TENANT_ID_ARG VARCHAR) RETURNS BOOLEAN ->
    CURRENT_ROLE() = 'ACCOUNTADMIN'
    OR CURRENT_ROLE() = TENANT_ID_ARG || '_ROLE'
    OR (
        CURRENT_ROLE() IS NULL
        AND EXISTS (
            SELECT 1
            FROM ECOS_RAW_TB_CONSUMER_ROLE_MAPPING
            WHERE CONSUMER_ACCOUNT = CURRENT_ACCOUNT()
              AND TENANT_ID        = TENANT_ID_ARG
        )
    );

ALTER VIEW ECOS_RAW_SV_STAGE_FILES_DOWNLOAD_MT
    ADD ROW ACCESS POLICY ECOS_RAW_PL_STAGE_FILES_MT ON (TENANT_ID);

-- ==============================================================================
-- 7. GRANTS TO BANK ROLES
-- Add or remove roles as needed for your tenants.
-- ==============================================================================
GRANT READ ON STAGE RAW_ST_DOC_MT TO ROLE BANK1_ROLE;
GRANT READ ON STAGE RAW_ST_DOC_MT TO ROLE BANK2_ROLE;

GRANT SELECT ON VIEW ECOS_RAW_SV_STAGE_FILES_DOWNLOAD_MT TO ROLE BANK1_ROLE;
GRANT SELECT ON VIEW ECOS_RAW_SV_STAGE_FILES_DOWNLOAD_MT TO ROLE BANK2_ROLE;

GRANT USAGE ON PROCEDURE ECOS_RAW_SP_REGISTER_FILE(VARCHAR, VARCHAR, VARCHAR, FLOAT) TO ROLE BANK1_ROLE;
GRANT USAGE ON PROCEDURE ECOS_RAW_SP_REGISTER_FILE(VARCHAR, VARCHAR, VARCHAR, FLOAT) TO ROLE BANK2_ROLE;

GRANT USAGE ON DATABASE IDENTIFIER($V_DATABASE) TO ROLE BANK1_ROLE;
GRANT USAGE ON DATABASE IDENTIFIER($V_DATABASE) TO ROLE BANK2_ROLE;

GRANT USAGE ON SCHEMA IDENTIFIER($V_SCHEMA_FQ) TO ROLE BANK1_ROLE;
GRANT USAGE ON SCHEMA IDENTIFIER($V_SCHEMA_FQ) TO ROLE BANK2_ROLE;

GRANT USAGE ON WAREHOUSE IDENTIFIER($V_WAREHOUSE) TO ROLE BANK1_ROLE;
GRANT USAGE ON WAREHOUSE IDENTIFIER($V_WAREHOUSE) TO ROLE BANK2_ROLE;

-- ==============================================================================
-- 8. EXAMPLE: Register a cross-account consumer
-- Use account LOCATOR (run SHOW SHARES to find it), not org.account format.
-- ==============================================================================
-- INSERT INTO ECOS_RAW_TB_CONSUMER_ROLE_MAPPING (CONSUMER_ACCOUNT, CONSUMER_ROLE, TENANT_ID) VALUES
--     ('<CONSUMER_ACCOUNT_LOCATOR>', 'BANK1_ROLE', 'BANK1'),
--     ('<CONSUMER_ACCOUNT_LOCATOR>', 'BANK2_ROLE', 'BANK2');

-- ==============================================================================
-- 9. CONTACTS & DATABASE STEWARDSHIP
-- ==============================================================================
CREATE CONTACT IF NOT EXISTS DataSteward EMAIL_DISTRIBUTION_LIST = '<steward@your-org.com>';
CREATE CONTACT IF NOT EXISTS Support EMAIL_DISTRIBUTION_LIST = '<support@your-org.com>';
CREATE CONTACT IF NOT EXISTS Approver EMAIL_DISTRIBUTION_LIST = '<approver@your-org.com>';
CREATE CONTACT IF NOT EXISTS "Security & compliance" EMAIL_DISTRIBUTION_LIST = '<compliance@your-org.com>';

-- Note: Snowflake supports only 3 purpose types: STEWARD, SUPPORT, ACCESS_APPROVAL.
-- "Security & compliance" contact is created but cannot be assigned a purpose type.
ALTER DATABASE IDENTIFIER($V_DATABASE) SET CONTACT
    STEWARD         = DataSteward,
    SUPPORT         = Support,
    ACCESS_APPROVAL = Approver;

-- ==============================================================================
-- DEPLOYMENT COMPLETE
-- ==============================================================================
SELECT 'MT Deployment completed successfully!' AS STATUS;
