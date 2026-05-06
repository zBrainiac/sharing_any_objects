USE DATABASE {{db}};
USE SCHEMA {{schema}};

CREATE OR REPLACE PROCEDURE ECOS_RAW_SP_REGISTER_FILE(
    TENANT_ID      VARCHAR(256),
    BUSINESS_UNIT  VARCHAR(256),
    FILE_TYPE      VARCHAR(256),
    FILE_PATH      VARCHAR(1024),
    FILE_NAME      VARCHAR(512),
    FILE_SIZE      FLOAT
)
RETURNS VARCHAR(2048)
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
    "    t.BUSINESS_UNIT = " + q + BUSINESS_UNIT + q + ", " +
    "    t.FILE_TYPE = " + q + FILE_TYPE + q + ", " +
    "    t.REFRESHED_AT = CURRENT_TIMESTAMP() " +
    "WHEN NOT MATCHED THEN INSERT (TENANT_ID, BUSINESS_UNIT, FILE_TYPE, FILE_NAME, FILE_PATH, FILE_SIZE) " +
    "VALUES (" + q + TENANT_ID + q + ", " + q + BUSINESS_UNIT + q + ", " + q + FILE_TYPE + q + ", " + q + FILE_NAME + q + ", " + q + FILE_PATH + q + ", " + FILE_SIZE + ")";

snowflake.execute({sqlText: mergeSql});
return "File registered: " + FILE_PATH + " for tenant: " + TENANT_ID + ", business_unit: " + BUSINESS_UNIT + ", type: " + FILE_TYPE;';

CREATE OR REPLACE SECURE VIEW ECOS_RAW_VW_STAGE_FILES_DOWNLOAD_MT
COMMENT = 'Per-tenant file listing with pre-signed download URLs - filtered by row access policy based on TENANT_ID'
AS
SELECT
    TENANT_ID,
    BUSINESS_UNIT,
    FILE_TYPE,
    FILE_NAME,
    FILE_PATH,
    FILE_SIZE,
    LAST_MODIFIED,
    REFRESHED_AT,
    BUILD_STAGE_FILE_URL('@{{db}}.{{schema}}.ECOS_RAW_ST_DOC_MT', FILE_PATH) AS DOWNLOAD_URL
FROM ECOS_RAW_TB_STAGE_FILES_MT;

CREATE OR REPLACE ROW ACCESS POLICY ECOS_RAW_PL_STAGE_FILES_MT
AS (TENANT_ID_ARG VARCHAR(256)) RETURNS BOOLEAN ->
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

ALTER VIEW ECOS_RAW_VW_STAGE_FILES_DOWNLOAD_MT
    ADD ROW ACCESS POLICY ECOS_RAW_PL_STAGE_FILES_MT ON (TENANT_ID);

CREATE APPLICATION PACKAGE IF NOT EXISTS ECOS_RAW_SHARING TYPE = DATA
    COMMENT = 'Declarative share for secure multi-tenant file access';

CREATE OR REPLACE TEMPORARY STAGE {{db}}.{{schema}}.ECOS_RAW_ST_SHARING_MANIFEST;

COPY INTO @{{db}}.{{schema}}.ECOS_RAW_ST_SHARING_MANIFEST/manifest.yml FROM (
    SELECT $$roles:
  - app_user:
      comment: "Read-only access to the secure file download view"

shared_content:
  databases:
    - ECO_DEV:
        schemas:
          - ECOS_RAW_V001:
              roles: [app_user]
              views:
                - ECOS_RAW_VW_STAGE_FILES_DOWNLOAD_MT:
                    roles: [app_user]
$$
)
FILE_FORMAT = (TYPE = CSV COMPRESSION = NONE FIELD_OPTIONALLY_ENCLOSED_BY = NONE ESCAPE = NONE ESCAPE_UNENCLOSED_FIELD = NONE)
SINGLE = TRUE OVERWRITE = TRUE;

COPY FILES INTO snow://package/ECOS_RAW_SHARING/versions/LIVE/
    FROM @{{db}}.{{schema}}.ECOS_RAW_ST_SHARING_MANIFEST
    FILES = ('manifest.yml');

ALTER APPLICATION PACKAGE ECOS_RAW_SHARING RELEASE LIVE VERSION;
