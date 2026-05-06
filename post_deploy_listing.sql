-- Create and publish the declarative sharing listing.
-- Requires: ACCOUNTADMIN role, ECOS_RAW_SHARING application package already released.
--
-- Account : zs28104.eu-central-1
-- Target  : SFSEEUROPE.MDAEPPEN_AWS_FFM3
--

USE ROLE ACCOUNTADMIN;

CREATE EXTERNAL LISTING IF NOT EXISTS ECOS_RAW_IRS_LISTING
APPLICATION PACKAGE ECOS_RAW_SHARING AS
$$
title: "IRS File Sharing"
subtitle: "Secure multi-tenant file access with row-level filtering"
description: "Per-tenant file download portal with row access policy. Consumers see only files tagged with their tenant ID."
listing_terms:
  type: "OFFLINE"
targets:
  accounts: ["SFSEEUROPE.MDAEPPEN_AWS_FFM3"]
auto_fulfillment:
  refresh_type: "SUB_DATABASE_WITH_REFERENCE_USAGE"
business_needs:
  - type: "CUSTOM"
    name: "Regulatory Reporting"
    description: "Secure, auditable file exchange between financial institutions with per-tenant isolation, business unit classification, and document type filtering."
data_attributes:
  geography:
    geo_option: "GLOBAL"
    granularity:
      - "REGION_CONTINENT"
  time:
    granularity: "DAILY"
    time_range:
      time_frame: "LAST"
      units: "MONTHS"
      value: 6
usage_examples:
  - title: "List all files for your tenant"
    description: "Returns all files visible to the current role with download URLs"
    query: "SELECT TENANT_ID, BUSINESS_UNIT, FILE_TYPE, FILE_NAME, FILE_SIZE, DOWNLOAD_URL FROM ECOS_RAW_VW_STAGE_FILES_DOWNLOAD_MT;"
  - title: "Filter by business unit and file type"
    description: "Find all IRS documents from the Tax business unit"
    query: "SELECT FILE_NAME, FILE_SIZE, DOWNLOAD_URL FROM ECOS_RAW_VW_STAGE_FILES_DOWNLOAD_MT WHERE BUSINESS_UNIT = 'Tax' AND FILE_TYPE = 'IRS';"
  - title: "Count files by classification"
    description: "Summary of available files grouped by business unit and type"
    query: "SELECT BUSINESS_UNIT, FILE_TYPE, COUNT(*) AS FILE_COUNT, SUM(FILE_SIZE) / 1024 AS TOTAL_SIZE_KB FROM ECOS_RAW_VW_STAGE_FILES_DOWNLOAD_MT GROUP BY BUSINESS_UNIT, FILE_TYPE ORDER BY BUSINESS_UNIT, FILE_TYPE;"
resharing:
  enabled: false
$$
PUBLISH = FALSE
REVIEW = FALSE;

ALTER LISTING ECOS_RAW_IRS_LISTING PUBLISH;
