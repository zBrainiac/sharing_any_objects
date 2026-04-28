CREATE DATABASE IF NOT EXISTS ECO_DEV
    COMMENT = 'Development database for ECO (Ecosystem) domain - secure file sharing';

USE DATABASE ECO_DEV;

CREATE SCHEMA IF NOT EXISTS ECOS_RAW_V001
    COMMENT = 'Raw data landing zone for ECO Sharing component';

CREATE DCM PROJECT IF NOT EXISTS ECO_DEV.ECOS_RAW_V001.SHARING_OBJECTS;
