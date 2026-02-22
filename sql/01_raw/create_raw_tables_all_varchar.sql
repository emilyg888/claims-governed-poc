-- ============================================================
-- RAW LAYER DDL
-- Snapshot columns are explicitly typed to align with pipeline SQL consumers.
-- Events table remains generic VARCHAR columns.
-- ============================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE CLAIMS_POC;
USE SCHEMA RAW;

-- ============================================================
-- SNAPSHOT TABLE
-- ============================================================

CREATE OR REPLACE TABLE CLAIMS_SNAPSHOT_NIGHTLY (
    -- Business columns (must match snapshot CSV order used in COPY projection)
    BATCH_DATE              DATE,
    SOURCE_SYSTEM           VARCHAR,
    CLAIM_ID                VARCHAR,
    CLAIM_VERSION           NUMBER,
    POLICY_ID               VARCHAR,
    CUSTOMER_ID             VARCHAR,
    EMPLOYER_ID             VARCHAR,
    PROVIDER_ID             VARCHAR,
    CLAIM_TYPE              VARCHAR,
    LODGEMENT_CHANNEL       VARCHAR,
    JURISDICTION            VARCHAR,
    LOSS_DATE               DATE,
    REPORT_DATE             DATE,
    OPEN_DATE               DATE,
    LIABILITY_DECISION      VARCHAR,
    LIABILITY_DECISION_DATE DATE,
    CLOSE_DATE              DATE,
    CLAIM_STATUS            VARCHAR,
    STATUS_EFFECTIVE_DATE   DATE,
    INJURY_CODE             VARCHAR,
    INJURY_SEVERITY         VARCHAR,
    R_TW_FLAG               VARCHAR,
    R_TW_DATE               DATE,
    CLAIM_AMOUNT_INCURRED   NUMBER(18,2),
    PAID_AMOUNT_TO_DATE     NUMBER(18,2),
    RESERVE_AMOUNT          NUMBER(18,2),
    CURRENCY                VARCHAR,
    SENSITIVE_FLAG          VARCHAR,
    PII_CLASS               VARCHAR,
    RECORD_HASH             VARCHAR,
    INGEST_TS               TIMESTAMP_NTZ,
    LAST_UPDATED_TS         TIMESTAMP_NTZ,

    -- Metadata
    SRC_FILENAME          VARCHAR,
    SRC_FILE_ROW_NUMBER   NUMBER,
    LOADED_AT             TIMESTAMP_NTZ
);

-- ============================================================
-- EVENTS TABLE
-- ============================================================

CREATE OR REPLACE TABLE CLAIMS_EVENTS_NIGHTLY (

    -- Business columns (adjust to match events CSV)
    COL_1  VARCHAR,
    COL_2  VARCHAR,
    COL_3  VARCHAR,
    COL_4  VARCHAR,
    COL_5  VARCHAR,
    COL_6  VARCHAR,
    COL_7  VARCHAR,
    COL_8  VARCHAR,
    COL_9  VARCHAR,
    COL_10 VARCHAR,

    -- Metadata
    SRC_FILENAME          VARCHAR,
    SRC_FILE_ROW_NUMBER   NUMBER,
    LOAD_TS               TIMESTAMP_NTZ
);
