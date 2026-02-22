USE DATABASE CLAIMS_POC;
USE SCHEMA SEM;

-- Return-to-work proxy: paid claims / total claims per batch date.
CREATE OR REPLACE VIEW VW_KPI_RTW_RATE AS
SELECT
  batch_date,
  IFF(claim_count = 0, 0, paid_claim_count::FLOAT / claim_count) AS rtw_rate
FROM GOLD.CLAIMS_MART;

-- Denial rate: denied claims / total claims per batch date.
CREATE OR REPLACE VIEW VW_KPI_DENIAL_RATE AS
SELECT
  batch_date,
  IFF(claim_count = 0, 0, denied_claim_count::FLOAT / claim_count) AS denial_rate
FROM GOLD.CLAIMS_MART;
