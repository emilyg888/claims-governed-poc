#!/usr/bin/env zsh

# Source this file to set Snowflake env vars for local runs:
#   source /Users/emilygao/Projects/snowflake/claims-governed-poc/config/snowflake_env.sh

export SNOWFLAKE_ACCOUNT="DXUDJSU-XYB39500"
export SNOWFLAKE_USER="EMILYG888USW"
export SNOWFLAKE_ROLE="ACCOUNTADMIN"
export SNOWFLAKE_WAREHOUSE="COMPUTE_WH"
export SNOWFLAKE_DATABASE="CLAIMS_POC"
export SNOWFLAKE_SCHEMA="RAW"

# Prompt for password only if not already set in the current shell.
if [[ -z "${SNOWFLAKE_PASSWORD:-}" ]]; then
  read -s "SNOWFLAKE_PASSWORD?Enter SNOWFLAKE_PASSWORD: "
  echo
  export SNOWFLAKE_PASSWORD
fi
