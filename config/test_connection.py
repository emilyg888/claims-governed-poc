"""Quick Snowflake connectivity check using env.dev.yaml values."""

import yaml
import snowflake.connector

# Load YAML config
with open("config/env.dev.yaml", "r") as f:
    config = yaml.safe_load(f)

sf = config["snowflake"]

conn = snowflake.connector.connect(
    account=sf["account"],
    user=sf["user"],
    password=sf["password"],
    warehouse=sf["warehouse"],
    database=sf["database"],
    # Any existing schema can be used for connection test.
    schema=sf["schema_gold"],
    role=sf["role"],
)

print("Connection successful")

conn.close()
