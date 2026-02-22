"""Run base Snowflake setup SQL from Python."""

from pipeline.common.snowflake_client import SnowflakeClient

client = SnowflakeClient()

# Read bootstrap SQL and execute each statement in sequence.
with open("sql/00_bootstrap/snowflake_setup.sql") as f:
    sql = f.read()

for statement in sql.split(";"):
    if statement.strip():
        client.execute(statement)

print("Bootstrap complete")
