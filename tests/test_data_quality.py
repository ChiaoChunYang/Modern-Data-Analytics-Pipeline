import pytest
import pandas as pd
import snowflake.connector
import os
from dotenv import load_dotenv

# Load Snowflake credentials from environment (best practice)
load_dotenv()

# Fixture to provide a Snowflake connection
@pytest.fixture(scope="module")
def snowflake_conn():
    conn = snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA'),
        role=os.getenv('SNOWFLAKE_ROLE')
    )
    yield conn
    conn.close()

# Helper function to run a query and return a DataFrame
def run_query(conn, query):
    cursor = conn.cursor()
    cursor.execute(query)
    columns = [col[0] for col in cursor.description]
    data = cursor.fetchall()
    return pd.DataFrame(data, columns=columns)

# Test 1: Primary Key Integrity - No Duplicate Campaign IDs
def test_no_duplicate_campaigns(snowflake_conn):
    query = """
        SELECT CAMPAIGN_ID, COUNT(*)
        FROM ANALYTICS.MARTS_MARKETING.FCT_CAMPAIGN_PERFORMANCE
        GROUP BY 1
        HAVING COUNT(*) > 1;
    """
    df = run_query(snowflake_conn, query)
    assert len(df) == 0, f"Found {len(df)} duplicate campaign IDs!"

# Test 2: Null Check - Mandatory Metrics Should Not Be Null
def test_no_null_metrics(snowflake_conn):
    query = """
        SELECT *
        FROM ANALYTICS.MARTS_MARKETING.FCT_CAMPAIGN_PERFORMANCE
        WHERE DAILY_SPEND IS NULL OR DAILY_CLICKS IS NULL OR DATE_DAY IS NULL;
    """
    df = run_query(snowflake_conn, query)
    assert len(df) == 0, f"Found {len(df)} records with NULL metrics!"

# Test 3: Logical Consistency - Spend Should Be Positive
def test_logical_spend_consistency(snowflake_conn):
    query = """
        SELECT *
        FROM ANALYTICS.MARTS_MARKETING.FCT_CAMPAIGN_PERFORMANCE
        WHERE DAILY_SPEND < 0;
    """
    df = run_query(snowflake_conn, query)
    assert len(df) == 0, f"Found {len(df)} records with negative spend!"

# Test 4: Relationship Test - All Campaigns in Fact table must exist in Dimension table
def test_referential_integrity(snowflake_conn):
    query = """
        SELECT FACT.CAMPAIGN_ID
        FROM ANALYTICS.MARTS_MARKETING.FCT_CAMPAIGN_PERFORMANCE FACT
        LEFT JOIN ANALYTICS.STAGING_MARKETING.STG_CAMPAIGNS DIM
            ON FACT.CAMPAIGN_ID = DIM.CAMPAIGN_ID
        WHERE DIM.CAMPAIGN_ID IS NULL;
    """
    df = run_query(snowflake_conn, query)
    assert len(df) == 0, "Found campaign IDs in Fact table that do not exist in the source Campaigns table!"
