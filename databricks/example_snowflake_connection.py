# Databricks notebook source
# Databricks notebook source
!pip install pandas
!pip install snowflake-connector-python
!pip install openpyxl
!pip install shap


# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt

from snowflake.connector import connect
import snowflake
import os

class SnowflakeConection(object):
    _connection = None
    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super(SnowflakeConection, cls).__new__(cls)
            cls._connection = open_snowflake_conn()            
        return cls.instance

def open_snowflake_conn():
    snowflake_conn = snowflake.connector.connect(
        user='',
        password='',
        account='kepler_prod.east-us-2.azure',
        warehouse='EMSDB_ANG_WH',
        database='INGESTION_DB',
        schema='EMSDB_ANG'
    )
    return snowflake_conn

# COMMAND ----------

def run_snowflake_query(snowflake_conn, query, columns_names):
    cur = snowflake_conn.cursor()
    rows = []
    try:
        cur.execute(query)
        row = cur.fetchone()
        while row != None:
            rows.append(row)
            row = cur.fetchone()
    finally:
        cur.close()
    df = pd.DataFrame(rows)
    df.columns = columns_names
    return df

# COMMAND ----------

snowflake_conn = SnowflakeConection()

# COMMAND ----------

query = ''
df = run_snowflake_query(snowflake_conn._connection, query_completions_wells, col_names)
