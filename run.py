import os
import pandas as pd
from dbconnect import DBConnect
from dotenv import load_dotenv

load_dotenv()

if __name__ == "__main__":
    conn_params = {
        "host": os.getenv("DB_HOST"),
        "database": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
    }
    dbconnect = DBConnect(conn_params, return_type=pd.DataFrame)
    # query = "SELECT * FROM procurement_383.bom_383_main"
    _query_params = {
        "schema": "test_schema",
        "table_name": "test_table",
        "columns": ["Name", "Grade"],
        "aggregate": {
            "part_level": "AVG"
        },
        "conditions": {
            "Grade": (5, ">")
        },
        "order_by": "Name",
        "group_by": ["Name"],
    }
    with dbconnect as cursor:
        # print(cursor.query(query))
        results = cursor.read(**_query_params)
        print(results)