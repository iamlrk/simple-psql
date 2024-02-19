import os
import pandas as pd
from dbconnect import DBConnect
import configparser


if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read('config.ini', encoding='utf-8')
    conn_params = {
        "host": config['DB']['DB_HOST'],
        "database": config['DB']['DB_NAME'],
        "user": config['DB']['DB_USER'].strip(),
        "password": config['DB']['DB_PASSWORD'].strip(),
        "port": config['DB']['DB_PORT'],
    }
    
  
    query = "SELECT * FROM procurement_383.bom_383_main"
    _query_params = {
        "schema": "public",
        "table_name": "film_list",
        "columns": ["category", "price"],
        "aggregate": {
            "price": "SUM"
        },
        "conditions": {
            "length": (60, ">")
        },
        "order_by": ("price", "DESC"),
        "group_by": ["category", "price"],
        "limit": 10,
    }
    with DBConnect(conn_params, return_type=pd.DataFrame) as cursor:
        # print(cursor.query(query))
        results = cursor.read(**_query_params)
        print(results)