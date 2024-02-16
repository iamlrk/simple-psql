import os
import pandas as pd
from simplepgsql import DBConnect
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
    
    dbconnect = DBConnect(conn_params, return_type=pd.DataFrame)
    query = "SELECT * FROM procurement_383.bom_383_main"
    read_query_params = {
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
    # write_query_params = {
    #     "schema": "public",
    #     "table_name": "staff",
    #     "mode": "INSERT",

    #     "data": {
    #         "first_name": "First",
    #         "last_name": "Last",
    #         "address_id": 60,
    #         "email": "first.last@email.com",
    #         "store_id": 1,
    #         "active": True,
    #         "username": "first.last",
    #         "password": "8cb2237d0679ca88db6464eac60da96345513964",
    #     }
    # }

    with dbconnect as cursor:
        # print(cursor.query(query))
        results = cursor.read(**read_query_params)
        print(results)
        # cursor.write(**write_query_params)