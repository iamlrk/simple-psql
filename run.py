import os
import pandas as pd
from simplepgsql import DBConnect
import configparser
import dotenv

dotenv.load_dotenv()

if __name__ == "__main__":
    # read data from config file
    config = configparser.ConfigParser()
    config.read("config.ini")
    conn_params = {
        "host": config['DB']['DB_HOST'],
        "database": config['DB']['DB_NAME'],
        "user": config['DB']['DB_USER'].strip(),
        "password": config['DB']['DB_PASSWORD'].strip(),
        "port": config['DB']['DB_PORT'],
    }
    
  
    # _query_params = {
    #     "schema": "public",
    #     "table_name": "film_list",
    #     "columns": ["category", "price"],
    #     "aggregate": {
    #         "price": "SUM"
    #     },
    #     "conditions": {
    #         "length": (60, ">")
    #     },
    #     "order_by": ("price", "DESC"),
    #     "group_by": ["category", "price"],
    #     "limit": 10,
    # }
    with DBConnect(conn_params, return_type=pd.DataFrame) as cursor:
        results = cursor.query("SELECT category, price FROM film_list LIMIT 10;", columns=["category", "price"])
        print(results)