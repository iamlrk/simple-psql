import os
import pandas as pd
from simplepgsql import DBConnect
from simplepgsql import SimplePgSQL
import configparser

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
    
  
    _query_params = {
        "schema": "public",
        "table": "film_list",
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

    # Using SimplePgSQL class
    pgsql = SimplePgSQL(conn_params, return_type=pd.DataFrame)

    _query = """
            SELECT category, SUM(price) from public.film_list where length > 60 group by category, price order by price DESC limit 10;
            """


    q_results = pgsql.execute(_query, columns=["category", "price"])
    r_results = pgsql.read(**_query_params)
    print(q_results)
    print(r_results)
    
    # Deprecated method to query data DO NOT USE. For backward compatibility only
    with DBConnect(conn_params, return_type=pd.DataFrame) as cursor:
        q_results = cursor.query("SELECT category, price FROM film_list LIMIT 10;", columns=["category", "price"])
        _query_params.pop("table")
        _query_params["table_name"] = "film_list"
        r_results = cursor.read(**_query_params)
        print(r_results)
        print(q_results)