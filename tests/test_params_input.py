import pandas as pd
from simplepgsql import PgSQLWizard, SimplePgSQL
from simplepgsql import Query
import configparser

def get_config():
    config = configparser.ConfigParser()
    config.read("config.ini")
    conn_params = {
        "host": config['DB']['DB_HOST'],
        "database": config['DB']['DB_NAME'],
        "user": config['DB']['DB_USER'].strip(),
        "password": config['DB']['DB_PASSWORD'].strip(),
        "port": config['DB']['DB_PORT'],
    }
    return conn_params


if __name__ == "__main__":
    # read data from config file
    conn_params = get_config()

    _query_params = {
        'schema': "public",
        'table': "film_list",
        'columns': ["category", "price"],
        # 'columns': "category",
        # 'aggregate': {
        #     "category": "COUNT"
        # },
        'where': {
            "length": (">", 60)
        },
        # 'order_by': {"price": 'DESC'},
        # 'group_by': ["category", "price"],
        # 'limit': 1,
    }

    pgsql = PgSQLWizard(**conn_params, return_type='pd.DataFrame')
    query_1 = Query().from_params(**_query_params)
    data = pgsql.read_column(query_1)
    print(data)


