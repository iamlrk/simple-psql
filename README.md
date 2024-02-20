# Simple PostgreSQL Interaction in Python

This project contains a Python script (`simplepgsql.py`) that provides a simple interface for interacting with a PostgreSQL database using psycopg2 library.

## Getting Started

Install via PIP

### Dependent Libraries

- Python 3.6+
- PostgreSQL
- psycopg2 Python library
- Pandas

## Usage

```python
from simplepgsql Import DBConnect

conn_params = {
        "host": config['DB']['DB_HOST'],
        "database": config['DB']['DB_NAME'],
        "user": config['DB']['DB_USER'].strip(),
        "password": config['DB']['DB_PASSWORD'].strip(),
        "port": config['DB']['DB_PORT'],
    }

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
        results = cursor.read(**_query_params)
        print(results)

```

### Output

```cmd
      category sum: price
0          New     109.78
1       Travel     109.78
2       Family      74.85
3        Games     129.74
4  Documentary      69.86
5    Animation      74.85
6       Sports     119.76
7       Comedy      94.81
8       Horror      89.82
9      Foreign      99.80
```

## License

This project is licensed under the [GNU GPL v3 License](./LICENSE).
