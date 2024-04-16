from typing_extensions import deprecated
import psycopg2
import pandas as pd
from psycopg2 import sql

@deprecated("Use SimplePgSQL class instead")
class DBConnect:
    def __init__(self, conn_params: dict, return_type: type = dict) -> None:
        """
        Initializes a DBConnect object.

        Parameters
        ----------
        conn_params : dict
            Connection parameters for the PostgreSQL database.
        return_type : type, optional
            The default return type for query results, by default dict.
            Possible types: list, dict, pd.DataFrame

        Raises
        ------
        ValueError
            If an invalid return type is specified.
        """
        self.comm_params = conn_params
        self.connection = None
        self.cursor = None
        self.result = None
        self.columns = None
        self.aggregate = None
        self.table_name = None
        self.conditions = None
        self.conjuction = None
        self.order_by = None
        self.group_by = None
        self.limit = None
        if return_type not in [list, dict, pd.DataFrame]:
            raise ValueError("Invalid return type")
        self.return_type = return_type

        # self.query_params = query_params

    def __enter__(self) -> 'DBConnect':
        """
        Establishes a connection to the PostgreSQL database and returns a cursor.

        Returns
        -------
        psycopg2.extensions.cursor
            The database cursor.

        Raises
        ------
        CredentialError
            If there is an error while connecting to the PostgreSQL database.
        """
        try:
            self.connection = psycopg2.connect(**self.comm_params)
            self.cursor = self.connection.cursor()
            return self

        except (Exception, psycopg2.Error) as error:
            raise error

    def query(self, query: str, columns: list|None=None) -> dict | list | pd.DataFrame:
        """
        Executes a SQL query.

        Parameters
        ----------
        query : str
            The SQL query to execute.

        Returns
        -------
        dict/list/pd.DataFrame
            The query results based on the return type specified.

        Raises
        ------
        ValueError
            If the query is empty or not a string, or if a non-SELECT query is provided.
        psycopg2.errors.GroupingError
            If there is an error related to grouping in the query.
        psycopg2.errors.InFailedSqlTransaction
            If there is an error related to a failed SQL transaction.
        Exception
            If there is any other error while executing the query.
        """
        if not query:
            raise ValueError("Query cannot be empty")
        if not isinstance(query, (str, sql.Composable)):
            raise ValueError("Query must be a string")
        if isinstance(query, str):
            _write_queries = ["INSERT", "UPDATE", "DELETE"]
            if columns:
                self.columns = columns
            else:
                raise ValueError("Columns must be specified")
            if any(_w in query.upper() for _w in _write_queries):
                raise ValueError("Only SELECT queries are allowed")
            self.query_type = "read"

        
        try:
            self.cursor.execute(query)
            if self.query_type == "read":
                self.result = self.cursor.fetchall()
                self.result = self.format_result()
                return self.result
            elif self.query_type == "write":
                self.connection.commit()
                return None
        except psycopg2.errors.GroupingError as error:
            raise error
        except psycopg2.errors.InFailedSqlTransaction as error:
            raise error
        except (Exception, psycopg2.Error) as error:
            raise error
        pass

    def construct_query(self) -> str:
        """
        Constructs a SQL query based on the query parameters.

        Returns
        -------
        str
            The constructed SQL query.
        """
        pass

    def validate_query_params(self) -> None:
        """
        Validates the query parameters.

        Raises
        ------
        ValueError
            If the query parameters are not as expected.
        """
        if not isinstance(self.columns, (list, type(None))):
            raise TypeError("Columns must be a list (or None for all columns)")
        if not isinstance(self.table_name, str):
            raise TypeError("Table name must be a string")
        if not isinstance(self.conditions, (dict, type(None))):
            raise TypeError("Conditions must be a dictionary")
        if not isinstance(self.aggregate, (dict, type(None))):
            raise TypeError("Aggregate must be a dictionary")
        if not isinstance(self.conjuction, (str, type(None))):
            raise TypeError("Conjunction must be a string")
        if not isinstance(self.order_by, (dict, type(None), str, tuple)):
            raise TypeError(
                "order_by must be a dictionary/tuple/string or None")
        if not isinstance(self.group_by, (list, type(None))):
            raise TypeError("Group must be a list")
        if not isinstance(self.limit, (int, type(None))):
            raise TypeError("Limit must be an integer")

    def read(self,
             schema: str,
             table_name: str,
             columns: list | None = None,
             aggregate: dict | None = None,
             conditions: dict | None = None,
             conjuction: str = None,
             order_by: str | tuple | dict | None = None,
             group_by: list | None = None,
             limit: int | None = None) -> dict | list | pd.DataFrame:
        """
        Reads data from a table in the database.

        Parameters
        ----------
        schema : str
            Schema Name in the database.
        table_name : str
            Table Name in the database.
        columns : list (or None), optional
            List of columns to retrieve. Leave None to retrieve all columns, by default None.
        aggregate : dict (or None), optional
            Aggregation functions to apply to specific columns, by default None.
            { column: aggregate type, ...}
            Possible aggregate types: AVG, COUNT, MAX, MIN, SUM
        conditions : dict (or None), optional
            Conditions to filter the data, by default None.
            { column: (value, operator), ...}
            Possible operators: =, <, >, <=, >=, <>, IN, NOT IN, BETWEEN, LIKE, ILIKE
        conjuction : str, optional
            The conjunction to use for multiple conditions, by default None.
        order_by : str/tuple/dict/None, optional
            The column(s) to order the data by, by default None.
            column name or (column name, direction) or {column name: direction}
            Possible directions: ASC, DESC
        group_by : list (or None), optional
            The columns to group the data by, by default None.
        limit : int (or None), optional
            The number of rows to retrieve, by default None.
        return_type : type, optional
            The type of the return value, by default class default i.e. dict.
            Possible types: list, dict, pd.DataFrame
            If not specified, the default return type defined in the constructor will be used.

        Returns
        -------
        dict/list/pd.DataFrame
            The retrieved data from the table based on the return type specified.

        Raises
        ------
        TypeError
            If the input types are not as expected.
        ValueError
            If group by is not specified for aggregate functions.
        """
        self.query_type = "read"
        self.schema = schema
        self.table_name = table_name
        self.conditions = conditions
        self.conjuction = conjuction
        self.order_by = order_by
        self.group_by = group_by
        self.limit = limit
        
        self.validate_query_params()

        if not columns:
            self.columns = self._get_column_names(schema, table_name)
        else:
            self.columns = columns

        if aggregate is not None:
            self.aggregate = aggregate
            if not group_by:
                raise ValueError(
                    "Group by must be specified for aggregate functions")

        query = sql.SQL("SELECT ").format()

        if aggregate is not None:
            # Apply aggregation function to columns specified in the `aggregate` dictionary, else use the column directly.
            columns_sql = [
                sql.SQL("{}({})").format(sql.SQL(aggregate.get(column)), sql.Identifier(
                    column)) if column in aggregate else sql.Identifier(column)
                for column in columns
            ]

        else:
            columns_sql = [sql.Identifier(column) for column in self.columns]

        # Constructing the complete SELECT statement
        query = sql.SQL("SELECT {}").format(sql.SQL(', ').join(columns_sql))

        query += sql.SQL(" FROM {}.{}").format(
            sql.Identifier(schema),
            sql.Identifier(table_name)
        )

        if conditions is not None:
            operators = {_o for _c, (_v, _o) in conditions.items()}
            where_clause = sql.SQL("WHERE ")
            spl_columns = ["BETWEEN"]

            if any(_o.upper() in operators for _o in spl_columns):
                if self.conjuction is None:
                    self.conjuction = "AND"
                
                # eg: {"time_stamp": (["2024-01-26 00:00:00", "2024-01-27 00:00:00"], "BETWEEN") }
                between_conditions = [sql.SQL("{column} {operator} {value1} AND {value2}").format(
                    column=sql.Identifier(_c),
                    operator=sql.SQL(_o.upper()),
                    value1=sql.Literal(_v[0]),
                    value2=sql.Literal(_v[1])
                ) for _c, (_v, _o) in conditions.items() if _o in spl_columns if _o == "BETWEEN"]
                
                where_clause = where_clause + sql.SQL(f' {self.conjuction} ').join(between_conditions)
                               

                # remove special condition from the conditions dictionary
                conditions = {column: (value, operator) for column, (value, operator) in conditions.items() if operator.upper() not in spl_columns}


            if len(conditions.keys()) > 0:
                if where_clause != sql.SQL("WHERE "):
                    where_clause = where_clause + sql.SQL(f' {self.conjuction} ')
                
                conditions = [(sql.Identifier(column), sql.Literal(value), sql.SQL(
                    operator)) for column, (value, operator) in conditions.items()]
                
                where_clause = where_clause + sql.SQL(f' {self.conjuction} ').join(
                    sql.SQL("{column} {operator} {value}").format(
                        column=column,
                        operator=operator,
                        value=value
                    )
                    for column, value, operator in conditions
                )            

            query += sql.SQL(" {where_clause}").format(
                where_clause=where_clause
            )
        

        if group_by is not None:
            query += sql.SQL(" GROUP BY {group}").format(
                group=sql.SQL(', ').join(map(sql.Identifier, group_by))
            )

        if order_by is not None:
            if isinstance(order_by, str):
                order_by = {order_by: "ASC"}
            elif isinstance(order_by, tuple):
                order_by = {order_by[0]: order_by[1]}

            order_by = [(sql.Identifier(column), sql.SQL(direction))
                        for column, direction in order_by.items()]
            order_by_clause = sql.SQL(', ').join(
                sql.SQL("{column} {direction}").format(
                    column=column,
                    direction=direction
                )
                for column, direction in order_by
            )
            query += sql.SQL(" ORDER BY {order_by_clause}").format(
                order_by_clause=order_by_clause
            )

        if limit is not None:
            query += sql.SQL(" LIMIT {limit}").format(limit=sql.Literal(limit))

        self.query(query)

        return self.result

    def format_result(self):
        """
        Formats the query result based on the return type.

        Returns
        -------
        dict/list/pd.DataFrame
            The formatted query result.
        """
        if self.return_type == list:
            return self.result
        elif self.return_type == dict:
            return {i: dict(zip(self.columns, row)) for i, row in enumerate(self.result)}
        elif self.return_type == pd.DataFrame:
            if not self.aggregate:
                return pd.DataFrame(self.result, columns=self.columns)
            else:
                _columns = [_col if _col not in self.aggregate else f"{self.aggregate[_col].lower()}: {_col}" for _col in self.columns]
                return pd.DataFrame(self.result, columns=_columns)

    def write(self, 
              schema: str, 
              table_name: str, 
              mode:str = None,
              data: dict | None = None 
              ):
        self.query_type = "write"
        if mode.lower().strip() not in ["insert", "update", "delete"]:
            raise ValueError("Invalid mode")

        self.schema = schema
        self.table_name = table_name
        self.mode = mode
        self.columns = data.keys()
        self.values = data.values()

        if self.mode == "INSERT":
            query = sql.SQL("INSERT INTO {}.{} ({}) VALUES ({})").format(
                sql.Identifier(schema),
                sql.Identifier(table_name),
                sql.SQL(', ').join(map(sql.Identifier, self.columns)),
                sql.SQL(', ').join(map(sql.Literal, self.values))
            )

        # print(query)
        self.query(query)

        return self.result

    @staticmethod
    def format_array_for_sql(array):
        """
        Formats an array to be used in an SQL query.

        Parameters
        ----------
        array : list or tuple
            The array to format.

        Returns
        -------
        str
            The formatted array as a string.
        """
        return ', '.join(list(array))

    def _get_column_names(self, schema, table_name):
        """
        Retrieves the column names of a table.

        Parameters
        ----------
        schema : str
            Schema Name in the database.
        table_name : str
            Table Name in the database.

        Returns
        -------
        list
            The column names of the table.
        """
        query = sql.SQL("SELECT * FROM {schema}.{table_name} LIMIT 1;").format(
            schema=sql.Identifier(schema),
            table_name=sql.Identifier(table_name)
        )
        self.cursor.execute(query)
        self.columns = [desc[0] for desc in self.cursor.description]
        return self.columns

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Closes the database connection and cursor.

        Parameters
        ----------
        exc_type : type
            The type of the exception that occurred, if any.
        exc_value : Exception
            The exception that occurred, if any.
        traceback : traceback
            The traceback of the exception, if any.
        """
        if self.connection:
            self.cursor.close()
            self.connection.close()
