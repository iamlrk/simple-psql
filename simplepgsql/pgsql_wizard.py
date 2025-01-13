from typing import Iterable, Union

import pandas as pd
import psycopg2
from psycopg2 import sql

from .build_query import Query


class PgSQLWizard:
    def __init__(self,
                 host: str,
                 database: str,
                 user: str,
                 password: str,
                 port: int,
                 return_type: str = 'dict'):
        self.columns = None
        self.__validate_return_type(return_type)
        self.return_type = return_type.strip().lower()
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.port = port
        self.query_type = 'read'
        self.auto_choose_best_return_type = False

    @staticmethod
    def __validate_return_type(return_type: str) -> None:
        valid_return_types = ['pd.DataFrame', 'dict', 'list', 'tuple', 'value', 'flat-list']
        __valid_return_types_lower_case = [r.lower() for r in valid_return_types]
        if return_type.strip().lower() not in __valid_return_types_lower_case:
            raise ValueError(f"Invalid return type: {return_type}. "
                             f"Valid return types are: \n{', '.join(valid_return_types)}.")

    def __enter__(self) -> "PgSQLWizard":
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
            self.connection = psycopg2.connect(
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password,
                port=self.port
            )
            self.cursor = self.connection.cursor()
            return self

        except (Exception, psycopg2.Error) as error:
            raise error

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

    def choose_best_return_type(self):
        if self.result is None:
            self.return_type = 'value'

        elif all(len(row) == 1 for row in self.result):
            self.return_type = 'flat-list'

        elif len(self.result) == 1 and len(self.result[0]) == 1:
            self.return_type = 'value'

        elif len(self.result) == 1:
            self.return_type = 'dict'

        else:
            self.return_type = 'dict'

    def format_result(self, columns: list | None = None):
        if self.auto_choose_best_return_type:
            self.choose_best_return_type()

        if self.return_type == 'dict':
            if columns is None:
                # raise error
                if not self.auto_choose_best_return_type:
                    raise ValueError("Columns cannot be None when return type is 'dict'.")

                # if no column names are provided, then return the result as a list of lists
                self.return_type = 'list'
                return format(columns)

            return [dict(zip(columns, row)) for row in self.result]

        elif self.return_type == 'list':
            return self.result

        elif self.return_type == 'flat-list':
            # to be used to give the result as a flat list
            # i.e., first value of each row in the result
            # best used when the result is a single column
            if len(self.result[0]) > 1:
                raise ValueError("Flat list can only be used when the result is a single column.")
            return [row[0] for row in self.result]

        elif self.return_type == 'tuple':
            return tuple([tuple(row) for row in self.result])

        elif self.return_type == 'value':
            return self.result[0][0]

        elif self.return_type == 'pd.dataframe':
            if columns is None:
                # raise error
                if not self.auto_choose_best_return_type:
                    raise ValueError("Columns cannot be None when return type is 'pd.DataFrame'.")

                # if no column names are provided, then return the result as a list of lists
                self.return_type = 'list'
                return format(columns)
            return pd.DataFrame(self.result, columns=columns)

        else:
            raise ValueError(f"Invalid return type: {self.return_type}. How the hell did you get here?")

    def __execute(self, query: 'Query') -> None:
        """
        Executes a SQL query.

        Parameters
        ----------
        query : str
            The SQL query to execute.

        Returns
        -------
        None
        """
        with self:
            self.cursor.execute(query.query)
            if self.query_type == "read":
                self.result = self.cursor.fetchall()
                self.result = self.format_result(query.columns)
                return self.result
            elif self.query_type == "write":
                self.connection.commit()
                return None
            else:
                return None

    def read(self,
             query: 'Query',
             return_type: str | None = None
             ) -> pd.DataFrame | dict | list | tuple | None:
        self.query_type = 'read'
        self.set_return_type(return_type)

        if "*" in query.columns:
            query.columns = self.get_column_names(query.schema, query.table)


        return self.__execute(query)

    def set_return_type(self, return_type: str):
        if return_type is not None:
            self.__validate_return_type(return_type)
            self.return_type = return_type

    def write(self, data: Union[pd.DataFrame, dict], query: 'Query') -> None:
        if isinstance(data, pd.DataFrame):
            data = data.to_dict(orient='records')

        if isinstance(data, dict):
            raise NotImplementedError

        self.query_type = 'write'



    def execute(self, query: 'Query', return_type: str | None = None):
        self.set_return_type(return_type)
        return self.__execute(query)

    def read_row(self, query: 'Query', return_type: str = 'list'):
        self.query_type = 'read'
        self.set_return_type(return_type)
        return self.__execute(query)

    def read_column(self, query: 'Query', return_type: str = 'flat-list'):
        self.query_type = 'read'
        self.set_return_type(return_type)
        return self.__execute(query)

    def read_value(self, query: 'Query', return_type: str = 'value'):
        self.query_type = 'read'
        self.set_return_type(return_type)
        return self.__execute(query)

    def get_column_names(self, schema: str, table: str):
        """
        Retrieves the column names of a table.

        Parameters
        ----------
        schema : str
            Schema Name in the database.
        table : str
            Table Name in the database.

        Returns
        -------
        list
            The column names of the table.
        """
        query = sql.SQL('SELECT * FROM {schema}.{table} LIMIT 1;').format(
                schema=sql.Identifier(schema), table=sql.Identifier(table)
        )
        with self:
            self.cursor.execute(query)
        return [desc[0] for desc in self.cursor.description]


    def get_tables(self):
        raise NotImplementedError