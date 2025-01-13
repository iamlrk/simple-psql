from typing import Iterable, Optional, Union

from psycopg2 import sql
from psycopg2.sql import Composed


class Query:
    """
    A class for building SQL queries dynamically using psycopg2's SQL module.

    Attributes:
    -----------
    schema : Optional[str]
        The schema of the table to be queried.
    table : Optional[str]
        The table to be queried.
    join : Optional[dict[str, str]]
        Join conditions in a dictionary format.
    columns : Optional[Union[str, list[str]]]
        Columns to be selected in the _query.
    aggregate : Optional[dict[str, str]]
        Aggregation functions (e.g., SUM, COUNT) for specific columns.
    where : Optional[dict[str, dict[str, str]]]
        Filtering conditions for the WHERE clause.
    conjunction : Optional[str]
        Logical conjunction for the WHERE clause (e.g., AND, OR).
    order_by : Optional[Union[str, dict[str, str]]]
        Column(s) and sort direction (ASC, DESC) for ORDER BY clause.
    group_by : Optional[Union[str, list[str]]]
        Column(s) for the GROUP BY clause.
    limit : Optional[str]
        Limit the number of rows returned by the _query.
    _query : Optional[sql.SQL]
        The generated SQL _query object.
    final_query : Optional[sql.SQL]
        Stores the final SQL _query object after building.
    auto_build : bool
        Automatically builds the _query upon setting parameters.

    Methods:
    --------
    from_string(_query: str, columns: Iterable) -> 'Query'
        Loads a _query from a raw string.
    from_params(*, schema: str, table: str, ...) -> 'Query'
        Builds the _query from individual parameters like schema, table, join, etc.
    build() -> Composed
        Constructs the SQL _query using the stored attributes.

    Example Usage:
    --------------
    q = Query()
    # Build from parameters
    q.from_params(
        schema="public",
        table="employees",
        columns=["name", "age", "salary"],
        where={"age": ("<", "30")},
        order_by="salary",
        limit="10"
    )
    # Build from string
    q.from_string("SELECT name, age, salary FROM public.employees", ["name", "age", "salary"])
    """

    def __init__(self):
        self.__build_source = None
        self.schema: Optional[str] = None
        self.table: Optional[str] = None
        self.join: Optional[dict[str, str]] = None
        self.columns: Optional[Union[str, list[str]]] = None
        self.aggregate: Optional[dict[str, str]] = None
        self.where: Optional[dict[str, dict[str, str]]] = None
        self.conjunction: Optional[str] = None
        self.order_by: Optional[Union[str, dict[str, str]]] = None
        self.group_by: Optional[Union[str, list[str]]] = None
        self.limit: Optional[str] = None
        self._query: Optional[sql.SQL] = None
        self.final_query: Optional[sql.SQL] = None
        self.auto_build: bool = True

    @property
    def query(self):
        return self._query

    def from_string(self, query: str, columns: Iterable) -> 'Query':
        """
        Loads a _query from a raw string and sets the columns attribute.

        Parameters:
        -----------
        _query : str
            The raw SQL _query string.
        columns : Iterable
            The list of columns involved in the _query.

        Returns:
        --------
        Query : The updated Query object.

        Example Usage:
        --------------
        q = Query()
        q.from_string("SELECT name, age FROM public.users", ["name", "age"])
        """
        try:
            self._query = sql.SQL(query)
            self.__build_source = 'string'
            self.columns = columns
            return self

        except TypeError as e:
            raise TypeError(f"Invalid _query: {e}")

        except ValueError as e:
            raise ValueError(f"Invalid _query: {e}")

    def from_params(self,
                    *,
                    schema: str,
                    table: str,
                    join: Optional[dict[str, str]] = None,
                    columns: Optional[Union[list[str], str]] = None,
                    where: Optional[dict[str, tuple[str, str]]] = None,
                    conjunction: Optional[str] = None,
                    order_by: Optional[Union[str, dict[str, str]]] = None,
                    aggregate: Optional[dict[str, str]] = None,
                    group_by: Optional[Union[str, Iterable]] = None,
                    limit: Optional[str] = None,
                    ) -> 'Query':
        """
        Constructs the _query by specifying its components.

        Parameters:
        -----------
        schema : str
            The schema where the table is located.
        table : str
            The table to select data from.
        join : Optional[dict[str, str]], default=None
            The join conditions for other tables.
        columns : Optional[Union[list[str], str]], default=None
            The columns to select.
        where : Optional[dict[str, tuple[str, str]]], default=None
            The WHERE clause conditions.
        conjunction : Optional[str], default=None
            Logical operator to use between WHERE conditions (e.g., AND, OR).
        order_by : Optional[Union[str, dict[str, str]]], default=None
            Column(s) to order by, along with sort direction (ASC, DESC).
        aggregate : Optional[dict[str, str]], default=None
            Aggregation functions for the selected columns.
        group_by : Optional[Union[str, Iterable]], default=None
            Columns to group by.
        limit : Optional[str], default=None
            The LIMIT clause.

        Returns:
        --------
        Query : The updated Query object.

        Example Usage:
        --------------
        q = Query()
        q.from_params(
            schema="public",
            table="users",
            columns=["name", "age"],
            where={"age": (">", "18")},
            conjunction="AND",
            order_by={"age": "ASC"},
            limit="100"
        )
        """
        self.__build_source = 'params'
        self.schema = schema
        self.table = table
        self.join = join
        self.columns = columns if isinstance(columns, list) else [columns]
        self.where = where
        self.conjunction = conjunction
        self.order_by = order_by
        self.aggregate = aggregate
        self.group_by = group_by
        self.limit = limit

        if self.auto_build:
            self._query = self.build()

        return self

    def build(self) -> Composed:
        """
        Constructs the SQL _query based on the provided attributes.

        Returns:
        --------
        Composed : The composed SQL _query.

        Raises:
        -------
        ValueError : If required fields are missing or invalid.

        Example Usage:
        --------------
        q = Query()
        q.from_params(schema="public", table="employees", columns=["name", "salary"], where={"age": (">", "30")})
        _query = q.build()
        """
        plain_string_query = []
        if self.__build_source == 'string':
            raise ValueError("Build is not necessary when _query is generated from string")

        _columns = self.columns if isinstance(self.columns, list) else [self.columns]
        columns_sql = [sql.Identifier(column) for column in _columns]
        _plain_columns = _columns

        if self.aggregate:
            if not all(column in self.columns for column in self.aggregate.keys()):
                raise ValueError("Columns in 'aggregate' must be in 'columns'")

            _temp_cols = []
            _plain_cols = []
            for i, cols in enumerate(_columns):
                if cols not in self.aggregate.keys():
                    _temp_cols.append(sql.Identifier(cols))
                    _plain_cols.append(cols)
                elif cols in self.aggregate.keys():
                    _plain_cols.append(f'{self.aggregate.get(cols)}({cols})')
                    _temp_cols.append(sql.SQL("{agg}({col})").format(agg=sql.SQL(self.aggregate.get(cols)),
                                                                     col=sql.Identifier(cols)))
                    self.columns[i] = f"{cols}.{self.aggregate.get(cols).lower()}"
            columns_sql = _temp_cols
            _plain_columns = _plain_cols

        if '*' in self.columns:
            _plain_columns = '*'
            columns_sql = [sql.SQL('*')]

        # Constructing the complete SELECT statement
        query = sql.SQL("SELECT {}").format(sql.SQL(', ').join(columns_sql))
        plain_string_query.append(f'SELECT {", ".join(_plain_columns)}')

        query += sql.SQL(" FROM {schema}.{table}").format(
                schema=sql.Identifier(self.schema),
                table=sql.Identifier(self.table)
        )

        plain_string_query.append(f'FROM {self.schema}.{self.table}')

        if self.where is not None:
            conditions = self.where
            operators = {_operator for _column, (_operator, _value) in self.where.items()}
            where_clause = sql.SQL("WHERE ")
            spl_columns = ["BETWEEN"]

            sql_conditions_list = []
            plain_text_conditions_list = []

            if any(_o.upper() in operators for _o in spl_columns):
                # Special handling for BETWEEN operator
                between_conditions = [sql.SQL("{column} {operator} {value1} AND {value2}").format(
                        column=sql.Identifier(_c),
                        operator=sql.SQL(_o.upper()),
                        value1=sql.Literal(_v[0]),
                        value2=sql.Literal(_v[1])
                ) for _c, (_o, _v) in self.where.items() if _o in spl_columns]

                plain_text_between_conditions = ["{column} {operator} {value1} AND {value2}".format(
                        column=_c,
                        operator=_o.upper(),
                        value1=_v[0],
                        value2=_v[1]
                ) for _c, (_o, _v) in self.where.items() if _o in spl_columns]

                sql_conditions_list.extend(between_conditions)
                plain_text_conditions_list.extend(plain_text_between_conditions)

                conditions = {column: (value, operator) for column, (operator, value) in conditions.items()
                              if operator.upper() not in spl_columns}

            if len(conditions.keys()) > 0:
                plain_text_conditions = [(column, value, operator)
                                         for column, (operator, value)
                                         in conditions.items()]

                conditions = [(sql.Identifier(column), sql.Literal(value), sql.SQL(
                        operator)) for column, (operator, value) in conditions.items()]

                sql_conditions_list.extend([sql.SQL("{column} {operator} {value}").format(
                        column=column,
                        operator=operator,
                        value=value
                ) for column, value, operator in conditions])

                plain_text_conditions_list.extend(["{column} {operator} {value}".format(
                        column=column,
                        operator=operator,
                        value=value
                ) for column, value, operator in plain_text_conditions])

            where_clause = where_clause + sql.SQL(f' {self.conjunction} ').join(sql_conditions_list)
            plain_text_where_clause = "WHERE " + f' {self.conjunction} '.join(plain_text_conditions_list)

            query += sql.SQL(" {where_clause}").format(
                    where_clause=where_clause
            )

            plain_string_query.append(plain_text_where_clause)

        if self.group_by is not None:
            query += sql.SQL(" GROUP BY {group}").format(
                    group=sql.SQL(', ').join(map(sql.Identifier, self.group_by))
            )
            plain_string_query.append(f' GROUP BY {", ".join(self.group_by)}')

        if self.order_by is not None:
            if isinstance(self.order_by, str):
                self.order_by = {self.order_by: "ASC"}

            order_by = [(sql.Identifier(column), sql.SQL(direction))
                        for column, direction in self.order_by.items()]

            order_by_clause = sql.SQL(', ').join(
                    sql.SQL("{column} {direction}").format(
                            column=column,
                            direction=direction
                    )
                    for column, direction in order_by
            )
            plain_text_order_by_clause = "ORDER BY " + ', '.join([f"{col} {dire}"
                                                                  for col, dire
                                                                  in self.order_by.items()])
            query += sql.SQL(" ORDER BY {order_by_clause}").format(
                    order_by_clause=order_by_clause
            )
            plain_string_query.append(plain_text_order_by_clause)

        if self.limit is not None:
            query += sql.SQL(" LIMIT {limit}").format(limit=sql.Literal(self.limit))
            plain_string_query.append(f'LIMIT {self.limit}')

        print(' '.join(plain_string_query))
        return query
