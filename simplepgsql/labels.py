import dataclasses


@dataclasses.dataclass
class Params:
    schema: str = "schema"
    table: str = "table"
    columns: str = "columns"
    aggregate: str = "aggregate"
    conditions: str = "conditions"
    conjunction: str = "conjunction"
    order_by: str = "order_by"
    group_by: str = "group_by"
    limit: str = "limit"


@dataclasses.dataclass
class StandardKeywords:
    ALL_COLUMNS: str = "*"
    WHERE: str = "WHERE"
    AND: str = "AND"
    OR: str = "OR"
    GREATER: str = ">"
    LESSER: str = "<"
    EQUALS: str = "="
    NOT_EQUALS: str = "!="
    BETWEEN: str = "BETWEEN"
    GREATER_OR_EQUALS: str = ">="
    LESS_OR_EQUALS: str = "<="
    LIKE: str = "LIKE"
    NOT: str = "NOT"
    ASCENDING: str = "ASC"
    DESCENDING: str = "DESC"

