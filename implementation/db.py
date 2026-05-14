from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any


class ValidationError(Exception):
    """Raised when a request cannot be safely executed."""


class SQLiteAdapter:
    SUPPORTED_OPERATORS = {
        "eq": "=",
        "ne": "!=",
        "gt": ">",
        "gte": ">=",
        "lt": "<",
        "lte": "<=",
        "like": "LIKE",
        "in": "IN",
    }
    SUPPORTED_METRICS = {"count", "avg", "sum", "min", "max"}

    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path).expanduser().resolve()

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def list_tables(self) -> list[str]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT name
                FROM sqlite_master
                WHERE type = 'table'
                  AND name NOT LIKE 'sqlite_%'
                ORDER BY name
                """
            ).fetchall()
        return [row["name"] for row in rows]

    def get_table_schema(self, table: str) -> list[dict[str, Any]]:
        table = self._validate_table(table)
        with self.connect() as conn:
            rows = conn.execute(f"PRAGMA table_info({self._quote_identifier(table)})").fetchall()
        return [
            {
                "name": row["name"],
                "type": row["type"],
                "nullable": not bool(row["notnull"]),
                "default": row["dflt_value"],
                "primary_key": bool(row["pk"]),
            }
            for row in rows
        ]

    def search(
        self,
        table: str,
        columns: list[str] | None = None,
        filters: Any | None = None,
        limit: int = 20,
        offset: int = 0,
        order_by: str | None = None,
        descending: bool = False,
    ) -> list[dict[str, Any]]:
        table = self._validate_table(table)
        table_columns = self._column_names(table)
        selected_columns = self._validate_columns(table, columns) if columns else table_columns
        limit = self._validate_limit(limit)
        offset = self._validate_offset(offset)

        select_sql = ", ".join(self._quote_identifier(column) for column in selected_columns)
        where_sql, params = self._build_where_clause(table, filters)
        sql = f"SELECT {select_sql} FROM {self._quote_identifier(table)}{where_sql}"

        if order_by is not None:
            order_by = self._validate_column(table, order_by)
            direction = "DESC" if bool(descending) else "ASC"
            sql += f" ORDER BY {self._quote_identifier(order_by)} {direction}"

        sql += " LIMIT ? OFFSET ?"
        params.extend([limit, offset])

        with self.connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [dict(row) for row in rows]

    def insert(self, table: str, values: dict[str, Any]) -> dict[str, Any]:
        table = self._validate_table(table)
        if not isinstance(values, dict) or not values:
            raise ValidationError("insert values must be a non-empty object")

        columns = self._validate_columns(table, list(values.keys()))
        placeholders = ", ".join("?" for _ in columns)
        column_sql = ", ".join(self._quote_identifier(column) for column in columns)
        sql = f"INSERT INTO {self._quote_identifier(table)} ({column_sql}) VALUES ({placeholders})"

        with self.connect() as conn:
            try:
                cursor = conn.execute(sql, [values[column] for column in columns])
                conn.commit()
            except sqlite3.IntegrityError as exc:
                raise ValidationError(f"insert violates database constraints: {exc}") from exc
            inserted = dict(values)
            primary_key = self._single_primary_key(table)
            if primary_key and primary_key not in inserted:
                inserted[primary_key] = cursor.lastrowid
            if primary_key:
                row = conn.execute(
                    f"""
                    SELECT *
                    FROM {self._quote_identifier(table)}
                    WHERE {self._quote_identifier(primary_key)} = ?
                    """,
                    [inserted[primary_key]],
                ).fetchone()
                if row:
                    return dict(row)

        return inserted

    def aggregate(
        self,
        table: str,
        metric: str,
        column: str | None = None,
        filters: Any | None = None,
        group_by: str | None = None,
    ) -> list[dict[str, Any]]:
        table = self._validate_table(table)
        metric = self._validate_metric(metric)

        if metric == "count":
            aggregate_expr = "COUNT(*)"
        else:
            if column is None:
                raise ValidationError(f"aggregate metric '{metric}' requires a column")
            column = self._validate_column(table, column)
            aggregate_expr = f"{metric.upper()}({self._quote_identifier(column)})"

        select_parts: list[str] = []
        if group_by is not None:
            group_by = self._validate_column(table, group_by)
            select_parts.append(self._quote_identifier(group_by))
        select_parts.append(f"{aggregate_expr} AS value")

        where_sql, params = self._build_where_clause(table, filters)
        sql = f"SELECT {', '.join(select_parts)} FROM {self._quote_identifier(table)}{where_sql}"
        if group_by is not None:
            sql += f" GROUP BY {self._quote_identifier(group_by)} ORDER BY {self._quote_identifier(group_by)} ASC"

        with self.connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [dict(row) for row in rows]

    def database_schema(self) -> dict[str, Any]:
        return {
            "tables": [
                {"name": table, "columns": self.get_table_schema(table)}
                for table in self.list_tables()
            ]
        }

    def _build_where_clause(self, table: str, filters: Any | None) -> tuple[str, list[Any]]:
        normalized = self._normalize_filters(filters)
        if not normalized:
            return "", []

        clauses: list[str] = []
        params: list[Any] = []
        for filter_item in normalized:
            column = self._validate_column(table, filter_item["column"])
            op_key = filter_item["op"]
            if op_key not in self.SUPPORTED_OPERATORS:
                raise ValidationError(f"unsupported filter operator '{op_key}'")
            value = filter_item["value"]
            sql_operator = self.SUPPORTED_OPERATORS[op_key]

            if op_key == "in":
                if not isinstance(value, (list, tuple)) or not value:
                    raise ValidationError("'in' filter requires a non-empty list of values")
                placeholders = ", ".join("?" for _ in value)
                clauses.append(f"{self._quote_identifier(column)} IN ({placeholders})")
                params.extend(value)
            else:
                clauses.append(f"{self._quote_identifier(column)} {sql_operator} ?")
                params.append(value)

        return " WHERE " + " AND ".join(clauses), params

    def _normalize_filters(self, filters: Any | None) -> list[dict[str, Any]]:
        if filters in (None, {}, []):
            return []
        if isinstance(filters, list):
            normalized = []
            for item in filters:
                if not isinstance(item, dict):
                    raise ValidationError("each filter must be an object")
                if not {"column", "op", "value"}.issubset(item):
                    raise ValidationError("filters must include column, op, and value")
                normalized.append({"column": item["column"], "op": item["op"], "value": item["value"]})
            return normalized
        if isinstance(filters, dict):
            normalized = []
            for column, condition in filters.items():
                if isinstance(condition, dict):
                    if len(condition) != 1:
                        raise ValidationError("filter condition objects must contain exactly one operator")
                    op, value = next(iter(condition.items()))
                else:
                    op, value = "eq", condition
                normalized.append({"column": column, "op": op, "value": value})
            return normalized
        raise ValidationError("filters must be an object or a list of filter objects")

    def _validate_table(self, table: str) -> str:
        if not isinstance(table, str) or not table:
            raise ValidationError("table must be a non-empty string")
        tables = self.list_tables()
        if table not in tables:
            raise ValidationError(f"unknown table '{table}'")
        return table

    def _validate_columns(self, table: str, columns: list[str]) -> list[str]:
        if not isinstance(columns, list) or not columns:
            raise ValidationError("columns must be a non-empty list")
        return [self._validate_column(table, column) for column in columns]

    def _validate_column(self, table: str, column: str) -> str:
        if not isinstance(column, str) or not column:
            raise ValidationError("column must be a non-empty string")
        columns = self._column_names(table)
        if column not in columns:
            raise ValidationError(f"unknown column '{column}' for table '{table}'")
        return column

    def _validate_metric(self, metric: str) -> str:
        if not isinstance(metric, str):
            raise ValidationError("metric must be a string")
        metric = metric.lower()
        if metric not in self.SUPPORTED_METRICS:
            raise ValidationError(f"unsupported aggregate metric '{metric}'")
        return metric

    def _validate_limit(self, limit: int) -> int:
        try:
            value = int(limit)
        except (TypeError, ValueError) as exc:
            raise ValidationError("limit must be an integer") from exc
        if value < 1 or value > 100:
            raise ValidationError("limit must be between 1 and 100")
        return value

    def _validate_offset(self, offset: int) -> int:
        try:
            value = int(offset)
        except (TypeError, ValueError) as exc:
            raise ValidationError("offset must be an integer") from exc
        if value < 0:
            raise ValidationError("offset must be greater than or equal to 0")
        return value

    def _column_names(self, table: str) -> list[str]:
        return [column["name"] for column in self.get_table_schema(table)]

    def _single_primary_key(self, table: str) -> str | None:
        primary_keys = [column["name"] for column in self.get_table_schema(table) if column["primary_key"]]
        return primary_keys[0] if len(primary_keys) == 1 else None

    def _quote_identifier(self, identifier: str) -> str:
        return '"' + identifier.replace('"', '""') + '"'
