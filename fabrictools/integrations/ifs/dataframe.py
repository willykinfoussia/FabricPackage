"""Convert IFS JSON payloads to Spark DataFrames."""

from __future__ import annotations

import json
from typing import Any, Optional

from pyspark.sql import DataFrame, SparkSession  # type: ignore[reportMissingImports]
from pyspark.sql.types import (  # type: ignore[reportMissingImports]
    BooleanType,
    DataType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from fabrictools.core import get_spark
from fabrictools.integrations.ifs.errors import IFSError
from fabrictools.integrations.ifs.odata import extract_entity_rows


def parse_ifs_data(ifs_data: str) -> list[dict[str, Any]]:
    """Parse an IFS JSON string into a list of entity rows.

    Accepts either a JSON array of entities or an OData collection object
    with a ``value`` array (as returned by IFS OData endpoints).

    :param ifs_data: JSON string (e.g. Fabric pipeline activity output).
    :raises IFSError: When the string is not valid JSON or has an unsupported shape.
    """
    if not ifs_data or not ifs_data.strip():
        return []
    try:
        payload = json.loads(ifs_data)
    except json.JSONDecodeError as exc:
        raise IFSError(f"IFS data is not valid JSON: {ifs_data[:200]}") from exc

    if isinstance(payload, list):
        return _normalize_entity_rows(payload)
    if isinstance(payload, dict):
        if "value" in payload:
            return extract_entity_rows(payload)
        return [payload]
    raise IFSError(
        f"IFS data must be a JSON array or OData object, got {type(payload).__name__}"
    )


def ifs_data_to_dataframe(
    ifs_data: str,
    *,
    spark: Optional[SparkSession] = None,
) -> DataFrame:
    """Build a Spark DataFrame from an IFS JSON string.

    Handles columns that are entirely ``null``, which Spark cannot infer on its own.

    :param ifs_data: JSON string — array of entities or OData ``{"value": [...]}``.
    :param spark: Optional Spark session; active session is used when omitted.
    """
    rows = parse_ifs_data(ifs_data)
    _spark = spark or get_spark()
    return rows_to_dataframe(rows, spark=_spark)


def rows_to_dataframe(
    rows: list[dict[str, Any]],
    *,
    spark: Optional[SparkSession] = None,
) -> DataFrame:
    """Build a Spark DataFrame from IFS entity rows (list of dicts)."""
    _spark = spark or get_spark()
    if not rows:
        return _spark.createDataFrame([], schema=StructType([]))
    schema = _infer_schema_from_rows(rows)
    normalized_rows = _normalize_rows_to_schema(rows, schema)
    return _spark.createDataFrame(normalized_rows, schema=schema)


def _normalize_entity_rows(rows: list[Any]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for item in rows:
        if isinstance(item, dict):
            normalized.append(item)
    return normalized


def _spark_type_for_value(value: Any) -> Optional[DataType]:
    if value is None:
        return None
    if isinstance(value, bool):
        return BooleanType()
    if isinstance(value, int) and not isinstance(value, bool):
        return LongType()
    if isinstance(value, float):
        return DoubleType()
    if isinstance(value, str):
        return StringType()
    return StringType()


def _merge_spark_types(
    left: Optional[DataType],
    right: Optional[DataType],
) -> Optional[DataType]:
    if left is None:
        return right
    if right is None:
        return left
    if type(left) is type(right):
        return left
    if isinstance(left, (LongType, DoubleType)) and isinstance(right, (LongType, DoubleType)):
        return DoubleType()
    return StringType()


def _column_order(rows: list[dict[str, Any]]) -> list[str]:
    ordered = list(rows[0].keys())
    seen = set(ordered)
    for row in rows[1:]:
        for key in row.keys():
            if key not in seen:
                ordered.append(key)
                seen.add(key)
    return ordered


def _normalize_value_for_type(value: Any, data_type: DataType) -> Any:
    if value is None:
        return None
    if isinstance(data_type, DoubleType) and isinstance(value, int) and not isinstance(
        value, bool
    ):
        return float(value)
    return value


def _normalize_rows_to_schema(
    rows: list[dict[str, Any]],
    schema: StructType,
) -> list[dict[str, Any]]:
    type_by_field = {field.name: field.dataType for field in schema.fields}
    return [
        {
            key: _normalize_value_for_type(
                value, type_by_field.get(key, StringType())
            )
            for key, value in row.items()
        }
        for row in rows
    ]


def _infer_schema_from_rows(rows: list[dict[str, Any]]) -> StructType:
    column_types: dict[str, Optional[DataType]] = {}
    for row in rows:
        for key, value in row.items():
            value_type = _spark_type_for_value(value)
            if key not in column_types:
                column_types[key] = value_type
            else:
                column_types[key] = _merge_spark_types(column_types[key], value_type)

    fields = [
        StructField(key, column_types[key] or StringType(), True)
        for key in _column_order(rows)
    ]
    return StructType(fields)
