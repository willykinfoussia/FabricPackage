"""Power Query ``Table.*`` functions on Spark DataFrames."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any, Union

from pyspark import StorageLevel
from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from fabrictools.transform.columns import cast_columns, remove_columns
from fabrictools.transform.pivot import build_tcd

from fabrictools.powerquery._common import (
    Order,
    agg_output_expr,
    pq_type_to_spark,
    resolve_column,
    resolve_columns,
)

__all__ = ["Table"]

_ColumnExpr = Union[Column, Callable[[], Column]]


def _as_column_expr(generator: _ColumnExpr) -> Column:
    if callable(generator):
        return generator()
    return generator


class Table:
    """Namespace for Power Query ``Table.*`` functions on Spark DataFrames.

    Column names are resolved like :py:func:`fabrictools.resolve_dataframe_column`
    (physical, normalized, or snake_case labels).

    .. rubric:: Example

    >>> from fabrictools import read_lakehouse, Table, List  # doctest: +SKIP
    >>> df = read_lakehouse("Lakehouse", "dbo/customer_projects")  # doctest: +SKIP
    >>> df = Table.Group(df, ["RAO CODE"], [("Amount", "AMOUNT CNY", List.Sum)])  # doctest: +SKIP
    """

    @staticmethod
    def Group(
        df: DataFrame,
        keys: Union[str, Sequence[str], set],
        aggregations: Sequence[tuple[str, str, str]],
    ) -> DataFrame:
        """Group rows and aggregate columns (Power Query ``Table.Group``).

        :param df: Input dataframe.
        :param keys: Group key column name(s).
        :param aggregations: ``(output_name, source_column, List.Sum|Max|...)`` tuples.
        :type df: ~pyspark.sql.DataFrame
        :type keys: str | collections.abc.Sequence[str] | set
        :type aggregations: collections.abc.Sequence[tuple[str, str, str]]

        :returns: Grouped and aggregated dataframe.
        :rtype: ~pyspark.sql.DataFrame

        :raises ValueError: If no key column resolves on ``df``.

        .. rubric:: Example

        >>> from fabrictools import Table, List  # doctest: +SKIP
        >>> df = Table.Group(df, {"RAO CODE"}, [  # doctest: +SKIP
        ...     ("Client", "END USER", List.Max),
        ...     ("Amount (Adjusted)", "OI ADJUSTED", List.Sum),
        ... ])
        """
        if isinstance(keys, str):
            key_list = [keys]
        else:
            key_list = list(keys)

        resolved_keys = resolve_columns(df, key_list)
        if not resolved_keys:
            raise ValueError("Table.Group requires at least one key column that resolves on the dataframe")

        agg_exprs: list[Column] = []
        for output_name, source_name, strategy in aggregations:
            source_col = resolve_column(df, source_name)
            if source_col is None:
                continue
            agg_exprs.append(agg_output_expr(output_name, source_col, strategy))

        if not agg_exprs:
            return df.select(*resolved_keys).dropDuplicates(resolved_keys)

        return df.groupBy(*resolved_keys).agg(*agg_exprs)

    @staticmethod
    def SelectRows(
        df: DataFrame,
        predicate: Column | None = None,
        *,
        not_null: Sequence[str] | None = None,
        any_not_null: Sequence[str] | None = None,
    ) -> DataFrame:
        """Filter rows (Power Query ``Table.SelectRows``).

        Pass a Spark boolean ``predicate``, or use ``not_null`` (AND) / ``any_not_null`` (OR)
        shorthands for common ``[col] <> null`` patterns.

        :param df: Input dataframe.
        :param predicate: Optional Spark boolean column expression.
        :param not_null: Keep rows where all listed columns are non-null.
        :param any_not_null: Keep rows where at least one listed column is non-null.
        :type df: ~pyspark.sql.DataFrame
        :type predicate: ~pyspark.sql.Column | None
        :type not_null: collections.abc.Sequence[str] | None
        :type any_not_null: collections.abc.Sequence[str] | None

        :returns: Filtered dataframe.
        :rtype: ~pyspark.sql.DataFrame

        .. rubric:: Example

        >>> from fabrictools import Table  # doctest: +SKIP
        >>> df = Table.SelectRows(df, not_null=["Project Number"])  # doctest: +SKIP
        >>> df = Table.SelectRows(df, any_not_null=["BUYER", "RAO CODE"])  # doctest: +SKIP
        """
        if predicate is not None:
            return df.filter(predicate)

        if not_null:
            condition = None
            for name in not_null:
                col = resolve_column(df, name)
                if col is None:
                    continue
                part = F.col(col).isNotNull()
                condition = part if condition is None else (condition & part)
            if condition is not None:
                return df.filter(condition)

        if any_not_null:
            condition = None
            for name in any_not_null:
                col = resolve_column(df, name)
                if col is None:
                    continue
                part = F.col(col).isNotNull()
                condition = part if condition is None else (condition | part)
            if condition is not None:
                return df.filter(condition)

        return df

    @staticmethod
    def AddColumn(
        df: DataFrame,
        name: str,
        generator: _ColumnExpr,
    ) -> DataFrame:
        """Add a computed column (Power Query ``Table.AddColumn``).

        :param df: Input dataframe.
        :param name: Name of the new column.
        :param generator: Spark ``Column`` expression or zero-argument callable returning one.
        :type df: ~pyspark.sql.DataFrame
        :type name: str
        :type generator: ~pyspark.sql.Column | collections.abc.Callable[[], ~pyspark.sql.Column]

        :returns: Dataframe with the added column.
        :rtype: ~pyspark.sql.DataFrame

        .. rubric:: Example

        >>> from fabrictools import Table, Date  # doctest: +SKIP
        >>> from pyspark.sql import functions as F  # doctest: +SKIP
        >>> df = Table.AddColumn(df, "Year", Date.Year(F.col("Date of Revenue recognition")))  # doctest: +SKIP
        """
        return df.withColumn(name, _as_column_expr(generator))

    @staticmethod
    def Sort(
        df: DataFrame,
        sort_order: Sequence[tuple[str, bool]],
    ) -> DataFrame:
        """Sort rows (Power Query ``Table.Sort``).

        :param df: Input dataframe.
        :param sort_order: Sequence of ``(column, Order.Ascending|Order.Descending)`` pairs.
        :type df: ~pyspark.sql.DataFrame
        :type sort_order: collections.abc.Sequence[tuple[str, bool]]

        :returns: Sorted dataframe.
        :rtype: ~pyspark.sql.DataFrame

        .. rubric:: Example

        >>> from fabrictools import Table, Order  # doctest: +SKIP
        >>> df = Table.Sort(df, [("Year", Order.Ascending), ("Date", Order.Ascending)])  # doctest: +SKIP
        """
        cols: list[str] = []
        ascending: list[bool] = []
        for col_name, is_asc in sort_order:
            resolved = resolve_column(df, col_name)
            if resolved is None:
                continue
            cols.append(resolved)
            ascending.append(is_asc)
        if not cols:
            return df
        return df.orderBy(*[F.col(c).asc() if asc else F.col(c).desc() for c, asc in zip(cols, ascending)])

    @staticmethod
    def SelectColumns(df: DataFrame, columns: Sequence[str]) -> DataFrame:
        """Keep only the listed columns in order (Power Query ``Table.SelectColumns``).

        :param df: Input dataframe.
        :param columns: Column names to keep, in desired order.
        :type df: ~pyspark.sql.DataFrame
        :type columns: collections.abc.Sequence[str]

        :returns: Projected dataframe.
        :rtype: ~pyspark.sql.DataFrame

        .. rubric:: Example

        >>> from fabrictools import Table  # doctest: +SKIP
        >>> df = Table.SelectColumns(df, ["Date", "Year", "RAO CODE", "Client"])  # doctest: +SKIP
        """
        resolved = resolve_columns(df, columns)
        if not resolved:
            return df
        return df.select(*resolved)

    @staticmethod
    def ReorderColumns(df: DataFrame, columns: Sequence[str]) -> DataFrame:
        """Move columns to the front; keep unlisted columns at the end (Power Query ``Table.ReorderColumns``).

        :param df: Input dataframe.
        :param columns: Column names to move to the front, in order.
        :type df: ~pyspark.sql.DataFrame
        :type columns: collections.abc.Sequence[str]

        :returns: Reordered dataframe.
        :rtype: ~pyspark.sql.DataFrame

        .. rubric:: Example

        >>> from fabrictools import Table  # doctest: +SKIP
        >>> df = Table.ReorderColumns(df, ["Year", "Project No.", "Turnover"])  # doctest: +SKIP
        """
        resolved = resolve_columns(df, columns)
        if not resolved:
            return df
        remaining = [c for c in df.columns if c not in resolved]
        return df.select(*resolved, *remaining)

    @staticmethod
    def RenameColumns(
        df: DataFrame,
        renames: Union[Sequence[tuple[str, str]], dict[str, str]],
    ) -> DataFrame:
        """Rename columns (Power Query ``Table.RenameColumns``).

        :param df: Input dataframe.
        :param renames: ``(old_name, new_name)`` pairs or mapping dict.
        :type df: ~pyspark.sql.DataFrame
        :type renames: collections.abc.Sequence[tuple[str, str]] | dict[str, str]

        :returns: Dataframe with renamed columns.
        :rtype: ~pyspark.sql.DataFrame

        .. rubric:: Example

        >>> from fabrictools import Table  # doctest: +SKIP
        >>> df = Table.RenameColumns(df, [  # doctest: +SKIP
        ...     ("Amount (Adjusted)", "Total Contract (Adjusted)"),
        ...     ("Amount", "Total Contract"),
        ... ])
        """
        pairs: list[tuple[str, str]]
        if isinstance(renames, dict):
            pairs = list(renames.items())
        else:
            pairs = list(renames)

        out = df
        for old, new in pairs:
            actual = resolve_column(out, old)
            if actual is not None and actual != new:
                out = out.withColumnRenamed(actual, new)
        return out

    @staticmethod
    def RemoveColumns(df: DataFrame, columns: Sequence[str]) -> DataFrame:
        """Remove columns (Power Query ``Table.RemoveColumns``).

        Delegates to :py:func:`fabrictools.remove_columns`.

        :param df: Input dataframe.
        :param columns: Column names to drop.
        :type df: ~pyspark.sql.DataFrame
        :type columns: collections.abc.Sequence[str]

        :returns: Dataframe without the listed columns.
        :rtype: ~pyspark.sql.DataFrame

        .. rubric:: Example

        >>> from fabrictools import Table  # doctest: +SKIP
        >>> df = Table.RemoveColumns(df, ["Backlog 2024", "Turnover 2021"])  # doctest: +SKIP
        """
        return remove_columns(df, columns=list(columns))

    @staticmethod
    def ReplaceValue(
        df: DataFrame,
        old_value: Any,
        new_value: Any,
        columns: Sequence[str],
    ) -> DataFrame:
        """Replace values in columns (Power Query ``Table.ReplaceValue``).

        :param df: Input dataframe.
        :param old_value: Value to replace; use ``None`` to match null cells.
        :param new_value: Replacement value.
        :param columns: Columns to update.
        :type df: ~pyspark.sql.DataFrame
        :type old_value: Any
        :type new_value: Any
        :type columns: collections.abc.Sequence[str]

        :returns: Dataframe with replaced values.
        :rtype: ~pyspark.sql.DataFrame

        .. rubric:: Example

        >>> from fabrictools import Table  # doctest: +SKIP
        >>> df = Table.ReplaceValue(df, None, "External", ["Interco"])  # doctest: +SKIP
        """
        out = df
        for name in columns:
            col = resolve_column(out, name)
            if col is None:
                continue
            if old_value is None:
                out = out.withColumn(
                    col,
                    F.when(F.col(col).isNull(), F.lit(new_value)).otherwise(F.col(col)),
                )
            else:
                out = out.withColumn(
                    col,
                    F.when(F.col(col) == F.lit(old_value), F.lit(new_value)).otherwise(F.col(col)),
                )
        return out

    @staticmethod
    def TransformColumnTypes(
        df: DataFrame,
        type_map: dict[str, Any],
    ) -> DataFrame:
        """Cast column types (Power Query ``Table.TransformColumnTypes``).

        Accepts Power Query type tokens (``type.text``, ``Percentage.Type``, ``Int64.Type``)
        and delegates to :py:func:`fabrictools.cast_columns`.

        :param df: Input dataframe.
        :param type_map: Mapping ``{column_name: pq_type}``.
        :type df: ~pyspark.sql.DataFrame
        :type type_map: dict[str, Any]

        :returns: Dataframe with cast columns.
        :rtype: ~pyspark.sql.DataFrame

        .. rubric:: Example

        >>> from fabrictools import Table, Percentage  # doctest: +SKIP
        >>> df = Table.TransformColumnTypes(df, {"% Completion": Percentage.Type})  # doctest: +SKIP
        """
        spark_map = {name: pq_type_to_spark(t) for name, t in type_map.items()}
        return cast_columns(df, spark_map)

    @staticmethod
    def TransformColumns(
        df: DataFrame,
        transforms: Sequence[tuple[str, Callable[[Column], Column]]],
    ) -> DataFrame:
        """Apply per-column transformers (Power Query ``Table.TransformColumns``).

        :param df: Input dataframe.
        :param transforms: ``(column_name, transformer)`` pairs; ``transformer`` receives
            a ``Column`` and returns a new ``Column``.
        :type df: ~pyspark.sql.DataFrame
        :type transforms: collections.abc.Sequence[tuple[str, collections.abc.Callable]]

        :returns: Transformed dataframe.
        :rtype: ~pyspark.sql.DataFrame

        .. rubric:: Example

        >>> from fabrictools import Table, Number  # doctest: +SKIP
        >>> df = Table.TransformColumns(df, [  # doctest: +SKIP
        ...     ("Total Invoice amount without VAT", Number.FromText),
        ... ])
        """
        out = df
        for col_name, transformer in transforms:
            resolved = resolve_column(out, col_name)
            if resolved is None:
                continue
            out = out.withColumn(resolved, transformer(F.col(resolved)))
        return out

    @staticmethod
    def Distinct(
        df: DataFrame,
        columns: Sequence[str] | None = None,
    ) -> DataFrame:
        """Remove duplicate rows (Power Query ``Table.Distinct``).

        :param df: Input dataframe.
        :param columns: Optional subset of columns to test for duplication; all columns if omitted.
        :type df: ~pyspark.sql.DataFrame
        :type columns: collections.abc.Sequence[str] | None

        :returns: Deduplicated dataframe.
        :rtype: ~pyspark.sql.DataFrame

        .. rubric:: Example

        >>> from fabrictools import Table  # doctest: +SKIP
        >>> df = Table.Distinct(df, ["RAO CODE", "END USER"])  # doctest: +SKIP
        """
        if columns:
            subset = resolve_columns(df, columns)
            return df.dropDuplicates(subset) if subset else df.dropDuplicates()
        return df.dropDuplicates()

    @staticmethod
    def Combine(tables: Sequence[DataFrame]) -> DataFrame:
        """Union tables by column name (Power Query ``Table.Combine``).

        :param tables: Dataframes to stack vertically.
        :type tables: collections.abc.Sequence[~pyspark.sql.DataFrame]

        :returns: Combined dataframe.
        :rtype: ~pyspark.sql.DataFrame

        :raises ValueError: If ``tables`` is empty.

        .. rubric:: Example

        >>> from fabrictools import Table  # doctest: +SKIP
        >>> combined = Table.Combine([df_2024, df_2025])  # doctest: +SKIP
        """
        if not tables:
            raise ValueError("Table.Combine requires at least one table")
        out = tables[0]
        for other in tables[1:]:
            out = out.unionByName(other, allowMissingColumns=True)
        return out

    @staticmethod
    def NestedJoin(
        left: DataFrame,
        right: DataFrame,
        join_keys: Sequence[str],
        *,
        how: str = "left",
        left_keys: Sequence[str] | None = None,
        right_keys: Sequence[str] | None = None,
    ) -> DataFrame:
        """Join two tables (Power Query ``Table.NestedJoin``).

        :param left: Left dataframe.
        :param right: Right dataframe.
        :param join_keys: Column names used on both sides when ``left_keys`` / ``right_keys`` omitted.
        :param how: Spark join type (``left``, ``inner``, ``right``, ``outer``).
        :param left_keys: Optional left-side key column names.
        :param right_keys: Optional right-side key column names.
        :type left: ~pyspark.sql.DataFrame
        :type right: ~pyspark.sql.DataFrame
        :type join_keys: collections.abc.Sequence[str]
        :type how: str
        :type left_keys: collections.abc.Sequence[str] | None
        :type right_keys: collections.abc.Sequence[str] | None

        :returns: Joined dataframe.
        :rtype: ~pyspark.sql.DataFrame

        .. rubric:: Example

        >>> from fabrictools import Table  # doctest: +SKIP
        >>> df = Table.NestedJoin(orders, customers, ["customer_id"], how="left")  # doctest: +SKIP
        """
        keys = list(join_keys)
        lk = list(left_keys) if left_keys else keys
        rk = list(right_keys) if right_keys else keys
        resolved_lk = resolve_columns(left, lk)
        resolved_rk = resolve_columns(right, rk)
        if not resolved_lk or not resolved_rk or len(resolved_lk) != len(resolved_rk):
            return left
        condition: Column | None = None
        for lcol, rcol in zip(resolved_lk, resolved_rk):
            part = left[lcol] == right[rcol]
            condition = part if condition is None else (condition & part)
        return left.join(right, condition, how)

    @staticmethod
    def Join(
        left: DataFrame,
        right: DataFrame,
        join_keys: Sequence[str],
        *,
        how: str = "left",
    ) -> DataFrame:
        """Simple join alias (Power Query ``Table.Join``).

        :param left: Left dataframe.
        :param right: Right dataframe.
        :param join_keys: Key column names (same on both sides).
        :param how: Spark join type.
        :type left: ~pyspark.sql.DataFrame
        :type right: ~pyspark.sql.DataFrame
        :type join_keys: collections.abc.Sequence[str]
        :type how: str

        :returns: Joined dataframe.
        :rtype: ~pyspark.sql.DataFrame

        .. rubric:: Example

        >>> from fabrictools import Table  # doctest: +SKIP
        >>> df = Table.Join(orders, products, ["sku"], how="inner")  # doctest: +SKIP
        """
        return Table.NestedJoin(left, right, join_keys, how=how)

    @staticmethod
    def FillDown(df: DataFrame, columns: Sequence[str]) -> DataFrame:
        """Propagate last non-null value downward (Power Query ``Table.FillDown``).

        :param df: Input dataframe.
        :param columns: Columns to fill.
        :type df: ~pyspark.sql.DataFrame
        :type columns: collections.abc.Sequence[str]

        :returns: Dataframe with nulls filled from above.
        :rtype: ~pyspark.sql.DataFrame

        .. rubric:: Example

        >>> from fabrictools import Table  # doctest: +SKIP
        >>> df = Table.FillDown(df, ["Category", "Subcategory"])  # doctest: +SKIP
        """
        resolved = resolve_columns(df, columns)
        if not resolved:
            return df
        out = df
        for col in resolved:
            w = Window.orderBy(F.monotonically_increasing_id()).rowsBetween(
                Window.unboundedPreceding, Window.currentRow
            )
            out = out.withColumn(col, F.last(F.col(col), ignorenulls=True).over(w))
        return out

    @staticmethod
    def FillUp(df: DataFrame, columns: Sequence[str]) -> DataFrame:
        """Propagate next non-null value upward (Power Query ``Table.FillUp``).

        :param df: Input dataframe.
        :param columns: Columns to fill.
        :type df: ~pyspark.sql.DataFrame
        :type columns: collections.abc.Sequence[str]

        :returns: Dataframe with nulls filled from below.
        :rtype: ~pyspark.sql.DataFrame

        .. rubric:: Example

        >>> from fabrictools import Table  # doctest: +SKIP
        >>> df = Table.FillUp(df, ["Category"])  # doctest: +SKIP
        """
        resolved = resolve_columns(df, columns)
        if not resolved:
            return df
        out = df
        for col in resolved:
            w = Window.orderBy(F.monotonically_increasing_id()).rowsBetween(
                Window.currentRow, Window.unboundedFollowing
            )
            out = out.withColumn(col, F.first(F.col(col), ignorenulls=True).over(w))
        return out

    @staticmethod
    def FirstN(df: DataFrame, count: int) -> DataFrame:
        """Return first N rows (Power Query ``Table.FirstN``).

        :param df: Input dataframe.
        :param count: Number of rows to keep.
        :type df: ~pyspark.sql.DataFrame
        :type count: int

        :returns: First ``count`` rows.
        :rtype: ~pyspark.sql.DataFrame

        .. rubric:: Example

        >>> from fabrictools import Table  # doctest: +SKIP
        >>> preview = Table.FirstN(df, 100)  # doctest: +SKIP
        """
        return df.limit(int(count))

    @staticmethod
    def Skip(df: DataFrame, count: int) -> DataFrame:
        """Skip first N rows (Power Query ``Table.Skip``).

        :param df: Input dataframe.
        :param count: Number of rows to skip from the top.
        :type df: ~pyspark.sql.DataFrame
        :type count: int

        :returns: Dataframe without the first ``count`` rows.
        :rtype: ~pyspark.sql.DataFrame

        .. rubric:: Example

        >>> from fabrictools import Table  # doctest: +SKIP
        >>> df = Table.Skip(raw_df, 9)  # doctest: +SKIP
        """
        n = int(count)
        if n <= 0:
            return df
        return df.offset(n)

    @staticmethod
    def LastN(df: DataFrame, count: int) -> DataFrame:
        """Return last N rows (Power Query ``Table.LastN``).

        :param df: Input dataframe.
        :param count: Number of rows to keep from the end.
        :type df: ~pyspark.sql.DataFrame
        :type count: int

        :returns: Last ``count`` rows.
        :rtype: ~pyspark.sql.DataFrame

        .. rubric:: Example

        >>> from fabrictools import Table  # doctest: +SKIP
        >>> tail = Table.LastN(df, 50)  # doctest: +SKIP
        """
        n = int(count)
        if n <= 0:
            return df
        w = Window.orderBy(F.monotonically_increasing_id().desc())
        ranked = df.withColumn("__pq_rn__", F.row_number().over(w))
        return ranked.filter(F.col("__pq_rn__") <= n).drop("__pq_rn__")

    @staticmethod
    def Range(df: DataFrame, offset: int, count: int) -> DataFrame:
        """Return a slice of rows (Power Query ``Table.Range``).

        :param df: Input dataframe.
        :param offset: Number of rows to skip.
        :param count: Number of rows to return after the offset.
        :type df: ~pyspark.sql.DataFrame
        :type offset: int
        :type count: int

        :returns: Row slice ``[offset, offset + count)``.
        :rtype: ~pyspark.sql.DataFrame

        .. rubric:: Example

        >>> from fabrictools import Table  # doctest: +SKIP
        >>> page = Table.Range(df, offset=20, count=10)  # doctest: +SKIP
        """
        return df.offset(int(offset)).limit(int(count))

    @staticmethod
    def DuplicateColumn(df: DataFrame, column: str, new_column: str) -> DataFrame:
        """Duplicate a column (Power Query ``Table.DuplicateColumn``).

        :param df: Input dataframe.
        :param column: Source column name.
        :param new_column: Name of the copy.
        :type df: ~pyspark.sql.DataFrame
        :type column: str
        :type new_column: str

        :returns: Dataframe with the duplicated column.
        :rtype: ~pyspark.sql.DataFrame

        .. rubric:: Example

        >>> from fabrictools import Table  # doctest: +SKIP
        >>> df = Table.DuplicateColumn(df, "Project No.", "Project No. backup")  # doctest: +SKIP
        """
        resolved = resolve_column(df, column)
        if resolved is None:
            return df
        return df.withColumn(new_column, F.col(resolved))

    @staticmethod
    def SplitColumn(
        df: DataFrame,
        column: str,
        delimiter: str,
        new_column_names: Sequence[str],
    ) -> DataFrame:
        """Split a column by delimiter (Power Query ``Table.SplitColumn``).

        :param df: Input dataframe.
        :param column: Column to split.
        :param delimiter: Split delimiter (regex-escaped by Spark ``split``).
        :param new_column_names: Names for each resulting part (by index).
        :type df: ~pyspark.sql.DataFrame
        :type column: str
        :type delimiter: str
        :type new_column_names: collections.abc.Sequence[str]

        :returns: Dataframe with split columns added.
        :rtype: ~pyspark.sql.DataFrame

        .. rubric:: Example

        >>> from fabrictools import Table  # doctest: +SKIP
        >>> df = Table.SplitColumn(df, "full_name", " ", ["first", "last"])  # doctest: +SKIP
        """
        resolved = resolve_column(df, column)
        if resolved is None:
            return df
        split_col = F.split(F.col(resolved), delimiter)
        out = df
        for i, new_name in enumerate(new_column_names):
            out = out.withColumn(new_name, split_col.getItem(i))
        return out

    @staticmethod
    def Pivot(
        df: DataFrame,
        group_columns: Sequence[str],
        pivot_column: str,
        value_column: str,
        *,
        agg: str = "sum",
    ) -> DataFrame:
        """Pivot a table (Power Query ``Table.Pivot``).

        Wraps :py:func:`fabrictools.build_tcd`.

        :param df: Input dataframe.
        :param group_columns: Row grouping columns.
        :param pivot_column: Column whose distinct values become new columns.
        :param value_column: Column to aggregate.
        :param agg: Aggregation name (``sum``, ``max``, ``avg``, …).
        :type df: ~pyspark.sql.DataFrame
        :type group_columns: collections.abc.Sequence[str]
        :type pivot_column: str
        :type value_column: str
        :type agg: str

        :returns: Pivoted dataframe.
        :rtype: ~pyspark.sql.DataFrame

        .. rubric:: Example

        >>> from fabrictools import Table  # doctest: +SKIP
        >>> wide = Table.Pivot(df, ["Region"], "Year", "Sales", agg="sum")  # doctest: +SKIP
        """
        values = {value_column: agg}
        return build_tcd(df, rows=list(group_columns), columns=pivot_column, values=values)

    @staticmethod
    def Unpivot(
        df: DataFrame,
        id_columns: Sequence[str],
        value_columns: Sequence[str],
        attribute_column: str = "Attribute",
        value_column: str = "Value",
    ) -> DataFrame:
        """Unpivot columns to rows (Power Query ``Table.Unpivot``).

        :param df: Input dataframe.
        :param id_columns: Identifier columns to keep fixed.
        :param value_columns: Wide columns to melt into rows.
        :param attribute_column: Name of the column holding former column names.
        :param value_column: Name of the column holding cell values.
        :type df: ~pyspark.sql.DataFrame
        :type id_columns: collections.abc.Sequence[str]
        :type value_columns: collections.abc.Sequence[str]
        :type attribute_column: str
        :type value_column: str

        :returns: Long-format dataframe.
        :rtype: ~pyspark.sql.DataFrame

        .. rubric:: Example

        >>> from fabrictools import Table  # doctest: +SKIP
        >>> long_df = Table.Unpivot(  # doctest: +SKIP
        ...     df, ["id"], ["Turnover 2023", "Turnover 2024"],
        ...     attribute_column="Year", value_column="Turnover",
        ... )
        """
        ids = resolve_columns(df, id_columns)
        vals = resolve_columns(df, value_columns)
        if not vals:
            return df
        parts: list[str] = []
        for v in vals:
            parts.append(f"'{v}'")
            parts.append(f"`{v}`")
        n = len(vals)
        stack_expr = ", ".join(parts)
        return df.select(
            *ids,
            F.expr(f"stack({n}, {stack_expr}) as ({attribute_column}, {value_column})"),
        )

    @staticmethod
    def ReplaceErrorValues(
        df: DataFrame,
        columns: Sequence[str],
        replacement: Any,
    ) -> DataFrame:
        """Replace null values after failed transforms (Power Query ``Table.ReplaceErrorValues``).

        :param df: Input dataframe.
        :param columns: Columns to scan for nulls.
        :param replacement: Value to substitute when null.
        :type df: ~pyspark.sql.DataFrame
        :type columns: collections.abc.Sequence[str]
        :type replacement: Any

        :returns: Dataframe with nulls replaced.
        :rtype: ~pyspark.sql.DataFrame

        .. rubric:: Example

        >>> from fabrictools import Table  # doctest: +SKIP
        >>> df = Table.ReplaceErrorValues(df, ["amount"], 0)  # doctest: +SKIP
        """
        out = df
        for name in columns:
            col = resolve_column(out, name)
            if col is None:
                continue
            out = out.withColumn(
                col,
                F.when(F.col(col).isNull(), F.lit(replacement)).otherwise(F.col(col)),
            )
        return out

    @staticmethod
    def Buffer(df: DataFrame) -> DataFrame:
        """Cache a table in memory and disk (Power Query ``Table.Buffer``).

        :param df: Input dataframe.
        :type df: ~pyspark.sql.DataFrame

        :returns: Persisted dataframe (same object, cached for reuse).
        :rtype: ~pyspark.sql.DataFrame

        .. rubric:: Example

        >>> from fabrictools import Table  # doctest: +SKIP
        >>> cached = Table.Buffer(df)  # doctest: +SKIP
        """
        return df.persist(StorageLevel.MEMORY_AND_DISK)
