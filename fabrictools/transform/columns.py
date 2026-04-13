"""Column name resolution (physical vs clean_data-style normalized) and helpers."""

from __future__ import annotations

import re
from collections.abc import Callable
from datetime import date, timedelta
from typing import Optional

from pyspark.sql import DataFrame

from fabrictools.quality.clean import _build_unique_column_names, _to_snake_case

# Excel / Power Query-style serial day 0 (Date.From in M uses the same origin as Excel).
PQ_EPOCH = date(1899, 12, 30)


def _pq_date_from_serial(n: int) -> Optional[date]:
    try:
        return PQ_EPOCH + timedelta(days=int(n))
    except (OverflowError, OSError, ValueError):
        return None


def _digit_count(s: str) -> int:
    return sum(1 for c in s if c.isdigit())


# Token: 4+ digits preceded by start or underscore, followed by underscore or end.
_PQ_SERIAL_TOKEN = re.compile(r"(?:^|_)(\d{4,})(?=_|$)")
# Underscore-led integer runs in the tail after a matched serial.
_PQ_TAIL_INT_SEGMENTS = re.compile(r"_(\d+)")


def _parse_pq_serial_column_name(name: str) -> tuple[Optional[date], Optional[int]]:
    """
    Parse a column name for a Power-Query-style day serial and optional suffix.

    Finds underscore-bounded digit tokens (4+ digits) left to right; the first whose
    integer serial yields a valid date from ``_pq_date_from_serial`` wins.
    The suffix is the last ``_digits`` segment after that token (e.g. ``45678_1``,
    ``col_45678_12``). Implemented with ``re`` (no ``str.split`` on ``_``).
    """
    if _digit_count(name) < 4:
        return None, None
    for m in _PQ_SERIAL_TOKEN.finditer(name):
        try:
            n = int(m.group(1))
        except ValueError:
            continue
        parsed_date = _pq_date_from_serial(n)
        if parsed_date is None:
            continue
        tail = name[m.end() :]
        suffixes = [int(x) for x in _PQ_TAIL_INT_SEGMENTS.findall(tail)]
        suffix = suffixes[-1] if suffixes else None
        return parsed_date, suffix
    return None, None


def _allocate_unique_column_name(base: str, taken: set[str]) -> str:
    if base not in taken:
        taken.add(base)
        return base
    k = 2
    while True:
        candidate = f"{base}_{k}"
        if candidate not in taken:
            taken.add(candidate)
            return candidate
        k += 1


# French month names for "mois année" column labels (index 1 = janvier).
_FR_MONTHS = (
    "",
    "janvier",
    "février",
    "mars",
    "avril",
    "mai",
    "juin",
    "juillet",
    "août",
    "septembre",
    "octobre",
    "novembre",
    "décembre",
)


def _format_mois_annee_fr(d: date, *, capitalize_month: bool = False) -> str:
    mois = _FR_MONTHS[d.month]
    if capitalize_month:
        mois = mois.capitalize()
    return f"{mois}_{d.year}"


def _rename_columns_pq_serial_common(
    df: DataFrame,
    *,
    prefix: str,
    include_suffix_in_name: bool,
    label_for_date: Callable[[date], str],
) -> DataFrame:
    cols = list(df.columns)
    kept: set[str] = set()
    to_rename: list[tuple[str, str]] = []
    for col in cols:
        parsed_date, suffix = _parse_pq_serial_column_name(col)
        if parsed_date is None:
            kept.add(col)
            continue
        body = f"{prefix}{label_for_date(parsed_date)}"
        if include_suffix_in_name and suffix is not None:
            body = f"{body}_{suffix}"
        to_rename.append((col, body))

    taken = set(kept)
    allocations: dict[str, str] = {}
    for old, proposed in to_rename:
        allocations[old] = _allocate_unique_column_name(proposed, taken)

    out = df
    for old, new in allocations.items():
        if old != new:
            out = out.withColumnRenamed(old, new)
    return out


def _resolve_column_name(df: DataFrame, name: str, *, side: str = "DataFrame") -> str:
    cols = [f.name for f in df.schema.fields]
    if name in cols:
        return name
    norm_list = _build_unique_column_names(cols)
    if name in norm_list:
        return cols[norm_list.index(name)]
    candidate = _to_snake_case(name)
    if candidate in norm_list:
        return cols[norm_list.index(candidate)]
    raise ValueError(
        f"{side} DataFrame has no column {name!r} "
        f"(not a physical name nor a name normalized like clean_data)"
    )


def remove_columns(df: DataFrame, *columns: str) -> DataFrame:
    """
    Drop columns by physical name or by the same resolution rules as ``merge_dataframes`` /
    ``clean_data`` (snake_case + unique suffixes).

    Parameters
    ----------
    df
        Input DataFrame.
    *columns
        One or more column labels to remove. Duplicate requests that resolve to the same
        physical column are dropped once.

    Raises
    ------
    ValueError
        If no column names are passed, or if a name cannot be resolved.
    """
    if not columns:
        raise ValueError("remove_columns requires at least one column name")
    resolved: list[str] = []
    seen: set[str] = set()
    for name in columns:
        actual = _resolve_column_name(df, name, side="DataFrame")
        if actual not in seen:
            seen.add(actual)
            resolved.append(actual)
    return df.drop(*resolved)


def rename_columns_pq_serial_to_dates(
    df: DataFrame,
    *,
    date_format: str = "%Y-%m-%d",
    prefix: str = "",
    include_suffix_in_name: bool = True,
) -> DataFrame:
    """
    Rename columns whose names embed a Power Query / Excel day serial (see ``PQ_EPOCH``).

    Non-matching columns keep their names. Target names that collide with an
    existing name or another rename get ``_2``, ``_3``, … appended.

    Parameters
    ----------
    df
        Input DataFrame.
    date_format
        ``strftime`` format for the date portion of the new name.
    prefix
        Prepended before the formatted date.
    include_suffix_in_name
        If True and a numeric suffix was parsed after the serial segment, append
        ``_{suffix}`` to the new column name.
    """
    return _rename_columns_pq_serial_common(
        df,
        prefix=prefix,
        include_suffix_in_name=include_suffix_in_name,
        label_for_date=lambda d: d.strftime(date_format),
    )


def rename_columns_pq_serial_to_mois_annee(
    df: DataFrame,
    *,
    prefix: str = "",
    include_suffix_in_name: bool = True,
    capitalize_month: bool = True,
) -> DataFrame:
    """
    Same as ``rename_columns_pq_serial_to_dates`` but the date part is French
    *mois année* (e.g. ``janvier_2024``), suitable for column names (underscore
    between month and year).

    Parameters
    ----------
    df
        Input DataFrame.
    prefix
        Prepended before the ``mois_annee`` label.
    include_suffix_in_name
        If True and a numeric suffix was parsed after the serial segment, append
        ``_{suffix}`` to the new column name.
    capitalize_month
        If True, capitalize the month (e.g. ``Janvier_2024``).
    """
    return _rename_columns_pq_serial_common(
        df,
        prefix=prefix,
        include_suffix_in_name=include_suffix_in_name,
        label_for_date=lambda d: _format_mois_annee_fr(
            d, capitalize_month=capitalize_month
        ),
    )
