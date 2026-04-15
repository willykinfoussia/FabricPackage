"""Column name resolution (physical vs clean_data-style normalized) and helpers."""

from __future__ import annotations

import calendar
import re
import unicodedata
from collections.abc import Callable, Collection, Sequence
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


def resolve_dataframe_column(df: DataFrame, name: str) -> str:
    """Resolve ``name`` to the physical column name on ``df``.

    Accepts the physical name, a :py:func:`fabrictools.clean_data`-style
    normalized label, or snake_case (same rules as :py:func:`fabrictools.merge_dataframes` / :py:func:`fabrictools.remove_columns`).

    :param df: Dataframe whose schema is searched.
    :param name: Logical, normalized, or physical column label.
    :type df: ~pyspark.sql.DataFrame
    :type name: str

    :returns: Physical column name present on ``df``.
    :rtype: str

    :raises ValueError: If ``name`` cannot be resolved.

    .. rubric:: Example

    >>> physical = resolve_dataframe_column(df, "Customer ID")  # doctest: +SKIP
    """
    return _resolve_column_name(df, name, side="DataFrame")


def rename_columns_normalized(df: DataFrame) -> DataFrame:
    """Rename every column to snake_case with ``_2``, ``_3``, … disambiguation.

    Uses the same name scheme as the rename step in
    :py:func:`fabrictools.clean_data`. Does not cast types, replace blanks,
    deduplicate rows, or drop rows.

    :param df: Input dataframe.
    :type df: ~pyspark.sql.DataFrame

    :returns: Dataframe with updated column names where needed.
    :rtype: ~pyspark.sql.DataFrame

    .. rubric:: Example

    >>> renamed = rename_columns_normalized(messy_cols_df)  # doctest: +SKIP
    """
    cols = list(df.columns)
    normalized = _build_unique_column_names(cols)
    if normalized == cols:
        return df
    return df.toDF(*normalized)


def remove_columns(df: DataFrame, *columns: str) -> DataFrame:
    """Drop columns by physical name or by the same resolution rules as :py:func:`fabrictools.merge_dataframes`.

    :param df: Input dataframe.
    :param columns: One or more labels; duplicates resolving to the same physical column are dropped once.
    :type df: ~pyspark.sql.DataFrame
    :type columns: str

    :returns: ``df`` without the resolved columns.
    :rtype: ~pyspark.sql.DataFrame

    :raises ValueError: If no names are passed or a name cannot be resolved.

    .. rubric:: Example

    >>> slim = remove_columns(df, "temp_flag", "raw_json_blob")  # doctest: +SKIP
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
    """Rename columns whose names embed a Power Query / Excel day serial (epoch ``PQ_EPOCH``).

    Non-matching columns are unchanged. Target collisions get ``_2``, ``_3``, … suffixes.

    :param df: Input dataframe.
    :param date_format: ``strftime`` format for the date portion of new names.
    :param prefix: Text prepended before the formatted date.
    :param include_suffix_in_name: If ``True``, append parsed numeric suffix after the serial segment.
    :type df: ~pyspark.sql.DataFrame
    :type date_format: str
    :type prefix: str
    :type include_suffix_in_name: bool

    :returns: Dataframe with renamed columns.
    :rtype: ~pyspark.sql.DataFrame

    .. rubric:: Example

    >>> dated = rename_columns_pq_serial_to_dates(  # doctest: +SKIP
    ...     pq_wide_df, date_format="%Y-%m-%d", prefix="d_"
    ... )
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
    """Like :py:func:`rename_columns_pq_serial_to_dates` but labels use French *mois année* (e.g. ``janvier_2024``).

    :param df: Input dataframe.
    :param prefix: Prepended before the month-year token.
    :param include_suffix_in_name: Append ``_{suffix}`` when a numeric suffix follows the serial in the source name.
    :param capitalize_month: If ``True``, capitalize the month word (e.g. ``Janvier_2024``).
    :type df: ~pyspark.sql.DataFrame
    :type prefix: str
    :type include_suffix_in_name: bool
    :type capitalize_month: bool

    :returns: Renamed dataframe.
    :rtype: ~pyspark.sql.DataFrame

    .. rubric:: Example

    >>> labeled = rename_columns_pq_serial_to_mois_annee(  # doctest: +SKIP
    ...     pq_wide_df, prefix="m_", capitalize_month=True
    ... )
    """
    return _rename_columns_pq_serial_common(
        df,
        prefix=prefix,
        include_suffix_in_name=include_suffix_in_name,
        label_for_date=lambda d: _format_mois_annee_fr(
            d, capitalize_month=capitalize_month
        ),
    )


# Default block labels for ``rename_columns_month_year_block_labels`` (projection-style).
_DEFAULT_MONTH_BLOCK_LABELS: tuple[str, ...] = (
    "Coûts prévisionnels (par mois)",
    "Coûts prévisionnels cumulés",
    "Avancement prévisionnel",
    "CA prévisionnel cumulé",
    "CA Monthly",
)

# French month words (lowercase) for parsing column names; same order as ``_FR_MONTHS[1:]``.
_FR_MONTH_WORDS: tuple[str, ...] = _FR_MONTHS[1:]

_MONTH_YEAR_HEAD = re.compile(r"^([A-Za-zÀ-ÿà-ÿ]+)[\s_]+(\d{4})\s*$")
_TRAILING_INDEX_TAIL = re.compile(r"^(.*?_\d+)_\d+$")


def _strip_accents(s: str) -> str:
    nk = unicodedata.normalize("NFKD", s)
    return "".join(c for c in nk if not unicodedata.combining(c))


def _strip_trailing_index(name: str) -> str:
    s = name.strip()
    pos = s.rfind(" (")
    if pos != -1 and s.endswith(")"):
        inside = s[pos + 2 : -1].strip()
        try:
            n = float(inside)
            if n == int(n):
                return s[:pos].strip()
        except ValueError:
            pass
    m = _TRAILING_INDEX_TAIL.match(s)
    if m:
        return m.group(1)
    return s


def _add_months(d: date, months: int) -> date:
    month = d.month - 1 + months
    year = d.year + month // 12
    month = month % 12 + 1
    last = calendar.monthrange(year, month)[1]
    day = min(d.day, last)
    return date(year, month, day)


def _try_parse_month_year(col_name: str) -> Optional[date]:
    """Strip trailing index then parse French month + 4-digit year (space or underscore)."""
    base = _strip_trailing_index(col_name)
    m = _MONTH_YEAR_HEAD.match(base.strip())
    if not m:
        return None
    month_word, year_s = m.group(1), m.group(2)
    key = _strip_accents(month_word).lower()
    month_idx: Optional[int] = None
    for i, fr in enumerate(_FR_MONTH_WORDS, start=1):
        if _strip_accents(fr).lower() == key:
            month_idx = i
            break
    if month_idx is None:
        return None
    try:
        y = int(year_s)
    except ValueError:
        return None
    return date(y, month_idx, 1)


def _rename_pairs_blocks(
    cols: list[str], labels: Sequence[str]
) -> list[tuple[str, str]]:
    block = 0
    in_block = False
    prev_month: Optional[date] = None
    pairs: list[tuple[str, str]] = []
    n_labels = len(labels)
    for col in cols:
        m = _try_parse_month_year(col)
        is_my = m is not None
        if in_block and prev_month is not None and is_my:
            continues = m == prev_month or m == _add_months(prev_month, 1)
        else:
            continues = False
        start_new_block = is_my and ((not in_block) or (not continues))
        if is_my:
            new_block = block + 1 if start_new_block else block
        else:
            new_block = block
        if is_my:
            if start_new_block or prev_month is None:
                new_prev = m
            elif m == _add_months(prev_month, 1):
                new_prev = m
            else:
                new_prev = prev_month
        else:
            new_prev = None
        label: Optional[str] = None
        if is_my and 1 <= new_block <= n_labels:
            label = labels[new_block - 1]
        new_name = f"{_strip_trailing_index(col)} [{label}]" if label else col
        if label is not None:
            pairs.append((col, new_name))
        block = new_block
        in_block = is_my
        prev_month = new_prev
    return pairs


def _unique_target_names(pairs: list[tuple[str, str]]) -> list[tuple[str, str]]:
    seen: dict[str, int] = {}
    out: list[tuple[str, str]] = []
    for old, new in pairs:
        n = seen.get(new, 0) + 1
        seen[new] = n
        if n > 1:
            new = f"{new}__{n}"
        out.append((old, new))
    return out


def _allocate_unique_rename_targets_against_schema(
    pairs: list[tuple[str, str]], df_column_names: list[str]
) -> list[tuple[str, str]]:
    sources = {old for old, _ in pairs}
    taken: set[str] = {c for c in df_column_names if c not in sources}
    out: list[tuple[str, str]] = []
    for old, proposed in pairs:
        final = _allocate_unique_column_name(proposed, taken)
        out.append((old, final))
    return out


def month_start_from_ca_monthly_col(col_name: str) -> Optional[date]:
    """Parse first-of-month from a column name: French *mois année* head, optional `` [label]`` suffix stripped.

    :param col_name: Wide column name (e.g. ``janvier_2024 [CA Monthly]``).
    :type col_name: str

    :returns: Parsed month start, or ``None`` if parsing fails.
    :rtype: datetime.date | None

    .. rubric:: Example

    >>> d0 = month_start_from_ca_monthly_col("janvier_2024 [CA Monthly]")  # doctest: +SKIP
    """
    base = col_name.split(" [", 1)[0] if " [" in col_name else col_name
    return _try_parse_month_year(base)


def rename_columns_month_year_block_labels(
    df: DataFrame,
    *,
    labels: Sequence[str] = _DEFAULT_MONTH_BLOCK_LABELS,
    exclude_columns: Collection[str] = ("__spark_row_order__",),
) -> DataFrame:
    """Rename contiguous French *mois année* column blocks using ordered ``labels`` (projection-style).

    Order follows ``df.columns`` after ``exclude_columns``. Rename targets disambiguate with
    ``__2``, ``__3``, … among new names, then ``_2``, ``_3``, … against the rest of the schema.

    :param df: Input wide dataframe.
    :param labels: Block markers in column order (defaults to built-in forecast / CA block set).
    :param exclude_columns: Column names ignored when scanning contiguous runs.
    :type df: ~pyspark.sql.DataFrame
    :type labels: collections.abc.Sequence[str]
    :type exclude_columns: collections.abc.Collection[str]

    :returns: Dataframe with renamed month columns.
    :rtype: ~pyspark.sql.DataFrame

    .. rubric:: Example

    >>> tagged = rename_columns_month_year_block_labels(  # doctest: +SKIP
    ...     wide_projection_df, labels=("Block A", "Block B")
    ... )
    """
    exclude = set(exclude_columns)
    cols = [c for c in df.columns if c not in exclude]
    pairs = _rename_pairs_blocks(cols, labels)
    pairs = _unique_target_names(pairs)
    pairs = _allocate_unique_rename_targets_against_schema(pairs, list(df.columns))
    out = df
    for old, new in pairs:
        if old != new:
            out = out.withColumnRenamed(old, new)
    return out
