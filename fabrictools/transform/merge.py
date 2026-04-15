"""Join a main DataFrame to a right side with prefixed, normalized column names."""

from __future__ import annotations

import ast
import inspect
import linecache
import textwrap
from typing import Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from fabrictools.quality.clean import _build_unique_column_names, _to_snake_case
from fabrictools.transform.columns import _resolve_column_name

DEFAULT_JOIN_PREFIX = "join"


def _is_merge_dataframes_call(node: ast.AST) -> bool:
    if not isinstance(node, ast.Call):
        return False
    func = node.func
    if isinstance(func, ast.Name):
        return func.id == "merge_dataframes"
    if isinstance(func, ast.Attribute):
        return func.attr == "merge_dataframes"
    return False


def _merge_join_arg_display_name(call: ast.Call) -> str | None:
    if len(call.args) >= 2:
        arg1 = call.args[1]
        if isinstance(arg1, ast.Name):
            return arg1.id
        if isinstance(arg1, ast.Attribute):
            return arg1.attr
        return None
    for kw in call.keywords:
        if kw.arg == "join_df":
            v = kw.value
            if isinstance(v, ast.Name):
                return v.id
            if isinstance(v, ast.Attribute):
                return v.attr
            return None
    return None


def _call_covers_lineno(node: ast.Call, lineno: int) -> bool:
    start = node.lineno
    end = getattr(node, "end_lineno", start)
    return start <= lineno <= end


def _extract_join_prefix_from_source(source: str, lineno: int) -> str | None:
    """Parse full cell/module source and infer join_df display name for the call at lineno."""
    block = textwrap.dedent(source).strip()
    if not block:
        return None
    try:
        tree = ast.parse(block)
    except SyntaxError:
        return None
    candidates: list[ast.Call] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not _is_merge_dataframes_call(node):
            continue
        if _call_covers_lineno(node, lineno):
            candidates.append(node)
    if not candidates:
        return None
    if len(candidates) > 1:

        def span(c: ast.Call) -> int:
            end = getattr(c, "end_lineno", c.lineno)
            return end - c.lineno

        candidates.sort(key=span)
    return _merge_join_arg_display_name(candidates[0])


def _logical_plan_children(plan) -> list:
    try:
        ch = plan.children()
    except Exception:
        return []
    if ch is None:
        return []
    out = []
    try:
        n = ch.size()
    except Exception:
        try:
            return list(ch)
        except Exception:
            return []
    for i in range(n):
        try:
            out.append(ch.apply(i))
        except Exception:
            try:
                out.append(ch.get(i))
            except Exception:
                try:
                    out.append(ch[i])
                except Exception:
                    pass
    return out


def _subquery_alias_from_logical_plan(plan) -> str | None:
    """First SubqueryAlias name in depth-first pre-order (matches e.g. ``df.alias('x')``)."""
    if plan is None:
        return None
    try:
        simple = plan.getClass().getSimpleName()
    except Exception:
        return None
    if simple == "SubqueryAlias":
        val = None
        try:
            val = plan.alias()
        except Exception:
            try:
                val = plan.name()
            except Exception:
                val = None
        if val is not None:
            if isinstance(val, str):
                s = val.strip()
            else:
                try:
                    s = str(val.name()).strip()
                except Exception:
                    s = str(val).strip()
            if s:
                return s
    for child in _logical_plan_children(plan):
        got = _subquery_alias_from_logical_plan(child)
        if got:
            return got
    return None


def _try_join_prefix_from_dataframe_alias(df: DataFrame) -> str | None:
    try:
        plan = df._jdf.queryExecution().analyzed()
    except Exception:
        return None
    return _subquery_alias_from_logical_plan(plan)


def _try_infer_join_prefix_from_call_site() -> str | None:
    frame = inspect.currentframe()
    outer = frame.f_back if frame else None
    if outer is None:
        return None
    filename = outer.f_code.co_filename
    lineno = outer.f_lineno

    def extract_from_block(block: str) -> str | None:
        block = textwrap.dedent(block).strip()
        if not block:
            return None
        try:
            tree = ast.parse(block)
        except SyntaxError:
            return None
        for node in ast.walk(tree):
            if not _is_merge_dataframes_call(node):
                continue
            return _merge_join_arg_display_name(node)
        return None

    info = inspect.getframeinfo(outer)
    if info.code_context:
        joined = "".join(info.code_context)
        got = extract_from_block(joined)
        if got:
            return got

    lines: list[str] = []
    for i in range(lineno, lineno + 40):
        line = linecache.getline(filename, i)
        if not line:
            break
        lines.append(line)
        got = extract_from_block("".join(lines))
        if got:
            return got

    try:
        full_source = inspect.getsource(outer.f_code)
    except (OSError, TypeError):
        full_source = None
    if full_source:
        got = _extract_join_prefix_from_source(full_source, outer.f_lineno)
        if got:
            return got

    return None


def merge_dataframes(
    main: DataFrame,
    join_df: DataFrame,
    join_columns: Sequence[str],
    keys: Sequence[tuple[str, str]],
    how: str = "left",
    *,
    join_prefix: str | None = None,
) -> DataFrame:
    """Left-join ``main`` to ``join_df`` and project right columns as ``{prefix}_{suffix}``.

    Prefix is snake_case from, in order: inferred ``join_df`` variable name at the
    call site, else first ``SubqueryAlias`` on ``join_df``'s analyzed plan, else
    ``join`` (see ``DEFAULT_JOIN_PREFIX``). Pass ``join_prefix`` to force a value. Suffixes match
    :py:func:`fabrictools.clean_data` uniqueness rules.

    :param main: Left dataframe.
    :param join_df: Right dataframe (only ``join_columns`` projected, plus key temps).
    :param join_columns: Right-side columns to expose with prefixed names.
    :param keys: ``(main_col, join_col)`` pairs for the join predicate (AND); names resolved per frame.
    :param how: Spark join type (e.g. ``left``, ``inner``).
    :param join_prefix: Optional explicit prefix (snake_cased); overrides inference.
    :type main: ~pyspark.sql.DataFrame
    :type join_df: ~pyspark.sql.DataFrame
    :type join_columns: collections.abc.Sequence[str]
    :type keys: collections.abc.Sequence[tuple[str, str]]
    :type how: str
    :type join_prefix: str | None

    :returns: Joined dataframe with temporary key columns dropped.
    :rtype: ~pyspark.sql.DataFrame

    :raises ValueError: If ``keys`` is empty.

    .. rubric:: Example

    >>> out = merge_dataframes(  # doctest: +SKIP
    ...     orders,
    ...     customers,
    ...     join_columns=["name", "segment"],
    ...     keys=[("customer_id", "id")],
    ...     join_prefix="cust",
    ... )
    """
    if not keys:
        raise ValueError("keys must contain at least one (main_key, join_key) pair")

    if join_prefix is not None:
        raw_prefix = join_prefix
    else:
        raw_prefix = _try_infer_join_prefix_from_call_site()
        if not raw_prefix:
            raw_prefix = _try_join_prefix_from_dataframe_alias(join_df)
        if not raw_prefix:
            raw_prefix = DEFAULT_JOIN_PREFIX
    prefix = _to_snake_case(raw_prefix)

    resolved_keys = [
        (
            _resolve_column_name(main, mk, side="main"),
            _resolve_column_name(join_df, jk, side="join_df"),
        )
        for mk, jk in keys
    ]

    temp_names = [f"_ft_join_k{i}" for i in range(len(keys))]
    exprs = []
    for i, (_mk_res, jk_res) in enumerate(resolved_keys):
        exprs.append(F.col(jk_res).alias(temp_names[i]))

    normalized_suffixes = _build_unique_column_names(
        [_to_snake_case(requested) for requested in join_columns]
    )
    for requested, suffix in zip(join_columns, normalized_suffixes):
        actual = _resolve_column_name(join_df, requested, side="join_df")
        exprs.append(F.col(actual).alias(f"{prefix}_{suffix}"))

    right_proj = join_df.select(*exprs)

    cond = None
    for i, (mk_res, _jk_res) in enumerate(resolved_keys):
        part = F.col(mk_res) == F.col(temp_names[i])
        cond = part if cond is None else (cond & part)

    out = main.join(right_proj, cond, how)
    for tn in temp_names:
        out = out.drop(tn)
    return out
