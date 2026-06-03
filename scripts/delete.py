"""Delete today's rows from a Delta Lakehouse table with fabrictools.

Run this script inside a Fabric notebook/session where Spark is available.
Update the configuration constants below before execution.
"""

from __future__ import annotations

from datetime import date

import fabrictools as ft

# ---------------------------------------------------------------------------
# Configuration (adapt these values to your Lakehouse/table paths)
# ---------------------------------------------------------------------------

LAKEHOUSE_NAME = "TODO_LAKEHOUSE_NAME"
PATH_SUIVI = "TODO_TABLE_PATH"
DATE_COLUMN = None  # None = first column of the table
OVERWRITE_TODAY = True


def _sql_date_equals(column: str, day: date) -> str:
    """Build a Delta delete predicate for rows whose date column equals ``day``."""
    escaped = column.replace("`", "``")
    return f"to_date(`{escaped}`) = '{day.isoformat()}'"


def delete_today_rows() -> None:
    """Remove rows dated today from the target Delta table when enabled."""
    if not OVERWRITE_TODAY:
        print("OVERWRITE_TODAY=False — no rows deleted.")
        return

    current_day = date.today()

    if DATE_COLUMN is None:
        df = ft.read_lakehouse(LAKEHOUSE_NAME, PATH_SUIVI)
        if not df.columns:
            raise ValueError(f"No columns found in '{PATH_SUIVI}'.")
        date_column = df.columns[0]
    else:
        date_column = DATE_COLUMN

    condition = _sql_date_equals(date_column, current_day)
    ft.delete_lakehouse(
        lakehouse_name=LAKEHOUSE_NAME,
        relative_path=PATH_SUIVI,
        condition=condition,
    )
    print(
        f"Deleted rows from '{PATH_SUIVI}' where "
        f"{date_column} = {current_day.isoformat()}"
    )


if __name__ == "__main__":
    delete_today_rows()
