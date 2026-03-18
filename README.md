# fabrictools

> User-friendly PySpark helpers for **Microsoft Fabric** — read, write, and merge Lakehouses and Warehouses with a single function call.

---

## Features

- **Auto-resolved paths** — pass a Lakehouse or Warehouse *name*, no ABFS URL configuration required
- **Auto-detected SparkSession** — uses `SparkSession.builder.getOrCreate()`, works seamlessly inside Fabric notebooks
- **Auto-detected format** on read — tries Delta → Parquet → CSV automatically
- **Auto-corrected Lakehouse read paths** — supports bare or partial paths (e.g. `customers`, `dbo/customers`) with fallback to `Tables/dbo/...` then `Files/...`
- **Delta merge (upsert)** — one-liner upsert into any Lakehouse Delta table
- **Generic data cleaning** — standard cleaning with one helper function
- **Silver metadata enrichment** — add ingestion/source metadata + `year/month/day` partitions
- **Data quality scan** — detect nulls, blank strings, duplicates, and naming collisions
- **Built-in logging** — every operation logs its resolved path, detected format, and row/column count

---

## Requirements

- Microsoft Fabric Spark runtime (provides `notebookutils`, `pyspark`, and `delta-spark`)
- Python >= 3.9

> **Local development:** install the `spark` extras to get PySpark and delta-spark.
> `notebookutils` is only available inside Fabric — functions that resolve paths will raise a clear `ValueError` outside Fabric.

---

## Installation

```bash
# Inside a Fabric notebook or pipeline
pip install fabrictools

# Local development (includes PySpark + delta-spark)
pip install "fabrictools[spark]"
```

---

## Quick start

```python
import fabrictools as ft
```

### Read a Lakehouse dataset

```python
# Auto-detects Delta → Parquet → CSV
df = ft.read_lakehouse("BronzeLakehouse", "sales/2024")

# Also accepts partial paths:
# - "customers"      -> tries Tables/dbo/customers then Files/customers
# - "dbo/customers"  -> tries Tables/dbo/customers
```

### Write to a Lakehouse

```python
ft.write_lakehouse(
    df,
    lakehouse_name="SilverLakehouse",
    relative_path="sales_clean",
    mode="overwrite",
    partition_by=["year", "month"],   # optional
)
```

### Merge (upsert) into a Delta table

```python
ft.merge_lakehouse(
    source_df=new_df,
    lakehouse_name="SilverLakehouse",
    relative_path="sales_clean",
    merge_condition="src.id = tgt.id",
    # update_set and insert_set are optional:
    # omit them to update/insert all columns automatically
)
```

### Clean data (generic)

```python
clean_df = ft.clean_data(df)
```

By default it:
- normalizes columns to unique `snake_case`
- trims string values
- converts blank strings to `null`
- removes exact duplicates
- drops rows where all fields are `null`

### Scan data quality issues

```python
scan_output = ft.scan_data_errors(df, include_samples=True)

# Spark DataFrame with all scan information (dataset metrics, per-column details, collisions)
scan_output["summary_df"].show(truncate=False)

# Plotly figure (auto bar/pie depending on issue categories)
scan_output["figure"].show()

# Optional helpers
print(scan_output["issue_totals"])
print(scan_output["collisions"])
```

`scan_data_errors` now returns a user-friendly bundle with a tabular summary DataFrame and a Plotly chart.

### Add Silver metadata + date partitions

```python
silver_df = ft.add_silver_metadata(
    df,
    source_lakehouse_name="RawLakehouse",
    source_relative_path="sales/raw",
    source_layer="bronze",  # optional
)
```

By default this adds:
- `ingestion_timestamp`
- `source_layer`
- `source_path` (resolved candidate path actually used for source read)
- `year`, `month`, `day`

### Read -> clean -> write in one call

```python
clean_df = ft.clean_and_write_data(
    source_lakehouse_name="RawLakehouse",
    source_relative_path="sales/raw",
    target_lakehouse_name="CuratedLakehouse",
    target_relative_path="sales/clean",
    mode="overwrite",
    partition_by=["year"],  # optional override
)
```

When `partition_by` is omitted, the helper writes with default partitions:
`["year", "month", "day"]`.

With explicit column mappings:

```python
ft.merge_lakehouse(
    source_df=new_df,
    lakehouse_name="SilverLakehouse",
    relative_path="sales_clean",
    merge_condition="src.id = tgt.id",
    update_set={"amount": "src.amount", "updated_at": "src.updated_at"},
    insert_set={"id": "src.id", "amount": "src.amount", "updated_at": "src.updated_at"},
)
```

### Read from a Warehouse

```python
df = ft.read_warehouse("MyWarehouse", "SELECT * FROM dbo.sales WHERE year = 2024")
```

### Write to a Warehouse

```python
ft.write_warehouse(
    df,
    warehouse_name="MyWarehouse",
    table="dbo.sales_clean",
    mode="overwrite",       # or "append"
    batch_size=10_000,      # optional, default 10 000
)
```

---

## API reference

### Lakehouse

| Function | Description |
|---|---|
| `read_lakehouse(lakehouse_name, relative_path, spark=None)` | Read a dataset — auto-detects Delta / Parquet / CSV |
| `write_lakehouse(df, lakehouse_name, relative_path, mode, partition_by, format, spark=None)` | Write a DataFrame (default: Delta, overwrite) |
| `merge_lakehouse(source_df, lakehouse_name, relative_path, merge_condition, update_set, insert_set, spark=None)` | Upsert via Delta merge |
| `clean_data(df, drop_duplicates, drop_all_null_rows)` | Apply standard generic cleaning to a DataFrame |
| `add_silver_metadata(df, source_lakehouse_name, source_relative_path, source_layer, ingestion_timestamp_col, source_layer_col, source_path_col, year_col, month_col, day_col, spark=None)` | Add Silver metadata and date partition columns, with resolved source path |
| `scan_data_errors(df, include_samples)` | Report common data-quality issues |
| `clean_and_write_data(source_lakehouse_name, source_relative_path, target_lakehouse_name, target_relative_path, mode, partition_by, spark=None)` | Read, clean, add Silver metadata, and write in one helper |

### Warehouse

| Function | Description |
|---|---|
| `read_warehouse(warehouse_name, query, spark=None)` | Run a SQL query, return a DataFrame |
| `write_warehouse(df, warehouse_name, table, mode, batch_size, spark=None)` | Write to a Warehouse table via JDBC |

---

## How path resolution works

```
lakehouse_name="BronzeLakehouse"
       │
       ▼
notebookutils.lakehouse.get("BronzeLakehouse")
       │
       ▼
lh.properties.abfsPath
= "abfss://bronze@<account>.dfs.core.windows.net"
       │
       ▼
full_path = abfsPath + "/" + relative_path
```

---

## Running the tests

```bash
pip install "fabrictools[dev]"
pytest
```

---

## Publishing to PyPI

See [docs/PYPI_PUBLISH.md](docs/PYPI_PUBLISH.md) for a step-by-step guide.

---

## License

MIT
