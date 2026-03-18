# fabrictools

> User-friendly PySpark helpers for **Microsoft Fabric** — read, write, and merge Lakehouses and Warehouses with a single function call.

---

## Features

- **Auto-resolved paths** — pass a Lakehouse or Warehouse *name*, no ABFS URL configuration required
- **Auto-detected SparkSession** — uses `SparkSession.builder.getOrCreate()`, works seamlessly inside Fabric notebooks
- **Auto-detected format** on read — tries Delta → Parquet → CSV automatically
- **Delta merge (upsert)** — one-liner upsert into any Lakehouse Delta table
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
