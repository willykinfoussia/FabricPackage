# fabrictools

> User-friendly PySpark helpers for **Microsoft Fabric** — read, write, and merge Lakehouses and Warehouses with a single function call.

---

## Features

- **Auto-resolved paths** — pass a Lakehouse or Warehouse *name*, no ABFS URL configuration required
- **Auto-detected SparkSession** — uses `SparkSession.builder.getOrCreate()`, works seamlessly inside Fabric notebooks
- **Auto-detected format** on read — tries Delta → Parquet → CSV automatically
- **Auto-corrected Lakehouse read paths** — supports bare or partial paths (e.g. `customers`, `dbo/customers`) with fallback to `Tables/dbo/...` then `Files/...`
- **Auto-corrected Lakehouse write paths** — partial paths are normalized to `Tables/dbo/...` while explicit `Files/...` paths are preserved
- **Delta merge (upsert)** — one-liner upsert into any Lakehouse Delta table
- **Generic data cleaning** — standard cleaning with one helper function
- **Silver metadata enrichment** — add ingestion/source metadata + `year/month/day` partitions
- **Data quality scan** — detect nulls, blank strings, duplicates, and naming collisions
- **Bulk lakehouse cleaning** — iterate all tables and copy cleaned outputs to another Lakehouse
- **Source → Prepared pipeline** — profile schema, resolve semantics, transform, optimize, and publish BI assets
- **Dimension generators** — build `dimension_date`, `dimension_country`, and `dimension_city` with Lakehouse/Warehouse writes
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

# Write path normalization:
# - "sales_clean"      -> Tables/dbo/sales_clean
# - "dbo/sales_clean"  -> Tables/dbo/sales_clean
# - "Tables/sales_clean" -> Tables/dbo/sales_clean
# - "Files/archive/sales_clean" stays unchanged
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

# The function displays the summary DataFrame and chart automatically by default.
# You can disable rendering with display_results=False.
scan_output = ft.scan_data_errors(df, include_samples=True, display_results=False)

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

### Bulk clean all Lakehouse tables

```python
bulk_result = ft.clean_and_write_all_tables(
    source_lakehouse_name="RawLakehouse",
    target_lakehouse_name="CuratedLakehouse",
    mode="overwrite",
    include_schemas=["dbo", "sales"],   # optional
    exclude_tables=["dbo.audit_log"],   # optional: accepts "table" or "schema.table"
    continue_on_error=True,             # optional
)

print(bulk_result["successful_tables"], bulk_result["failed_tables"])
```

The helper scans `Tables/<schema>/<table>` in the source Lakehouse and writes to
the same relative path in the target Lakehouse.

### Source -> Prepared pipeline (independent steps)

Each phase can be called independently, or orchestrated by helpers.

```python
# 1) Discovery
schema_hash = ft.snapshot_source_schema(
    source_lakehouse_name="CleanLakehouse",
    source_relative_path="Tables/dbo/orders_clean",
)

# 2) Resolver (3 layers + unresolved audit)
source_df = ft.read_lakehouse("CleanLakehouse", "Tables/dbo/orders_clean")
mappings = ft.resolve_columns(
    df=source_df,
    source_lakehouse_name="CleanLakehouse",
    schema_hash=schema_hash,
    sample_size=500,
    profiling_confidence_threshold=0.80,
)

# 3) Transformation
prepared_df = ft.transform_to_prepared(
    df=source_df,
    resolved_mappings=mappings,
    source_lakehouse_name="CleanLakehouse",
)

# 4) Write + optimization
ft.write_prepared_table(
    df=prepared_df,
    resolved_mappings=mappings,
    target_lakehouse_name="BusinessReadyLakehouse",
    target_relative_path="Tables/dbo/orders_prepared",
    mode="overwrite",
)

# 5) Optional BI outputs
agg_tables = ft.generate_prepared_aggregations(
    source_lakehouse_name="CleanLakehouse",
    target_lakehouse_name="BusinessReadyLakehouse",
    target_relative_path="Tables/dbo/orders_prepared",
    resolved_mappings=mappings,
)
```

Config tables for this pipeline are stored in the **source lakehouse** under `Tables/config/`:
- `source_schema_snapshot`
- `prefix_rules`
- `profiling_cache`
- `mapping_rules`
- `code_labels`
- `pipeline_audit_log`

Orchestration helpers:

```python
# Single table
prepared_df = ft.prepare_and_write_data(
    source_lakehouse_name="CleanLakehouse",
    source_relative_path="Tables/dbo/orders_clean",
    target_lakehouse_name="BusinessReadyLakehouse",
    target_relative_path="Tables/dbo/orders_prepared",
    mode="overwrite",
)

# Bulk mode (same pattern as clean_and_write_all_tables)
result = ft.prepare_and_write_all_tables(
    source_lakehouse_name="CleanLakehouse",
    target_lakehouse_name="BusinessReadyLakehouse",
    mode="overwrite",
    include_schemas=["dbo"],
    continue_on_error=True,
)
print(result["successful_tables"], result["failed_tables"])
```

You can also drive the orchestration with an explicit `TABLES_CONFIG`:

```python
TABLES_CONFIG = [
    {
        "bronze_path": "Tables/dbo/fact_sale",
        "silver_table": "Tables/dbo/fact_sale",
        "partition_by": [],
        "mode": "overwrite",  # overwrite | append | merge
    },
    {
        "bronze_path": "Tables/dbo/dimension_customer",
        "silver_table": "Tables/dbo/dimension_customer",
        "partition_by": [],
        "mode": "append",
    },
    {
        "bronze_path": "Tables/dbo/fact_sale_updates",
        "silver_table": "Tables/dbo/fact_sale",
        "partition_by": [],
        "mode": "merge",
        "merge_condition": "src.sale_id = tgt.sale_id",  # required when mode='merge'
    },
]

bulk_result = ft.clean_and_write_all_tables(
    source_lakehouse_name="RawLakehouse",
    target_lakehouse_name="CuratedLakehouse",
    tables_config=TABLES_CONFIG,
    continue_on_error=True,
)
```

`tables_config` keys:
- `bronze_path` (required): source relative path in Lakehouse.
- `silver_table` (required): target relative path in Lakehouse.
- `partition_by` (optional): partition columns used by overwrite/append writes.
- `mode` (required): `overwrite`, `append`, or `merge`.
- `merge_condition` (required when `mode="merge"`): join condition used by Delta merge.

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

### Generate dimension tables

```python
dims = ft.generate_dimensions(
    lakehouse_name="CuratedLakehouse",
    warehouse_name="MyWarehouse",
    include_date=True,
    include_country=True,
    include_city=True,
    # Optional range for dimension_date; defaults to rolling -10y / +2y
    start_date="2015-01-01",
    end_date="2030-12-31",
    # Optional fiscal year start month for dimension_date (1=Jan ... 12=Dec)
    fiscal_year_start_month=1,
    # Optional controls for countrystatecity-countries source
    countries_limit=None,
    include_states_metadata=True,
    fail_on_source_error=True,
    # Optional city filters (narrow down which cities are generated)
    city_regions=["Europe"],                   # filter by region
    city_subregions=["Western Europe"],        # filter by subregion
    city_countries=["FR", "DEU", "Belgium"],   # filter by code2/code3/name
)

# Access generated DataFrames
dims["dimension_date"].show(5)
dims["dimension_country"].show(5)
dims["dimension_city"].show(5)
```

`dimension_city` now includes country attributes: `country_code_3`, `country_key`, `region`, and `subregion`.

You can also filter cities directly via `build_dimension_city`:

```python
city_df = ft.build_dimension_city(
    regions=["Americas"],
    countries=["US", "CAN"],
)
```

Filters accept case-insensitive values and are combined with AND logic.
The `countries` parameter accepts any mix of `country_code_2`, `country_code_3`, or `country_name`.

`dimension_date` also includes calendar/fiscal attributes and labels:
- `short_month`, `calendar_year`, `calendar_month`
- `fiscal_year`, `fiscal_month` (from `fiscal_year_start_month`, default `1`)
- `calendar_year_label`, `calendar_month_label` (format: `CYyyyy-MMM`)
- `fiscal_year_label`, `fiscal_month_label` (format: `FYyyyy-MMM`)
- `iso_week_number`

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
| `clean_and_write_all_tables(source_lakehouse_name, target_lakehouse_name, mode, partition_by, tables_config, include_schemas, exclude_tables, continue_on_error, spark=None)` | Discover tables or use config entries, then clean/write or merge per table |

### Warehouse

| Function | Description |
|---|---|
| `read_warehouse(warehouse_name, query, spark=None)` | Run a SQL query, return a DataFrame |
| `write_warehouse(df, warehouse_name, table, mode, batch_size, spark=None)` | Write to a Warehouse table via JDBC |

### Dimensions

| Function | Description |
|---|---|
| `build_dimension_date(start_date=None, end_date=None, fiscal_year_start_month=1, lakehouse_name=None, lakehouse_relative_path=None, mode="overwrite", spark=None)` | Build a date dimension DataFrame (with calendar/fiscal columns, labels, ISO week number) and optionally write it to Lakehouse |
| `build_dimension_country(countries_limit=None, fail_on_source_error=True, lakehouse_name=None, lakehouse_relative_path=None, mode="overwrite", spark=None)` | Build country dimension and optionally write it to Lakehouse |
| `build_dimension_city(countries_limit=None, include_states_metadata=True, fail_on_source_error=True, regions=None, subregions=None, countries=None, lakehouse_name=None, lakehouse_relative_path=None, mode="overwrite", spark=None)` | Build city dimension (with country attributes, plus optional region/subregion/country filters) and optionally write it to Lakehouse |
| `generate_dimensions(lakehouse_name, warehouse_name, ..., city_regions=None, city_subregions=None, city_countries=None, ...)` | Build and write selected dimensions to Lakehouse and Warehouse |

### Source to Prepared

| Function | Description |
|---|---|
| `snapshot_source_schema(source_lakehouse_name, source_relative_path, spark=None)` | Passive profiling of source table + schema hash generation |
| `resolve_columns(df, source_lakehouse_name, schema_hash=None, sample_size=500, profiling_confidence_threshold=0.80, unresolved_webhook_url=None, spark=None)` | 3-layer semantic resolver with confidence score and unresolved fallback audit |
| `transform_to_prepared(df, resolved_mappings, source_lakehouse_name, spark=None)` | Apply semantic casts, code labels, and date-derived columns in one select pass |
| `write_prepared_table(df, resolved_mappings, target_lakehouse_name, target_relative_path, mode="overwrite", max_partitions_guard=500, vacuum_retention_hours=168, spark=None)` | Write prepared table with partition strategy and conditional maintenance |
| `generate_prepared_aggregations(source_lakehouse_name, target_lakehouse_name, target_relative_path, resolved_mappings, spark=None)` | Build default prepared aggregations (`prepared_agg_jour`, `prepared_agg_semaine`, `prepared_agg_region`) |
| `publish_semantic_model(target_lakehouse_name, agg_tables, resolved_mappings, power_bi_workspace_id, power_bi_token, spark=None)` | Publish/update semantic model via Power BI/Fabric REST API |
| `prepare_and_write_data(source_lakehouse_name, source_relative_path, target_lakehouse_name, target_relative_path, ...)` | End-to-end orchestration for one table |
| `prepare_and_write_all_tables(source_lakehouse_name, target_lakehouse_name, ...)` | Bulk orchestration with discovery or explicit table config |

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
