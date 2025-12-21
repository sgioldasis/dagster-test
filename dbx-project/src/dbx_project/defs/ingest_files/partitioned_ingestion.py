import os
from pathlib import Path
from typing import Optional

import pandas as pd
import duckdb
from dagster import asset, AssetExecutionContext, AssetKey, MonthlyPartitionsDefinition

# Monthly partitions starting at the beginning of each month
monthly_partitions = MonthlyPartitionsDefinition(start_date="2025-01-01", end_offset=1)


def _duckdb_path() -> str:
    return os.environ.get("LOCAL_DUCKDB_PATH", "/tmp/jaffle_platform.duckdb")


def _write_partition_to_table(
    con: duckdb.DuckDBPyConnection,
    table: str,
    df: pd.DataFrame,
    partition_col: str = "order_date",
    partition_start: pd.Timestamp = None,
    partition_end: pd.Timestamp = None,
    context: AssetExecutionContext = None,
):
    """Delete any rows in the target table for the partition range, then insert the new rows.

    If the target table does not exist, create it from the partition dataframe.
    """
    # Ensure date strings for SQL
    start_str = partition_start.strftime("%Y-%m-%d") if partition_start is not None else None
    end_str = partition_end.strftime("%Y-%m-%d") if partition_end is not None else None

    # Register df as a temporary view for SQL operations
    con.register("_part_df", df)

    # Get column names for INSERT statement
    df_columns = ', '.join([f'"{col}"' for col in df.columns])
    
    # If table exists, delete partition range and insert
    try:
        con.execute(f"SELECT 1 FROM {table} LIMIT 1")
        if partition_start is not None and partition_end is not None:
            con.execute(
                f"DELETE FROM {table} WHERE {partition_col} >= DATE '{start_str}' AND {partition_col} < DATE '{end_str}'"
            )
        # Insert partition rows - use column names to avoid schema mismatch
        con.execute(f"INSERT INTO {table} ({df_columns}) SELECT * FROM _part_df")
    except Exception as e:
        # Table does not exist or some other error: create it from partition dataframe
        if context:
            context.log.info(f"Creating table {table} (error was: {e})")
        con.execute(f"CREATE TABLE {table} AS SELECT * FROM _part_df")


@asset(
    key=AssetKey(["target", "main", "raw_orders"]),
    partitions_def=monthly_partitions,
    group_name="ingestion_partitioned",
)
def raw_orders(context: AssetExecutionContext):
    """Partitioned ingestion for orders: writes monthly slices into DuckDB table main.raw_orders."""
    csv_path = Path("data") / "raw_orders.csv"
    if not csv_path.exists():
        context.log.warning(f"Source CSV not found: {csv_path}; skipping partition {context.partition_key}")
        return

    df = pd.read_csv(csv_path, parse_dates=["order_date"])  # expects an order_date column

    partition_start = pd.to_datetime(context.partition_key)
    partition_end = (partition_start + pd.offsets.MonthBegin(1))

    part_df = df[(df["order_date"] >= partition_start) & (df["order_date"] < partition_end)].copy()

    if part_df.empty:
        context.log.info(f"No rows for partition {context.partition_key}; nothing to write")
        return

    db_path = _duckdb_path()
    con = duckdb.connect(database=db_path, read_only=False)
    try:
        _write_partition_to_table(
            con=con,
            table="main.raw_orders",
            df=part_df,
            partition_col="order_date",
            partition_start=partition_start,
            partition_end=partition_end,
            context=context,
        )
        context.log.info(f"Wrote {len(part_df)} rows to main.raw_orders for partition {context.partition_key}")
    finally:
        con.close()


@asset(
    key=AssetKey(["target", "main", "raw_payments"]),
    partitions_def=monthly_partitions,
    group_name="ingestion_partitioned",
)
def raw_payments(context: AssetExecutionContext):
    """Partitioned ingestion for payments: writes monthly slices into DuckDB table main.raw_payments."""
    csv_path = Path("data") / "raw_payments.csv"
    if not csv_path.exists():
        context.log.warning(f"Source CSV not found: {csv_path}; skipping partition {context.partition_key}")
        return

    # Read payments CSV (no order_date column)
    payments_df = pd.read_csv(csv_path)

    # Read orders CSV to get order dates for joining
    orders_csv_path = Path("data") / "raw_orders.csv"
    if not orders_csv_path.exists():
        context.log.warning(f"Orders CSV not found: {orders_csv_path}; cannot partition payments")
        return

    orders_df = pd.read_csv(orders_csv_path, parse_dates=["order_date"])

    # Join payments with orders to get order_date for partitioning
    df = pd.merge(payments_df, orders_df[["id", "order_date"]],
                  left_on="order_id", right_on="id", how="left", suffixes=("", "_order"))
    # Remove the redundant id column from orders
    df = df.drop(columns=["id_order"]) if "id_order" in df.columns else df

    partition_start = pd.to_datetime(context.partition_key)
    partition_end = (partition_start + pd.offsets.MonthBegin(1))

    # Filter by order_date (from the joined data)
    part_df = df[(df["order_date"] >= partition_start) & (df["order_date"] < partition_end)].copy()

    if part_df.empty:
        context.log.info(f"No rows for partition {context.partition_key}; nothing to write")
        return

    db_path = _duckdb_path()
    con = duckdb.connect(database=db_path, read_only=False)
    try:
        _write_partition_to_table(
            con=con,
            table="main.raw_payments",
            df=part_df,
            partition_col="order_date",
            partition_start=partition_start,
            partition_end=partition_end,
            context=context,
        )
        context.log.info(f"Wrote {len(part_df)} rows to main.raw_payments for partition {context.partition_key}")
    finally:
        con.close()
