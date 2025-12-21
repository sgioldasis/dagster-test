# Testing Partitioned Ingestion for raw_orders and raw_payments

## Commands to Test Partitioned Ingestion

### 1. Test raw_orders Partitioned Ingestion
To test the partitioned ingestion for `raw_orders`, run the following command:

```bash
cd dbx-project && uv run dagster asset materialize --select "target.main.orders" --module-name dbx_project.definitions
```

### 2. Test raw_payments Partitioned Ingestion
To test the partitioned ingestion for `raw_payments`, run the following command:

```bash
cd dbx-project && dagster asset materialize --select raw_payments --partition "2023-01-01"
```

### 3. Test Multiple Partitions
To test multiple partitions for both assets, you can use the following commands:

```bash
cd dbx-project && dagster asset materialize --select raw_orders --partition "2023-02-01"
cd dbx-project && dagster asset materialize --select raw_payments --partition "2023-02-01"
```

## Expected Output
- The commands should execute successfully, and logs should indicate the number of rows written to the DuckDB tables for the specified partitions.
- For example:
  ```
  INFO: Wrote 100 rows to main.raw_orders for partition 2023-01-01
  ```

## Validation Steps
1. **Check Logs**: Verify that the logs show successful ingestion for the specified partitions.
2. **Query DuckDB**: Connect to the DuckDB database and verify the data for the specified partitions:
   ```bash
   duckdb /tmp/jaffle_platform.duckdb
   ```
   Then run SQL queries to check the data:
   ```sql
   SELECT COUNT(*) FROM main.raw_orders WHERE order_date >= '2023-01-01' AND order_date < '2023-02-01';
   SELECT COUNT(*) FROM main.raw_payments WHERE order_date >= '2023-01-01' AND order_date < '2023-02-01';
   ```

## Troubleshooting
- **Missing CSV Files**: Ensure the CSV files (`data/raw_orders.csv` and `data/raw_payments.csv`) are present in the `dbx-project/data` directory.
- **DuckDB Path**: Verify that the DuckDB database path is correctly set in the environment variable `LOCAL_DUCKDB_PATH`.
- **Dependencies**: Ensure all dependencies (`duckdb`, `pandas`) are installed and up to date.

## Next Steps
- After successful testing, you can schedule the partitioned ingestion for all required partitions or set up a schedule for regular updates.