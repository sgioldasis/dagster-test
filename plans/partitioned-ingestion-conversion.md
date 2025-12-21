# Plan for Converting raw_orders and raw_payments to Partitioned Ingestion

## Current State
- **raw_orders and raw_payments**: Currently commented out in `replication_prod.yaml` and not configured for partitioned ingestion.
- **Partitioned Implementation**: A partitioned ingestion implementation already exists in `partitioned_ingestion.py` for both `raw_orders` and `raw_payments`.

## Goal
Convert the ingestion of `raw_orders` and `raw_payments` to use the partitioned ingestion approach defined in `partitioned_ingestion.py`.

## Steps to Convert

### 1. Enable Partitioned Assets
The partitioned assets for `raw_orders` and `raw_payments` are already defined in `partitioned_ingestion.py`. These assets:
- Use `MonthlyPartitionsDefinition` starting from `2018-01-01`.
- Read from CSV files (`data/raw_orders.csv` and `data/raw_payments.csv`).
- Write partitioned data to DuckDB tables (`main.raw_orders` and `main.raw_payments`).

### 2. Update Configuration Files
- **replication_prod.yaml**: Uncomment and update the configurations for `raw_orders` and `raw_payments` to align with the partitioned ingestion approach.
- **replication_prod_from_csv.yaml**: Ensure this file is updated to reflect the partitioned ingestion setup if it is used for CSV-based ingestion.

### 3. Verify Data Sources
- Ensure the CSV files (`data/raw_orders.csv` and `data/raw_payments.csv`) are available and contain the required `order_date` column for partitioning.

### 4. Test the Partitioned Ingestion
- Run the partitioned ingestion for a specific partition to verify it works as expected:
  ```bash
  dagster asset materialize --select raw_orders --partition "2023-01-01"
  dagster asset materialize --select raw_payments --partition "2023-01-01"
  ```

### 5. Monitor and Validate
- Check the logs to ensure the partitioned ingestion completes successfully.
- Verify the data in the DuckDB tables (`main.raw_orders` and `main.raw_payments`) for the specified partitions.

### 6. Update Documentation
- Document the changes in the project's README or a dedicated documentation file to reflect the new partitioned ingestion setup.

## Expected Outcome
- `raw_orders` and `raw_payments` will be ingested in monthly partitions, improving performance and manageability.
- The partitioned ingestion will allow for incremental updates and easier maintenance of large datasets.

## Risks and Mitigations
- **Data Consistency**: Ensure the partitioned ingestion does not overlap or miss data. Validate the partition logic in `partitioned_ingestion.py`.
- **Performance**: Monitor the performance of the partitioned ingestion, especially for large datasets.
- **Dependencies**: Ensure all dependencies (e.g., `duckdb`, `pandas`) are installed and up to date.

## Next Steps
1. Uncomment and update the configurations in `replication_prod.yaml`.
2. Test the partitioned ingestion for a sample partition.
3. Validate the data and logs.
4. Document the changes and update the project documentation.