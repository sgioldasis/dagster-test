# Test Documentation for Partitioned Ingestion

This document provides an overview of the tests in `test_partitioned_ingestion.py` and what each test is checking.

## Test Suite: `TestPartitionedIngestion`

### 1. `test_write_partition_to_table_new_table`
**Purpose**: Tests writing a partition to a new table.
**What it checks**:
- Ensures that data for a specific partition (January 2025) is correctly written to a new table.
- Verifies that the table is created and contains the expected number of records.
- Confirms that the partition column (`order_date`) is included in the table.

### 2. `test_write_partition_to_table_existing_table`
**Purpose**: Tests writing a partition to an existing table.
**What it checks**:
- Ensures that writing to an existing table deletes the old partition data and re-inserts the new data.
- Verifies that the data is updated correctly (e.g., changing the status of an order).
- Confirms that the table retains the correct number of records after the update.

### 3. `test_write_partition_to_table_empty_partition`
**Purpose**: Tests writing an empty partition.
**What it checks**:
- Ensures that the function does not fail when an empty partition is provided.
- Verifies that no data is written to the table or that the table does not exist after the operation.

### 4. `test_raw_orders_ingestion`
**Purpose**: Tests the `raw_orders` ingestion function.
**What it checks**:
- Ensures that the `raw_orders` function correctly ingests data for a specific partition (January 2025).
- Verifies that the data is written to the database and can be queried.
- Confirms that only the data for the specified partition is loaded.

### 5. `test_raw_payments_ingestion`
**Purpose**: Tests the `raw_payments` ingestion function.
**What it checks**:
- Ensures that the `raw_payments` function correctly ingests payment data and joins it with order data for a specific partition (January 2025).
- Verifies that the joined data is written to the database and includes the expected columns (e.g., `order_date` from the join).

### 6. `test_monthly_partitions_definition`
**Purpose**: Tests the definition of monthly partitions.
**What it checks**:
- Ensures that the `monthly_partitions` function returns a list of partition keys.
- Verifies that the partition keys are in the expected format (e.g., `YYYY-MM-DD`).

### 7. `test_duckdb_path_environment_variable`
**Purpose**: Tests the `_duckdb_path` function with a custom environment variable.
**What it checks**:
- Ensures that the `_duckdb_path` function respects the `LOCAL_DUCKDB_PATH` environment variable.
- Verifies that the function returns the custom path when the environment variable is set.

### 8. `test_duckdb_path_default`
**Purpose**: Tests the `_duckdb_path` function with the default path.
**What it checks**:
- Ensures that the `_duckdb_path` function returns the default path (`/tmp/jaffle_platform.duckdb`) when the `LOCAL_DUCKDB_PATH` environment variable is not set.

### 9. `test_raw_orders_missing_csv`
**Purpose**: Tests the `raw_orders` function when the CSV file is missing.
**What it checks**:
- Ensures that the function does not fail when the CSV file is missing.
- Verifies that no data is written to the database or that the table does not exist after the operation.

### 10. `test_raw_payments_missing_orders_csv`
**Purpose**: Tests the `raw_payments` function when the orders CSV file is missing.
**What it checks**:
- Ensures that the function does not fail when the orders CSV file is missing.
- Verifies that no data is written to the database or that the table does not exist after the operation.

### 11. `test_partition_filtering`
**Purpose**: Tests that partition filtering works correctly.
**What it checks**:
- Ensures that data for multiple partitions (January and February 2025) is correctly filtered and loaded.
- Verifies that the data for each partition is written to the same table and can be queried separately.
- Confirms that the total number of records matches the sum of records from each partition.
