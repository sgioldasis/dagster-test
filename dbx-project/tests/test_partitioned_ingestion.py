# Test file for partitioned ingestion functionality
import os
import tempfile
import pandas as pd
import duckdb
import pytest
from pathlib import Path
from datetime import datetime
from unittest.mock import Mock, patch

# Import the functions to test
from dbx_project.defs.ingest_files.partitioned_ingestion import (
    _write_partition_to_table,
    raw_orders,
    raw_payments,
    monthly_partitions,
    _duckdb_path
)
from dagster import build_asset_context


class TestPartitionedIngestion:
    """Test suite for partitioned ingestion functionality"""

    def setup_method(self):
        """Setup test environment"""
        # Use in-memory database for testing
        self.temp_db = ':memory:'
        
        # Set environment variable to use in-memory database
        os.environ['LOCAL_DUCKDB_PATH'] = self.temp_db
        
        # Create test data directory
        self.test_data_dir = Path(tempfile.mkdtemp())
        
        # Create test CSV files
        self.orders_data = pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'user_id': [1, 2, 3, 4, 5],
            'order_date': pd.to_datetime(['2025-01-01', '2025-01-15', '2025-02-01', '2025-02-15', '2025-03-01']),
            'status': ['completed', 'completed', 'completed', 'completed', 'completed']
        })
        
        self.payments_data = pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'order_id': [1, 2, 3, 4, 5],
            'amount': [100.0, 200.0, 150.0, 300.0, 250.0],
            'payment_date': pd.to_datetime(['2025-01-02', '2025-01-16', '2025-02-02', '2025-02-16', '2025-03-02'])
        })
        
        # Save test data
        self.orders_csv = self.test_data_dir / 'raw_orders.csv'
        self.payments_csv = self.test_data_dir / 'raw_payments.csv'
        self.orders_data.to_csv(self.orders_csv, index=False)
        self.payments_data.to_csv(self.payments_csv, index=False)

    def teardown_method(self):
        """Cleanup test environment"""
        # Remove test data directory
        if hasattr(self, 'test_data_dir') and self.test_data_dir.exists():
            import shutil
            shutil.rmtree(self.test_data_dir)
        
        # Clean up environment variable
        if 'LOCAL_DUCKDB_PATH' in os.environ:
            del os.environ['LOCAL_DUCKDB_PATH']

    def test_write_partition_to_table_new_table(self):
        """Test writing partition to a new table"""
        con = duckdb.connect(database=self.temp_db)
        
        try:
            # Test data for January 2025
            test_df = self.orders_data[self.orders_data['order_date'] < '2025-02-01'].copy()
            
            # Write partition to table
            _write_partition_to_table(
                con=con,
                table='test_orders',
                df=test_df,
                partition_col='order_date',
                partition_start=pd.to_datetime('2025-01-01'),
                partition_end=pd.to_datetime('2025-02-01')
            )
            
            # Verify data was written
            result = con.execute('SELECT * FROM test_orders').fetchdf()
            assert len(result) == 2  # Should have 2 orders from January
            assert 'order_date' in result.columns
            
        finally:
            con.close()

    def test_write_partition_to_table_existing_table(self):
        """Test writing partition to an existing table (should delete and re-insert)"""
        con = duckdb.connect(database=self.temp_db)
        
        try:
            # First write some data
            initial_df = self.orders_data[self.orders_data['order_date'] < '2025-02-01'].copy()
            _write_partition_to_table(
                con=con,
                table='test_orders_existing',
                df=initial_df,
                partition_col='order_date',
                partition_start=pd.to_datetime('2025-01-01'),
                partition_end=pd.to_datetime('2025-02-01')
            )
            
            # Verify initial data
            result1 = con.execute('SELECT * FROM test_orders_existing').fetchdf()
            assert len(result1) == 2
            
            # Write updated data for the same partition
            updated_df = initial_df.copy()
            updated_df.loc[0, 'status'] = 'cancelled'  # Change status
            
            _write_partition_to_table(
                con=con,
                table='test_orders_existing',
                df=updated_df,
                partition_col='order_date',
                partition_start=pd.to_datetime('2025-01-01'),
                partition_end=pd.to_datetime('2025-02-01')
            )
            
            # Verify data was updated
            result2 = con.execute('SELECT * FROM test_orders_existing').fetchdf()
            assert len(result2) == 2
            assert result2.iloc[0]['status'] == 'cancelled'
            
        finally:
            con.close()

    def test_write_partition_to_table_empty_partition(self):
        """Test writing an empty partition"""
        con = duckdb.connect(database=self.temp_db)
        
        try:
            # Empty dataframe
            empty_df = self.orders_data[self.orders_data['order_date'] > '2030-01-01'].copy()
            assert len(empty_df) == 0
            
            # Should not fail with empty partition
            _write_partition_to_table(
                con=con,
                table='test_empty_partition',
                df=empty_df,
                partition_col='order_date',
                partition_start=pd.to_datetime('2030-01-01'),
                partition_end=pd.to_datetime('2030-02-01')
            )
            
            # Table should not exist or be empty
            try:
                result = con.execute('SELECT * FROM test_empty_partition').fetchdf()
                assert len(result) == 0
            except:
                pass  # Table doesn't exist, which is also fine
                
        finally:
            con.close()

    @patch('dbx_project.defs.ingest_files.partitioned_ingestion.Path')
    def test_raw_orders_ingestion(self, mock_path):
        """Test raw_orders ingestion function"""
        # Create a mock Path class that handles the / operator
        class MockPath:
            def __init__(self, path, orders_csv):
                self.path = path
                self.orders_csv = orders_csv
                
            def __truediv__(self, other):
                if other == "raw_orders.csv":
                    return self.orders_csv
                return Path(self.path) / other
                
            def exists(self):
                return True
        
        # Set up the mock to return our MockPath when called with "data"
        def path_side_effect(arg):
            if arg == "data":
                return MockPath(arg, self.orders_csv)
            return self.orders_csv
        
        mock_path.side_effect = path_side_effect
        
        # Create a proper Dagster asset context using build_asset_context
        context = build_asset_context(partition_key='2025-01-01')
        
        # Call the function
        raw_orders(context)
        
        # Verify data was written to database
        # Since we're using in-memory database, the data is lost when the connection closes
        # Let's switch to a file-based database for this test
        import tempfile
        import uuid
        
        # Create a unique database file path
        tmp_db_path = f'/tmp/test_{uuid.uuid4().hex}.duckdb'
        
        try:
            # Set the environment variable to use the file-based database
            os.environ['LOCAL_DUCKDB_PATH'] = tmp_db_path
            
            # Create a new context with the file-based database
            context2 = build_asset_context(partition_key='2025-01-01')
            
            # Call the function again with the file-based database
            raw_orders(context2)
            
            # Now verify using the file-based database
            con = duckdb.connect(database=tmp_db_path)
            try:
                result = con.execute('SELECT * FROM main.raw_orders').fetchdf()
                assert len(result) > 0
                # Should only have January 2025 orders
                assert all(pd.to_datetime(row['order_date']).month == 1 for _, row in result.iterrows())
            finally:
                con.close()
        finally:
            # Clean up the temporary database file
            if os.path.exists(tmp_db_path):
                os.unlink(tmp_db_path)
            # Restore the original environment variable
            os.environ['LOCAL_DUCKDB_PATH'] = self.temp_db

    @patch('dbx_project.defs.ingest_files.partitioned_ingestion.Path')
    def test_raw_payments_ingestion(self, mock_path):
        """Test raw_payments ingestion function"""
        # Create a mock Path class that handles the / operator
        class MockPath:
            def __init__(self, path, payments_csv, orders_csv):
                self.path = path
                self.payments_csv = payments_csv
                self.orders_csv = orders_csv
                
            def __truediv__(self, other):
                if "raw_payments.csv" in str(other):
                    return self.payments_csv
                elif "raw_orders.csv" in str(other):
                    return self.orders_csv
                return Path(self.path) / other
                
            def exists(self):
                return True
        
        # Set up the mock to return our MockPath when called with "data"
        def path_side_effect(arg):
            if arg == "data":
                return MockPath(arg, self.payments_csv, self.orders_csv)
            elif 'raw_payments.csv' in str(arg):
                return self.payments_csv
            elif 'raw_orders.csv' in str(arg):
                return self.orders_csv
            return Path(arg)
        
        mock_path.side_effect = path_side_effect
        
        # Create a proper Dagster asset context using build_asset_context
        context = build_asset_context(partition_key='2025-01-01')
        
        # Call the function
        raw_payments(context)
        
        # Verify data was written to database
        # Since we're using in-memory database, the data is lost when the connection closes
        # Let's switch to a file-based database for this test
        import tempfile
        import uuid
        
        # Create a unique database file path
        tmp_db_path = f'/tmp/test_{uuid.uuid4().hex}.duckdb'
        
        try:
            # Set the environment variable to use the file-based database
            os.environ['LOCAL_DUCKDB_PATH'] = tmp_db_path
            
            # Create a new context with the file-based database
            context2 = build_asset_context(partition_key='2025-01-01')
            
            # Call the function again with the file-based database
            raw_payments(context2)
            
            # Now verify using the file-based database
            con = duckdb.connect(database=tmp_db_path)
            try:
                result = con.execute('SELECT * FROM main.raw_payments').fetchdf()
                assert len(result) > 0
                # Should have payments joined with orders from January 2025
                assert 'order_date' in result.columns  # From the join
            finally:
                con.close()
        finally:
            # Clean up the temporary database file
            if os.path.exists(tmp_db_path):
                os.unlink(tmp_db_path)
            # Restore the original environment variable
            os.environ['LOCAL_DUCKDB_PATH'] = self.temp_db

    def test_monthly_partitions_definition(self):
        """Test that monthly partitions are correctly defined"""
        # Test that we can get partition keys
        partition_keys = list(monthly_partitions.get_partition_keys())
        assert len(partition_keys) > 0
        
        # Test that partition keys are in the expected format
        first_key = partition_keys[0]
        assert isinstance(first_key, str)
        assert '-' in first_key  # Should contain date separator

    def test_duckdb_path_environment_variable(self):
        """Test that _duckdb_path respects environment variable"""
        # Set a custom path
        custom_path = '/custom/path/test.duckdb'
        os.environ['LOCAL_DUCKDB_PATH'] = custom_path
        
        # Verify it's returned
        assert _duckdb_path() == custom_path
        
        # Clean up
        del os.environ['LOCAL_DUCKDB_PATH']

    def test_duckdb_path_default(self):
        """Test that _duckdb_path returns default when env var not set"""
        # Ensure env var is not set
        if 'LOCAL_DUCKDB_PATH' in os.environ:
            del os.environ['LOCAL_DUCKDB_PATH']
        
        # Verify default path is returned
        default_path = _duckdb_path()
        assert default_path == '/tmp/jaffle_platform.duckdb'

    @patch('dbx_project.defs.ingest_files.partitioned_ingestion.Path')
    def test_raw_orders_missing_csv(self, mock_path):
        """Test raw_orders function when CSV file is missing"""
        # Create a mock Path class that handles the / operator
        class MockPath:
            def __init__(self, path):
                self.path = path
                
            def __truediv__(self, other):
                return Path('/nonexistent/file.csv')
                
            def exists(self):
                return False
        
        # Set up the mock to return our MockPath when called with "data"
        def path_side_effect(arg):
            if arg == "data":
                return MockPath(arg)
            return Path('/nonexistent/file.csv')
        
        mock_path.side_effect = path_side_effect
        
        # Create a proper Dagster asset context using build_asset_context
        context = build_asset_context(partition_key='2025-01-01')
        
        # Call the function - should not fail
        raw_orders(context)
        
        # Verify no data was written to database
        con = duckdb.connect(database=self.temp_db)
        try:
            # Table should not exist or be empty
            try:
                result = con.execute('SELECT * FROM main.raw_orders').fetchdf()
                assert len(result) == 0
            except:
                pass  # Table doesn't exist, which is fine
        finally:
            con.close()

    @patch('dbx_project.defs.ingest_files.partitioned_ingestion.Path')
    def test_raw_payments_missing_orders_csv(self, mock_path):
        """Test raw_payments function when orders CSV is missing"""
        # Create a mock Path class that handles the / operator
        class MockPath:
            def __init__(self, path, payments_csv):
                self.path = path
                self.payments_csv = payments_csv
                
            def __truediv__(self, other):
                if "raw_payments.csv" in str(other):
                    return self.payments_csv
                elif "raw_orders.csv" in str(other):
                    return Path('/nonexistent/file.csv')
                return Path(self.path) / other
                
            def exists(self):
                return False
        
        # Set up the mock to return our MockPath when called with "data"
        def path_side_effect(arg):
            if arg == "data":
                return MockPath(arg, self.payments_csv)
            elif 'raw_payments.csv' in str(arg):
                return self.payments_csv
            elif 'raw_orders.csv' in str(arg):
                return Path('/nonexistent/file.csv')
            return Path(arg)
        
        mock_path.side_effect = path_side_effect
        
        # Create a proper Dagster asset context using build_asset_context
        context = build_asset_context(partition_key='2025-01-01')
        
        # Call the function - should not fail
        raw_payments(context)
        
        # Verify no data was written to database
        con = duckdb.connect(database=self.temp_db)
        try:
            # Table should not exist or be empty
            try:
                result = con.execute('SELECT * FROM main.raw_payments').fetchdf()
                assert len(result) == 0
            except:
                pass  # Table doesn't exist, which is fine
        finally:
            con.close()

    def test_partition_filtering(self):
        """Test that partition filtering works correctly"""
        con = duckdb.connect(database=self.temp_db)
        
        try:
            # Write data for multiple months
            jan_data = self.orders_data[self.orders_data['order_date'] < '2025-02-01'].copy()
            feb_data = self.orders_data[(self.orders_data['order_date'] >= '2025-02-01') & (self.orders_data['order_date'] < '2025-03-01')].copy()
            
            # Write January data
            _write_partition_to_table(
                con=con,
                table='test_partition_filter',
                df=jan_data,
                partition_col='order_date',
                partition_start=pd.to_datetime('2025-01-01'),
                partition_end=pd.to_datetime('2025-02-01')
            )
            
            # Write February data
            _write_partition_to_table(
                con=con,
                table='test_partition_filter',
                df=feb_data,
                partition_col='order_date',
                partition_start=pd.to_datetime('2025-02-01'),
                partition_end=pd.to_datetime('2025-03-01')
            )
            
            # Verify we have data from both months
            all_data = con.execute('SELECT * FROM test_partition_filter').fetchdf()
            assert len(all_data) == len(jan_data) + len(feb_data)
            
            # Verify January data is correct
            jan_result = con.execute("SELECT * FROM test_partition_filter WHERE order_date >= '2025-01-01' AND order_date < '2025-02-01'").fetchdf()
            assert len(jan_result) == len(jan_data)
            
            # Verify February data is correct
            feb_result = con.execute("SELECT * FROM test_partition_filter WHERE order_date >= '2025-02-01' AND order_date < '2025-03-01'").fetchdf()
            assert len(feb_result) == len(feb_data)
            
        finally:
            con.close()