# src/dlt_project/defs/ingestion/dlt_pipeline.py
import dlt
import pandas as pd

@dlt.resource(name="raw_customers", write_disposition="replace")
def raw_customers():
    """Load raw customers data"""
    df = pd.read_csv("data/raw_customers.csv")
    yield from df.to_dict(orient="records")

@dlt.resource(name="raw_orders", write_disposition="replace")
def raw_orders():
    """Load raw orders data"""
    df = pd.read_csv("data/raw_orders.csv")
    yield from df.to_dict(orient="records")

@dlt.resource(name="raw_payments", write_disposition="replace")
def raw_payments():
    """Load raw payments data"""
    df = pd.read_csv("data/raw_payments.csv")
    yield from df.to_dict(orient="records")

@dlt.source
def target_data():
    """Example dlt source for target main data"""
    yield raw_customers
    yield raw_orders
    yield raw_payments