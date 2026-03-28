"""Test script to verify Databricks credentials are working."""

import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

print("Databricks Environment Variables:")
print(f"  DATABRICKS_HOST: {os.environ.get('DATABRICKS_HOST', 'NOT SET')}")
print(f"  DATABRICKS_TOKEN: {'***SET***' if os.environ.get('DATABRICKS_TOKEN') else 'NOT SET'}")
print(f"  DATABRICKS_WAREHOUSE_ID: {os.environ.get('DATABRICKS_WAREHOUSE_ID', 'NOT SET')}")
print(f"  DATABRICKS_NOTEBOOK_JOB_ID: {os.environ.get('DATABRICKS_NOTEBOOK_JOB_ID', 'NOT SET')}")

# Try to create a client and list jobs
print("\n--- Testing Databricks Connection ---")
try:
    from databricks.sdk import WorkspaceClient
    
    host = os.environ.get('DATABRICKS_HOST')
    token = os.environ.get('DATABRICKS_TOKEN')
    
    if not host or not token:
        print("ERROR: Missing DATABRICKS_HOST or DATABRICKS_TOKEN")
        exit(1)
    
    print(f"Creating WorkspaceClient for host: {host}")
    client = WorkspaceClient(host=host, token=token)
    
    print("Listing jobs...")
    jobs = list(client.jobs.list(limit=5))
    print(f"  Found {len(jobs)} jobs")
    
    for job in jobs:
        print(f"    - Job ID: {job.job_id}, Name: {job.settings.name if job.settings else 'N/A'}")
    
    # Try to get the specific job
    job_id = os.environ.get('DATABRICKS_NOTEBOOK_JOB_ID')
    if job_id:
        print(f"\nLooking up specific job ID: {job_id}")
        try:
            job = client.jobs.get(job_id=int(job_id))
            print(f"  Found job: {job.settings.name if job.settings else 'N/A'}")
            print(f"  Job settings: {job.settings}")
        except Exception as e:
            print(f"  ERROR: Could not get job {job_id}: {e}")
    
    print("\n✓ Credentials are working!")
    
except Exception as e:
    print(f"\n✗ Error: {e}")
    import traceback
    traceback.print_exc()
