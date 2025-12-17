# src/dbx_project/definitions.py

from pathlib import Path
from dagster import load_from_defs_folder
from dotenv import load_dotenv

# You still need this to make the env var available to the component
load_dotenv()

# Just load the component folder directly. The component will handle the rest.
defs = load_from_defs_folder(
    path_within_project=Path(__file__).parent / 'defs' / 'ingest_files'
)