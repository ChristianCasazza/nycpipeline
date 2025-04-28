#For filesystem operations
import os

# Import asset names from the respective groups
from pipeline.datasets import *

# Define the base path relative to the location where we will keep our data lake of parquet files. Our lake base path is one folder back, then data/opendata
LAKE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "opendata"))

# Base path to store our DuckDB We store this DuckDB file directly in our app folder, where it will be used to power our data application
WAREHOUSE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "duckdb", "data.duckdb")) 

# Path to where we will store our Dagster logs
DAGSTER_PATH=os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "logs"))





SINGLE_FILE_ASSETS_PATHS = {
    asset_name: f"{LAKE_PATH}/{asset_name}"
    for asset_name in SINGLE_FILE_ASSETS_NAMES
}



PARTITIONED_ASSETS_PATHS = {
    asset_name: f"{LAKE_PATH}/{asset_name}"
    for asset_name in PARTITIONED_ASSETS_NAMES
}
