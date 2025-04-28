import os
from pathlib import Path
from dagster import asset, AssetExecutionContext

from pipeline.constants import (
    SINGLE_FILE_ASSETS_PATHS,
    PARTITIONED_ASSETS_PATHS,
    WAREHOUSE_PATH,
)
from pipeline.utils.duckdb_wrapper import DuckDBWrapper

@asset(
    deps=[
        "example_single_file_asset",
        "example_partitioned_file_asset",
    ],
    compute_kind="DuckDB",
    group_name="Warehouse",
)
def duckdb_warehouse(context: AssetExecutionContext):
    """
    Creates a persistent DuckDB file at WAREHOUSE_PATH and registers each
    asset as a DuckDB view. Partitioned assets use a different method.
    """

    # 1) Ensure the output directory exists
    warehouse_dir = Path(WAREHOUSE_PATH).parent
    warehouse_dir.mkdir(parents=True, exist_ok=True)

    # 2) Remove any existing DuckDB file
    if os.path.exists(WAREHOUSE_PATH):
        os.remove(WAREHOUSE_PATH)
        context.log.info(f"Existing DuckDB file at {WAREHOUSE_PATH} deleted.")

    # 3) Create a new DuckDB connection (and file)
    duckdb_wrapper = DuckDBWrapper(WAREHOUSE_PATH)
    context.log.info(f"New DuckDB file created at {WAREHOUSE_PATH}")

    # 4) Register non-partitioned assets
    non_partitioned = SINGLE_FILE_ASSETS_PATHS
    if non_partitioned:
        table_names = list(non_partitioned.keys())
        repo_root = os.path.dirname(WAREHOUSE_PATH)
        first_path = next(iter(non_partitioned.values()))
        base_path = os.path.relpath(os.path.dirname(first_path), repo_root)

        duckdb_wrapper.bulk_register_data(
            repo_root=repo_root,
            base_path=base_path,
            table_names=table_names,
            wildcard="*.parquet",
            as_table=False,
            show_tables=False,
        )

    # 5) Register partitioned assets
    partitioned = PARTITIONED_ASSETS_PATHS
    if partitioned:
        table_names = list(partitioned.keys())
        repo_root = os.path.dirname(WAREHOUSE_PATH)
        first_path = next(iter(partitioned.values()))
        base_path = os.path.relpath(os.path.dirname(first_path), repo_root)

        duckdb_wrapper.bulk_register_partitioned_data(
            repo_root=repo_root,
            base_path=base_path,
            table_names=table_names,
            wildcard="year=*/month=*/*.parquet",
            as_table=False,
            show_tables=False,
        )

    # 6) Clean up
    duckdb_wrapper.con.close()
    context.log.info("Connection to DuckDB closed.")

    return None
