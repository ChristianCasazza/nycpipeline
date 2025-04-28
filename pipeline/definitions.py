import os
from dagster import Definitions, load_assets_from_modules
from pipeline.constants import LAKE_PATH

# 1) Ingestion assets module
import pipeline.assets.ingestion.custom_assets as ingestion_module


# 4) IO Managers
from pipeline.resources.io_managers.fastopendata_single_io_manager import FastOpenDataSingleFileParquetIoManager
from pipeline.resources.io_managers.fastopendata_partitioned_parquet_io_manager import FastOpenDataPartitionedParquetIOManager

singlefile_io_manager = FastOpenDataSingleFileParquetIoManager(base_dir=LAKE_PATH)
partitioned_io_manager = FastOpenDataPartitionedParquetIOManager(base_dir=LAKE_PATH)

# 5) Load assets from each module
ingestion_assets = load_assets_from_modules([ingestion_module])


# 6) Combine all asset definitions
all_assets = [
    *ingestion_assets
]

# 7) Wire up resources
resources = {
    "fastopendata_singlefile_io_manager": singlefile_io_manager,
    "fastopendata_partitioned_parquet_io_manager": partitioned_io_manager,
}

# 8) Final Definitions
defs = Definitions(
    assets=all_assets,
    resources=resources,
)
