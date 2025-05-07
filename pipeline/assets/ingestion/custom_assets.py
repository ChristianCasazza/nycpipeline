from dagster import asset, MetadataValue


@asset(
    name="example_single_file_asset",
    io_manager_key="fastopendata_singlefile_io_manager",
    group_name="single_file_example",
    tags={"example_single_type_one": "example_tag_one", "example_single_type_one": "example_tag_one"},
    metadata={"data_url": MetadataValue.url("https://data.ny.gov/example/link/to/nycopendataportal")},
)
def example_single_file_asset():
    return None

@asset(
    name="example_partitioned_file_asset",
    io_manager_key="fastopendata_partitioned_parquet_io_manager",
    group_name="partitioned_asset_example",
    tags={"example_partitioned_type_one": "example_tag_one", "example_partitioned_type_one": "example_tag_one"},
    metadata={"data_url": MetadataValue.url("https://data.ny.gov/example/link/to/nycopendataportal")},
)
def example_partitioned_file_asset():
    return {
        "start_year": 2023,
        "start_month": 3,
        "end_year": 2024,
        "end_month": 12,
    }