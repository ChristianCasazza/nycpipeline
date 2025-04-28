import os
import requests
from dagster import ConfigurableIOManager, OutputContext, InputContext

class FastOpenDataSingleFileParquetIoManager(ConfigurableIOManager):
    """
    An IO Manager that, upon asset materialization, downloads a *single* Parquet file
    from a known remote URL and saves it to LAKE_PATH/asset_name/asset_name.parquet.
    
    - 'handle_output' is invoked after the asset function runs, but in this design,
      the asset itself doesn't generate the data. We do a direct download from the
      remote location.
    - 'load_input' is not strictly necessary if your downstream ops don't need to
      load the file as a DataFrame. You can omit it or provide a minimal pass-through.
    """

    base_dir: str  # e.g., LAKE_PATH from constants.py

    def handle_output(self, context: OutputContext, obj) -> None:
        """
        Called when materializing the asset. We'll:
         - Determine the local output path
         - Download the single parquet file from the remote location
           e.g. https://fastopendata.org/<asset_name>/<asset_name>.parquet
         - Store it at LAKE_PATH/<asset_name>/<asset_name>.parquet
        """
        asset_name = context.asset_key.to_python_identifier()
        context.log.info(f"[FastOpenDataSingleFileParquetIoManager] handle_output for '{asset_name}'.")
        
        # Construct the local folder and local file path
        local_asset_dir = os.path.join(self.base_dir, asset_name)
        os.makedirs(local_asset_dir, exist_ok=True)
        local_file_path = os.path.join(local_asset_dir, f"{asset_name}.parquet")
        
        # If the file already exists locally, you can choose to skip or re-download:
        if os.path.exists(local_file_path):
            context.log.info(f"Local file already exists at {local_file_path}. Re-downloading anyway.")
        
        # Example remote URL; adjust to your actual naming convention
        remote_url = f"https://fastopendata.org/{asset_name}/{asset_name}.parquet"
        context.log.info(f"Downloading single-file asset from {remote_url} -> {local_file_path}")
        
        # Download the file
        self._download_file(remote_url, local_file_path)
        
        context.log.info(f"Successfully saved single-file parquet asset at {local_file_path}")

    def load_input(self, context: InputContext):
        """
        If you need to load the single-file for a downstream op/asset:
        - You can read the local parquet here using Polars or PyArrow, etc.
        - If you do *not* intend to read it into memory, you can just return
          the local file path or None. This method is optional.
        """
        asset_name = context.asset_key.to_python_identifier()
        local_asset_dir = os.path.join(self.base_dir, asset_name)
        local_file_path = os.path.join(local_asset_dir, f"{asset_name}.parquet")

        if not os.path.exists(local_file_path):
            # If not present, optionally download here as well
            remote_url = f"https://fastopendata.org/{asset_name}/{asset_name}.parquet"
            context.log.info(f"File not found locally; downloading from {remote_url}")
            self._download_file(remote_url, local_file_path)

        # Return the local file path, or read it into memory if you like
        if os.path.exists(local_file_path):
            context.log.info(f"Returning local file path: {local_file_path}")
            return local_file_path  # or read it, e.g. pl.read_parquet(local_file_path)
        else:
            context.log.warn(f"File not found: {local_file_path}")
            return None

    def _download_file(self, file_url: str, local_path: str):
        """
        Utility method to download a file from an HTTP endpoint.
        """
        with requests.get(file_url, stream=True) as r:
            r.raise_for_status()
            with open(local_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
