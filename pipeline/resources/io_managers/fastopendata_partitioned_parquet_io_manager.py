from __future__ import annotations

import threading
from pathlib import Path
from typing import Dict, List, Tuple

import requests
import polars as pl
from tenacity import retry, wait_fixed, stop_after_attempt, retry_if_exception_type
from dagster import ConfigurableIOManager, OutputContext, InputContext

# Thread‐local session
_THREAD_LOCAL = threading.local()
def _session() -> requests.Session:
    if not hasattr(_THREAD_LOCAL, "session"):
        _THREAD_LOCAL.session = requests.Session()
    return _THREAD_LOCAL.session

class FastOpenDataPartitionedParquetIOManager(ConfigurableIOManager):
    """
    Downloads exactly one Parquet file per (year, month), always named with `_1`.
    Files are fetched sequentially, one full‐size GET at a time.
    """

    base_dir: str

    def handle_output(self, context: OutputContext, obj: Dict[str, int]) -> None:
        asset = context.asset_key.to_python_identifier()
        log = context.log

        # 1) parse date window
        try:
            y0, m0, y1, m1 = (
                int(obj["start_year"]),
                int(obj["start_month"]),
                int(obj["end_year"]),
                int(obj["end_month"]),
            )
        except (KeyError, TypeError, ValueError):
            log.warning(f"{asset}: invalid or missing date range → skipping.")
            return

        # 2) build partition list
        parts: List[Tuple[int, int]] = []
        y, m = y0, m0
        while (y, m) <= (y1, m1):
            parts.append((y, m))
            m += 1
            if m == 13:
                y, m = y + 1, 1

        # 3) sequentially download each partition
        downloaded = []
        for y, m in parts:
            result = self._download_one(asset, y, m, log)
            if result:
                downloaded.append(result)

        log.info(f"{asset}: {len(downloaded)} files downloaded/verified.")

    def _download_one(
        self, asset: str, year: int, month: int, log
    ) -> Path | None:
        """
        Fetch a single file named `{asset}_{YYYY}{MM}_1.parquet`.
        """
        fn = f"{asset}_{year:04d}{month:02d}_1.parquet"
        url = (
            f"https://fastopendata.org/{asset}/"
            f"year={year:04d}/month={month:02d}/"
            f"{fn}"
        )
        dest = Path(self.base_dir) / asset / f"year={year:04d}" / f"month={month:02d}" / fn
        dest.parent.mkdir(parents=True, exist_ok=True)

        # skip if already present and size matches
        if dest.exists():
            head = _session().head(url, timeout=10)
            if head.ok and int(head.headers.get("content-length", 0)) == dest.stat().st_size:
                log.debug(f"{asset} {fn}: exists → skipping.")
                return dest

        return self._fetch(url, dest, log)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_fixed(2),
        reraise=True,
        retry=retry_if_exception_type(
            (
                requests.exceptions.ChunkedEncodingError,
                requests.exceptions.ConnectionError,
                requests.exceptions.ReadTimeout,
            )
        ),
    )
    def _fetch(self, url: str, dest: Path, log) -> Path | None:
        r = _session().get(url, stream=True, timeout=(5, 30))
        if r.status_code == 404:
            log.debug(f"404 at {url} → no file.")
            return None
        r.raise_for_status()

        with dest.open("wb") as fh:
            for chunk in r.iter_content(chunk_size=2**20):  # 1 MiB
                fh.write(chunk)

        log.info(f"Downloaded {dest}")
        return dest

    def load_input(self, context: InputContext):
        asset = context.asset_key.to_python_identifier()
        root = Path(self.base_dir) / asset
        if not root.exists():
            return None

        files = sorted(root.rglob("*.parquet"))
        if not files:
            return None

        return pl.concat([pl.read_parquet(p) for p in files])
