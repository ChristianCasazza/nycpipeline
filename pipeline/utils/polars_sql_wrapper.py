from __future__ import annotations

import glob
from pathlib import Path
from typing import Iterable, Sequence

import polars as pl
from rich.console import Console
from rich.table import Table


class PolarsSQLWrapper:
    """
    Tiny helper around `polars.SQLContext`.
      • register single-file or Hive-partitioned datasets (lazy)
      • keep a catalogue → `show_tables()`
      • run ad-hoc SQL with `.run_query(sql)`
      • pull a registered table as a LazyFrame via `.lazy(name)`
    """

    def __init__(self) -> None:
        self.ctx = pl.SQLContext()
        # _catalogue maps each view name → the underlying LazyFrame
        self._catalogue: dict[str, pl.LazyFrame] = {}
        self.console = Console()

    def _register(self, name: str, lf: pl.LazyFrame) -> None:
        """Register in both SQLContext and our local catalogue."""
        self.ctx.register(name, lf)
        self._catalogue[name] = lf

    def register_data_view(
        self,
        paths: Sequence[Path | str],
        table_names: Sequence[str],
    ) -> None:
        """Register each single-file dataset under the given table name."""
        if len(paths) != len(table_names):
            raise ValueError("len(paths) must equal len(table_names)")

        for p, name in zip(paths, table_names):
            p = Path(p)
            pattern = str(p)
            matches = glob.glob(pattern)
            if not matches:
                self.console.print(
                    f"[yellow]Skipping {name} – no files for pattern {pattern}"
                )
                continue

            ext = p.suffix.lower()
            try:
                if ext == ".parquet":
                    lf = pl.scan_parquet(pattern)
                elif ext == ".csv":
                    lf = pl.scan_csv(pattern)
                elif ext == ".json":
                    lf = pl.scan_ndjson(pattern)
                else:
                    raise ValueError(f"Unsupported file type: {ext}")

                self._register(name, lf)
                self.console.print(
                    f"[green]View '{name}' registered ({len(matches)} file(s))"
                )
            except Exception as e:
                self.console.print(f"[red]Skipping {name}: {e}")

    def register_partitioned_data_view(
        self,
        base_path: Path | str,
        table_name: str,
        wildcard: str = "year=*/month=*/*.parquet",
    ) -> None:
        """Register a Hive-partitioned dataset under table_name."""
        base_path = Path(base_path)
        pattern = str(base_path / wildcard)
        if not glob.glob(pattern):
            self.console.print(
                f"[yellow]Skipping {table_name} – no parquet files for {pattern}"
            )
            return

        lf = pl.scan_parquet(pattern, hive_partitioning=True)
        self._register(table_name, lf)
        self.console.print(
            f"[green]Partitioned view '{table_name}' registered"
        )

    def bulk_register_data(
        self,
        repo_root: Path | str,
        base_path: str,
        table_names: Iterable[str],
        wildcard: str = "*.parquet",
    ) -> None:
        """Bulk-register single-file datasets under each name in table_names."""
        repo_root = Path(repo_root)
        paths = [repo_root / base_path / name / wildcard for name in table_names]
        self.register_data_view(paths, table_names)

    def bulk_register_partitioned_data(
        self,
        repo_root: Path | str,
        base_path: str,
        table_names: Iterable[str],
        wildcard: str = "year=*/month=*/*.parquet",
    ) -> None:
        """Bulk-register Hive-partitioned datasets."""
        repo_root = Path(repo_root)
        for name in table_names:
            self.register_partitioned_data_view(
                base_path=repo_root / base_path / name,
                table_name=name,
                wildcard=wildcard,
            )

    def show_tables(self) -> None:
        """Print all registered views and whether they’re lazy."""
        table = Table(title="Polars-SQL registered views", show_lines=True)
        table.add_column("View name", style="bold yellow")
        table.add_column("Lazy ?", justify="center", style="cyan")

        for name, lf in self._catalogue.items():
            table.add_row(name, str(isinstance(lf, pl.LazyFrame)))

        self.console.print(table)

    def run_query(
        self, sql: str, collect: bool = True
    ) -> pl.DataFrame | pl.LazyFrame:
        """
        Execute a SQL string against the registered datasets.

        If collect=True, returns a materialized DataFrame; else returns LazyFrame.
        """
        result = self.ctx.execute(sql)
        return result.collect() if collect else result

    def lazy(self, table_name: str) -> pl.LazyFrame:
        """
        Return the registered LazyFrame for `table_name`.
        Allows you to bypass SQL and use the full Polars DataFrame API.
        """
        try:
            return self._catalogue[table_name]
        except KeyError:
            raise KeyError(f"No table registered as '{table_name}'")
