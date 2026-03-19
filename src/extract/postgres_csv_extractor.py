import logging
import pandas as pd
from pathlib import Path
from typing import List
from sqlalchemy import text
from sqlalchemy.engine import Engine
from src.db.database import DatabaseEngineProvider

class PostgresTableCsvExporter:
	"""Exports PostgreSQL tables as CSV files."""

	def __init__(
		self,
		output_dir: str,
		engine_provider: DatabaseEngineProvider,
	) -> None:
		self._owns_engine_provider = engine_provider is None
		self._engine_provider = engine_provider
		self.database = engine_provider.database_source
		self.schema = engine_provider.default_schema
		self.output_dir = Path(output_dir)
		self.output_dir.mkdir(parents=True, exist_ok=True)
		self.engine: Engine = self._create_sqlalchemy_engine()

	def _create_sqlalchemy_engine(self) -> Engine:
		return self._engine_provider.get_engine(self.database)

	def export_table_to_csv(self, table_name: str) -> Path:
		"""Exports one PostgreSQL table to a CSV file."""
		result_query = self._engine_provider.query(
			text(f'SELECT * FROM "{self.schema}"."{table_name}"'),
			self.database
		)
		df = pd.DataFrame(result_query)

		output_path = self.output_dir / f"{table_name}.csv"
		df.to_csv(output_path, index=False)

		logging.info(f"Exported table '{table_name}' with {len(df)} rows -> {output_path}")
		return output_path

	def export_all_tables_to_csv(self) -> None:
		"""Exports all schema tables to CSV files."""
		generated_files: List[Path] = []
		list_table_names = self._engine_provider.list_tables(self.database)

		for table_name in list_table_names:
			generated_files.append(self.export_table_to_csv(table_name))

		logging.info(f"Exported {len(generated_files)} tables to CSV files in '{self.output_dir}' directory.")

	def close_connection(self) -> None:
		if self._owns_engine_provider:
			self._engine_provider.dispose(self.database)