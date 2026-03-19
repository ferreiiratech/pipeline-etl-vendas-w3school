import logging
from pathlib import Path
import pandas as pd
from sqlalchemy.engine import Engine
from src.db.database import DatabaseEngineProvider


class GoldCsvRepository:
    """Persists gold-layer dimensional tables to CSV files."""

    def __init__(self, output_dir: str = "data/gold") -> None:
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.logger = logging.getLogger(self.__class__.__name__)

    def save(self, dataframe: pd.DataFrame, table_name: str) -> Path:
        """Save a dataframe as CSV to the gold layer."""
        output_path = self.output_dir / f"{table_name}.csv"
        dataframe.to_csv(output_path, index=False)
        self.logger.info(
            f"Saved table '{table_name}' with {len(dataframe)} rows -> {output_path}"
        )
        return output_path

    def save_all(self, dataframes: dict[str, pd.DataFrame]) -> list[Path]:
        """Save multiple dataframes to CSV."""
        generated_files: list[Path] = []
        for table_name, dataframe in dataframes.items():
            generated_files.append(self.save(dataframe, table_name))
        return generated_files


class GoldPostgresRepository:
    """Persists gold-layer dimensional tables to PostgreSQL database (banco2)."""

    def __init__(
        self,
        engine_provider: DatabaseEngineProvider,
    ) -> None:
        """Initialize PostgreSQL connection for gold layer.
        
        Args:
            env_variables: Mapping with POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER, POSTGRES_PASSWORD
            database: Target database name (default: banco2)
        """
        self.database = engine_provider.database_target
        self.logger = logging.getLogger(self.__class__.__name__)
        self._owns_engine_provider = engine_provider is None
        self._engine_provider = engine_provider
        self.engine: Engine = self._engine_provider.get_engine(self.database)

    def save(self, dataframe: pd.DataFrame, table_name: str) -> None:
        """Save a dataframe to PostgreSQL table.
        
        Args:
            dataframe: DataFrame to persist
            table_name: Name of the target table
        """
        self._engine_provider.save_dataframe(
            dataframe, 
            table_name, 
            self.database
        )

    def save_all(self, dataframes: dict[str, pd.DataFrame]) -> None:
        """Save multiple dataframes to PostgreSQL."""
        for table_name, dataframe in dataframes.items():
            self.save(dataframe, table_name)

    def drop_table(self, table_name: str) -> None:
        """Drop a table from the database."""
        self.engine_provider.drop_table(table_name, self.database)

    def close(self) -> None:
        """Close the database connection."""
        self._engine_provider.dispose(self.database)
