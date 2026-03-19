import logging
from dataclasses import dataclass
from collections.abc import Mapping
import sys
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

@dataclass(frozen=True)
class DatabaseConnectionSettings:
    """Value object with database connection settings."""

    host: str
    port: int
    user: str
    password: str
    database_source: str
    database_target: str
    schema: str

    @classmethod
    def from_env_variables(
        cls,
        env_variables: Mapping[str, str],
    ) -> "DatabaseConnectionSettings":
        required = (
            "DATABASE_HOST", 
            "DATABASE_PORT", 
            "DATABASE_USER", 
            "DATABASE_PASSWORD", 
            "DATABASE_SCHEMA",
            "DATABASE_DB_SOURCE", 
            "DATABASE_DB_TARGET"
        )

        missing = [key for key in required if not env_variables.get(key)]
        if missing:
            raise ValueError(
                f"Missing required database environment variables: {' , '.join(missing)}"
            )

        return cls(
            host=env_variables["DATABASE_HOST"],
            port=int(env_variables["DATABASE_PORT"]),
            user=env_variables["DATABASE_USER"],
            password=env_variables["DATABASE_PASSWORD"],
            database_source=env_variables["DATABASE_DB_SOURCE"],
            database_target=env_variables["DATABASE_DB_TARGET"],
            schema=env_variables["DATABASE_SCHEMA"],
        )

class DatabaseEngineProvider:
    """Creates and reuses SQLAlchemy engines keyed by database name."""

    def __init__(self, settings: DatabaseConnectionSettings) -> None:
        self._settings = settings
        self._engines: dict[str, Engine] = {}
        self._logger = logging.getLogger(self.__class__.__name__)
        self.test_connection(self.database_source)
        self.test_connection(self.database_target)

    @property
    def database_source(self) -> str:
        return self._settings.database_source

    @property
    def database_target(self) -> str:
        return self._settings.database_target

    @property
    def default_schema(self) -> str | None:
        return self._settings.schema

    def _build_connection_url(self, database: str) -> str:
        return (
            f"postgresql+psycopg2://{self._settings.user}:{self._settings.password}"
            f"@{self._settings.host}:{self._settings.port}/{database}"
        )

    def get_engine(self, database: str) -> Engine:
        engine = self._engines.get(database)

        if engine is None:
            connection_url = self._build_connection_url(database)
            engine = create_engine(connection_url)
            self._engines[database] = engine

            self._logger.info(
                "Created SQLAlchemy engine for PostgreSQL database '%s'", database
            )

        return engine

    def test_connection(self, database: str) -> None:
        """Tests the database connection and raises an error if it fails."""
        try:
            with self.get_engine(database).connect() as connection:
                connection.execute(text("SELECT 1"))
            self._logger.info(f"Successfully connected to database '{database}'")
        except Exception as e:
            self._logger.error(f"Failed to connect to database '{database}'.")
            sys.exit(1)

    def list_tables(self, database: str) -> list[str]:
        """Lists tables in the configured schema of the target database."""
        query = text(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = :schema
              AND table_type = 'BASE TABLE'
            ORDER BY table_name
            """
        )

        with self.get_engine(database).connect() as connection:
            result = connection.execute(query, {"schema": self.default_schema})
            return [row[0] for row in result.fetchall()]

    def query(self, sql: text, database: str) -> list[tuple]:
        """Executes a raw SQL query and returns the results."""
        with self.get_engine(database).connect() as connection:
            result = connection.execute(sql)
            return result.fetchall()
        
    def save_dataframe(self, df, table_name: str, database: str) -> None:
        """Saves a DataFrame to a PostgreSQL table using SQLAlchemy."""
        engine = self.get_engine(database)
        df.to_sql(table_name, engine, if_exists="replace", index=False)
        self._logger.info(
            f"Saved table '{table_name}' with {len(df)} rows to PostgreSQL database '{database}'"
        )
    
    def drop_table(self, table_name: str, database: str) -> None:
        """Drops a table from the target database if it exists."""
        query = text(f'DROP TABLE IF EXISTS "{self.default_schema}"."{table_name}" CASCADE')
        
        with self.get_engine(database).connect() as connection:
            connection.execute(query)

            self._logger.info(
                f"Dropped table '{table_name}' from PostgreSQL database '{self.database}' (if it existed)"
            )

    def dispose(self, database: str) -> None:
        engine = self._engines.pop(database, None)

        if engine is not None:
            engine.dispose()
            self._logger.info("Disposed SQLAlchemy engine for database '%s'", database)

    def dispose_all(self) -> None:
        for database, engine in list(self._engines.items()):
            engine.dispose()
            self._logger.info("Disposed SQLAlchemy engine for database '%s'", database)
        
        self._engines.clear()
