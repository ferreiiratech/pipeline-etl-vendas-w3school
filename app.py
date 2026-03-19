import logging
from src.db.database import DatabaseConnectionSettings, DatabaseEngineProvider
from src.extract.extract_file_csv import extract_file_csv
from src.extract.postgres_csv_extractor import PostgresTableCsvExporter
from src.load.gold import GoldCsvRepository, GoldPostgresRepository
from src.load.quarantine import QuarantineCsvRepository
from src.load.silver import SilverCsvRepository
from src.modeling.dimensional_service import DimensionalModelingService
from src.pipeline.gold_runner import GoldPipelineRunner
from src.pipeline.runner import PipelineRunner
from src.transform.fk_validator import ForeignKeyValidationService
from src.transform.registry import TransformerRegistry
from src.utils.env.index import EnvVariables

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

if __name__ == "__main__":
    env_variables = EnvVariables.from_environment()
    connection_settings = DatabaseConnectionSettings.from_env_variables(env_variables)
    engine_provider = DatabaseEngineProvider(connection_settings)

    try:
    # Extract from Postgres 
        postgresTableCsvExporter = PostgresTableCsvExporter(
            output_dir="data/bronze",
            engine_provider=engine_provider
        )
        postgresTableCsvExporter.export_all_tables_to_csv()

        # Initiate repositories to save to silver and quarantine 
        quarantine_repository = QuarantineCsvRepository(output_dir="data/quarantine")
        silver_repository = SilverCsvRepository(output_dir="data/silver")

        # Initiate FK validation service 
        fk_validation_service = ForeignKeyValidationService(
            quarantine_handler=quarantine_repository.send,
        )

        # Initiate transformer registry 
        transformer_registry = TransformerRegistry()

        # Run the pipeline 
        # The runner will extract CSV files from the bronze layer,
        # apply transformations, validate foreign keys, and save
        # valid records to the silver layer while sending
        # invalid records to quarantine.
        runner = PipelineRunner(
            extract_csv=extract_file_csv,
            transformer_registry=transformer_registry,
            fk_validation_service=fk_validation_service,
            save_to_silver=silver_repository.save,
            send_to_quarantine=quarantine_repository.send,
        )

        runner.run()

        # Run gold pipeline (silver -> gold dimensional modeling) 
        # The gold runner will:
        # 1. Extract normalized tables from silver layer
        # 2. Build dimensional tables (dim_customer, dim_product, dim_employee, dim_shipper, dim_date)
        # 3. Build fact table (fact_sales)
        # 4. Persist to CSV (data/gold/) and PostgreSQL (banco2)
        gold_csv_repository = GoldCsvRepository(output_dir="data/gold")
        gold_postgres_repository = GoldPostgresRepository(
            engine_provider=engine_provider,
        )

        dimensional_service = DimensionalModelingService()

        gold_runner = GoldPipelineRunner(
            extract_csv=extract_file_csv,
            dimensional_service=dimensional_service,
            save_to_csv=gold_csv_repository.save,
            save_to_postgres=gold_postgres_repository.save,
        )

        gold_runner.run()
    finally:
        engine_provider.dispose_all()