import logging
from typing import Callable, Mapping, Optional
import pandas as pd
from src.modeling.dimensional_service import DimensionalModelingService


class GoldPipelineRunner:
    """Orchestrates silver→gold transformation: builds dimensional tables and persists to CSV and PostgreSQL."""

    def __init__(
        self,
        extract_csv: Callable[[str], pd.DataFrame],
        dimensional_service: DimensionalModelingService,
        save_to_csv: Callable[[pd.DataFrame, str], None],
        save_to_postgres: Callable[[pd.DataFrame, str], None],
        silver_files: Mapping[str, str] | None = None,
    ) -> None:
        """Initialize the gold pipeline runner.
        
        Args:
            extract_csv: Callable to read CSV files
            dimensional_service: Service to build dimensional tables
            save_to_csv: Callable to persist DataFrame to CSV (signature: save(df, table_name))
            save_to_postgres: Callable to persist DataFrame to PostgreSQL (signature: save(df, table_name))
            silver_files: Map of table_name -> file_path for silver CSVs
        """
        self._extract_csv = extract_csv
        self._dimensional_service = dimensional_service
        self._save_to_csv = save_to_csv
        self._save_to_postgres = save_to_postgres
        self._silver_files = dict(silver_files) if silver_files else self.default_silver_files()
        self.logger = logging.getLogger(self.__class__.__name__)

    @staticmethod
    def default_silver_files() -> dict[str, str]:
        """Default paths to silver-layer CSV files."""
        return {
            "categories": "data/silver/categories.csv",
            "customers": "data/silver/customers.csv",
            "employees": "data/silver/employees.csv",
            "order_details": "data/silver/order_details.csv",
            "orders": "data/silver/orders.csv",
            "products": "data/silver/products.csv",
            "shippers": "data/silver/shippers.csv",
            "suppliers": "data/silver/suppliers.csv",
        }

    def _extract_silver(self) -> dict[str, pd.DataFrame]:
        """Extract all silver-layer tables."""
        self.logger.info("Extracting silver-layer tables...")
        silver_tables = {}
        
        for table_name, file_path in self._silver_files.items():
            try:
                df = self._extract_csv(file_path)
                silver_tables[table_name] = df
                self.logger.info(f"Extracted '{table_name}' with {len(df)} rows from {file_path}")
            except FileNotFoundError:
                self.logger.error(f"Silver file not found: {file_path}")
                raise
        
        return silver_tables

    def _build_dimensions(self, silver_tables: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
        """Build all dimensional and fact tables from silver data."""
        self.logger.info("Building dimensional tables...")
        
        gold_tables = self._dimensional_service.build_all_gold_tables(silver_tables)
        
        return gold_tables

    def _validate_gold_tables(self, gold_tables: dict[str, pd.DataFrame]) -> bool:
        """Validate gold tables for data quality."""
        self.logger.info("Validating gold tables...")
        
        validation_passed = True
        
        # Validate dimensions have no duplicate surrogate keys
        for dim_name in ["dim_customer", "dim_product", "dim_employee", "dim_shipper", "dim_date"]:
            if dim_name not in gold_tables:
                self.logger.error(f"Missing dimension: {dim_name}")
                validation_passed = False
                continue
            
            df = gold_tables[dim_name]
            key_col = next(col for col in df.columns if col.endswith("_key"))
            
            duplicates = df[key_col].duplicated().sum()
            if duplicates > 0:
                self.logger.error(f"Found {duplicates} duplicate surrogate keys in {dim_name}")
                validation_passed = False
            else:
                self.logger.info(f"{dim_name}: {len(df)} rows, no duplicate keys")
        
        # Validate fact table
        if "fact_sales" in gold_tables:
            fact = gold_tables["fact_sales"]
            
            # Check for null foreign keys
            fk_cols = ["date_key", "customer_key", "product_key", "employee_key", "shipper_key"]
            for col in fk_cols:
                nulls = fact[col].isna().sum()
                if nulls > 0:
                    self.logger.error(f"Found {nulls} null values in fact_sales.{col}")
                    validation_passed = False
            
            # Check for invalid quantities and prices
            invalid_qty = (fact["quantity"] <= 0).sum()
            if invalid_qty > 0:
                self.logger.error(f"Found {invalid_qty} invalid quantities (≤ 0) in fact_sales")
                validation_passed = False
            
            invalid_price = (fact["unit_price"] < 0).sum()
            if invalid_price > 0:
                self.logger.error(f"Found {invalid_price} invalid prices (< 0) in fact_sales")
                validation_passed = False
            
            invalid_revenue = (fact["revenue"] < 0).sum()
            if invalid_revenue > 0:
                self.logger.error(f"Found {invalid_revenue} invalid revenues (< 0) in fact_sales")
                validation_passed = False
            
            if validation_passed:
                self.logger.info(f"fact_sales: {len(fact)} rows, all measures valid")
        
        return validation_passed

    def _save_gold(self, gold_tables: dict[str, pd.DataFrame]) -> None:
        """Persist gold tables to CSV and PostgreSQL."""
        self.logger.info("Persisting gold tables to CSV and PostgreSQL...")
        
        for table_name, df in gold_tables.items():
            # Save to CSV
            try:
                self._save_to_csv(df, table_name)
                self.logger.info(f"Saved '{table_name}' to CSV")
            except Exception as e:
                self.logger.error(f"Failed to save '{table_name}' to CSV: {e}")
                raise
            
            # Save to PostgreSQL
            try:
                self._save_to_postgres(df, table_name)
                self.logger.info(f"Saved '{table_name}' to PostgreSQL (banco2)")
            except Exception as e:
                self.logger.error(f"Failed to save '{table_name}' to PostgreSQL: {e}")
                raise

    def run(self) -> dict[str, pd.DataFrame]:
        """Execute the silver→gold pipeline.
        
        Returns:
            Dictionary of gold tables: dim_customer, dim_product, dim_employee, dim_shipper, dim_date, fact_sales
        """
        self.logger.info("=" * 80)
        self.logger.info("STARTING GOLD PIPELINE RUNNER (silver → gold)")
        self.logger.info("=" * 80)
        
        try:
            # 1. Extract silver tables
            silver_tables = self._extract_silver()
            
            # 2. Build dimensional tables
            gold_tables = self._build_dimensions(silver_tables)
            
            # 3. Validate gold tables
            if not self._validate_gold_tables(gold_tables):
                self.logger.warning("Gold table validation found issues (non-blocking)")
            
            # 4. Persist gold tables
            self._save_gold(gold_tables)
            
            self.logger.info("=" * 80)
            self.logger.info("GOLD PIPELINE COMPLETED SUCCESSFULLY")
            self.logger.info("=" * 80)
            
            return gold_tables
            
        except Exception as e:
            self.logger.error(f"Gold pipeline failed: {e}", exc_info=True)
            raise
