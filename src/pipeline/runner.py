import logging
from typing import Callable, Mapping, Sequence

import pandas as pd

from src.transform.fk_validator import ForeignKeyRule, ForeignKeyValidationService
from src.transform.registry import TransformerRegistry

class PipelineRunner:
    """Orchestrates bronze extraction, transformations, FK checks, and silver loading."""

    def __init__(
        self,
        extract_csv: Callable[[str], pd.DataFrame],
        transformer_registry: TransformerRegistry,
        fk_validation_service: ForeignKeyValidationService,
        save_to_silver: Callable[[pd.DataFrame, str], None],
        send_to_quarantine: Callable[[pd.DataFrame, str, str], None],
        bronze_files: Mapping[str, str] | None = None,
        fk_rules: Sequence[ForeignKeyRule] | None = None,
    ) -> None:
        self._extract_csv = extract_csv
        self._transformer_registry = transformer_registry
        self._fk_validation_service = fk_validation_service
        self._save_to_silver = save_to_silver
        self._send_to_quarantine = send_to_quarantine
        self._bronze_files = dict(bronze_files) if bronze_files else self.default_bronze_files()
        self._fk_rules = list(fk_rules) if fk_rules else self.default_fk_rules()

    @staticmethod
    def default_bronze_files() -> dict[str, str]:
        return {
            "categories": "data/bronze/categories.csv",
            "customers": "data/bronze/customers.csv",
            "employees": "data/bronze/employees.csv",
            "order_details": "data/bronze/orderdetails.csv",
            "orders": "data/bronze/orders.csv",
            "products": "data/bronze/products.csv",
            "shippers": "data/bronze/shippers.csv",
            "suppliers": "data/bronze/suppliers.csv",
        }

    @staticmethod
    def default_fk_rules() -> list[ForeignKeyRule]:
        return [
            ForeignKeyRule("orders", "customer_id", "customers"),
            ForeignKeyRule("orders", "employee_id", "employees"),
            ForeignKeyRule("orders", "shipper_id", "shippers"),
            ForeignKeyRule("order_details", "order_id", "orders"),
            ForeignKeyRule("order_details", "product_id", "products"),
            ForeignKeyRule("products", "supplier_id", "suppliers"),
            ForeignKeyRule("products", "category_id", "categories"),
        ]

    def _extract_bronze(self) -> dict[str, pd.DataFrame]:
        raw_dataframes: dict[str, pd.DataFrame] = {}
        for table_name, file_path in self._bronze_files.items():
            raw_dataframes[table_name] = self._extract_csv(file_path)
        return raw_dataframes

    def _pre_quarantine(self, raw_dataframes: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
        order_details = raw_dataframes["order_details"]
        negative_qty_mask = order_details["quantity"].astype(float) < 0

        if negative_qty_mask.any():
            self._send_to_quarantine(
                order_details[negative_qty_mask],
                table_name="order_details",
                reason="negative quantity value",
            )

        raw_dataframes["order_details"] = order_details[~negative_qty_mask]
        return raw_dataframes

    def _transform(self, raw_dataframes: Mapping[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
        return self._transformer_registry.transform_all(raw_dataframes)

    def _validate_foreign_keys(
        self,
        transformed_dataframes: dict[str, pd.DataFrame],
    ) -> dict[str, pd.DataFrame]:
        return self._fk_validation_service.apply_rules(transformed_dataframes, self._fk_rules)

    def _save_silver(self, transformed_dataframes: Mapping[str, pd.DataFrame]) -> None:
        for table_name, dataframe in transformed_dataframes.items():
            self._save_to_silver(dataframe, table_name)

    def run(self) -> dict[str, pd.DataFrame]:
        raw_dataframes = self._extract_bronze()
        raw_dataframes = self._pre_quarantine(raw_dataframes)

        transformed_dataframes = self._transform(raw_dataframes)
        transformed_dataframes = self._validate_foreign_keys(transformed_dataframes)

        self._save_silver(transformed_dataframes)
        logging.info("Pipeline completed successfully.")
        return transformed_dataframes
