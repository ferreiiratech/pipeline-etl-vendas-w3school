from typing import Mapping
import pandas as pd
from src.transform.core.contracts import Transformer
from src.transform.transform_category import CategoryTransformer
from src.transform.transform_customers import CustomersTransformer
from src.transform.transform_employees import EmployeesTransformer
from src.transform.transform_order_details import OrderDetailsTransformer
from src.transform.transform_orders import OrdersTransformer
from src.transform.transform_products import ProductsTransformer
from src.transform.transform_shippers import ShippersTransformer
from src.transform.transform_suppliers import SuppliersTransformer


class TransformerRegistry:
    """Registry and execution facade for entity transformers."""

    @staticmethod
    def default_transformers() -> dict[str, Transformer]:
        return {
            "categories": CategoryTransformer(),
            "customers": CustomersTransformer(),
            "employees": EmployeesTransformer(),
            "order_details": OrderDetailsTransformer(),
            "orders": OrdersTransformer(),
            "products": ProductsTransformer(),
            "shippers": ShippersTransformer(),
            "suppliers": SuppliersTransformer(),
        }

    def transform_table(self, table_name: str, dataframe: pd.DataFrame) -> pd.DataFrame:
        if table_name not in self.default_transformers():
            raise ValueError(f"No transformer registered for table '{table_name}'.")
        
        return self.default_transformers()[table_name].transform(dataframe)

    def transform_all(
            self, 
            raw_dataframes: Mapping[str, pd.DataFrame]
        ) -> dict[str, pd.DataFrame]:

        transformed: dict[str, pd.DataFrame] = {}

        for table_name, transformer in self.default_transformers().items():
            if table_name not in raw_dataframes:
                raise ValueError(f"Missing extracted DataFrame for table '{table_name}'.")
            
            transformed[table_name] = transformer.transform(raw_dataframes[table_name])

        return transformed
