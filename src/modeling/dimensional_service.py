import logging
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd


class DimensionalModelingService:
    """Service to build dimensional tables from normalized silver-layer data."""

    def __init__(self) -> None:
        self.logger = logging.getLogger(self.__class__.__name__)
        self._surrogate_key_counters = {
            "customer_key": 1,
            "product_key": 1,
            "employee_key": 1,
            "shipper_key": 1,
            "sales_key": 1,
        }

    def build_dim_customer(self, customers_df: pd.DataFrame) -> pd.DataFrame:
        """Build dimension table for customers with surrogate keys.
        
        Args:
            customers_df: Silver-layer customers table
            
        Returns:
            Dimensional table with customer_key, customer_id, customer_name, contact_name, city, postal_code, country
        """
        self.logger.info("Building dim_customer...")
        
        df = customers_df.copy()
        
        # Create surrogate key
        df.insert(0, "customer_key", range(1, len(df) + 1))
        
        # Rename columns
        df = df.rename(
            columns={
                "id": "customer_id",
                "name": "customer_name",
            }
        )
        
        # Select relevant columns
        dim_customer = df[
            [
                "customer_key",
                "customer_id",
                "customer_name",
                "contact_name",
                "city",
                "postal_code",
                "country",
            ]
        ]
        
        self.logger.info(f"dim_customer built with {len(dim_customer)} rows")
        return dim_customer

    def build_dim_product(
        self,
        products_df: pd.DataFrame,
        categories_df: pd.DataFrame,
        suppliers_df: pd.DataFrame,
    ) -> pd.DataFrame:
        """Build dimension table for products with surrogate keys.
        
        Joins products with categories and suppliers.
        
        Args:
            products_df: Silver-layer products table
            categories_df: Silver-layer categories table
            suppliers_df: Silver-layer suppliers table
            
        Returns:
            Dimensional table with product_key, product_id, product_name, category_name, supplier_name, unit, price
        """
        self.logger.info("Building dim_product...")
        
        # Copy datasets
        df = products_df.copy()
        df_categories = categories_df.copy()
        df_suppliers = suppliers_df.copy()
        
        # Join products with categories on category_id
        df = df.merge(
            df_categories[["id", "name"]].rename(columns={"id": "cat_id", "name": "category_name"}),
            left_on="category_id",
            right_on="cat_id",
            how="left",
        )
        
        # Join with suppliers on supplier_id
        df = df.merge(
            df_suppliers[["id", "name"]].rename(columns={"id": "supp_id", "name": "supplier_name"}),
            left_on="supplier_id",
            right_on="supp_id",
            how="left",
        )
        
        # Create surrogate key
        df.insert(0, "product_key", range(1, len(df) + 1))
        
        # Select and rename relevant columns (id refers to product id)
        dim_product = df[
            ["product_key", "id", "product_name", "category_name", "supplier_name", "unit", "price"]
        ].rename(columns={"id": "product_id"})
        
        self.logger.info(f"dim_product built with {len(dim_product)} rows")
        return dim_product

    def build_dim_employee(self, employees_df: pd.DataFrame) -> pd.DataFrame:
        """Build dimension table for employees with surrogate keys.
        
        Args:
            employees_df: Silver-layer employees table
            
        Returns:
            Dimensional table with employee_key, employee_id, first_name, last_name, birth_date
        """
        self.logger.info("Building dim_employee...")
        
        df = employees_df.copy()
        
        # Create surrogate key
        df.insert(0, "employee_key", range(1, len(df) + 1))
        
        # Rename columns
        df = df.rename(columns={"id": "employee_id"})
        
        # Select relevant columns
        dim_employee = df[
            ["employee_key", "employee_id", "first_name", "last_name", "birth_date"]
        ]
        
        self.logger.info(f"dim_employee built with {len(dim_employee)} rows")
        return dim_employee

    def build_dim_shipper(self, shippers_df: pd.DataFrame) -> pd.DataFrame:
        """Build dimension table for shippers with surrogate keys.
        
        Args:
            shippers_df: Silver-layer shippers table
            
        Returns:
            Dimensional table with shipper_key, shipper_id, shipper_name, phone
        """
        self.logger.info("Building dim_shipper...")
        
        df = shippers_df.copy()
        
        # Create surrogate key
        df.insert(0, "shipper_key", range(1, len(df) + 1))
        
        # Rename columns
        df = df.rename(
            columns={
                "id": "shipper_id",
                "name": "shipper_name",
            }
        )
        
        # Select relevant columns
        dim_shipper = df[["shipper_key", "shipper_id", "shipper_name", "phone"]]
        
        self.logger.info(f"dim_shipper built with {len(dim_shipper)} rows")
        return dim_shipper

    def build_dim_date(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """Build artificial date dimension table.
        
        Generates a date dimension from start_date to end_date with day, month, quarter, year, weekday attributes.
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            
        Returns:
            Dimensional table with date_key, full_date, day, month, month_name, quarter, year, weekday
        """
        self.logger.info(f"Building dim_date from {start_date} to {end_date}...")
        
        # Parse dates
        start = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date)
        
        # Generate date range
        date_range = pd.date_range(start=start, end=end, freq="D")
        
        # Create dataframe
        df = pd.DataFrame({"full_date": date_range})
        
        # Create date_key as YYYYMMDD integer
        df["date_key"] = df["full_date"].dt.strftime("%Y%m%d").astype(int)
        
        # Extract date attributes
        df["day"] = df["full_date"].dt.day
        df["month"] = df["full_date"].dt.month
        df["month_name"] = df["full_date"].dt.strftime("%B")
        df["quarter"] = df["full_date"].dt.quarter
        df["year"] = df["full_date"].dt.year
        df["weekday"] = df["full_date"].dt.strftime("%A")
        
        # Convert full_date to date only
        df["full_date"] = df["full_date"].dt.date
        
        # Select and reorder columns
        dim_date = df[
            [
                "date_key",
                "full_date",
                "day",
                "month",
                "month_name",
                "quarter",
                "year",
                "weekday",
            ]
        ]
        
        self.logger.info(f"dim_date built with {len(dim_date)} days")
        return dim_date

    def build_fact_sales(
        self,
        orders_df: pd.DataFrame,
        order_details_df: pd.DataFrame,
        products_df: pd.DataFrame,
        dim_customer: pd.DataFrame,
        dim_product: pd.DataFrame,
        dim_employee: pd.DataFrame,
        dim_shipper: pd.DataFrame,
        dim_date: pd.DataFrame,
    ) -> pd.DataFrame:
        """Build fact table for sales transactions.
        
        Grain: One row per order line item (order_details).
        
        Args:
            orders_df: Silver-layer orders table
            order_details_df: Silver-layer order_details table
            products_df: Silver-layer products table (to get unit price)
            dim_customer: Customer dimension
            dim_product: Product dimension
            dim_employee: Employee dimension
            dim_shipper: Shipper dimension
            dim_date: Date dimension
            
        Returns:
            Fact table with sales_key, date_key, customer_key, product_key, employee_key, shipper_key, quantity, unit_price, revenue
        """
        self.logger.info("Building fact_sales...")
        
        # Start with order_details
        fact = order_details_df.copy()
        
        # Join with products to get unit price
        products_subset = products_df[["id", "price"]].rename(columns={"price": "unit_price"})
        fact = fact.merge(
            products_subset,
            left_on="product_id",
            right_on="id",
            how="left",
        )
        
        # Join with orders to get customer_id, employee_id, shipper_id, order_date
        orders_subset = orders_df[["id", "customer_id", "employee_id", "shipper_id", "order_date"]]
        fact = fact.merge(
            orders_subset,
            left_on="order_id",
            right_on="id",
            how="left",
        )
        
        # Create mappings from natural to surrogate keys
        customer_mapping = dim_customer[["customer_key", "customer_id"]].drop_duplicates()
        product_mapping = dim_product[["product_key", "product_id"]].drop_duplicates()
        employee_mapping = dim_employee[["employee_key", "employee_id"]].drop_duplicates()
        shipper_mapping = dim_shipper[["shipper_key", "shipper_id"]].drop_duplicates()
        
        # Create date_key from order_date
        fact["order_date"] = pd.to_datetime(fact["order_date"])
        fact["date_key"] = fact["order_date"].dt.strftime("%Y%m%d").astype(int)
        
        # Map natural keys to surrogate keys
        fact = fact.merge(
            customer_mapping,
            left_on="customer_id",
            right_on="customer_id",
            how="left",
        )
        
        fact = fact.merge(
            product_mapping,
            left_on="product_id",
            right_on="product_id",
            how="left",
        )
        
        fact = fact.merge(
            employee_mapping,
            left_on="employee_id",
            right_on="employee_id",
            how="left",
        )
        
        fact = fact.merge(
            shipper_mapping,
            left_on="shipper_id",
            right_on="shipper_id",
            how="left",
        )
        
        # Calculate revenue = quantity * unit_price
        fact["revenue"] = fact["quantity"] * fact["unit_price"]
        
        # Create sales_key
        fact.insert(0, "sales_key", range(1, len(fact) + 1))
        
        # Select relevant columns
        fact_sales = fact[
            [
                "sales_key",
                "date_key",
                "customer_key",
                "product_key",
                "employee_key",
                "shipper_key",
                "quantity",
                "unit_price",
                "revenue",
            ]
        ]
        
        # Validate no nulls in foreign keys
        fk_columns = ["date_key", "customer_key", "product_key", "employee_key", "shipper_key"]
        for col in fk_columns:
            null_count = fact_sales[col].isna().sum()
            if null_count > 0:
                self.logger.warning(f"Found {null_count} null values in {col}")
        
        self.logger.info(f"fact_sales built with {len(fact_sales)} rows")
        return fact_sales

    def build_all_gold_tables(
        self,
        silver_tables: dict[str, pd.DataFrame],
    ) -> dict[str, pd.DataFrame]:
        """Build all dimensional and fact tables from silver-layer data.
        
        Args:
            silver_tables: Dictionary with keys: categories, customers, employees, orders, order_details, products, shippers, suppliers
            
        Returns:
            Dictionary with keys: dim_customer, dim_product, dim_employee, dim_shipper, dim_date, fact_sales
        """
        self.logger.info("Building all gold-layer tables...")
        
        # Extract minimum and maximum dates from orders
        orders_df = silver_tables["orders"]
        orders_df["order_date"] = pd.to_datetime(orders_df["order_date"])
        min_date = orders_df["order_date"].min().strftime("%Y-%m-%d")
        max_date = orders_df["order_date"].max().strftime("%Y-%m-%d")
        
        # Build dimensions
        dim_customer = self.build_dim_customer(silver_tables["customers"])
        dim_product = self.build_dim_product(
            silver_tables["products"],
            silver_tables["categories"],
            silver_tables["suppliers"],
        )
        dim_employee = self.build_dim_employee(silver_tables["employees"])
        dim_shipper = self.build_dim_shipper(silver_tables["shippers"])
        dim_date = self.build_dim_date(start_date=min_date, end_date=max_date)
        
        # Build fact table
        fact_sales = self.build_fact_sales(
            orders_df,
            silver_tables["order_details"],
            silver_tables["products"],
            dim_customer,
            dim_product,
            dim_employee,
            dim_shipper,
            dim_date,
        )
        
        gold_tables = {
            "dim_customer": dim_customer,
            "dim_product": dim_product,
            "dim_employee": dim_employee,
            "dim_shipper": dim_shipper,
            "dim_date": dim_date,
            "fact_sales": fact_sales,
        }
        
        self.logger.info(f"All gold tables built successfully")
        return gold_tables
