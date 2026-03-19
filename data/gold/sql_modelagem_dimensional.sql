PRAGMA foreign_keys = ON;
BEGIN TRANSACTION;

DROP TABLE IF EXISTS dim_customer;
DROP TABLE IF EXISTS dim_product;
DROP TABLE IF EXISTS dim_employee;
DROP TABLE IF EXISTS dim_shipper;
DROP TABLE IF EXISTS dim_date;
DROP TABLE IF EXISTS fact_sales;

CREATE TABLE dim_customer (
    customer_key INTEGER PRIMARY KEY,
    customer_id INTEGER,
    customer_name TEXT,
    contact_name TEXT,
    city TEXT,
    postal_code TEXT,
    country TEXT
);

CREATE TABLE dim_product (
    product_key INTEGER PRIMARY KEY,
    product_id INTEGER,
    product_name TEXT,
    category_name TEXT,
    supplier_name TEXT,
    unit TEXT,
    price DECIMAL(10,2)
);

CREATE TABLE dim_employee (
    employee_key INTEGER PRIMARY KEY,
    employee_id INTEGER,
    first_name TEXT,
    last_name TEXT,
    birth_date DATE
);

CREATE TABLE dim_shipper (
    shipper_key INTEGER PRIMARY KEY,
    shipper_id INTEGER,
    shipper_name TEXT,
    phone TEXT
);

CREATE TABLE dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE,
    day INTEGER,
    month INTEGER,
    month_name TEXT,
    quarter INTEGER,
    year INTEGER,
    weekday TEXT
);

CREATE TABLE fact_sales ( 
    sales_key INTEGER PRIMARY KEY,
    date_key INTEGER,
    customer_key INTEGER,
    product_key INTEGER,
    employee_key INTEGER,
    shipper_key INTEGER,
    quantity INTEGER,
    unit_price DECIMAL(10,2),
    revenue DECIMAL(10,2),

    CONSTRAINT fk_sales_date
        FOREIGN KEY (date_key)
        REFERENCES dim_date(date_key),

    CONSTRAINT fk_sales_customer
        FOREIGN KEY (customer_key)
        REFERENCES dim_customer(customer_key),

    CONSTRAINT fk_sales_product
        FOREIGN KEY (product_key)
        REFERENCES dim_product(product_key),

    CONSTRAINT fk_sales_employee
        FOREIGN KEY (employee_key)
        REFERENCES dim_employee(employee_key),

    CONSTRAINT fk_sales_shipper
        FOREIGN KEY (shipper_key)
        REFERENCES dim_shipper(shipper_key)

);

COMMIT;