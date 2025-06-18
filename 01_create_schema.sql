-- E-Commerce Data Warehouse Schema
-- Star Schema Design with Fact and Dimension Tables

-- Create database and schema
CREATE SCHEMA IF NOT EXISTS ecommerce_dw;
SET search_path TO ecommerce_dw;

-- Dimension Tables

-- Customer Dimension (SCD Type 2 for historical tracking)
CREATE TABLE dim_customer (
    customer_key SERIAL PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    phone VARCHAR(20),
    date_of_birth DATE,
    gender VARCHAR(10),
    address_line1 VARCHAR(255),
    address_line2 VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(50),
    postal_code VARCHAR(20),
    country VARCHAR(50),
    customer_segment VARCHAR(50), -- VIP, Regular, New, etc.
    registration_date DATE,
    is_active BOOLEAN DEFAULT TRUE,
    effective_date DATE NOT NULL DEFAULT CURRENT_DATE,
    expiry_date DATE DEFAULT '9999-12-31',
    is_current BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Product Dimension
CREATE TABLE dim_product (
    product_key SERIAL PRIMARY KEY,
    product_id VARCHAR(50) UNIQUE NOT NULL,
    product_name VARCHAR(255) NOT NULL,
    brand VARCHAR(100),
    category VARCHAR(100),
    subcategory VARCHAR(100),
    unit_cost DECIMAL(10,2),
    unit_price DECIMAL(10,2),
    supplier VARCHAR(100),
    color VARCHAR(50),
    size VARCHAR(50),
    weight DECIMAL(8,2),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Date Dimension
CREATE TABLE dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE NOT NULL,
    day_of_week INTEGER,
    day_name VARCHAR(20),
    day_of_month INTEGER,
    day_of_year INTEGER,
    week_of_year INTEGER,
    month_number INTEGER,
    month_name VARCHAR(20),
    quarter INTEGER,
    year INTEGER,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN,
    fiscal_year INTEGER,
    fiscal_quarter INTEGER
);

-- Geography Dimension
CREATE TABLE dim_geography (
    geography_key SERIAL PRIMARY KEY,
    city VARCHAR(100),
    state VARCHAR(50),
    country VARCHAR(50),
    region VARCHAR(50),
    continent VARCHAR(50),
    timezone VARCHAR(50)
);

-- Sales Channel Dimension
CREATE TABLE dim_channel (
    channel_key SERIAL PRIMARY KEY,
    channel_id VARCHAR(50) UNIQUE NOT NULL,
    channel_name VARCHAR(100) NOT NULL,
    channel_type VARCHAR(50), -- Online, Mobile, Store, Partner
    channel_description TEXT
);

-- Fact Tables

-- Sales Fact Table (main transactional data)
CREATE TABLE fact_sales (
    sales_key SERIAL PRIMARY KEY,
    order_id VARCHAR(50) NOT NULL,
    order_line_id VARCHAR(50) NOT NULL,
    customer_key INTEGER REFERENCES dim_customer(customer_key),
    product_key INTEGER REFERENCES dim_product(product_key),
    date_key INTEGER REFERENCES dim_date(date_key),
    geography_key INTEGER REFERENCES dim_geography(geography_key),
    channel_key INTEGER REFERENCES dim_channel(channel_key),
    
    -- Measures
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    discount_amount DECIMAL(10,2) DEFAULT 0,
    tax_amount DECIMAL(10,2) DEFAULT 0,
    shipping_cost DECIMAL(10,2) DEFAULT 0,
    total_amount DECIMAL(12,2) NOT NULL,
    profit_margin DECIMAL(10,2),
    
    -- Flags
    is_returned BOOLEAN DEFAULT FALSE,
    is_exchanged BOOLEAN DEFAULT FALSE,
    
    -- Timestamps
    order_timestamp TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Customer Behavior Fact Table (aggregated metrics)
CREATE TABLE fact_customer_behavior (
    behavior_key SERIAL PRIMARY KEY,
    customer_key INTEGER REFERENCES dim_customer(customer_key),
    date_key INTEGER REFERENCES dim_date(date_key),
    
    -- Behavioral Metrics
    website_visits INTEGER DEFAULT 0,
    pages_viewed INTEGER DEFAULT 0,
    time_on_site INTEGER DEFAULT 0, -- in minutes
    cart_additions INTEGER DEFAULT 0,
    cart_abandonments INTEGER DEFAULT 0,
    email_opens INTEGER DEFAULT 0,
    email_clicks INTEGER DEFAULT 0,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for Performance Optimization
CREATE INDEX idx_fact_sales_customer ON fact_sales(customer_key);
CREATE INDEX idx_fact_sales_product ON fact_sales(product_key);
CREATE INDEX idx_fact_sales_date ON fact_sales(date_key);
CREATE INDEX idx_fact_sales_order ON fact_sales(order_id);
CREATE INDEX idx_fact_sales_timestamp ON fact_sales(order_timestamp);

CREATE INDEX idx_dim_customer_id ON dim_customer(customer_id);
CREATE INDEX idx_dim_customer_email ON dim_customer(email);
CREATE INDEX idx_dim_customer_current ON dim_customer(is_current);

CREATE INDEX idx_dim_product_id ON dim_product(product_id);
CREATE INDEX idx_dim_product_category ON dim_product(category);
CREATE INDEX idx_dim_product_active ON dim_product(is_active);

CREATE INDEX idx_dim_date_full ON dim_date(full_date);
CREATE INDEX idx_dim_date_year_month ON dim_date(year, month_number);

-- Constraints and Data Quality Rules
ALTER TABLE fact_sales ADD CONSTRAINT chk_quantity_positive CHECK (quantity > 0);
ALTER TABLE fact_sales ADD CONSTRAINT chk_unit_price_positive CHECK (unit_price >= 0);
ALTER TABLE fact_sales ADD CONSTRAINT chk_total_amount_positive CHECK (total_amount >= 0);

ALTER TABLE dim_customer ADD CONSTRAINT chk_email_format CHECK (email ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$');
ALTER TABLE dim_product ADD CONSTRAINT chk_price_positive CHECK (unit_price >= 0 AND unit_cost >= 0); 