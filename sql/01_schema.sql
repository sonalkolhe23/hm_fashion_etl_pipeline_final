
--  H&M Fashion Retail — PostgreSQL Schema
--  Tables: dim_articles, dim_customers, dim_date, fact_sales
--  View:   v_sales_enriched


DROP TABLE IF EXISTS fact_sales   CASCADE;
DROP TABLE IF EXISTS dim_articles  CASCADE;
DROP TABLE IF EXISTS dim_customers CASCADE;
DROP TABLE IF EXISTS dim_date      CASCADE;

-- ── Articles (from Object Storage) ────────────────────────────
CREATE TABLE dim_articles (
    article_id                    VARCHAR(20)  PRIMARY KEY,
    product_code                  VARCHAR(20),
    prod_name                     VARCHAR(255),
    product_type_no               INTEGER,
    product_type_name             VARCHAR(100),
    product_group_name            VARCHAR(100),
    graphical_appearance_no       INTEGER,
    graphical_appearance_name     VARCHAR(100),
    colour_group_code             VARCHAR(10),
    colour_group_name             VARCHAR(100),
    perceived_colour_value_name   VARCHAR(100),
    perceived_colour_master_name  VARCHAR(100),
    department_no                 INTEGER,
    department_name               VARCHAR(100),
    index_code                    VARCHAR(10),
    index_name                    VARCHAR(100),
    index_group_no                INTEGER,
    index_group_name              VARCHAR(100),
    section_no                    INTEGER,
    section_name                  VARCHAR(100),
    garment_group_no              INTEGER,
    garment_group_name            VARCHAR(100),
    detail_desc                   TEXT
);

-- ── Customers (from Object Storage) ───────────────────────────
CREATE TABLE dim_customers (
    customer_id              VARCHAR(80)  PRIMARY KEY,
    fn                       FLOAT,
    active                   FLOAT,
    club_member_status       VARCHAR(255),
    fashion_news_frequency   VARCHAR(255),
    age                      FLOAT,
    postal_code              VARCHAR(255),
    age_group                VARCHAR(20)
);

-- ── Date dimension ─────────────────────────────────────────────
CREATE TABLE dim_date (
    date_id      DATE        PRIMARY KEY,
    year         INTEGER,
    quarter      INTEGER,
    month        INTEGER,
    month_name   VARCHAR(20),
    week         INTEGER,
    day_of_week  INTEGER,
    day_name     VARCHAR(20),
    is_weekend   BOOLEAN
);

-- ── Fact Sales (from On-Premise transactions file) ─────────────
CREATE TABLE fact_sales (
    sale_id          BIGSERIAL    PRIMARY KEY,
    customer_id      VARCHAR(80)  NOT NULL REFERENCES dim_customers(customer_id),
    article_id       VARCHAR(20)  NOT NULL REFERENCES dim_articles(article_id),
    transaction_date DATE         NOT NULL REFERENCES dim_date(date_id),
    price            NUMERIC(10,4),
    sales_channel    INTEGER,
    source_file      VARCHAR(255)
);

-- ── Indexes ────────────────────────────────────────────────────
CREATE INDEX idx_fact_customer  ON fact_sales(customer_id);
CREATE INDEX idx_fact_article   ON fact_sales(article_id);
CREATE INDEX idx_fact_date      ON fact_sales(transaction_date);
CREATE INDEX idx_fact_channel   ON fact_sales(sales_channel);
CREATE INDEX idx_art_dept       ON dim_articles(department_name);
CREATE INDEX idx_art_group      ON dim_articles(index_group_name);
CREATE INDEX idx_art_garment    ON dim_articles(garment_group_name);
CREATE INDEX idx_cust_age       ON dim_customers(age_group);
CREATE INDEX idx_cust_club      ON dim_customers(club_member_status);

-- ── Enriched view (joins all 4 tables) ────────────────────────
CREATE OR REPLACE VIEW v_sales_enriched AS
SELECT
    f.sale_id,
    f.transaction_date,
    d.year,
    d.quarter,
    d.month,
    d.month_name,
    d.week,
    d.is_weekend,
    a.article_id,
    a.prod_name,
    a.product_type_name,
    a.product_group_name,
    a.department_name,
    a.index_name,
    a.index_group_name,
    a.garment_group_name,
    a.colour_group_name,
    a.section_name,
    c.customer_id,
    c.age,
    c.age_group,
    c.club_member_status,
    c.fashion_news_frequency,
    f.price,
    f.sales_channel,
    CASE f.sales_channel
        WHEN 1 THEN 'Online'
        WHEN 2 THEN 'In-Store'
        ELSE 'Unknown'
    END AS channel_name
FROM fact_sales   f
JOIN dim_articles  a ON f.article_id       = a.article_id
JOIN dim_customers c ON f.customer_id      = c.customer_id
JOIN dim_date      d ON f.transaction_date = d.date_id;
