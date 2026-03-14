--  H&M Business Objectives 

-- OBJECTIVE 1: Average sale price per category per year
SELECT year, index_group_name AS category,
       COUNT(*) AS num_transactions,
       ROUND(AVG(price)::NUMERIC,4) AS avg_unit_price,
       ROUND(SUM(price)::NUMERIC,2) AS total_revenue
FROM v_sales_enriched
GROUP BY year, index_group_name
ORDER BY year, total_revenue DESC;

-- OBJECTIVE 2: Top 20 best-selling articles
SELECT article_id, prod_name, product_type_name, index_group_name,
       COUNT(*) AS times_purchased,
       ROUND(SUM(price)::NUMERIC,2) AS total_revenue,
       ROUND(AVG(price)::NUMERIC,4) AS avg_price
FROM v_sales_enriched
GROUP BY article_id, prod_name, product_type_name, index_group_name
ORDER BY times_purchased DESC LIMIT 20;

-- OBJECTIVE 3: Monthly revenue trend
SELECT year, month, month_name,
       COUNT(*) AS num_transactions,
       COUNT(DISTINCT customer_id) AS unique_customers,
       ROUND(SUM(price)::NUMERIC,2) AS total_revenue,
       ROUND(AVG(price)::NUMERIC,4) AS avg_price
FROM v_sales_enriched
GROUP BY year, month, month_name
ORDER BY year, month;

-- OBJECTIVE 4: Customer segmentation by age group
SELECT age_group, club_member_status,
       COUNT(DISTINCT customer_id) AS unique_customers,
       COUNT(*) AS num_purchases,
       ROUND(SUM(price)::NUMERIC,2) AS total_revenue,
       ROUND(AVG(price)::NUMERIC,4) AS avg_spend_per_purchase
FROM v_sales_enriched
GROUP BY age_group, club_member_status
ORDER BY total_revenue DESC;

-- OBJECTIVE 5: Online vs In-Store by department
SELECT department_name, channel_name,
       COUNT(*) AS num_transactions,
       ROUND(SUM(price)::NUMERIC,2) AS total_revenue,
       ROUND(AVG(price)::NUMERIC,4) AS avg_price
FROM v_sales_enriched
GROUP BY department_name, channel_name
ORDER BY department_name, total_revenue DESC;

-- OBJECTIVE 6: Customer loyalty and repeat purchases
WITH cs AS (
    SELECT customer_id, age_group, club_member_status,
           COUNT(*) AS total_purchases,
           ROUND(SUM(price)::NUMERIC,2) AS lifetime_value
    FROM v_sales_enriched
    GROUP BY customer_id, age_group, club_member_status
)
SELECT age_group, club_member_status,
       COUNT(*) AS total_customers,
       ROUND(AVG(total_purchases),2) AS avg_purchases,
       ROUND(AVG(lifetime_value)::NUMERIC,2) AS avg_lifetime_value,
       SUM(CASE WHEN total_purchases > 1 THEN 1 ELSE 0 END) AS repeat_buyers,
       SUM(CASE WHEN total_purchases > 5 THEN 1 ELSE 0 END) AS loyal_buyers
FROM cs
GROUP BY age_group, club_member_status
ORDER BY avg_lifetime_value DESC;
