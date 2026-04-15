/*
================
DDL Script: Create Gold Views
================
Script Purpose : 
	This script creates views for the Gold layer in the data warehouse.
	The Gold layer represents the final dimension and fact table (Star schema)

	Each view performs transformations and combines data from the Silver layer
	to produce a clean, enriched, and business-ready dataset.

Usage : 
	- These views can be queired directly for analytics and reporting
*/


-- create dimension customer

IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
	DROP VIEW gold.dim_customers;
GO

SELECT
ci.cst_id,
ci.cst_key,
ci.cst_firstname,
ci.cst_lastname,
ci.cst_marital_status,
ci.cst_gndr,
ci.cst_create_date,
ca.bdate,
ca.gen,
la.CID,
la.CNTRY
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 AS ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 AS la
ON ci.cst_key = la.CID

-- After joining table, check if any duplicates were introduced by the join logic : GROUP BY subquery

SELECT cst_id, COUNT(*) FROM (
	SELECT
	ci.cst_id,
	ci.cst_key,
	ci.cst_firstname,
	ci.cst_lastname,
	ci.cst_marital_status,
	ci.cst_gndr,
	ci.cst_create_date,
	ca.bdate,
	ca.gen,
	la.CID,
	la.CNTRY
	FROM silver.crm_cust_info AS ci
	LEFT JOIN silver.erp_cust_az12 AS ca
	ON ci.cst_key = ca.cid
	LEFT JOIN silver.erp_loc_a101 AS la
	ON ci.cst_key = la.CID )t
	GROUP BY cst_id
	HAVING COUNT(*) > 1
	
-- data integration for same columns in different tables - Surrogate Keys, SELECT DISTINCT, CASE WHEN, COALESCE
-- Gold layer : use View instead of Tables (no stored procedures)
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
	DROP VIEW gold.dim_customers;
GO
	
CREATE VIEW gold.dim_customers AS 
SELECT
ROW_NUMBER() OVER(ORDER BY cst_id) AS customer_key, -- surrogate keys - system-generated unique identifier assigned to each record in a table
ci.cst_id AS customer_id,
ci.cst_key AS customer_number,
ci.cst_firstname AS first_name,
ci.cst_lastname AS last_name,
la.cntry AS country,
ci.cst_marital_status AS marital_status,
CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr -- CRM is the Master table for gender Info
ELSE COALESCE(ca.gen, 'n/a')
END AS gender,
ca.bdate AS birthdate,
ci.cst_create_date AS create_date
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 AS ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 AS la
ON ci.cst_key = la.CID

-- Build Gold Layer - Create Dimension Products
-- After joining table, check if any duplicates were introduced by the join logic : GROUP BY subquery
SELECT prd_key, COUNT(*) FROM (
SELECT
	ROW_NUMBER() OVER(ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,
	pn.prd_id AS product_id,
	pn.prd_key AS product_number,
	pn.prd_nm AS product_name,
	pn.category_id AS category_id,
	pc.cat AS category,
	pc.subcat AS subcategory,
	pc.maintenance,
	pn.prd_cost AS cost,
	pn.prd_line AS product_line,
	pn.prd_start_dt AS start_date
FROM silver.crm_prd_info AS pn
LEFT JOIN silver.erp_px_cat_g1v2 AS pc
ON pn.category_id = pc.id
WHERE prd_end_dt IS NULL  )t -- filter out all historical data
GROUP BY prd_key
HAVING COUNT(*) >1

-- Create View Dimension Products
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
	DROP VIEW gold.dim_products;
GO

CREATE VIEW gold.dim_products AS
SELECT
	ROW_NUMBER() OVER(ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,
	pn.prd_id AS product_id,
	pn.prd_key AS product_number,
	pn.prd_nm AS product_name,
	pn.category_id AS category_id,
	pc.cat AS category,
	pc.subcat AS subcategory,
	pc.maintenance,
	pn.prd_cost AS cost,
	pn.prd_line AS product_line,
	pn.prd_start_dt AS start_date
FROM silver.crm_prd_info AS pn
LEFT JOIN silver.erp_px_cat_g1v2 AS pc
ON pn.category_id = pc.id
WHERE prd_end_dt IS NULL

--- Building Fact (Data Lookup)- use the dimension's surrogate keys instead of IDs to easily connect facts with dimensions
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
	DROP VIEW gold.fact_sales;
GO

CREATE VIEW gold.fact_sales AS

SELECT 	
sd.sls_ord_num AS order_number,
pr.product_key,
cu.customer_key,
sd.sls_order_dt AS order_date,
sd.sls_ship_dt AS shipping_date,
sd.sls_due_dt AS due_date,
sd.sls_sales AS sales_amount,
sd.sls_quantity AS quantity,
sd.sls_price AS price
FROM silver.crm_sales_details AS sd
LEFT JOIN gold.dim_products AS pr
ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers AS cu
ON sd.sls_cust_id = cu.customer_id
