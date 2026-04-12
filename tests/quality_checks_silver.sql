/*
============
Quality Checks
==============
Script Purpose :
  This script performs various quality checks for data consistency, accuracy and standarization across the 'silver' schema. 
  It includes checks for : 
  - Null or duplicate primary keys
  - unwanted spaces in string fields
  - data standarization and consistency
  - invalid data ranges and orders
  - data consistency bewteen related fields

Usage Notes:
  - Run these checks after data loading Silver Layer
  - Investigate and resolve any discrepencies found during the checks
========
*/

-- check for unwanted spaces
-- expectation: no result

SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

CASE UPPER(TRIM(cst_marital_status))
  WHEN 'S' THEN 'Single'
  WHEN 'M' THEN 'Married' 
  ELSE 'n/a' 
END AS cst_marital_status_clean,

-- Check for NULLs or negative numbers
-- expectation : no result 
SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL

-- Data standarization & Consistency
SELECT DISTINCT prd_line
FROM bronze.crm_prd_info

-- Check for Invalid Date Orders
SELECT * 
FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt

-- identify out-of-range dates
SELECT DISTINCT
bdate
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE()

-- Check for NULLs or Duplicates in Primary Key
-- Expectation : No Result
SELECT
cst_id,
COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL

-- Check for duplicates in primary keys
FROM (
   SELECT *, 
   ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
   FROM bronze.crm_cust_info
   ) AS temp
WHERE flag_last = 1

-- checking for invalid data orders
SELECT *
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt

-- identify out-of-range dates
SELECT DISTINCT
bdate
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE()

