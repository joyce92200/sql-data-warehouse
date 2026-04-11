--- creation, data cleansing, data ingestion of silver.crm_cust_info
IF OBJECT_ID('silver.crm_cust_info', 'U') IS NOT NULL
DROP TABLE silver.crm_cust_info;

CREATE TABLE silver.crm_cust_info (
   cst_id INT,
   cst_key NVARCHAR(50),
   cst_firstname NVARCHAR(50),
   cst_lastname NVARCHAR(50),
   cst_material_status NVARCHAR(50),
   cst_gndr NVARCHAR(50),  
   cst_create_date DATE,
   dwh_create_date DATETIME2 DEFAULT GETDATE()
);
--- creation, data cleansing, data ingestion of silver.crm_prd_info
IF OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL
DROP TABLE silver.crm_prd_info;

CREATE TABLE silver.crm_prd_info (
   prd_id INT,
   category_id NVARCHAR(50),
   prd_key NVARCHAR(50),
   prd_nm NVARCHAR(50),
   prd_cost INT,
   prd_line NVARCHAR(50),
   prd_start_dt DATE,
   prd_end_dt DATE,
   dwh_create_date DATETIME2 DEFAULT GETDATE()
);

INSERT INTO silver.crm_prd_info (
   prd_id,
   category_id,
   prd_key,
   prd_nm,
   prd_cost,
   prd_line,
   prd_start_dt,
   prd_end_dt
)

SELECT
   prd_id,
   REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS category_id,
   SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
   prd_nm,
   ISNULL(prd_cost,0) AS prd_cost,
CASE UPPER(TRIM(prd_line))
	WHEN 'M' THEN 'Mountain'
	WHEN 'R' THEN 'Road'
	WHEN 'S' THEN 'Other Sales'
	WHEN 'T' THEN 'Touring'
	ELSE 'n/a'
END 
   AS prd_line,
CAST(
   prd_start_dt 
   AS DATE
   ) AS prd_start_dt,
CAST(
   LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt ) -1 
   AS DATE
   ) AS prd_end_dt --- calculate end date as one day before the next start date
FROM bronze.crm_prd_info
--end

--- creation, data cleansing, data ingestion of silver.crm_sales_details
IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
DROP TABLE silver.crm_sales_details;

CREATE TABLE silver.crm_sales_details (
   sls_ord_num NVARCHAR(50),
   sls_prd_key NVARCHAR(50),
   sls_cust_id INT,
   sls_order_dt NVARCHAR(50),   
   sls_ship_dt NVARCHAR(50),    
   sls_due_dt NVARCHAR(50),     
   sls_sales INT,
   sls_quantity INT,
   sls_price INT,
   dwh_create_date DATETIME2 DEFAULT GETDATE()
);

SELECT 
NULLIF(sls_due_dt, 0) AS sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0 
OR LEN(sls_due_dt) != 8 
OR sls_due_dt > 20500101
OR sls_due_dt < 19000101

SELECT 
   sls_ord_num,
   sls_prd_key,
   sls_cust_id,
   CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
   ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) 
   END AS sls_order_dt, 
   CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
   ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) 
   END AS sls_ship_dt,    
   CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
   ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) 
   END AS sls_due_dt,
   sls_sales,
   sls_quantity,
   sls_price
FROM bronze.crm_sales_details




--end


IF OBJECT_ID('silver.erp_CUST_AZ12', 'U') IS NOT NULL
DROP TABLE silver.erp_cust_az12;

CREATE TABLE silver.erp_cust_az12 (
CID NVARCHAR(50),
BDATE NVARCHAR(50),
GEN NVARCHAR(10),
dwh_create_date DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID('silver.erp_LOC_A101', 'U') IS NOT NULL
DROP TABLE silver.erp_loc_a101;

CREATE TABLE silver.erp_loc_a101 (
CID NVARCHAR(50),
CNTRY NVARCHAR(50),
dwh_create_date DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID('silver.erp_PX_CAT_G1V2', 'U') IS NOT NULL
DROP TABLE silver.erp_px_cat_g1v2;

CREATE TABLE silver.erp_px_cat_g1v2 (
ID NVARCHAR(50),
CAT NVARCHAR(50),
SUBCAT NVARCHAR(50),
MAINTENANCE NVARCHAR(50),
dwh_create_date DATETIME2 DEFAULT GETDATE()
);

-- (template) check for unwanted spaces
-- expectation: no result

SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

-- (template) check for NULLs or negative numbers
-- expectation : no result 
SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL

-- (tempalte) data standarization & consistency
SELECT DISTINCT prd_line
FROM bronze.crm_prd_info

-- (template) check for NULLs or Duplicates in Primary Key
-- Expectation : No Result
SELECT
cst_id,
COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL

-- (template) data cleansing
INSERT INTO silver.crm_cust_info(
   cst_id,
   cst_key,
   cst_firstname,
   cst_lastname,
   cst_marital_status,
   cst_gndr,
   cst_create_date)
   
SELECT 
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname_clean,
TRIM(cst_lastname) AS cst_lastname_clean,
CASE 
    WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
    WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married' 
    ELSE 'n/a' 
END AS cst_marital_status_clean,
CASE
    WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
    WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male' 
    ELSE 'n/a' 
END AS cst_gndr_clean,
cst_create_date
FROM (
   SELECT *, 
   ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
   FROM bronze.crm_cust_info
   ) AS temp
WHERE flag_last = 1

