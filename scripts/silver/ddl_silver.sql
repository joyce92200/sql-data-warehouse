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

IF OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL
   DROP TABLE silver.crm_prd_info;

CREATE TABLE silver.crm_prd_info (
    prd_id INT,
    prd_key NVARCHAR(50),
    prd_nm NVARCHAR(50),
    prd_cost INT,
    prd_line NVARCHAR(50),
    prd_start_dt DATETIME,
    prd_end_dt DATETIME,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);

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


-- Quality Check : Check for NULLs or Duplicates in Primary Key
-- Expectation : No Result
SELECT
cst_id,
COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL


-- data cleansing
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
