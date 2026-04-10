-- daily used script to refresh  
TRUNCATE TABLE bronze.crm_cust_info
BULK INSERT bronze.crm_cust_info
FROM 'C:\Users\jowor\OneDrive\Documents\1_Project\Baraa\source_crm\cust_info.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);

TRUNCATE TABLE bronze.crm_prd_info -- refresh
BULK INSERT bronze.crm_prd_info
FROM 'C:\Users\jowor\OneDrive\Documents\1_Project\Baraa\source_crm\prd_info.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);

TRUNCATE TABLE bronze.crm_sales_details -- refresh
BULK INSERT bronze.crm_sales_details
FROM 'C:\Users\jowor\OneDrive\Documents\1_Project\Baraa\source_crm\sales_details.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);

TRUNCATE TABLE bronze.erp_cust_az12 -- refresh
BULK INSERT bronze.erp_cust_az12
FROM 'C:\Users\jowor\OneDrive\Documents\1_Project\Baraa\source_erp\CUST_AZ12.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);

TRUNCATE TABLE bronze.erp_loc_a101 -- refresh
BULK INSERT bronze.erp_loc_a101
FROM 'C:\Users\jowor\OneDrive\Documents\1_Project\Baraa\source_erp\LOC_A101.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);

TRUNCATE TABLE bronze.erp_px_cat_g1v2 -- refresh
BULK INSERT bronze.erp_px_cat_g1v2
FROM 'C:\Users\jowor\OneDrive\Documents\1_Project\Baraa\source_erp\PX_CAT_G1V2.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);

--  save frequently used SQL code in stored procedures in database
CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN
  DECLARE @start_time DATETIME , @end_time DATETIME;

  BEGIN TRY 
    PRINT '================================';
    PRINT 'Loading Bronze Layer';
    PRINT '================================';
  
    PRINT '--------------------------------';
    PRINT 'Loading CRM Tables';
    PRINT '--------------------------------';

    SET @start_time = GETDATE(); 
    PRINT '>> Truncating Table: bronze.crm_cust_info'
    --- make the table empty
    TRUNCATE TABLE bronze.crm_cust_info;
  
    PRINT '>> Inserting Data Into: bronze.crm_cust_info'
    --- full load from scratch
    BULK INSERT bronze.crm_cust_info
    FROM
    WITH (
      FIRSTROW = 2,
      FIELDTERMINATOR = ',',
      TABLOCK
      );
   SET @end_time = GETDATE(); 
   PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
   PRINT '>> ---------' ; 

END TRY
BEGIN CATCH
  PRINT '========================================='
  PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
  PRINT 'Error Message' + ERROR_MESSAGE();
  PRINT 'Error Number' + CAST(ERROR_NUMBER() AS NVARCHAR);
  PRINT '========================================='
END CATCH
END

--test the stored procedure to load the bronze layer
EXEC bronze.load_bronze

--- check the number of rows
SELECT COUNT(*)
FROM bronze.crm_cust_info

  PRINT '--------------------------------';
  PRINT 'Loading ERP Tables';
  PRINT '--------------------------------';
