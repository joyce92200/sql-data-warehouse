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
