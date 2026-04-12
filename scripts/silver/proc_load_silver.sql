/*
================================================================
Stored Procedure : Load Silver Layer (Source -> silver)
================================================================
Script Purpose: 
	This stored procedure loads data into the 'silver' schema from external CSV files.
	It performs the following actions: 
	- Truncate the silver tables before loading data.
	- Uses the 'BULK INSERT' command to load data from csv files to silver tables. 

Parameters: 
	None.
	This stored procedure does not accept any parameters or return any values.

usage Example:
	EXEC silver.load_silver;
=====================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS 
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY --first block to handle errors
		SET @batch_start_time = GETDATE();
		PRINT '================================';
	    PRINT 'Loading silver Layer';
	    PRINT '================================';
		-- daily used script to refresh  (CRM)
	    PRINT '--------------------------------';
	    PRINT 'Loading CRM Tables';
	    PRINT '--------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info
	
		PRINT '>> Inserting Data Into: silver.crm_cust_info';
		BULK INSERT silver.crm_cust_info
		FROM 'C:\Users\jowor\OneDrive\Documents\1_Project\Baraa\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ---------' ; 

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info -- refresh
	
		PRINT '>> Inserting Data Into: silver.crm_prd_info';
		BULK INSERT silver.crm_prd_info
		FROM 'C:\Users\jowor\OneDrive\Documents\1_Project\Baraa\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ---------' ; 

		SET @end_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details -- refresh
		
		PRINT '>> Inserting Data Into: silver.crm_sales_details';
		BULK INSERT silver.crm_sales_details
		FROM 'C:\Users\jowor\OneDrive\Documents\1_Project\Baraa\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ---------' ; 
	
	    PRINT '--------------------------------';
	    PRINT 'Loading ERP Tables';
	    PRINT '--------------------------------';
	
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12 -- refresh
	
		PRINT '>> Inserting Data Into: silver.erp_cust_az12';
		BULK INSERT silver.erp_cust_az12
		FROM 'C:\Users\jowor\OneDrive\Documents\1_Project\Baraa\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ---------' ; 

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101 -- refresh
	
		PRINT '>> Inserting Data Into: silver.erp_loc_a101';
		BULK INSERT silver.erp_loc_a101
		FROM 'C:\Users\jowor\OneDrive\Documents\1_Project\Baraa\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ---------' ; 

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2 -- refresh
	
		PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';
		BULK INSERT silver.erp_px_cat_g1v2
		FROM 'C:\Users\jowor\OneDrive\Documents\1_Project\Baraa\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ---------' ; 

		SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading silver Layer is Completed';
		PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
	
	END TRY 
	BEGIN CATCH -- SQL runs the TRY block and if it fails, it runs the CATCH block to handle the error
		PRINT '========================================='
		PRINT 'ERROR OCCURED DURING LOADING silver LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Number' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error State' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '========================================='
	END CATCH
		
END 
