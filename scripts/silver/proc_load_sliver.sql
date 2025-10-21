/*
===================================================================================
Store Procedure: Load Sliver Layer (Bronze -> Sliver)
===================================================================================
Script Purpose:
  This store procedure performs the ETL (Extract, Transform, Load) process to
  populate the 'sliver' scheme table from the 'bronze' schema.
  Action performed:
  - Truncate sliver table.
  - Import transformed and cleaned data from Bronze into sliver table

Parameters:
  None.
This stored procedure does not accept any parameters or retuen any value.

Usage Example:
  EXEC sliver.load_sliver;
======================================================================================
*/



CREATE OR ALTER PROCEDURE sliver.load_sliver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '===================================================';
		PRINT ' Loading Sliver Layer';
		PRINT '===================================================';

		PRINT '----------------------------------------------------';
		PRINT ' Loading CRM Table';
		PRINT '----------------------------------------------------';

		-- Loadig sliver.crm_cust_info
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: sliver.crm_cust_info'
		TRUNCATE TABLE sliver.crm_cust_info;
		PRINT ' Inserting Data Into: sliver.crm_cust_info'
		INSERT INTO sliver.crm_cust_info (
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
		TRIM(cst_firstname) AS cst_firstname,
		TRIM(cst_lastname) AS cst_lastname,
		CASE WHEN UPPER(TRIM(cst_gndr)) = 'S' THEN 'Single'
			WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Married'
			ELSE 'n/a'
		END cst_marital_status,-- Normalize material status value to readable format

		CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
			WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
			ELSE 'n/a'
		END cst_gndr, -- Normalize gender values to readable format
		cst_create_date
		FROM(
		SELECT
		*,
		ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
		FROM bronze.crm_cust_info
		WHERE cst_id IS NOT NULL
		)t WHERE flag_last = 1 -- Select the most recent recent record per customer
		SET @end_time = GETDATE();
		PRINT '>> Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds'
		print '>> ------------';

		-- Loadig sliver.crm_prd_info
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: sliver.crm_prd_info'
		TRUNCATE TABLE sliver.crm_prd_info;
		PRINT ' Inserting Data Into: sliver.crm_prd_info'
		INSERT INTO sliver.crm_prd_info (
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
		)
		SELECT
		prd_id,
		REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, -- Extract Category ID
		SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,        -- Extract product key
		prd_nm,
		ISNULL(prd_cost, 0) AS prd_cost,
		CASE  WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
			WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
			WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
			WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
			ELSE 'n/a'
		END AS prd_line, -- Map product line codes to descriptive values
		CAST (prd_start_dt AS DATE) AS prd_start_dt,
		CAST (LEAD(prd_end_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) -1 AS DATE) AS prd_end_dt -- Calculate date as one day before the next start date
		FROM bronze.crm_prd_info
		SET @end_time = GETDATE();
		PRINT '>> Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds'
		print '>> ------------';

		-- Loadig sliver.crm_sales_details
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: sliver.crm_sales_details'
		TRUNCATE TABLE sliver.crm_sales_details;
		PRINT ' Inserting Data Into: sliver.crm_sales_details'
		INSERT INTO sliver.crm_sales_details (
		sls_ord_num,
		sls_prd_key,
		sls_cust_id ,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_sales,
		sls_quantity,
		sls_price
		)
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
		CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price)
			THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales ---- Recalculate sales if original value is missing or incorrect
		END AS sls_sales, sls_quantity,
		CASE WHEN sls_price IS NULL OR sls_price <= 0
			THEN sls_sales / NULLIF(sls_quantity, 0)
		ELSE sls_price ---- Derived price is original value is invalid
		END AS sls_price
		FROM bronze.crm_sales_details
		SET @end_time = GETDATE();
		PRINT '>> Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds'
		print '>> ------------';

		PRINT '----------------------------------------------------';
		PRINT ' Loading ERP Table';
		PRINT '----------------------------------------------------';

		-- Loadig sliver.erp_cust_az12
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: sliver.erp_cust_az12'
		TRUNCATE TABLE sliver.erp_cust_az12;
		PRINT ' Inserting Data Into: sliver.erp_cust_az12'
		INSERT INTO sliver.erp_cust_az12 (cid, bdate, gen) 
		SELECT 
		CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) -- Remove 'NAS' prefix if present
			ELSE cid
		END AS cid,
		CASE WHEN bdate > GETDATE() THEN NULL
			ELSE bdate
		END AS bdate, -- Set future birthday to NULL
		CASE WHEN UPPER(TRIM(gen)) IN ('F', 'Female') THEN 'Female'
			WHEN UPPER(TRIM(gen)) IN ('M', 'Male') THEN 'Male'
			ELSE 'n/a'
		END AS gen -- Normalize gender values and handle unknown cases
		FROM bronze.erp_cust_az12
		SET @end_time = GETDATE();
		PRINT '>> Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds'
		print '>> ------------';

		-- Loadig sliver.erp_loc_a101
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: sliver.erp_loc_a101'
		TRUNCATE TABLE sliver.erp_loc_a101;
		PRINT ' Inserting Data Into: sliver.erp_loc_a101'
		INSERT INTO sliver.erp_loc_a101(cid, cntry)
		SELECT 
		REPLACE (cid, '-', '') cid,
		CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
			WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United State'
			WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
		ELSE TRIM(cntry)
		END AS cntry -- Normalize and Handle missing or blank country codes
		FROM bronze.erp_loc_a101
		SET @end_time = GETDATE();
		PRINT '>> Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds'
		print '>> ------------';

		-- Loadig sliver.erp_cust_az12
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: sliver.erp_px_cat_g1v2'
		TRUNCATE TABLE sliver.erp_px_cat_g1v2;
		PRINT ' Inserting Data Into: sliver.erp_px_cat_g1v2'
		INSERT INTO sliver.erp_px_cat_g1v2
		(id, cat, subcat, maintenance)
		SELECT  
		id,
		cat,
		subcat,
		maintenance
		FROM bronze.erp_px_cat_giv2
		SET @end_time = GETDATE();
		PRINT '>> Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds'
		print '>> ------------';

		SET @batch_end_time = GETDATE();
		PRINT '=================================================='
		PRINT 'Loading sliver layer is completed';
		PRINT ' - Total load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT ' ================================================='

	END TRY
	BEGIN CATCH
		PRINT '=================================================='
		PRINT ' ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '==================================================';
	END CATCH
END
