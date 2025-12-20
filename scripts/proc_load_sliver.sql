EXEC sliver.load_sliver;
CREATE OR ALTER PROCEDURE sliver.load_sliver
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE 
        @batch_start_time DATETIME = GETDATE(),
        @start_time DATETIME,
        @end_time DATETIME;

    BEGIN TRY
        PRINT '==================================================';
        PRINT ' Loading Silver Layer';
        PRINT '==================================================';

        /* ==================================================
           CRM – CUSTOMER
        ================================================== */
        SET @start_time = GETDATE();
        TRUNCATE TABLE sliver.crm_cust_info;

        INSERT INTO sliver.crm_cust_info
        (
            cst_id,
            cst_key,
            cst_firstname,
            cst_lastname,
            cst_marital_status,
            cst_gndr,
            cst_create_date
        )
        SELECT
            cst_id,
            cst_key,
            TRIM(cst_firstname),
            TRIM(cst_lastname),
            CASE UPPER(TRIM(cst_marital_status))
                WHEN 'S' THEN 'Single'
                WHEN 'M' THEN 'Married'
                ELSE 'n/a'
            END,
            CASE UPPER(TRIM(cst_gndr))
                WHEN 'F' THEN 'Female'
                WHEN 'M' THEN 'Male'
                ELSE 'n/a'
            END,
            cst_create_date
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY cst_id
                       ORDER BY cst_create_date DESC
                   ) AS rn
            FROM bronze.crm_cust_info
            WHERE cst_id IS NOT NULL
        ) t
        WHERE rn = 1;

        PRINT 'CRM Customers loaded in '
              + CAST(DATEDIFF(SECOND, @start_time, GETDATE()) AS VARCHAR) + ' sec';

        /* ==================================================
           CRM – PRODUCT
        ================================================== */
        SET @start_time = GETDATE();
        TRUNCATE TABLE sliver.crm_prd_info;

        INSERT INTO sliver.crm_prd_info
        (
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
            REPLACE(LEFT(prd_key, 5), '-', '_'),
            SUBSTRING(prd_key, 7, LEN(prd_key)),
            prd_nm,
            ISNULL(prd_cost, 0),
            CASE UPPER(TRIM(prd_line))
                WHEN 'M' THEN 'Mountain'
                WHEN 'R' THEN 'Road'
                WHEN 'S' THEN 'Other Sales'
                WHEN 'T' THEN 'Touring'
                ELSE 'n/a'
            END,
            CAST(prd_start_dt AS DATE),
            CAST(
                LEAD(prd_start_dt) OVER (
                    PARTITION BY prd_key
                    ORDER BY prd_start_dt
                ) - 1 AS DATE
            )
        FROM bronze.crm_prd_info;

        PRINT 'CRM Products loaded in '
              + CAST(DATEDIFF(SECOND, @start_time, GETDATE()) AS VARCHAR) + ' sec';

        /* ==================================================
           CRM – SALES
        ================================================== */
        SET @start_time = GETDATE();
        TRUNCATE TABLE sliver.crm_sales_details;

        INSERT INTO sliver.crm_sales_details
        (
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
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
            TRY_CONVERT(DATE, CAST(sls_order_dt AS CHAR(8))),
            TRY_CONVERT(DATE, CAST(sls_ship_dt AS CHAR(8))),
            TRY_CONVERT(DATE, CAST(sls_due_dt AS CHAR(8))),
            CASE
                WHEN sls_sales IS NULL OR sls_sales <= 0
                    THEN sls_quantity * ABS(sls_price)
                ELSE sls_sales
            END,
            sls_quantity,
            CASE
                WHEN sls_price IS NULL OR sls_price <= 0
                    THEN sls_sales / NULLIF(sls_quantity, 0)
                ELSE sls_price
            END
        FROM bronze.crm_sales_details;

        PRINT 'CRM Sales loaded in '
              + CAST(DATEDIFF(SECOND, @start_time, GETDATE()) AS VARCHAR) + ' sec';

        /* ==================================================
           ERP – CUSTOMER
        ================================================== */
        SET @start_time = GETDATE();
        TRUNCATE TABLE sliver.erp_cust_az12;

        INSERT INTO sliver.erp_cust_az12 (cid, bdate, gen)
        SELECT
            CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) ELSE cid END,
            CASE WHEN bdate > GETDATE() THEN NULL ELSE bdate END,
            CASE UPPER(TRIM(gen))
                WHEN 'F' THEN 'Female'
                WHEN 'M' THEN 'Male'
                ELSE 'n/a'
            END
        FROM bronze.erp_cust_az12;

        PRINT 'ERP Customers loaded in '
              + CAST(DATEDIFF(SECOND, @start_time, GETDATE()) AS VARCHAR) + ' sec';

        /* ==================================================
           ERP – LOCATION
        ================================================== */
        SET @start_time = GETDATE();
        TRUNCATE TABLE sliver.erp_loc_a101;

        INSERT INTO sliver.erp_loc_a101 (cid, cntry)
        SELECT
            REPLACE(cid, '-', ''),
            CASE
                WHEN cntry IN ('US', 'USA') THEN 'United States'
                WHEN cntry = 'DE' THEN 'Germany'
                WHEN cntry IS NULL OR TRIM(cntry) = '' THEN 'n/a'
                ELSE TRIM(cntry)
            END
        FROM bronze.erp_loc_a101;

        PRINT 'ERP Locations loaded in '
              + CAST(DATEDIFF(SECOND, @start_time, GETDATE()) AS VARCHAR) + ' sec';

        /* ==================================================
           ERP – PRODUCT CATEGORY
        ================================================== */
        SET @start_time = GETDATE();
        TRUNCATE TABLE sliver.erp_px_cat_g1v2;

        INSERT INTO sliver.erp_px_cat_g1v2
        (
            id,
            cat,
            subcat,
            maintenance
        )
        SELECT
            id,
            cat,
            subcat,
            maintenance
        FROM bronze.erp_px_cat_giv2;

        PRINT 'ERP Product Categories loaded in '
              + CAST(DATEDIFF(SECOND, @start_time, GETDATE()) AS VARCHAR) + ' sec';

        PRINT '==================================================';
        PRINT ' Silver Layer Load Completed';
        PRINT ' Total Duration: '
              + CAST(DATEDIFF(SECOND, @batch_start_time, GETDATE()) AS VARCHAR) + ' sec';
        PRINT '==================================================';

    END TRY
    BEGIN CATCH
        THROW;
    END CATCH
END;
GO

