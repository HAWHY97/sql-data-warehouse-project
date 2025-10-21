/*
===============================================================================
DDL Script: Create Sliver Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'sliver' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'sliver' Tables
===============================================================================
*/

IF OBJECT_ID('sliver.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE sliver.crm_cust_info;
GO

CREATE TABLE sliver.crm_cust_info (
    cst_id              INT,
    cst_key             NVARCHAR(50),
    cst_firstname       NVARCHAR(50),
    cst_lastname        NVARCHAR(50),
    cst_marital_status  NVARCHAR(50),
    cst_gndr            NVARCHAR(50),
    cst_create_date     DATE,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('sliver.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE sliver.crm_prd_info;
GO

CREATE TABLE sliver.crm_prd_info (
    prd_id       INT,
	cat_id		 INT,
    prd_key      NVARCHAR(50),
    prd_nm       NVARCHAR(50),
    prd_cost     INT,
    prd_line     NVARCHAR(50),
    prd_start_dt DATETIME,
    prd_end_dt   DATETIME,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('sliver.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE sliver.crm_sales_details;
GO

CREATE TABLE sliver.crm_sales_details (
    sls_ord_num  NVARCHAR(50),
    sls_prd_key  NVARCHAR(50),
    sls_cust_id  INT,
    sls_order_dt DATE,
    sls_ship_dt  DATE,
    sls_due_dt   DATE,
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('sliver.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE sliver.erp_loc_a101;
GO

CREATE TABLE sliver.erp_loc_a101 (
    cid    NVARCHAR(50),
    cntry  NVARCHAR(50),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('sliver.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE sliver.erp_cust_az12;
GO

CREATE TABLE sliver.erp_cust_az12 (
    cid    NVARCHAR(50),
    bdate  DATE,
    gen    NVARCHAR(50),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('sliver.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE sliver.erp_px_cat_g1v2;
GO

CREATE TABLE sliver.erp_px_cat_g1v2 (
    id           NVARCHAR(50),
    cat          NVARCHAR(50),
    subcat       NVARCHAR(50),
    maintenance  NVARCHAR(50),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO




