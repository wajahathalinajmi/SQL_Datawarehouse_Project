/*
====================================================================================================================================================
This scripts creates 6 diff tables in the silver schema taken from crm and erp datasets
Tables consists diff datatypes for diff columns and follow and naming convention with dataset names a fist part and column name as the next part
When adding excess columns be sure to follow the same exact pattern
=====================================================================================================================================================
WARNING

This script delete the table in the silver schema if they already exits, so be sure to usr them accordinly while having a proper backup of the data

======================================================================================================================================================
*/


--removes table if any
if OBJECT_ID('silver.crm_cust_info', 'U') is not null
	drop table silver.crm_cust_info;

-- creates table cust_info from crm
create table silver.crm_cust_info(
	  cst_id              INT,
    cst_key             NVARCHAR(50),
    cst_firstname       NVARCHAR(50),
    cst_lastname        NVARCHAR(50),
    cst_marital_status  NVARCHAR(50),
    cst_gndr            NVARCHAR(50),
    cst_create_date     DATE,
	  dwh_create_date		DATETIME2 DEFAULT GETDATE()
);


-- removes table if any
if OBJECT_ID('silver.crm_prd_info', 'U') is not null
	drop table silver.crm_prd_info;


-- creates table prd_info from crm
CREATE TABLE silver.crm_prd_info (
    prd_id          INT,
    cat_id          NVARCHAR(50),
    prd_key         NVARCHAR(50),
    prd_nm          NVARCHAR(50),
    prd_cost        INT,
    prd_line        NVARCHAR(50),
    prd_start_dt    DATE,
    prd_end_dt      DATE,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);

--removes table if any
if OBJECT_ID('silver.crm_sales_details', 'U') is not null
	drop table silver.crm_sales_details;


-- creates table sales_details from crm
CREATE TABLE silver.crm_sales_details (
    sls_ord_num     NVARCHAR(50),
    sls_prd_key     NVARCHAR(50),
    sls_cust_id     INT,
    sls_order_dt    DATE,
    sls_ship_dt     DATE,
    sls_due_dt      DATE,
    sls_sales       INT,
    sls_quantity    INT,
    sls_price       INT,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);

-- removes table if any
if OBJECT_ID('silver.erp_loc_a10', 'U') is not null
	drop table silver.erp_loc_a101;


-- creates table loc_a101 from erp
create table silver.erp_loc_a101(
	cid    NVARCHAR(50),
    cntry  NVARCHAR(50),
	dwh_create_date		DATETIME2 DEFAULT GETDATE()
);

-- removes table if any
if OBJECT_ID('silver.erp_cust_az12', 'U') is not null
	drop table silver.erp_cust_az12;


-- creates table cust_az12 from erp
create table silver.erp_cust_az12(
	cid    NVARCHAR(50),
    bdate  DATE,
    gen    NVARCHAR(50),
	dwh_create_date		DATETIME2 DEFAULT GETDATE()
);

-- removes table if any
if OBJECT_ID('erp_px_cat_g1v2', 'U') is not null
	drop table silver.erp_px_cat_g1v2;


-- creates table px_cat_g1v2 from erp
create table silver.erp_px_cat_g1v2(
	id           NVARCHAR(50),
    cat          NVARCHAR(50),
    subcat       NVARCHAR(50),
    maintenance  NVARCHAR(50),
	dwh_create_date		DATETIME2 DEFAULT GETDATE()
);
