/*
====================================================================================================================================================
This scripts creates 6 diff tables in the bronze schema taken from crm and erp datasets
Tables consists diff datatypes for diff columns and follow and naming convention with dataset names a fist part and column name as the next part
When adding excess columns be sure to follow the same exact pattern
=====================================================================================================================================================
WARNING

This script delete the table in the bronze schema if they already exits, so be sure to usr them accordinly while having a proper backup of the data

======================================================================================================================================================
*/


--removes table if any
if OBJECT_ID('bronze.crm_cust_info', 'U') is not null
	drop table bronze.crm_cust_info;

go

-- creates table cust_info from crm
create table bronze.crm_cust_info(
	cst_id              INT,
    cst_key             NVARCHAR(50),
    cst_firstname       NVARCHAR(50),
    cst_lastname        NVARCHAR(50),
    cst_marital_status  NVARCHAR(50),
    cst_gndr            NVARCHAR(50),
    cst_create_date     DATE
);

go

-- removes table if any
if OBJECT_ID('bronze.crm_prd_info', 'U') is not null
	drop table bronze.crm_prd_info;

go

-- creates table prd_info from crm
create table bronze.crm_prd_info(
	prd_id       INT,
    prd_key      NVARCHAR(50),
    prd_nm       NVARCHAR(50),
    prd_cost     INT,
    prd_line     NVARCHAR(50),
    prd_start_dt DATETIME,
    prd_end_dt   DATETIME
);

--removes table if any
if OBJECT_ID('bronze.crm_sales_details', 'U') is not null
	drop table bronze.crm_sales_details;

go

-- creates table sales_details from crm
create table bronze.crm_sales_details(
	sls_ord_num  NVARCHAR(50),
    sls_prd_key  NVARCHAR(50),
    sls_cust_id  INT,
    sls_order_dt INT,
    sls_ship_dt  INT,
    sls_due_dt   INT,
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT
)

-- removes table if any
if OBJECT_ID('bronze.erp_loc_a10', 'U') is not null
	drop table erp_loc_a101;

go

-- creates table loc_a101 from erp
create table bronze.erp_loc_a101(
	cid    NVARCHAR(50),
    cntry  NVARCHAR(50)
);

-- removes table if any
if OBJECT_ID('bronze.erp_cust_az12', 'U') is not null
	drop table erp_cust_az12;

go

-- creates table cust_az12 from erp
create table bronze.erp_cust_az12(
	cid    NVARCHAR(50),
    bdate  DATE,
    gen    NVARCHAR(50)
);

-- removes table if any
if OBJECT_ID('erp_px_cat_g1v2', 'U') is not null
	drop table erp_px_cat_g1v2;

go

-- creates table px_cat_g1v2 from erp
create table bronze.erp_px_cat_g1v2(
	id           NVARCHAR(50),
    cat          NVARCHAR(50),
    subcat       NVARCHAR(50),
    maintenance  NVARCHAR(50)
);
