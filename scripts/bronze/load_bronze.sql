/*
===========================================================================================================
script info::

This scripts empties the existing contains of the tables witihin the bronze layer and inserts values into them
from the truncate and load format by bulk insert command for csv files parsings

=============================================================================================================
WARNING::::::::::

Becareful when running the script as it empties the table to fill the values for diff files, do this if backup is already
in position

also this script can update the values if the file parsings updates constantly.

*/

-- emptying table cust_info
truncate table bronze.crm_cust_info;

-- inserting values in cust_info
bulk insert bronze.crm_cust_info
from 'C:\Users\wajah\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
with(
	firstrow = 2,
	fieldterminator =',',
	tablock
)
;

go 

-- emptying table prd_info
truncate table bronze.crm_prd_info;

-- filling values in prd_info
bulk insert bronze.crm_prd_info
from 'C:\Users\wajah\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
with(
	firstrow = 2,
	fieldterminator =',',
	tablock
)
;

go 

-- emptying table sales_details
truncate table bronze.crm_sales_details;

-- inserting values in sales_details
bulk insert bronze.crm_sales_details
from 'C:\Users\wajah\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
with(
	firstrow = 2,
	fieldterminator =',',
	tablock
)
;

go 
-- emptying table cust_az12
truncate table bronze.erp_cust_az12;

--inserting in cust_az12
bulk insert bronze.erp_cust_az12
from 'C:\Users\wajah\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
with(
	firstrow = 2,
	fieldterminator =',',
	tablock
)
;

go 

-- emptying table loc_a101
truncate table bronze.erp_loc_a101;

-- inserting in loc_a101
bulk insert bronze.erp_loc_a101
from 'C:\Users\wajah\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
with(
	firstrow = 2,
	fieldterminator =',',
	tablock
)
;

go 

-- emptying table px_cat_g1v2
truncate table bronze.erp_px_cat_g1v2;

-- inserting into px_cat_g1v2
bulk insert bronze.erp_px_cat_g1v2
from 'C:\Users\wajah\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
with(
	firstrow = 2,
	fieldterminator =',',
	tablock
)
;
