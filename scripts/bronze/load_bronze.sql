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

This script can automatically load the data with the stored procedure
*/
exec bronze.load_bronze;

create or alter procedure bronze.load_bronze as 
begin 
	declare @begin_time datetime, @complete_time datetime
	set @begin_time = getdate()

		declare @start_time datetime, @end_time datetime
		print '================================'
		print '>> loading bronze layers'
		print '================================'

		print '================================'
		print '>> loading crm tables'
		print '================================'

		set @start_time = getdate()
		print '================================'
		print '>> truncating cust_info'
		print '================================'
		-- emptying table cust_info
		truncate table bronze.crm_cust_info;
		print '================================'
		print '>> inserting  cust_info'
		print '================================'
		-- inserting values in cust_info
		bulk insert bronze.crm_cust_info
		from 'C:\Users\wajah\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with(
			firstrow = 2,
			fieldterminator =',',
			tablock
		)
		;
		print '================================'
		print '>> succesfully inserted into cust_info'
		print '================================'
		set @end_time = getdate()
		print '================================'
		print '>> time take to load cust_info ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds'
		print '================================'


	
		set @start_time = getdate()
		print '================================'
		print '>> truncating prd_info'
		print '================================'
		-- emptying table prd_info
		truncate table bronze.crm_prd_info;
		print '================================'
		print '>> inserting into prd_info'
		print '================================'
		-- filling values in prd_info
		bulk insert bronze.crm_prd_info
		from 'C:\Users\wajah\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with(
			firstrow = 2,
			fieldterminator =',',
			tablock
		)
		;
		print '================================'
		print '>> succesfully loaded into prd_info'
		print '================================'
		set @end_time = getdate()
		print '================================'
		print '>> time take to load prd_info ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds'
		print '================================'
	

		set @start_time = getdate()
		print '================================'
		print '>> truncating sales_details'
		print '================================'
		-- emptying table sales_details
		truncate table bronze.crm_sales_details;
		print '================================'
		print '>> inserting into sales_details'
		print '================================'
		-- inserting values in sales_details
		bulk insert bronze.crm_sales_details
		from 'C:\Users\wajah\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with(
			firstrow = 2,
			fieldterminator =',',
			tablock
		)
		;
		print '================================'
		print '>> succesfully inserted into sales_details'
		print '================================'
		print '================================'
		print '>> time take to load sales_details ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds'
		print '================================'
	 

		print '================================'
		print '>> loading erp tables'
		print '================================'

		set @start_time = getdate()
		print '================================'
		print '>> truncating cust_az12'
		print '================================'
		-- emptying table cust_az12
		truncate table bronze.erp_cust_az12;
		print '================================'
		print '>> inserting into erp_cust_az12'
		print '================================'
		--inserting in cust_az12
		bulk insert bronze.erp_cust_az12
		from 'C:\Users\wajah\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		with(
			firstrow = 2,
			fieldterminator =',',
			tablock
		)
		;
		print '================================'
		print '>> succesfully inserted into erp_cust_az12'
		print '================================'
		set @end_time = getdate()
		print '================================'
		print '>> time take to load cust_az12 ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds'
		print '================================'
	

		set @start_time = getdate()
		print '================================'
		print '>> truncating erp_loc_a101'
		print '================================'
		-- emptying table loc_a101
		truncate table bronze.erp_loc_a101;
		print '================================'
		print '>> inserting into erp_loc_a101'
		print '================================'
		-- inserting in loc_a101
		bulk insert bronze.erp_loc_a101
		from 'C:\Users\wajah\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		with(
			firstrow = 2,
			fieldterminator =',',
			tablock
		)
		;
		print '================================'
		print '>> succesfully inserted into erp_loc_a101'
		print '================================'
		set @end_time = getdate()
		print '================================'
		print '>> time take to load erp_loc_a101 ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds'
		print '================================'



		set @start_time = GETDATE()
		print '================================'
		print '>> truncating erp_px_cat_g1v2'
		print '================================'
		-- emptying table px_cat_g1v2
		truncate table bronze.erp_px_cat_g1v2;
		print '================================'
		print '>> inserting into px_cat_g1v2'
		print '================================'
		-- inserting into px_cat_g1v2
		bulk insert bronze.erp_px_cat_g1v2
		from 'C:\Users\wajah\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		with(
			firstrow = 2,
			fieldterminator =',',
			tablock
		)
		;
		print '================================'
		print '>> succesfully inserted into erp_px_cat_g1v2'
		print '================================'
		set @end_time = getdate()
		print '================================'
		print '>> time take to load px_cat_g1v2 ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds'
		print '================================'

		print '================================'
		print '>> succesfully loaded everything into bronze layers'
		print '================================'
	set @complete_time = getdate()
	print '================================'
	print '>> time take to load all bronze layers ' + cast(datediff(second, @begin_time, @complete_time) as nvarchar) + ' seconds'
	print '================================'

end
