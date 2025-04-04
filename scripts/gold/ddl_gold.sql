/* 
=========================================================================================================
this script creates two dimension views namely customers and products and one facts view namely sales
the purpose of the scipt is to join all the req cols from all crm and products tables from silver layer 
into dimensions and facts to get a goo and easier understanding of data
=========================================================================================================
*/

-- check if view exists and drops to create new
if object_id('gold.dim_customers', 'V') is not null
	drop view gold.dim_customers

go
  
/*
=====================================================
creating cusutomers view
=====================================================
*/
  
-- create a view dim_customers
create view gold.dim_customers as 
select 
row_number() over(order by ci.cst_id) as customer_key,
ci.cst_id as customer_id,
ci.cst_key as customer_number,
ci.cst_firstname as first_name,
ci.cst_lastname as last_name,
ci.cst_marital_status as marital_status,
case 
	when ci.cst_gndr != 'n/a' then ci.cst_gndr
	else coalesce(erp_ci.gen, 'n/a') -- check for null values and replaces with n/a
end gender,
erp_loc.cntry as country,
erp_Ci.bdate as birthdate,
ci.cst_create_date as create_date
from silver.crm_cust_info ci
left join silver.erp_cust_az12 erp_ci
on ci.cst_key = erp_ci.cid
left join silver.erp_loc_a101 erp_loc
on ci.cst_key = erp_loc.cid

/*
=====================================================
creating products view
=====================================================
*/

-- check if view exists and drops to create new
if object_id('gold.dim_products', 'V') is not null
	drop view gold.dim_products

go

-- create a view dim_products
create view gold.dim_products as 
select
ROW_NUMBER() over(order by cp.prd_id) as product_key,
cp.prd_id as product_id,
cp.prd_key as product_number,
cp.prd_nm as product_name,
cp.cat_id as category_id,
ep.cat as category,
ep.subcat as subcategory,
cp.prd_line as product_line,
cp.prd_cost as product_cost,
ep.maintenance as maintenance,
cp.prd_start_dt as product_start_date,
cp.prd_end_dt as product_end_date
from silver.crm_prd_info cp
left join
silver.erp_px_cat_g1v2 ep
on cp.cat_id = ep.id
where cp.prd_end_dt is null

/*
=====================================================
creating sales view
=====================================================
*/
-- check if view exists and drops to create new
if object_id('gold.fact_sales', 'V') is not null
	drop view gold.fact_sales

go

-- create a view fact_sales
create view gold.fact_sales as
SELECT 
sls_ord_num as order_number,
dp.product_key,
dc.customer_key,
sls_order_dt as order_date,
sls_ship_dt as shipping_Date,
sls_due_dt as due_date,
sls_sales as sales_amount,
sls_quantity as quantity,
sls_price as price
FROM SILVER.crm_sales_details cs
left join gold.dim_customers dc
on cs.sls_cust_id = dc.customer_id
left join gold.dim_products dp
on cs.sls_prd_key = dp.product_number
