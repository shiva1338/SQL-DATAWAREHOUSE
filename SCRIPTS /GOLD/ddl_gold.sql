


/* 
====================================================================================
   DDL SCRIPTS ; GOLD VIEWS
====================================================================================
  SCRIPT PURPOSE : 
        This Script creates views for the gold layer in datawarehouse
        This gold layer represents final dimension and fact tables ( Star Schema)

        Each view performs transformations and combine the data from silver layer 
           to produce a clean , enriched and business-ready datasets.
 
 USAGE: 
       These views can be queried directly for analytics and reports
 =====================================================================================
   */

if object_id ('gold.dim_customers','V') is not null
drop view gold.dim_customers;
go

/*==========================================================
   CREATE DIMENSION ; DIM_CUSTOMERS
============================================================*/

create view gold.dim_customers as
select
       row_number() over (order by cp.cst_id) as customer_key ,
       cp.cst_id as customer_id,
       cp. cst_key as customer_number,
       cp.cst_firstname as firstname,
       cp.cst_lastname as lastname,
       cl.c_country as country,
       cs.cus_birthdate as birthdate,
       case when cp.cst_gender != 'N/A' then cp.cst_gender
            else coalesce ( cs.cus_gender,'N/A')
        end as gender,
       cp. cst_marital_status as marital_status,
       cp. cst_create_date as create_date
from silver.crm_cust_info  as cp
left join silver.erp_cust_ss  cs
on cp.cst_key = cs.cus_id
left join silver.erp_log as cl
on cp.cst_key = cl.c_id

go

/*==========================================================
   CREATE DIMENSION ; DIM_PRODUCTS
============================================================*/

if object_id ('gold.dim_products','V') is not null
drop view gold.dim_products ;
go

create view gold.dim_products as
select
       row_number() over (order by sp.prd_start_date,sp.prd_key) as product_key,
       sp.prd_id as product_id,
       sp.prd_key as product_number,
       sp.prd_name as product_name,
       sp.prd_cat_id as category_id,
       ep.prdd_cat as category,
       ep.prdd_subcat as subcategory,
       ep.prdd_maintaince as maintaince,
       sp.prd_cost as product_cost,
       sp.prd_line product_line,
       sp.prd_start_date as start_datee   
from silver.crm_prd_info as sp
left join silver.erp_prd_cat as ep
on sp.prd_cat_id = ep.prdd_id
where sp.prd_end_date is  null --FILTER ALL HISTORICALL DATA

go

/*==========================================================
   CREATE FACT ; FACT_SALES
============================================================*/

if object_id ('gold.fact_sales','V') is not null
drop view gold.fact_sales
go

create view gold.fact_sales as
select 
      fs.sls_ord_num as order_number,     
      dp.product_key,
      dc.customer_key,
       fs.sls_order_date as order_date,
       fs.sls_ship_date as shipping_date,
       fs.sls_due_date as due_date,
      fs. sls_sales  as sales_amount,
      fs. sls_quantity as quantity,
      fs. sls_price as price
from silver.crm_sales as fs
left join gold.dim_customers as dc
on fs.sls_cust_id = dc.customer_id
left join gold.dim_products as dp
on fs.sls_prd_key = dp.product_number


