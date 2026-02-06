/*==========================================
  DDL SCRIPT FOR BRONZE LAYER 
===========================================*/

if object_id ('bronze.crm_cust_info','U') is not null 
   drop table bronze.crm_cust_info
   go

   /*=================================
   create table bronze customer info
   ==================================*/
create table bronze.crm_cust_info (
			cst_id int ,
			cst_key nvarchar(50),
			cst_firstname nvarchar(50),
			cst_lastname nvarchar(50),
			cst_marital_status nvarchar(50),
			cst_gndr nvarchar(50),
			cst_create_date date
            )

if object_id ('bronze.crm_prd_info','U') is not null
 drop table bronze.crm_prd_info
 go 
/* =====================================
creating table bronze productinfo
========================================*/
create table bronze.crm_prd_info (
			 prd_id int ,
			 prd_key nvarchar(50),
			 prd_name nvarchar(50),
			 prd_cost int ,
			 prd_line nvarchar (50),
			 prd_start_date datetime,
			 prd_end_date datetime
             )

if object_id ('bronze.crm_sales','U') is not null
drop table bronze.crm_sales
go 
/*========================================
---- creating table bronze salesinfo 
==========================================*/
create table bronze.crm_sales (
			sls_ord_num nvarchar (50),
			sls_prd_key nvarchar(50),
			sls_cust_id int,
			sls_order_date int,
			sls_ship_date int,
			sls_due_date int,
			sls_sales int,
			sls_quantity int,
			sls_price int
			)

if object_id ('bronze.erp_cust_ss','U') is not null
drop table bronze.erp_cust_ss
go 
/*=========================================
---- creating table bronze cust_ss
===========================================*/
create table bronze.erp_cust_ss (
				cus_id nvarchar(50),
				cus_birthdate date,
				cus_gender nvarchar(50)
				)

if object_id ('bronze.erp_log','U') is not null
drop table bronze.erp_log
go 
/*========================================
--- creating table bronze cust_log
==========================================*/

create table bronze.erp_log (
				c_id nvarchar(50),
				c_country nvarchar(50)
				)

if object_id ('bronze.erp_prd_cat','U') is not null
drop table bronze.erp_prd_cat
go 
/*================================================
----- creating table bronze product category
==================================================*/
create table bronze.erp_prd_cat(
			prdd_id nvarchar(50),
			prdd_cat nvarchar(50),
			prdd_subcat nvarchar(50),
			prdd_maintaince nvarchar(50)
			)





