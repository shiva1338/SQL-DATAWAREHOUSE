/*==========================================
  DDL SCRIPT FOR silver LAYER 
===========================================*/

if object_id ('silver.crm_cust_info','U') is not null 
   drop table silver.crm_cust_info
   go

   /*=================================
   create table silver customer info
   ==================================*/
create table silver.crm_cust_info (
			cst_id int ,
			cst_key nvarchar(50),
			cst_firstname nvarchar(50),
			cst_lastname nvarchar(50),
			cst_marital_status nvarchar(50),
			cst_gender nvarchar(50),
			cst_create_date date
            )

if object_id ('silver.crm_prd_info','U') is not null
 drop table silver.crm_prd_info
 go 
/* =====================================
creating table silver productinfo
========================================*/
create table silver.crm_prd_info (
			 prd_id int ,
			 prd_key nvarchar(50),
			 prd_cat_id nvarchar(50),
			 prd_name nvarchar(50),
			 prd_cost int ,
			 prd_line nvarchar (50),
			 prd_start_date date,
			 prd_end_date date
             )

if object_id ('silver.crm_sales','U') is not null
drop table silver.crm_sales
go 
/*========================================
---- creating table silver salesinfo 
==========================================*/
create table silver.crm_sales (
			sls_ord_num nvarchar (50),
			sls_prd_key nvarchar(50),
			sls_cust_id int,
			sls_order_date date,
			sls_ship_date date,
			sls_due_date date,
			sls_sales int,
			sls_quantity int,
			sls_price int
			)

if object_id ('silver.erp_cust_ss','U') is not null
drop table silver.erp_cust_ss
go 
/*=========================================
---- creating table silver cust_ss
===========================================*/
create table silver.erp_cust_ss (
				cus_id nvarchar(50),
				cus_birthdate date,
				cus_gender nvarchar(50)
				)

if object_id ('silver.erp_log','U') is not null
drop table silver.erp_log
go 
/*========================================
--- creating table silver cust_log
==========================================*/

create table silver.erp_log (
				c_id nvarchar(50),
				c_country nvarchar(50)
				)

if object_id ('silver.erp_prd_cat','U') is not null
drop table silver.erp_prd_cat
go 
/*================================================
----- creating table silver product category
==================================================*/
create table silver.erp_prd_cat(
			prdd_id nvarchar(50),
			prdd_cat nvarchar(50),
			prdd_subcat nvarchar(50),
			prdd_maintaince nvarchar(50)
			)




