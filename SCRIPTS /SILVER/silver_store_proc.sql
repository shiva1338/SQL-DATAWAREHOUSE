
/*===================================================
  EXEC silver.load_silver
====================================================*/

create or alter procedure silver.load_silver as 
begin 
      
      DECLARE @STARTTIME  DATETIME , @ENDTIME DATETIME
      BEGIN TRY 
/*===============================================
  DATA CLEANING FOR TABLE ; SILVER.CRM_CUST-INFO
  ===============================================*/
SET @STARTTIME = GETDATE()
print '=================================================
       >>>>> SILVER LAYER 
 ======================================================'
PRINT '>>>>> TRUNCATE TABLE: SILVER.CRM_CUST_INFO'

truncate table  silver.crm_cust_info ;

PRINT '>>>>> INSERT INTO TABLE ; SILVER.CRM_CUST_INFO'
insert into  silver.crm_cust_info (
cst_id,
cst_key,
cst_firstname,
cst_lastname,
cst_marital_status,
cst_gender,
cst_create_date 
)

select cst_id,
cst_key,
trim(cst_firstname) as cst_firstname,
trim(cst_lastname) as cst_lastname,
case when cst_marital_status = 'M' then 'Married'
     when cst_marital_status = 'S' then 'Single'
     else 'N/A'
end as cst_marital_status,
case when cst_gndr = 'M' then 'Male'
     when cst_gndr = 'F' then 'Female'
     else 'N/A'
end as  cst_gender,
cst_create_date 
from ( 
     select cst_id,
            cst_key,
            cst_firstname,
            cst_lastname,
            cst_marital_status,
            cst_gndr,
            cst_create_date ,
            row_number() over(partition by cst_id order by cst_create_date desc) as r
            from bronze.crm_cust_info 
            
        )t
 where r = 1 and cst_id is not null
 PRINT '==============================================='
 /*=============================================
  DATA CLEANING FOR TABLE ; SILVER.CRM_PRD_INFO
  =============================================*/
  PRINT '>>>>> TRUNCATE TABLE: SILVER.crm_prd_info'
  PRINT '>>>>> INSERT INTO TABLE ; SILVER.crm_prd_info'
  /*=======================================================*/
--- TRUNCATE TABLE
 truncate table  silver.crm_prd_info ;
 --- INSERT DATA 
insert into  silver.crm_prd_info (
prd_id,
prd_key,
prd_cat_id,
prd_name,
prd_cost,
prd_line,
prd_start_date,
prd_end_date 
)

select prd_id,
SUBSTRING(trim(prd_key),7,len(prd_key)) as prd_key ,
replace (left (trim(prd_key),5),'-','_') as prd_cat_id,
trim(prd_name) as prd_name,
isnull (prd_cost,0) as prd_cost,
case upper(trim(prd_line)) 
    when 'M' then 'Mountain'
     when 'R' then 'Road'
     when 'T' then 'Touring'
     when 'S' then 'Othersales'
     else 'N/A'
end as prd_line,
cast (prd_start_date as date) as prd_start_date,
cast(lead(prd_start_date) over(partition by prd_key order by prd_start_date) - 1 as date) as prd_end_date
from bronze.crm_prd_info
PRINT '============================================='
/*===============================================
  DATA CLEANING FOR TABLE ; SILVER.CRM_SALES
  =============================================*/
  PRINT '>>>>> TRUNCATE TABLE: SILVER.crm_sales'
  PRINT '>>>>> INSERT INTO TABLE ; SILVER.crm_sales'
  /*======================================================*/
---- TRUNCATE TABLE
truncate table silver.crm_sales;
---- INSERT DATA INN TABLE
insert into silver.crm_sales (
            sls_ord_num ,
			sls_prd_key,
			sls_cust_id ,
			sls_order_date ,
			sls_ship_date ,
			sls_due_date,
			sls_sales ,
			sls_quantity ,
			sls_price 
			)


select sls_ord_num,
sls_prd_key,
sls_cust_id,
case when len(sls_order_date) != 8  then null
       else cast (cast( sls_order_date as varchar) as date)
end as sls_order_date,
case when len(sls_ship_date) != 8  then null
     else cast (cast( sls_ship_date as varchar) as date)
end as sls_ship_date,
case when len(sls_due_date) != 8  then null
       else cast (cast( sls_due_date as varchar) as date)
end as sls_due_date,
case when sls_sales is null or sls_sales <= 0 or sls_sales != abs(sls_price) * sls_quantity
      then abs(sls_price) * sls_quantity
    else sls_sales
end as sls_sales,
CASE 
            WHEN sls_quantity IS NULL 
              OR sls_quantity <= 0
            THEN sls_sales / NULLIF(sls_price,0)
            ELSE ABS(sls_quantity)
        END AS sls_quantity,
case when sls_price is null or sls_price <= 0 
          then sls_sales/nullif (sls_quantity,0) 
    else abs(sls_price)
end as sls_price
from bronze.crm_sales
PRINT '==============================================='
/*==============================================
  DATA CLEANING FOR TABLE ; SILVER.ERP_CUST_SS
  =============================================*/
   PRINT '>>>>> TRUNCATE TABLE: SILVER.ERP_CUST_SS'
  PRINT '>>>>> INSERT INTO TABLE ; SILVER.ERP_CUST_SS'
  /*======================================================*/

  ---- TRUNCATE TABLE
truncate table silver.erp_cust_ss ;
---- INSERT DATA INTO TABLE
insert into silver.erp_cust_ss (
            cus_id,
            cus_birthdate,
            cus_gender
            )

select substring (cus_id,4,len(cus_id)) as cus_id,
case when cus_birthdate > getdate () then null
     else cus_birthdate
end as cus_birthdate,
case when cus_gender = 'M' or cus_gender = 'Male' then 'Male'
     when cus_gender = 'F' or cus_gender = 'Female' then 'Female'
     else 'N/A'
end as cus_gender
from bronze.erp_cust_ss

PRINT '=============================================='
/*===============================================
  DATA CLEANING FOR TABLE ; SILVER.ERP_LOG
  =============================================*/
  PRINT '>>>>> TRUNCATE TABLE: SILVER.ERP_LOG'
  PRINT '>>>>> INSERT INTO TABLE ; SILVER.ERP_LOG'
  /*======================================================*/

---- TRUNCATE TABLE
truncate table silver.erp_log;
----- INSERT DATA INTO TABLE
insert into silver.erp_log (
            c_id,
            c_country
            )
select replace(c_id,'-','') as c_id,
     CASE 
    WHEN UPPER(TRIM(c_country)) IN ('DE', 'GERMANY') 
        THEN 'GERMANY'
    WHEN UPPER(TRIM(c_country)) IN ('USA', 'US', 'UNITED STATES') 
        THEN 'UNITED STATES'
    WHEN c_country IS NULL OR TRIM(c_country) = '' 
        THEN 'N/A'
    ELSE UPPER(TRIM(c_country))
END as  c_country
from bronze.erp_log 

PRINT '============================================'
/*==========================================
  DATA CLEANING FOR TABLE ; SILVER.ERP_PRD_CAT
  =============================================*/
   PRINT '>>>>> TRUNCATE TABLE: SILVER.ERP_PRD_CAT'
  PRINT '>>>>> INSERT INTO TABLE ; SILVER.ERP_PRD_CAT'
  /*======================================================*/

---- TRUNCATE TABLE
truncate table silver.erp_prd_cat ;
----- INSERT DATA INTO TABLE
insert into  silver.erp_prd_cat (
            prdd_id,
            prdd_cat,
            prdd_subcat,
            prdd_maintaince
            )
      
select prdd_id,
trim(prdd_cat) as prdd_cat,
trim(prdd_subcat) as prdd_subcat,
trim (prdd_maintaince) as prdd_maintaince
from bronze.erp_prd_cat
PRINT '============================================'
SET @ENDTIME = GETDATE()
/*===============================================
  TIME DURATION 
  ===========================================*/
 PRINT '=================================================='
 PRINT '>>> TIME DURATION' + ' ' +  CAST (DATEDIFF(SECOND,@STARTTIME,@ENDTIME) AS NVARCHAR)+ 'SECONDS'
 PRINT '==================================================='
 END TRY 
     BEGIN CATCH
    PRINT '=========================';
    PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
    PRINT 'ERROR MESSAGE: ' + ERROR_MESSAGE();
    PRINT 'ERROR NUMBER: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
    PRINT 'ERROR STATE: ' + CAST(ERROR_STATE() AS NVARCHAR);
    PRINT '=========================';
END CATCH
end
