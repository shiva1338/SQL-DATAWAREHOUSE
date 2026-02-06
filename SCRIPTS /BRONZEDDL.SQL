/*================================================
  EXEC bronze.load_bronze
==================================================*/


CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN 
     
     DECLARE @STARTTIME DATETIME ,  @ENDTIME DATETIME ;
     SET @STARTTIME = GETDATE() ;
     BEGIN TRY 
/* ================================================
   INSERT DATA INTO BRONZE FROM SOURCE FILES
=================================================*/

/* ===================================================
   BULK INSERT INTO BRONZE.CUSTOMERINFO
======================================================*/
 PRINT '==================================================='
 PRINT '>>>>CRM TABLES '
 PRINT 'TRUNCATE TABLE ; BRONZE.CRM_CUST-INFO' 
 /*=====================================================*/
 TRUNCATE TABLE bronze.crm_cust_info;
 /*=====================================================*/
 PRINT '>>>>INSERT INTO ; BRONZE.CRM_CUST_INFO'
 /*=====================================================*/
 BULK INSERT bronze.crm_cust_info
 FROM 'C:\Users\Shiva\Downloads\ff\source_crm\cust_info.csv'
 WITH ( 
        FIRSTROW = 2 ,
        FIELDTERMINATOR = ',',
        TABLOCK 
        )
/* ===================================================================
    BULK INSERT INTO TABLE BRONZE.PRODUCTINFO . SOURCE ; CRM
=====================================================================*/
PRINT 'TRUNCATE TABLE BRONZE.CRM_PRD_INFO'
/*===================================================================*/
  TRUNCATE TABLE bronze.crm_prd_info ;
/*===================================================================*/
  PRINT 'INSERT BRONZE.CRM_PRD_INFO'
 /*=================================================================*/
    BULK INSERT bronze.crm_prd_info 
    FROM 'C:\Users\Shiva\Downloads\ff\source_crm\prd_info.csv'
    WITH ( 
           FIRSTROW = 2,
           FIELDTERMINATOR = ',' ,
           TABLOCK
           )
/* ====================================================================
    BULK INSERT INTO TABLE BRONZE.SALES . SOURCE;CRM 
======================================================================*/
PRINT 'TRUNCATE TABLE BRONZE,CEM_SALES'
/*====================================================================*/
   TRUNCATE TABLE bronze.crm_sales ;
/*======================================================================*/
PRINT  'INSERT TABLE BRONZE.CRM_SALES'
/*========================================================================*/
   BULK INSERT bronze.crm_sales
   FROM 'C:\Users\Shiva\Downloads\ff\source_crm\sales_details.csv'
   WITH ( 
         FIRSTROW = 2,
         FIELDTERMINATOR = ',' ,
         TABLOCK
         )
/* =====================================================================
    BULK INSERT INTO TABLE BRONZE.CUST_SS . SOURCE;ERP
=======================================================================*/
PRINT '=============================================================='
PRINT '>>>>>ERP TABLES '
PRINT '>>>>>TRUNCATE TABLE BRONZE.CUST_SS '
/*=====================================================================*/
   TRUNCATE TABLE bronze.erp_cust_ss ;
 /*====================================================================*/
 PRINT '>>>>> INSERT TABLE BRONZE,CUST_SS'
 /*====================================================================*/
    BULK INSERT bronze.erp_cust_ss
    FROM 'C:\Users\Shiva\Downloads\ff\source_erp\CUST_AZ12.csv'
    WITH (
           FIRSTROW = 2 ,
           FIELDTERMINATOR = ',' ,
           TABLOCK 
           )
           
 /*========================================================================
     BULK INSERT INTO TABLE BRONZE.ERP_LOG . SOURCE;ERP
==========================================================================*/
  PRINT '>>>>> TRUNCATE TALE BRONZE.ERP_LOG'
/*========================================================================*/
  TRUNCATE TABLE bronze.erp_log ;
/*========================================================================*/
PRINT '>>>>INSERT TABLE BRONZE.ERP=LOG'
/*=========================================================================*/
  BULK INSERT bronze.erp_log
  FROM 'C:\Users\Shiva\Downloads\ff\source_erp\LOC_A101.csv'
  WITH ( 
         FIRSTROW = 2 ,
         FIELDTERMINATOR = ',' ,
         TABLOCK
         )
 /*========================================================================
     BULK INSERT INTO TABLE BRONZE.PRD_CAT . SOURCE;ERP
 =========================================================================*/
 PRINT '>>>>> TRUNCATE TABLE BRONZE.ERP_PRD_CAT'
 /*=======================================================================*/
 TRUNCATE TABLE  bronze.erp_prd_cat ;
 /*=========================================================================*/
 PRINT '>>>> INSERT TABLE BRONZE,ERP_PRD_CAT'
 /*=======================================================================*/
 BULK INSERT  bronze.erp_prd_cat
 FROM 'C:\Users\Shiva\Downloads\ff\source_erp\PX_CAT_G1V2.csv'
 WITH (
       FIRSTROW = 2 ,
       FIELDTERMINATOR = ',' ,
       TABLOCK
       )
   SET @ENDTIME = GETDATE() ;
   PRINT '=======================================================================';
   PRINT 'TIME DURATION' +' ' + CAST(DATEDIFF(SECOND,@STARTTIME,@ENDTIME) AS NVARCHAR) + 'SECONDS'
   PRINT '=======================================================================';

END TRY 
     BEGIN CATCH
    PRINT '=========================';
    PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
    PRINT 'ERROR MESSAGE: ' + ERROR_MESSAGE();
    PRINT 'ERROR NUMBER: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
    PRINT 'ERROR STATE: ' + CAST(ERROR_STATE() AS NVARCHAR);
    PRINT '=========================';
END CATCH
END
   
