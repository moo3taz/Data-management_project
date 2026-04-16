IF OBJECT_ID('bronze_layer.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE bronze_layer.crm_cust_info;
GO
create table bronze_layer.crm_cust_info(
cst_id int,
cst_key nvarchar(50),
cst_first_name nvarchar(50),
cst_last_name nvarchar(50),
cst_material_status nvarchar(50),
cst_gndr nvarchar(50),
cst_create_date date,
);
GO


IF OBJECT_ID('bronze_layer.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE bronze_layer.crm_prd_info;
GO
create table bronze_layer.crm_prd_info(
prd_id int,
prd_key nvarchar(50),
prd_nm nvarchar(50),
prd_cost int,
prd_line nvarchar(50),
prd_start_dt datetime,
prd_end_dt datetime,
);
GO

IF OBJECT_ID('bronze_layer.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE bronze_layer.crm_sales_details;
GO
CREATE TABLE bronze_layer.crm_sales_details (
    sls_ord_num  NVARCHAR(50),
    sls_prd_key  NVARCHAR(50),
    sls_cust_id  INT,
    sls_order_dt INT,
    sls_ship_dt  INT,
    sls_due_dt   INT,
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT
);
GO

IF OBJECT_ID('bronze_layer.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE bronze_layer.erp_loc_a101;
GO
CREATE TABLE bronze_layer.erp_loc_a101 (
    cid    NVARCHAR(50),
    cntry  NVARCHAR(50)
);
GO

IF OBJECT_ID('bronze_layer.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE bronze_layer.erp_cust_az12;
GO
CREATE TABLE bronze_layer.erp_cust_az12 (
    cid    NVARCHAR(50),
    bdate  DATE,
    gen    NVARCHAR(50)
);
GO

IF OBJECT_ID('bronze_layer.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE bronze_layer.erp_px_cat_g1v2;
GO
CREATE TABLE bronze_layer.erp_px_cat_g1v2 (
    id           NVARCHAR(50),
    cat          NVARCHAR(50),
    subcat       NVARCHAR(50),
    maintenance  NVARCHAR(50)
);
GO



---insert csv files into tables-----
---ETL----------------------------
---building bronze layer---------



create or alter procedure bronze_layer.load_bronze as
begin
declare @start_time datetime , @end_time datetime
   begin try
print'========================';
print'loading the bronze layer';
print'========================';


print'========================';
print'loading crm tables';
print'========================';

set @start_time=getdate();
truncate table bronze_layer.crm_cust_info
bulk insert bronze_layer.crm_cust_info
from 'C:\Users\Password\Desktop\datawarehouse pro\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
with(
firstrow = 2,
fieldterminator =',',
tablock 
);
set @end_time=getdate();
print'loading duration ' + cast(datediff(second , @start_time ,@end_time) as nvarchar) +'seconds'

set @start_time=getdate();
truncate table bronze_layer.crm_prd_info
bulk insert bronze_layer.crm_prd_info
from 'C:\Users\Password\Desktop\datawarehouse pro\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
with(
firstrow = 2,
fieldterminator =',',
tablock 
);
set @end_time=getdate();
print'loading duration ' +  cast(datediff(second , @start_time ,@end_time) as nvarchar) +'seconds'

set @start_time=getdate();
truncate table bronze_layer.crm_sales_details
bulk insert bronze_layer.crm_sales_details
from 'C:\Users\Password\Desktop\datawarehouse pro\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
with(
firstrow = 2,
fieldterminator =',',
tablock 
);
set @end_time=getdate();
print'loading duration ' +  cast(datediff(second , @start_time ,@end_time) as nvarchar) + 'seconds'

print'========================';
print'loading erp tables';
print'========================';

set @start_time=getdate();
truncate table bronze_layer.erp_loc_a101
bulk insert bronze_layer.erp_loc_a101
from 'C:\Users\Password\Desktop\datawarehouse pro\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
with(
firstrow = 2,
fieldterminator =',',
tablock 
);
set @end_time=getdate();
print'loading duration ' +  cast(datediff(second , @start_time ,@end_time) as nvarchar) + 'seconds'

set @start_time=getdate();
truncate table bronze_layer.erp_cust_az12
bulk insert bronze_layer.erp_cust_az12
from 'C:\Users\Password\Desktop\datawarehouse pro\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
with(
firstrow = 2,
fieldterminator =',',
tablock 
);
set @end_time=getdate();
print'loading duration ' +  cast(datediff(second , @start_time ,@end_time) as nvarchar) + 'seconds'

set @start_time=getdate();
truncate table bronze_layer.erp_px_cat_g1v2
bulk insert bronze_layer.erp_px_cat_g1v2
from 'C:\Users\Password\Desktop\datawarehouse pro\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
with(
firstrow = 2,
fieldterminator =',',
tablock 
);
set @end_time=getdate();
print'loading duration ' +  cast(datediff(second , @start_time ,@end_time) as nvarchar) +  'seconds'
  end try
  begin catch
  print'========================';
  print'error occured during loading layer';
  print'========================';
  end catch
end;

exec bronze_layer.load_bronze
