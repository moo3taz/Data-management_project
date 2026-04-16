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
