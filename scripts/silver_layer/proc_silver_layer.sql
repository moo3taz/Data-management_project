-----insert into silver layer------
----first table--------
create or alter procedure silver_layer.load_silver as
begin

declare @start_time datetime , @end_time datetime
   begin try
print'========================';
print'loading the silver layer';
print'========================';


print'========================';
print'loading crm tables';
print'========================';
set @start_time=getdate();
	truncate table silver_layer.crm_cust_info;
	insert into silver_layer.crm_cust_info(
	cst_id,
	cst_key ,
	cst_first_name,
	cst_last_name,
	cst_material_status,
	cst_gndr,
	cst_create_date
	)
	select cst_id,
	cst_key ,
	trim(cst_first_name) as trim_fname ,
	trim(cst_last_name) as trim_lname,

	case when upper(trim(cst_material_status)) = 'S' then 'Single'
		 when upper(trim(cst_material_status)) = 'M' then 'Married'
		 else 'n\a'
	end cst_material_status,

	case when upper(trim(cst_gndr)) = 'F' then 'Female'
		 when upper(trim(cst_gndr)) = 'M' then 'Male'
		 else 'n\a'
	end cst_gndr,
	cst_create_date
	from (
		select *,
		ROW_NUMBER() over (partition by cst_id order by cst_create_date desc) as flag_test
		from bronze_layer.crm_cust_info
		where cst_id is not null
		)t where flag_test = 1
set @end_time=getdate();
print'loading duration ' + cast(datediff(second , @start_time ,@end_time) as nvarchar) +'seconds'

	-----insert into silver layer------
	----second table--------
set @start_time=getdate();
	truncate table silver_layer.crm_prd_info;
	insert into silver_layer.crm_prd_info(
	prd_id,
	cat_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
	)
	select 
	prd_id,
	replace(SUBSTRING(prd_key, 1 , 5),'-' ,'_') as cat_id,
	SUBSTRING(prd_key,7 , LEN(prd_key)) as prd_key,
	prd_nm,
	isnull(prd_cost, 0) as prd_cost,
	case when UPPER(trim(prd_line)) = 'M' then 'Mountain'
		 when UPPER(trim(prd_line)) = 'R' then 'Road'
		 when UPPER(trim(prd_line)) = 'S' then 'Other sales'
		 when UPPER(trim(prd_line)) = 'T' then 'touring'
		 else 'n\a'
	end as prd_line,
	cast(prd_start_dt as date)  as prd_start_dt,
	cast (lead(prd_start_dt) over (partition by prd_key order by prd_start_dt)-1  as date)as prd_end_dt
	from bronze_layer.crm_prd_info
set @end_time=getdate();
print'loading duration ' + cast(datediff(second , @start_time ,@end_time) as nvarchar) +'seconds'

	------insert into third table-----
set @start_time=getdate();
	truncate table silver_layer.crm_sales_details;
	insert into silver_layer.crm_sales_details(
	sls_ord_num ,
		sls_prd_key  ,
		sls_cust_id  ,
		sls_order_dt ,
		sls_ship_dt  ,
		sls_due_dt   ,
		sls_sales   ,
		sls_quantity ,
		sls_price   
	)
	select 
		sls_ord_num,
		sls_prd_key,  
		sls_cust_id ,  
		case when sls_order_dt = 0 or len(sls_order_dt)!=8 then null
			 else cast(cast(sls_order_dt as varchar) as date)
			 end sls_order_dt,
		case when sls_ship_dt = 0 or len(sls_ship_dt)!=8 then null
			 else cast(cast(sls_ship_dt as varchar) as date)
			 end sls_ship_dt,
		case when sls_due_dt = 0 or len(sls_due_dt)!=8 then null
			 else cast(cast(sls_due_dt as varchar) as date)
			 end sls_due_dt,	 
		case when  sls_sales is null or sls_sales <=0 or sls_sales!=sls_quantity * abs(sls_price)
		 then sls_quantity * abs(sls_price)
		 else sls_sales
	end  as sls_sales,

		case when  sls_price is null or sls_price <=0 
		 then sls_sales / nullif(sls_quantity,0)
		 else sls_price
	end as sls_price,
		sls_quantity  
	from bronze_layer.crm_sales_details
set @end_time=getdate();
print'loading duration ' + cast(datediff(second , @start_time ,@end_time) as nvarchar) +'seconds'

	-------insert into first table of erp------
set @start_time=getdate();
	truncate table silver_layer.erp_cust_az12;
	insert into silver_layer.erp_cust_az12(
	cid,
	bdate,
	gen
	)
	select  
	case when cid like 'NAS%' then substring(cid , 4 , len(cid))
		 else cid
	end cid,
	case when bdate > Getdate() then null
		 else bdate
	end bdate,
	case when upper(trim(gen)) in( 'M' ,'Male') then 'Male'
		 when upper(trim(gen)) in( 'F' ,'Female') then 'Female'
		 else 'n\a'
	end gen
	from bronze_layer.erp_cust_az12

set @end_time=getdate();
print'loading duration ' + cast(datediff(second , @start_time ,@end_time) as nvarchar) +'seconds'

	-------  insert into second table of erp------
set @start_time=getdate();
	truncate table silver_layer.erp_loc_a101;
	insert into silver_layer.erp_loc_a101(
	cid,
	cntry
	)
	select 
	replace(cid , '-','') cid,
	case when trim(cntry) = 'DE' then 'Germany'
		 when trim(cntry) in ('US','USA') then 'United States'
		 when trim(cntry) = '' or cntry is null then 'n/a'
		 else trim(cntry)
	end cntry 
	from bronze_layer.erp_loc_a101

set @end_time=getdate();
print'loading duration ' + cast(datediff(second , @start_time ,@end_time) as nvarchar) +'seconds'

	-------  insert into third table of erp------
set @start_time=getdate();
	truncate table silver_layer.erp_px_cat_g1v2;
	insert into  silver_layer.erp_px_cat_g1v2(
	id,
	cat,
	subcat,
	maintenance
	)
	select id,
	cat,
	subcat,
	maintenance
	from bronze_layer.erp_px_cat_g1v2

set @end_time=getdate();
print'loading duration ' +  cast(datediff(second , @start_time ,@end_time) as nvarchar) +  'seconds'
  end try
  begin catch
  print'========================';
  print'error occured during loading layer';
  print'========================';
  end catch
end

