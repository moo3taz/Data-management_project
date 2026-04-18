--check for nulls or duplicates in primary key--
select
cst_id,
count(*)
from bronze_layer.crm_cust_info
group by cst_id
having count(*) > 1 or cst_id is null

----remove duplicates----------
select * from (
select *,
ROW_NUMBER() over (partition by cst_id order by cst_create_date desc) as flag_test
from bronze_layer.crm_cust_info
)t where flag_test = 1

----check unewanted spaces---
select cst_first_name
from bronze_layer.crm_cust_info
where cst_first_name != trim(cst_first_name)

-----remove un wanted spaces--------
select cst_id,cst_key ,trim(cst_first_name) as trim_fname 
,trim(cst_last_name) as trim_lname, 
cst_gndr,cst_create_date   from (
select *,
ROW_NUMBER() over (partition by cst_id order by cst_create_date desc) as flag_test
from bronze_layer.crm_cust_info
where cst_id is not null
)t where flag_test = 1

-----data consistency-----
select distinct cst_gndr 
from bronze_layer.crm_cust_info

----------first table of crm-------------
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


-----------second table of crm---------------
select * from bronze_layer.crm_prd_info

--check for nulls or duplicates in primary key--
select
prd_id,
count(*)
from bronze_layer.crm_prd_info
group by prd_id
having count(*) > 1 or prd_id is null
----all is good-----------------

-----------second table of crm---------------
select 
prd_id,
prd_key,
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


--------third table of crm---------

---check prd_line-----
select distinct prd_line
from bronze_layer.crm_prd_info

---check dates--------
select 
nullif(sls_due_dt ,0) sls_due_dt
from bronze_layer.crm_sales_details
where sls_due_dt <=0 or len(sls_due_dt)!=8

-----check business rule-----
----sales = quantity * price---
---negative , zeros , nulls not allowed---
select distinct
sls_sales as old_sls_sales,   
    sls_quantity, 
    sls_price as old_sls_price,
case when  sls_sales is null or sls_sales <=0 or sls_sales!=sls_quantity * abs(sls_price)
     then sls_quantity * abs(sls_price)
	 else sls_sales
end  as sls_sales,

case when  sls_price is null or sls_price <=0 
     then sls_sales / nullif(sls_quantity,0)
	 else sls_price
end as sls_price
from bronze_layer.crm_sales_details
where sls_sales != sls_quantity * sls_price
or sls_sales is null or sls_quantity is null or sls_price is null
or sls_sales <=0 or sls_quantity <=0 or sls_price <=0
order by sls_sales, sls_quantity,  sls_price   


--------third table of crm---------
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
end as sls_sales,

	case when  sls_price is null or sls_price <=0 
     then sls_sales / nullif(sls_quantity,0)
	 else sls_price
end as sls_price,
    sls_quantity  
from bronze_layer.crm_sales_details
where sls_prd_key not in (select prd_key from silver_layer.crm_prd_info)

------first table of erp------
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


------second table of erp------
---remove - in cid--------
select 
replace(cid , '-','') cid,
case when trim(cntry) = 'DE' then 'Germany'
     when trim(cntry) in ('US','USA') then 'United States'
	 when trim(cntry) = '' or cntry is null then 'n/a'
	 else trim(cntry)
end cntry 
from bronze_layer.erp_loc_a101 

----data consistency----------
select distinct cntry asold_cntry,
case when trim(cntry) = 'DE' then 'Germany'
     when trim(cntry) in ('US','USA') then 'United States'
	 when trim(cntry) = '' or cntry is null then 'n/a'
	 else trim(cntry)
end cntry 
from bronze_layer.erp_loc_a101 
order by cntry


------third table of erp------
select id,
cat,
subcat,
maintenance
from bronze_layer.erp_px_cat_g1v2
----check for unwanted spaces-----
select id,
cat,
subcat,
maintenance
from bronze_layer.erp_px_cat_g1v2
where cat != trim(cat) or subcat != trim(subcat) or maintenance != trim(maintenance)

-----data consistency--------
select distinct cat , subcat , maintenance
from bronze_layer.erp_px_cat_g1v2

