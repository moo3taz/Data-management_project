------------joining customers table (dimensions)------------------
create view gold_layer.dim_customer as
select  
	ROW_NUMBER() over (order by cst_id) as customer_key,
	ci.cst_id as customer_id,
	ci.cst_key as customer_number,
	ci.cst_first_name as first_name,
	ci.cst_last_name as last_name,
	la.cntry as country,
	ci.cst_material_status as marital_status,
	case when ci.cst_gndr != 'n\a' then ci.cst_gndr
		 else coalesce(ca.gen , 'n\a')
	end as gender,
	ca.bdate as birth_date,
	ci.cst_create_date as create_date
from silver_layer.crm_cust_info as ci
left join silver_layer.erp_cust_az12 as ca
on ci.cst_key = ca.cid
left join silver_layer.erp_loc_a101 as la
on ci.cst_key = la.cid

-----to check duplicates---------------------
select cst_id , COUNT(*) from
(select  
ci.cst_id,
ci.cst_key,
ci.cst_first_name,
ci.cst_last_name,
ci.cst_material_status,
ci.cst_gndr,
ci.cst_create_date,
ca.bdate,
ca.gen,
la.cntry
from silver_layer.crm_cust_info as ci
left join silver_layer.erp_cust_az12 as ca
on ci.cst_key = ca.cid
left join silver_layer.erp_loc_a101 as la
on ci.cst_key = la.cid
) t group by cst_id
having count(*) > 1

--------------data integration----------
select distinct
ci.cst_gndr,
ca.gen,
case when ci.cst_gndr != 'n\a' then ci.cst_gndr
     else coalesce(ca.gen , 'n\a')
end as new_gen
from silver_layer.crm_cust_info as ci
left join silver_layer.erp_cust_az12 as ca
on ci.cst_key = ca.cid
left join silver_layer.erp_loc_a101 as la
on ci.cst_key = la.cid
order by ci.cst_gndr,ca.gen

----check gold view----------
select  distinct gender from gold_layer.dim_customer


-----------------------------------------------------------
------------joining products table (dimensions)------------------------
create view gold_layer.dim_products as
select 
ROW_NUMBER() over(order by pn.prd_start_dt ,pn.prd_key) as product_key,
	pn.prd_id as product_id,
	pn.prd_key as product_number,
	pn.prd_nm as product_name,
	pn.cat_id as category_id,
	pc.cat as category,
	pc.subcat as subcategory,
	pc.maintenance,
	pn.prd_cost as product_cost,
	pn.prd_line as product_line,
	pn.prd_start_dt as start_date
from silver_layer.crm_prd_info as pn 
left join silver_layer.erp_px_cat_g1v2 as pc
on pn.cat_id = pc.id
where prd_end_dt is null--- filter out all historical data

---------------- check no duplicates---------
select prd_key , count(*) from
(
select 
	pn.prd_id,
	pn.cat_id,
	pn.prd_key,
	pn.prd_nm,
	pn.prd_cost,
	pn.prd_line,
	pn.prd_start_dt,
	pc.cat,
	pc.subcat,
	pc.maintenance
from silver_layer.crm_prd_info as pn 
left join silver_layer.erp_px_cat_g1v2 as pc
on pn.cat_id = pc.id
where prd_end_dt is null
) t group by prd_key
having count(*) > 1

-----check second gold view-----
select * from gold_layer.dim_products

-------fact table----------------------
create view gold_layer.fact_sales as
select sd.sls_ord_num as order_number,
       pr.product_key,
	   cu.customer_key,
       sd.sls_order_dt as order_date,
       sd.sls_ship_dt as shipping_date,
       sd.sls_due_dt as due_date,
       sd.sls_sales as sales_amount,
       sd.sls_quantity as sales_quantity,
       sd.sls_price as price
from silver_layer.crm_sales_details as sd
left join gold_layer.dim_products as pr
on sd.sls_prd_key = pr.product_number
left join gold_layer.dim_customer as cu
on sd.sls_cust_id =cu.customer_id

-------check quality of fact table----
select * from gold_layer.fact_sales f
left join gold_layer.dim_customer  c
on f.customer_key= c.customer_key
left join gold_layer.dim_products p
on p.product_key = f.product_key
where p.product_key is null


select * from gold_layer.fact_sales
