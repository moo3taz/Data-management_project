---explore all objects in database------
select * from INFORMATION_SCHEMA.TABLES

--explore all columns in database------
select * from INFORMATION_SCHEMA.COLUMNS
where TABLE_NAME ='dim_customer'


------Dimension exploration---------
---explore all coutries of our customers--
select distinct country from gold_layer.dim_customer

---explore all categories----
select distinct category,subcategory ,product_name
from gold_layer.dim_products
order by 1,2,3

-----Date exploration---------
----find the date of first and last order--
select min(order_date) first_order_date,
max(order_date) last_order_date
from gold_layer.fact_sales

---how many years of sales are avaliable--
select
min(order_date) first_order_date,
max(order_date) last_order_date,
DATEDIFF(YEAR,min(order_date),max(order_date)) orders_range_years
from gold_layer.fact_sales

---youngest and oldest customer---
select 
min(birth_date)  as oldest_customer,
DATEDIFF(year, min(birth_date), GETDATE()) oldest_age,
max(birth_date) as youngest_customer,
DATEDIFF(year, max(birth_date), GETDATE()) youngest_age
from gold_layer.dim_customer

select * from gold_layer.fact_sales
------Measures Exploration---------
----total sales----
select sum(sales_amount) as total_sales
from gold_layer.fact_sales

---how many items are sold--------
select sum(sales_quantity) as total_items
from gold_layer.fact_sales

----Average prive---------------
select avg(price) as averge_price
from gold_layer.fact_sales

----total number of orders-------
select count(order_number) as total_orders
from gold_layer.fact_sales

select count(distinct order_number) as total_orders
from gold_layer.fact_sales

---total number of products----
select count( product_key) as total_products
from gold_layer.dim_products

------total number of customers----
select count( customer_key) as total_customers
from gold_layer.dim_customer

------------------------the Report-----------------
select 'total_sales' as measure_name, sum(sales_amount) as measure_value from gold_layer.fact_sales
union all
select 'total_quantity' ,sum(sales_quantity) from gold_layer.fact_sales
union all
select 'averge_price' ,avg(price) from gold_layer.fact_sales
union all
select 'total.nr_orders' ,count(distinct order_number) from gold_layer.fact_sales
union all
select 'total.nr_products' ,count( product_key) from gold_layer.dim_products
union all
select 'total_customers',count( customer_key) from gold_layer.dim_customer


------Insights (measures by dimensions)-----------------

----------------total number of customers by country-----
select country ,
count(customer_key) as total_customers
from gold_layer.dim_customer
group by country
order by total_customers desc

------------total customers by gender-------
select gender,
count(customer_key) as total_customers
from gold_layer.dim_customer
group by gender
order by total_customers desc

-----total product by category-------
select category,
count(product_key) as total_products
from gold_layer.dim_products
group by category
order by total_products desc

----------average cost in each category----
select category,
avg(product_cost) as avg_cost
from gold_layer.dim_products
group by category
order by avg_cost desc

-----total revenue for each category------
select
category,
sum(f.sales_amount) as total_revenue
from gold_layer.fact_sales f
left join gold_layer.dim_products p
on p.product_key=f.product_key
group by category
order by total_revenue desc

-----total revenue for each country------
select
country,
sum(f.sales_amount) as total_revenue
from gold_layer.fact_sales f
left join gold_layer.dim_customer c
on c.customer_key=f.customer_key
group by country
order by total_revenue desc

-----total sold items for each country------
select
country,
sum(f.sales_quantity) as total_sold_items
from gold_layer.fact_sales f
left join gold_layer.dim_customer c
on c.customer_key=f.customer_key
group by country
order by total_sold_items desc

---------total revenue generated for each customer-----
select
c.customer_key,
c.first_name,
c.last_name,
sum(f.sales_amount) as total_revenue
from gold_layer.fact_sales f
left join gold_layer.dim_customer c
on c.customer_key=f.customer_key
group by c.customer_key, c.first_name, c.last_name
order by total_revenue desc


----------Ranking--------------
----order value of dimension by measures---
---which best 5 products generate highest revenue---
select top 5
p.product_name,
sum(f.sales_amount) as total_revenue,
FORMAT(sum(f.sales_amount) *1.0 / sum(sum(f.sales_amount))
over(), 'p') as revenue_percentage
from gold_layer.fact_sales f
left join gold_layer.dim_products p
on p.product_key=f.product_key
group by p.product_name
order by total_revenue desc

---which best 5 products generate highest revenue---
select top 5
p.subcategory,
sum(f.sales_amount) as total_revenue,
FORMAT(sum(f.sales_amount) *1.0 / sum(sum(f.sales_amount))
over(), 'p') as revenue_percentage
from gold_layer.fact_sales f
left join gold_layer.dim_products p
on p.product_key=f.product_key
group by p.subcategory
order by total_revenue desc

---which worst 5 products generate lowest revenue---
select top 5
p.product_name,
sum(f.sales_amount) as total_revenue,
FORMAT(sum(f.sales_amount) *1.0 / sum(sum(f.sales_amount))
over(), 'p') as revenue_percentage
from gold_layer.fact_sales f
left join gold_layer.dim_products p
on p.product_key=f.product_key
group by p.product_name
order by total_revenue 

------most categories generate highest revenue--------
select 
p.category,
sum(f.sales_amount) as total_revenue,
FORMAT(sum(f.sales_amount) *1.0 / sum(sum(f.sales_amount))
over(), 'p') as revenue_percentage
from gold_layer.fact_sales f
left join gold_layer.dim_products p
on p.product_key=f.product_key
group by p.category
order by total_revenue desc

---which 5 products most sold---
select top 5
p.product_name,
p.category,
count(f.sales_amount) as total_revenue
from gold_layer.fact_sales f
left join gold_layer.dim_products p
on p.product_key=f.product_key
group by p.product_name ,p.category
order by total_revenue desc

---top 10 customers generate highest revenue---
select top 10
c.customer_key,
c.first_name,
c.last_name,
sum(f.sales_amount) as total_revenue
from gold_layer.fact_sales f
left join gold_layer.dim_customer c
on c.customer_key=f.customer_key
group by c.customer_key, c.first_name, c.last_name
order by total_revenue desc


----- 3 customers with fewest orderd placed
select top 3
c.customer_key,
c.first_name,
c.last_name,
count(distinct order_number) as total_orders
from gold_layer.fact_sales f
left join gold_layer.dim_customer c
on c.customer_key=f.customer_key
group by c.customer_key, c.first_name, c.last_name
order by total_orders 
