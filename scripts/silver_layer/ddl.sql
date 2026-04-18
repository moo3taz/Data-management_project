IF OBJECT_ID('silver_layer.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE silver_layer.crm_cust_info;
GO
create table silver_layer.crm_cust_info(
cst_id int,
cst_key nvarchar(50),
cst_first_name nvarchar(50),
cst_last_name nvarchar(50),
cst_material_status nvarchar(50),
cst_gndr nvarchar(50),
cst_create_date date,
dwh_create_date datetime2 default getdate()
);
GO


IF OBJECT_ID('silver_layer.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE silver_layer.crm_prd_info;
GO
create table silver_layer.crm_prd_info(
prd_id int,
cat_id nvarchar(50),
prd_key nvarchar(50),
prd_nm nvarchar(50),
prd_cost int,
prd_line nvarchar(50),
prd_start_dt date,
prd_end_dt date,
dwh_create_date datetime2 default getdate()
);
GO

IF OBJECT_ID('silver_layer.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE silver_layer.crm_sales_details;
GO
CREATE TABLE silver_layer.crm_sales_details (
    sls_ord_num  NVARCHAR(50),
    sls_prd_key  NVARCHAR(50),
    sls_cust_id  INT,
    sls_order_dt date,
    sls_ship_dt  date,
    sls_due_dt   date,
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT,
	dwh_create_date datetime2 default getdate()
);
GO

IF OBJECT_ID('silver_layer.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE silver_layer.erp_loc_a101;
GO
CREATE TABLE silver_layer.erp_loc_a101 (
    cid    NVARCHAR(50),
    cntry  NVARCHAR(50),
	dwh_create_date datetime2 default getdate()
);
GO

IF OBJECT_ID('silver_layer.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE silver_layer.erp_cust_az12;
GO
CREATE TABLE silver_layer.erp_cust_az12 (
    cid    NVARCHAR(50),
    bdate  DATE,
    gen    NVARCHAR(50),
	dwh_create_date datetime2 default getdate()
);
GO

IF OBJECT_ID('silver_layer.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE silver_layer.erp_px_cat_g1v2;
GO
CREATE TABLE silver_layer.erp_px_cat_g1v2 (
    id           NVARCHAR(50),
    cat          NVARCHAR(50),
    subcat       NVARCHAR(50),
    maintenance  NVARCHAR(50),
	dwh_create_date datetime2 default getdate()
);
GO

