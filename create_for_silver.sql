CREATE SCHEMA IF NOT EXISTS silver;


DROP TABLE IF EXISTS silver.crm_cust_info;

CREATE TABLE silver.crm_cust_info (
    cst_id              VARCHAR(50),
	cst_cat_id			VARCHAR(50),
    cst_key             VARCHAR(50),
    cst_first_name      VARCHAR(50),
    cst_last_name       VARCHAR(50),
    cst_marital_status  VARCHAR(50), -- Check if your CSV has 'material' or 'marital'
    cst_gender          VARCHAR(50),
    cst_create_date     VARCHAR(50),
	dwh_create_date 	DATE DEFAULT CURRENT_DATE
);



DROP TABLE IF EXISTS silver.crm_prd_info;

CREATE TABLE silver.crm_prd_info (
    prd_id          VARCHAR(50),
    prd_key         VARCHAR(50),
    prd_name        VARCHAR(50),
    prd_cost        VARCHAR(50),
    prd_line        VARCHAR(50),
    prd_start_date  VARCHAR(50),
    prd_end_date    VARCHAR(50),
	dwh_create_date 	DATE DEFAULT CURRENT_DATE
);


DROP TABLE IF EXISTS silver.crm_sales_info;

CREATE TABLE silver.crm_sales_info (
    sls_ord_num     VARCHAR(50),
    sls_prd_key     VARCHAR(50),
    sls_cust_id     VARCHAR(50),
    sls_order_dt    VARCHAR(50),
    sls_ship_dt     VARCHAR(50),
    sls_due_dt      VARCHAR(50),
    sls_sales       VARCHAR(50),
    sls_quantity    VARCHAR(50),
    sls_price       VARCHAR(50),
	dwh_create_date 	DATE DEFAULT CURRENT_DATE
);


DROP TABLE IF EXISTS silver.erp_cust_info;

CREATE TABLE silver.erp_cust_info (
    cust_id     VARCHAR(50),
    birth_date  VARCHAR(50),
    gender      VARCHAR(50),
	dwh_create_date 	DATE DEFAULT CURRENT_DATE
);



DROP TABLE IF EXISTS silver.erp_cust_region;

CREATE TABLE silver.erp_cust_region (
    cust_id     VARCHAR(50),
    country     VARCHAR(50),
	dwh_create_date 	DATE DEFAULT CURRENT_DATE
);


DROP TABLE IF EXISTS silver.erp_prd_info;

CREATE TABLE silver.erp_prd_info (
    prd_id       VARCHAR(50),
    cat          VARCHAR(50),
    subcat       VARCHAR(50),
    maintenance  VARCHAR(50),
	dwh_create_date 	DATE DEFAULT CURRENT_DATE
);


select * from silver.crm_cust_info;
