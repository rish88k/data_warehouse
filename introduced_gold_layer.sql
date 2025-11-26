
CREATE VIEW gold.dim_customers as
	select 
		row_number() over (order by cst_id) as customer_key,
		ci.cst_id as customer_id,
		ci.cst_key as customer_number,
		ci.cst_first_name as first_name,
		ci.cst_last_name as last_name,
		ci.cst_marital_status as marital_status,
		case 
			when ci.cst_gender != 'n/a' then ci.cst_gender
			else coalesce(dj.gender, 'n/a')
		end as gender,
		ci.cst_create_date as create_date,
		dj.birth_date as birth_date,
		ek.country as country
	from silver.crm_cust_info ci
	left join silver.erp_cust_info dj
	on ci.cst_key = dj.cust_id
	left join silver.erp_cust_region ek
	on ci.cst_key = ek.cust_id;


select gender, count(gender) as count_gen from silver.erp_cust_info group by gender;
select * from silver.erp_prd_info;
select * from silver.crm_prd_info;






CREATE VIEW gold.dim.products as
	select 
	row_number() over (order by ci.prd_start_date, ci.prd_key) as prd_key
	ci.prd_id as product_id,
	ci.prd_number as product_number,
	ci.prd_name as product_name,
	ci.prd_cost as product_cost,
	ci.prd_line as product_line,
	ci.prd_start_date as start_date,
	dj.cat as category,
	dj.subcat as subcategory,
	dj.maintenance as maintenance
	from silver.crm_prd_info ci
	left join silver.erp_prd_info dj
	on ci.prd_cat_id = dj.prd_id
	where ci.prd_end_date is null



CREATE VIEW gold.fact_sales as
select
pr.product_key,
cu.customer_key,
sd.ord_num as order_number,
sd.prd_key as product_key,
sd.cust_id as customer_id,
sd.order_date as order_date,
sd.ship_date as ship_date,
sd.due_date as due_date,
sd.sales as sales,
sd.quantity as quantity,
sd.price as price
from silver.crm_sales_details sd
left join gold.dim_products pr
on sd.sls_prd_key = pr.product_number
left join gold.dim_customers cu


