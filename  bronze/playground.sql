select cst_id, count(*)
from bronze.crm_cust_info
group by cst_id
having count(*)>1 or cst_id is null;


select * from bronze.crm_cust_info
where cst_id = 29466;




select * from(
select *, row_number() over (partition by cst_id order by cst_create_date desc)
as flag_last from bronze.crm_cust_info
)t where flag_last=1;



-- check for unwanted spaces


select cst_first_name from bronze.crm_cust_info
where cst_first_name != trim(cst_first_name);

select cst_last_name from bronze.crm_cust_info
where cst_last_name != trim(cst_last_name);

select cst_gender from bronze.crm_cust_info
where cst_gender != trim(cst_gender);

select * from bronze.crm_prd_info;
select * from bronze.crm_cust_info;
select * from silver.crm_cust_info;
select * from silver.crm_prd_info;






select * from bronze.crm_sales_info
where length(sls_order_date) < 8;
select * from silver.crm_cust_info;



select * from bronze.crm_sales_info
where sls_cust_id not in (select cst_id from silver.crm_cust_info);




select * from silver.crm_sales_info;

select * from bronze.crm_sales_info;

select * from bronze.crm_sales_info
where cast(sls_order_date) as date > cast(sls_ship_date) as date;



select sls_quantity, sls_sales, sls_price from bronze.crm_sales_info
where sls_quantity is null or sls_price is null or sls_sales is null
or sls_sales != sls_price * sls_quantity or
sls_quantity <=0 or sls_sales <=0 or sls_price <=0;




SELECT sls_ord_num, sls_order_date
FROM bronze.crm_sales_info
WHERE LENGTH(sls_order_date) = 8
AND (
    CAST(SUBSTRING(sls_order_date, 5, 2) AS INT) > 12  -- Check for Month > 12
    OR 
    CAST(SUBSTRING(sls_order_date, 7, 2) AS INT) > 31  -- Check for Day > 31
);



select sls_order_date, to_date(sls_order_date, 'YYYY-MM-DD')AS cast_date
from bronze.crm_sales_info;

select * from bronze.crm_sales_info
where sls_order_date= '20101229';