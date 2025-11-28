insert into silver.crm_cust_info(
	cst_id,
	cst_key,
	cst_first_name,
	cst_last_name,
	cst_marital_status,
	cst_gender,
	cst_create_date)

select 
cst_id,
cst_key,
trim(cst_first_name) as cst_first_name,
trim(cst_last_name) as cst_last_name,
case 
	when upper(trim(cst_marital_status)) = 'S' then 'Single'
	when upper(trim(cst_marital_status)) = 'M' then 'Married'
	else 'n/a'
end as cst_marital_status,
case 
	when upper(trim(cst_gender))= 'M' then 'Male'
	when upper(trim(cst_gender))= 'F' then 'Female'
	else 'n/a'
end as cst_gender,
cst_create_date
from (
select *, row_number() over (partition by cst_id order by cst_create_date desc)
as flag_last from bronze.crm_cust_info
where cst_id is not null
)t where flag_last=1;





alter table silver.crm_prd_info
add column prd_cat_id varchar(50);

INSERT INTO silver.crm_prd_info (
    prd_id,
    prd_cat_id,
    prd_key,
    prd_name,
    prd_cost,
    prd_line,
    prd_start_date,
    prd_end_date
)
/* Step 1: Clean and Transform the data first */
WITH CleanedData AS (
    SELECT 
        prd_id,
        REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS prd_cat_id,
        SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key, -- Use LENGTH() if Spark/Postgres
        prd_name,
        COALESCE(prd_cost, 0) AS prd_cost,
        CASE 
            WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
            WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
            WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
            WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
            ELSE 'n/a'
        END AS prd_line,
        prd_start_date
    FROM bronze.crm_prd_info
)
/* Step 2: Calculate History (SCD Type 2 Logic) on the cleaned data */
SELECT 
    prd_id,
    prd_cat_id,
    prd_key,
    prd_name,
    prd_cost,
    prd_line,
    prd_start_date,
    /* Calculate End Date: Take the NEXT start date and subtract 1 day */
    LEAD(prd_start_date) OVER (PARTITION BY prd_key ORDER BY prd_start_date) - 1 AS prd_end_date
FROM CleanedData;

SELECT * FROM SILVER.CRM_PRD_INFO;