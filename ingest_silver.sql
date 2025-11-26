CREATE PROCEDURE silver.load_procedure_final()
language plpgsql
as $$ 


DECLARE
start_time TIMESTAMP;
end_time TIMESTAMP;

BEGIN
	RAISE NOTICE '=============================';
	RAISE NOTICE 'loading silver layer';
	
	
	
	
	RAISE NOTICE 'truncating silver.crm_cust_info';
	TRUNCATE TABLE silver.crm_cust_info;
	RAISE NOTICE 'inserting silver.crm_cust_info';
	
	start_time:=now();
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
	end_time:= now();
	
	RAISE NOTICE 'start_time: %, end_time: %, DURATION: %', start_time, end_time, (end_time - start_time);
	RAISE NOTICE 'LOADED CUST_INFO';
	
	
	
	
	
	
	RAISE NOTICE 'truncating silver.crm_prd_info';
	TRUNCATE TABLE silver.crm_prd_info;
	RAISE NOTICE 'inserting silver.crm_prd_info';
	
	start_time:= now();
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
	end_time:=now();
	
	RAISE NOTICE 'start_time: %, end_time: %, DURATION: %', start_time, end_time, (end_time - start_time);
	RAISE NOTICE 'LOADED PROD_INFO';
	
	
	RAISE NOTICE 'truncating silver.crm_sales_info';
	TRUNCATE TABLE silver.crm_sales_info;
	RAISE NOTICE 'inserting silver.crm_sales_info';
	
	start_time:= now();
		INSERT INTO silver.crm_sales_info(
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price)
			
		select 
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		case 
			when sls_order_date = '0' or length(sls_order_date) != 8 then null
			-- T0_DATE function could also be BUT IT DIDNT WORK HERE. LOOK IT UP.
			else sls_order_date::DATE
		end as sls_order_date,
		sls_ship_date,
		sls_due_date,
		case 
			when sls_sales is null or sls_sales != sls_quantity * abs(sls_price) or sls_sales<=0
			then sls_quantity * abs(sls_price)
			else sls_sales
		end as sls_sales,
		sls_quantity,
		case 
			when sls_price<=0 or sls_price is null then
			sls_sales/nullif(sls_quantity, 0)
			else sls_price
		end as sls_price
		from bronze.crm_sales_info;
	end_time:=now();
	
	RAISE NOTICE 'start_time: %, end_time: %, DURATION: %', start_time, end_time, (end_time - start_time);
	RAISE NOTICE 'LOADED SALES_INFO';
	
	
	RAISE NOTICE 'truncating silver.erp_cust_info';
	TRUNCATE TABLE silver.erp_cust_info;
	RAISE NOTICE 'inserting silver.erp_cust_info';
	
	start_time:= now();
		insert into silver.erp_cust_info(
		cust_id,
		birth_date,
		gender )
		
		select 
		case 
			when cust_id like 'NAS%' THEN substring(cust_id, 4, length(cust_id))
			else cust_id
		end as cust_id,
		case 
			when birth_date > current_date or birth_date < '1925-11-26'
			then NULL
			else birth_date
		end as birth_date,
		case 
			when upper(trim(gender)) in ('F', 'female') then 'Female'
			when upper(trim(gender)) in ('M', 'Male') then 'Male'
			else 'n/a'
		end as gender
		from bronze.erp_cust_info;
	end_time:=now();
	
	RAISE NOTICE 'start_time: %, end_time: %, DURATION: %', start_time, end_time, (end_time - start_time);
	RAISE NOTICE 'LOADED ERP_CUST_INFO';
	
	
	RAISE NOTICE 'truncating silver.erp_cust_region';
	TRUNCATE TABLE silver.erp_cust_region;
	RAISE NOTICE 'inserting silver.erp_cust_region';
	
	start_time:= now();
		insert into silver.erp_cust_region(
		cust_id,
		country )
		
		select replace(cust_id, '-', '') cust_id,
		case 
			when trim(country)= '' or null then 'n/a'
			when trim(country)= 'DE'then 'Germany'
			when trim(country) in ('US','USA') then 'United States'
			else trim(country)
		end as country
		from bronze.erp_cust_region;
	end_time:=now();
	
	RAISE NOTICE 'start_time: %, end_time: %, DURATION: %', start_time, end_time, (end_time - start_time);
	RAISE NOTICE 'LOADED ERP_CUST_REGION';
	
	
	RAISE NOTICE 'truncating silver.erp_prd_info';
	TRUNCATE TABLE silver.erp_prd_info;
	RAISE NOTICE 'inserting silver.erp_prd_info';
	
	start_time:= now();
		insert into silver.erp_prd_info(
		prd_id,
		cat,
		subcat,
		maintenance )
		select 
		prd_id,
		cat,
		subcat,
		maintenance
		from bronze.erp_prd_info;
	end_time:=now();
	
	RAISE NOTICE 'start_time: %, end_time: %, DURATION: %', start_time, end_time, (end_time - start_time);
	RAISE NOTICE 'LOADED ERP_PRD_INFO';

	RAISE NOTICE 'LOADED ALL TABLES';
	
	EXCEPTION
	WHEN OTHERS THEN
	RAISE NOTICE 'error message: %', sqlerrm;
END;
$$


