CREATE PROCEDURE bronze.load_procedure_FINAL()
LANGUAGE plpgsql
AS $$

	DECLARE 
		start_time TIMESTAMP;
		end_time TIMESTAMP;
BEGIN 
	RAISE NOTICE '=============================';
	RAISE NOTICE 'loading bronze layer';




	start_time:= now();
	COPY bronze.crm_cust_info
	FROM '/Users/rish88k/Downloads/sql-data-warehouse-project/datasets/source_crm/cust_info.csv'
	WITH (
		FORMAT csv,
		HEADER true,
		NULL '0'
	);
	end_time:= now();
	RAISE NOTICE 'start_time: %, end_time: %, DURATION: %', start_time, end_time, (end_time - start_time);
	RAISE NOTICE 'LOADED CUST_INFO';


	start_time:= now();
	COPY bronze.crm_prd_info
	FROM '/Users/rish88k/Downloads/sql-data-warehouse-project/datasets/source_crm/prd_info.csv'
	WITH (
		FORMAT csv,
		HEADER true,
		NULL '0'
	);
	end_time:= now();
	RAISE NOTICE 'start_time: %, end_time: %, DURATION: %', start_time, end_time, (end_time - start_time);
	RAISE NOTICE 'LOADED PROD_INFO';


	
	start_time:= now();
	COPY bronze.crm_sales_info
	FROM '/Users/rish88k/Downloads/sql-data-warehouse-project/datasets/source_crm/sales_details.csv'
	WITH (
		FORMAT csv,
		HEADER true,
		NULL '0'
	);
	end_time:= now();
	RAISE NOTICE 'start_time: %, end_time: %, DURATION: %', start_time, end_time, (end_time - start_time);
	RAISE NOTICE 'LOADED SALES_INFO';



	start_time:= now();
	COPY bronze.erp_cust_info
	FROM '/Users/rish88k/Downloads/sql-data-warehouse-project/datasets/source_erp/CUST_AZ12.csv'
	WITH (
		FORMAT csv,
		HEADER true,
		NULL '0'
	);
	end_time:= now();
	RAISE NOTICE 'start_time: %, end_time: %, DURATION: %', start_time, end_time, (end_time - start_time);
	RAISE NOTICE 'LOADED ERP_CUST_INFO';



	start_time:= now();
	COPY bronze.erp_cust_region
	FROM '/Users/rish88k/Downloads/sql-data-warehouse-project/datasets/source_erp/LOC_A101.csv'
	WITH (
		FORMAT csv,
		HEADER true,
		NULL '0'
	);
	end_time:= now();
	RAISE NOTICE 'start_time: %, end_time: %, DURATION: %', start_time, end_time, (end_time - start_time);
	RAISE NOTICE 'LOADED ERP_CUST_REGION';


	start_time:= now();
	COPY bronze.erp_prd_info
	FROM '/Users/rish88k/Downloads/sql-data-warehouse-project/datasets/source_erp/PX_CAT_G1V2.csv'
	WITH (
		FORMAT csv,
		HEADER true,
		NULL '0'
	);
	end_time:= now();
	RAISE NOTICE 'start_time: %, end_time: %, DURATION: %', start_time, end_time, (end_time - start_time);
	RAISE NOTICE 'LOADED ERP_PROD_INFO';


RAISE NOTICE 'ALL TABLES LOADED';

EXCEPTION 
	WHEN OTHERS THEN
	RAISE NOTICE 'ERROR MESSAGE: %', SQLERRM;
END;
$$