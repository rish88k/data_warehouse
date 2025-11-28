ALTER TABLE bronze.crm_cust_info
ALTER COLUMN cst_id type integer
using(
CASE
WHEN cst_id = '0' THEN NULL
WHEN cst_id = '' THEN NULL
ELSE cst_id
END
)::integer,
ALTER COLUMN cst_key type varchar(50)
using(
CASE
WHEN cst_key = '0' THEN NULL
WHEN cst_key = '' THEN NULL
WHEN cst_key = ' ' THEN NULL
ELSE cst_key
END
)::VARCHAR(50),
ALTER COLUMN cst_first_name type varchar(50)
using(
CASE
WHEN cst_first_name = '0' THEN NULL
WHEN cst_first_name = '' THEN NULL
WHEN cst_first_name = ' ' THEN NULL
ELSE cst_first_name
END
)::varchar(50),
ALTER COLUMN cst_last_name type varchar(50)
using(
CASE
WHEN cst_last_name = '0' THEN NULL
WHEN cst_last_name = '' THEN NULL
WHEN cst_last_name = ' ' THEN NULL
ELSE cst_last_name
END
)::varchar(50),
ALTER COLUMN cst_marital_status type varchar(50)
using(
CASE
WHEN cst_marital_status = '0' THEN NULL
WHEN cst_marital_status = '' THEN NULL
WHEN cst_marital_status = ' ' THEN NULL
ELSE cst_marital_status
END
)::varchar(50),
ALTER COLUMN cst_gender type varchar(50)
using(
CASE
WHEN cst_gender = '0' THEN NULL
WHEN cst_gender = '' THEN NULL
WHEN cst_gender = ' ' THEN NULL
ELSE cst_gender
END
)::varchar(50),
ALTER COLUMN cst_create_date TYPE DATE
USING (
    CASE
        WHEN cst_create_date::TEXT = '0' THEN NULL
        WHEN cst_create_date::TEXT = '' THEN NULL
        WHEN cst_create_date::TEXT = ' ' THEN NULL
    -- <--- Catch numbers like "32154" and not make them NULL
        ELSE cst_create_date
    END
)::DATE;

ALTER TABLE bronze.crm_prd_info
    -- 1. Product ID
    ALTER COLUMN prd_id TYPE INTEGER
    USING (
        CASE
            WHEN prd_id::TEXT = '0' THEN NULL
            WHEN prd_id::TEXT = '' THEN NULL
            WHEN prd_id::TEXT = ' ' THEN NULL
            ELSE prd_id
        END
    )::INTEGER,

    -- 2. Product Key
    ALTER COLUMN prd_key TYPE VARCHAR(50)
    USING (
        CASE
            WHEN prd_key::TEXT = '0' THEN NULL
            WHEN prd_key::TEXT = '' THEN NULL
            WHEN prd_key::TEXT = ' ' THEN NULL
            ELSE prd_key
        END
    )::VARCHAR(50),

    -- 3. Product Name
    ALTER COLUMN prd_name TYPE VARCHAR(50) -- Standard name is usually prd_nm
    USING (
        CASE
            WHEN prd_name::TEXT = '0' THEN NULL
            WHEN prd_name::TEXT = '' THEN NULL
            WHEN prd_name::TEXT = ' ' THEN NULL
            ELSE prd_name
        END
    )::VARCHAR(50),

    -- 4. Cost
    ALTER COLUMN prd_cost TYPE INTEGER
    USING (
        CASE
            WHEN prd_cost::TEXT = '0' THEN NULL
            WHEN prd_cost::TEXT = '' THEN NULL
            WHEN prd_cost::TEXT = ' ' THEN NULL
            ELSE prd_cost
        END
    )::INTEGER,

    -- 5. Line
    ALTER COLUMN prd_line TYPE VARCHAR(50)
    USING (
        CASE
            WHEN prd_line::TEXT = '0' THEN NULL
            WHEN prd_line::TEXT = '' THEN NULL
            WHEN prd_line::TEXT = ' ' THEN NULL
            ELSE prd_line
        END
    )::VARCHAR(50),

    -- 6. Start Date
   ALTER COLUMN prd_start_date TYPE DATE
    USING (
        CASE
            WHEN prd_start_date::TEXT = '0' THEN NULL
            WHEN prd_start_date::TEXT = '' THEN NULL
            ELSE prd_start_date
        END
    )::DATE,

    ALTER COLUMN prd_end_date TYPE DATE
    USING (
        CASE
            WHEN prd_end_date::TEXT = '0' THEN NULL
            WHEN prd_end_date::TEXT = '' THEN NULL
            ELSE prd_end_date
        END
    )::DATE;



ALTER TABLE bronze.crm_sales_info
    -- 1. Order Number (Kept as VARCHAR as order IDs often have letters)
    ALTER COLUMN sls_ord_num TYPE VARCHAR(50)
    USING (
        CASE
            WHEN sls_ord_num::TEXT = '0' THEN NULL
            WHEN sls_ord_num::TEXT = '' THEN NULL
            WHEN sls_ord_num::TEXT = ' ' THEN NULL
            ELSE sls_ord_num
        END
    )::VARCHAR(50),

    -- 2. Product Key
    ALTER COLUMN sls_prd_key TYPE VARCHAR(50)
    USING (
        CASE
            WHEN sls_prd_key::TEXT = '0' THEN NULL
            WHEN sls_prd_key::TEXT = '' THEN NULL
            WHEN sls_prd_key::TEXT = ' ' THEN NULL
            ELSE sls_prd_key
        END
    )::VARCHAR(50),

    -- 3. Customer ID
    ALTER COLUMN sls_cust_id TYPE INTEGER
    USING (
        CASE
            WHEN sls_cust_id::TEXT = '0' THEN NULL
            WHEN sls_cust_id::TEXT = '' THEN NULL
            WHEN sls_cust_id::TEXT = ' ' THEN NULL
            ELSE sls_cust_id
        END
    )::INTEGER,

    -- 4. Order Date
	-- FOUND A DIRTY ENTRY THAT HAD ONLY 5 INT SO COULD NOT BE CASTED 
	-- INTO A DATE BY THE ALTER APPROACH DUE TO WHICH THIS QUERY WAS NOT RUN
	-- AND CLEANING OF THIS COLUMN IS HENCEFORTH REQUIRED.
	
    ALTER COLUMN sls_order_date TYPE DATE
    USING (
        CASE
            WHEN sls_order_date::TEXT = '0' THEN NULL
            WHEN sls_order_date::TEXT = '' THEN NULL
            ELSE sls_order_date
        END
    )::DATE,

    ALTER COLUMN sls_ship_date TYPE DATE
    USING (
        CASE
            WHEN sls_ship_date::TEXT = '0' THEN NULL
            WHEN sls_ship_date::TEXT = '' THEN NULL
            ELSE sls_ship_date
        END
    )::DATE,

	ALTER COLUMN sls_due_date TYPE DATE
    USING (
        CASE
            WHEN sls_due_date::TEXT = '0' THEN NULL
            WHEN sls_due_date::TEXT = '' THEN NULL
            ELSE sls_due_date
        END
    )::DATE,
    

    -- 7. Sales (Integer)
    ALTER COLUMN sls_sales TYPE INTEGER
    USING (
        CASE
            WHEN sls_sales::TEXT = '0' THEN NULL
            WHEN sls_sales::TEXT = '' THEN NULL
            WHEN sls_sales::TEXT = ' ' THEN NULL
            ELSE sls_sales
        END
    )::INTEGER,

    -- 8. Quantity (Integer)
    ALTER COLUMN sls_quantity TYPE INTEGER
    USING (
        CASE
            WHEN sls_quantity::TEXT = '0' THEN NULL
            WHEN sls_quantity::TEXT = '' THEN NULL
            WHEN sls_quantity::TEXT = ' ' THEN NULL
            ELSE sls_quantity
        END
    )::INTEGER,

    -- 9. Price (Integer)
    ALTER COLUMN sls_price TYPE INTEGER
    USING (
        CASE
            WHEN sls_price::TEXT = '0' THEN NULL
            WHEN sls_price::TEXT = '' THEN NULL
            WHEN sls_price::TEXT = ' ' THEN NULL
            ELSE sls_price
        END
    )::INTEGER;




ALTER TABLE bronze.erp_cust_info
    -- 1. Customer ID (Usually string in ERP, but likely INT here)
    ALTER COLUMN cust_id TYPE VARCHAR(50)
    USING (
        CASE
            WHEN cust_id::TEXT = '0' THEN NULL
            WHEN cust_id::TEXT = '' THEN NULL
            WHEN cust_id::TEXT = ' ' THEN NULL
            ELSE cust_id
        END
    )::VARCHAR(50),

    -- 2. Birth Date
    ALTER COLUMN birth_date TYPE DATE
    USING (
        CASE
            WHEN birth_date::TEXT = '0' THEN NULL
            WHEN birth_date::TEXT = '' THEN NULL
            WHEN birth_date::TEXT = ' ' THEN NULL
            ELSE birth_date
        END
    )::DATE,

    -- 3. Gender
    ALTER COLUMN gender TYPE VARCHAR(50)
    USING (
        CASE
            WHEN gender::TEXT = '0' THEN NULL
            WHEN gender::TEXT = '' THEN NULL
            WHEN gender::TEXT = ' ' THEN NULL
            ELSE gender
        END
    )::VARCHAR(50);





ALTER TABLE bronze.erp_cust_region
    -- 1. Customer ID
    ALTER COLUMN cust_id TYPE VARCHAR(50)
    USING (
        CASE
            WHEN cust_id::TEXT = '0' THEN NULL
            WHEN cust_id::TEXT = '' THEN NULL
            WHEN cust_id::TEXT = ' ' THEN NULL
            ELSE cust_id
        END
    )::VARCHAR(50),

    -- 2. Country
    ALTER COLUMN country TYPE VARCHAR(50)
    USING (
        CASE
            WHEN country::TEXT = '0' THEN NULL
            WHEN country::TEXT = '' THEN NULL
            WHEN country::TEXT = ' ' THEN NULL
            ELSE country
        END
    )::VARCHAR(50);



ALTER TABLE bronze.erp_prd_info
    -- 1. Product ID
    ALTER COLUMN prd_id TYPE VARCHAR(50)
    USING (
        CASE
            WHEN prd_id::TEXT = '0' THEN NULL
            WHEN prd_id::TEXT = '' THEN NULL
            WHEN prd_id::TEXT = ' ' THEN NULL
            ELSE prd_id
        END
    )::VARCHAR(50),

    -- 2. Category
    ALTER COLUMN cat TYPE VARCHAR(50)
    USING (
        CASE
            WHEN cat::TEXT = '0' THEN NULL
            WHEN cat::TEXT = '' THEN NULL
            WHEN cat::TEXT = ' ' THEN NULL
            ELSE cat
        END
    )::VARCHAR(50),

    -- 3. Sub Category
    ALTER COLUMN subcat TYPE VARCHAR(50)
    USING (
        CASE
            WHEN subcat::TEXT = '0' THEN NULL
            WHEN subcat::TEXT = '' THEN NULL
            WHEN subcat::TEXT = ' ' THEN NULL
            ELSE subcat
        END
    )::VARCHAR(50),

    -- 4. Maintenance
    ALTER COLUMN maintenance TYPE VARCHAR(50)
    USING (
        CASE
            WHEN maintenance::TEXT = '0' THEN NULL
            WHEN maintenance::TEXT = '' THEN NULL
            WHEN maintenance::TEXT = ' ' THEN NULL
            ELSE maintenance
        END
    )::VARCHAR(50);