
-- Create procedure to create/drop bronze schema objects
CREATE OR REPLACE PROCEDURE bronze.create_bronze_objects()
LANGUAGE plpgsql
AS $$
BEGIN
    -- create schema if not exists
    PERFORM 1 FROM pg_namespace WHERE nspname = 'bronze';
    IF NOT FOUND THEN
        EXECUTE 'CREATE SCHEMA bronze';
    END IF;

    -- Drop tables if they exist (CASCADE to remove dependent objects)
    EXECUTE 'DROP TABLE IF EXISTS bronze.crm_cust_info CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS bronze.crm_prd_info CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS bronze.crm_sales_details CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS bronze.erp_loc_a101 CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS bronze.erp_cust_az12 CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2 CASCADE';

    -- Create crm_cust_info
    EXECUTE $sql$
    CREATE TABLE bronze.crm_cust_info (
        cst_id              integer,
        cst_key             varchar(50),
        cst_firstname       varchar(50),
        cst_lastname        varchar(50),
        cst_marital_status  varchar(50),
        cst_gndr            varchar(50),
        cst_create_date     date
    );
    $sql$;

    -- Create crm_prd_info
    EXECUTE $sql$
    CREATE TABLE bronze.crm_prd_info (
        prd_id       integer,
        prd_key      varchar(50),
        prd_nm       varchar(50),
        prd_cost     integer,
        prd_line     varchar(50),
        prd_start_dt timestamp without time zone,
        prd_end_dt   timestamp without time zone
    );
    $sql$;

    -- Create crm_sales_details (date columns as text for safe import)
    EXECUTE $sql$
    CREATE TABLE bronze.crm_sales_details (
        sls_ord_num  varchar(50),
        sls_prd_key  varchar(50),
        sls_cust_id  integer,
        sls_order_dt text,    -- use text for safe COPY/import, convert later to date
        sls_ship_dt  text,    -- use text for safe COPY/import
        sls_due_dt   text,    -- use text for safe COPY/import
        sls_sales    integer,
        sls_quantity integer,
        sls_price    integer
    );
    $sql$;

    -- Create erp_loc_a101
    EXECUTE $sql$
    CREATE TABLE bronze.erp_loc_a101 (
        cid    varchar(50),
        cntry  varchar(50)
    );
    $sql$;

    -- Create erp_cust_az12
    EXECUTE $sql$
    CREATE TABLE bronze.erp_cust_az12 (
        cid    varchar(50),
        bdate  date,
        gen    varchar(50)
    );
    $sql$;

    -- Create erp_px_cat_g1v2
    EXECUTE $sql$
    CREATE TABLE bronze.erp_px_cat_g1v2 (
        id           varchar(50),
        cat          varchar(50),
        subcat       varchar(50),
        maintenance  varchar(50)
    );
    $sql$;

END;
$$;
