CREATE FUNCTION public.createpartition(table_name character varying, forecast_id integer) RETURNS character varying
    LANGUAGE plpgsql
    AS $$
DECLARE
    partition_name varchar(30);
    sql_query text;
BEGIN
    partition_name = table_name || '_' ||  forecast_id::varchar(10);
    sql_query = format('CREATE TABLE %I PARTITION OF %I FOR VALUES IN (%L)', partition_name, table_name, forecast_id);   
    EXECUTE sql_query;
    RETURN partition_name;
EXCEPTION
    WHEN duplicate_table THEN 
        RAISE NOTICE 'caught duplicate_table'; 
        RETURN 'duplicate_table'; 
    WHEN OTHERS THEN 
        RAISE NOTICE 'caught others'; 
        RETURN 'others';   
END;
$$; 

CREATE FUNCTION public.droppartition(table_name character varying, forecast_id integer) RETURNS character varying
    LANGUAGE plpgsql
    AS $$
DECLARE
    partition_name varchar(30);
    sql_query text;
    sql_state text;
BEGIN
    partition_name = table_name || '_' ||  forecast_id::varchar(10);
    sql_query = format('ALTER TABLE %I DETACH PARTITION %I', table_name, partition_name); 
    EXECUTE sql_query;  
    sql_query = format('DROP TABLE %I', partition_name);   
    EXECUTE sql_query;
    RETURN partition_name;
EXCEPTION
    WHEN undefined_table THEN 
        RAISE NOTICE 'caught undefined_table'; 
        RETURN 'undefined_table'; 
    WHEN OTHERS THEN 
        GET STACKED DIAGNOSTICS sql_state = RETURNED_SQLSTATE;
        RAISE NOTICE 'caught others'; 
        RETURN sql_state;   
END;
$$;
