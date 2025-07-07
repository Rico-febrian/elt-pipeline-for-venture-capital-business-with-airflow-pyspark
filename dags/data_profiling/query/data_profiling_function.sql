-- This function generates a data profile and quality report for all tables in the database.
CREATE OR REPLACE FUNCTION data_profile_quality()
RETURNS TABLE(
    schema TEXT,
    table_name TEXT,
    n_rows BIGINT,
    n_cols INTEGER,
    data_profile JSONB,
    data_quality JSONB
) AS $$
DECLARE
    r RECORD;
    row_count BIGINT;
    col_count INTEGER;
    col_info JSONB := '{}'::JSONB;
    quality_info JSONB := '{}'::JSONB;
    column_name TEXT;
    column_data_type TEXT;
    sample_query TEXT;
    sample_data JSONB;
    not_null_count BIGINT;
    negative_values_count BIGINT;
    percentage_completeness NUMERIC(5, 2);
    data_quality_completeness_result TEXT;
    is_negative_values BOOLEAN;
    column_quality JSONB;
BEGIN
    -- Loop through each table in the database
    FOR r IN 
        SELECT n.nspname AS schema_name, c.relname AS table_name
        FROM pg_class c
        JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE c.relkind = 'r'  -- Only consider regular tables
          AND n.nspname NOT IN ('information_schema', 'pg_catalog')  -- Exclude system schemas
    LOOP
        -- Get row count
        EXECUTE format('SELECT COUNT(*) FROM %I.%I', r.schema_name, r.table_name) INTO row_count;

        -- Get column count
        EXECUTE format('SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = %L AND table_name = %L', r.schema_name, r.table_name) INTO col_count;

        -- Initialize JSONB objects for column information and quality results
        col_info := '{}'::JSONB;
        quality_info := '{}'::JSONB;

        -- Loop through each column in the current table
        FOR column_name IN
            SELECT a.attname
            FROM pg_attribute a
            JOIN pg_class c ON a.attrelid = c.oid
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE c.relkind = 'r'
              AND a.attnum > 0
              AND n.nspname = r.schema_name
              AND c.relname = r.table_name
        LOOP
            -- Construct sample query for each column
            sample_query := format('
                SELECT JSONB_AGG(value) 
                FROM (
                    SELECT DISTINCT %I::TEXT AS value
                    FROM %I.%I
                    WHERE %I IS NOT NULL
                    LIMIT 5
                ) subquery',
                column_name, r.schema_name, r.table_name, column_name);

            -- Get sample data for the column
            EXECUTE sample_query INTO sample_data;

            -- Get column data type
            EXECUTE format('
                SELECT pg_catalog.format_type(a.atttypid, a.atttypmod)
                FROM pg_attribute a
                JOIN pg_class c ON a.attrelid = c.oid
                WHERE a.attname = %L AND c.relname = %L AND a.attrelid = (SELECT oid FROM pg_class WHERE relname = %L AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = %L))',
                column_name, r.table_name, r.table_name, r.schema_name) INTO column_data_type;

            -- Calculate the number of non-null values for the column
            EXECUTE format('SELECT COUNT(*) FROM %I.%I WHERE %I IS NOT NULL', r.schema_name, r.table_name, column_name) INTO not_null_count;

            -- Calculate the percentage completeness
            IF row_count > 0 THEN
                percentage_completeness := (not_null_count::NUMERIC / row_count::NUMERIC) * 100;
            ELSE
                percentage_completeness := 0;
            END IF;

            -- Determine if the column contains negative values, only if it is numeric
            IF column_data_type IN ('int2', 'int4', 'int8', 'float4', 'float8', 'numeric') THEN
                EXECUTE format('SELECT COUNT(*) FROM %I.%I WHERE %I::NUMERIC < 0', r.schema_name, r.table_name, column_name) INTO negative_values_count;
            ELSE
                negative_values_count := 0;
            END IF;
            is_negative_values := negative_values_count > 0;

            -- Determine the data quality completeness result
            data_quality_completeness_result := CASE 
                WHEN percentage_completeness > 90 THEN 'Good'
                ELSE 'Bad'
            END;

            -- Build JSON for the column information
            col_info := col_info || JSONB_BUILD_OBJECT(
                column_name, JSONB_BUILD_OBJECT(
                    'data_type', column_data_type,
                    'sample_data', sample_data
                )
            );

            -- Build JSON for the column quality
            column_quality := JSONB_BUILD_OBJECT(
                'percentage_completeness', percentage_completeness,
                'data_quality_completeness_result', data_quality_completeness_result,
                'is_negative_values', is_negative_values
            );

            -- Add column quality to quality_info JSONB
            quality_info := quality_info || JSONB_BUILD_OBJECT(column_name, column_quality);
        END LOOP;

        -- Return the results for each table
        RETURN QUERY 
        SELECT 
            r.schema_name::TEXT AS schema,
            r.table_name::TEXT AS table_name,
            row_count AS n_rows,
            col_count AS n_cols,
            col_info AS data_profile,
            quality_info AS data_quality;
    END LOOP;
END;
$$ LANGUAGE plpgsql;