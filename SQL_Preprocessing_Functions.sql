---------------------------- SQL FUNCTIONS ----------------------------

-- Create a function to return the table shape to facilitate comparison with Python preprocessing
CREATE OR REPLACE FUNCTION table_shape(p_schema TEXT, p_table TEXT)
RETURNS TABLE(row_count BIGINT, column_count BIGINT) AS $$
BEGIN
    RETURN QUERY EXECUTE format(
        'SELECT COUNT(*) AS row_count,
                (SELECT COUNT(*) 
                 FROM information_schema.columns 
                 WHERE table_schema = %L AND table_name = %L
                ) AS column_count
         FROM %I.%I',
        p_schema, p_table, p_schema, p_table
    );
END;
$$ LANGUAGE plpgsql;


-- Create a function to return a summary of null values per column
CREATE OR REPLACE FUNCTION table_summary(p_schema TEXT, p_table TEXT)
RETURNS TABLE(
    col_name TEXT,
    total_rows BIGINT,
    null_count BIGINT
) AS $$
DECLARE
    col_rec RECORD;
    sql TEXT;
    row_total BIGINT;
BEGIN
    -- Get total number of rows in the table
    EXECUTE format('SELECT COUNT(*)::BIGINT FROM %I.%I', p_schema, p_table) INTO row_total;

    -- Loop through each column
    FOR col_rec IN
        SELECT column_name AS col_name_alias
        FROM information_schema.columns
        WHERE table_schema = p_schema
          AND table_name = p_table
    LOOP
        sql := format(
            'SELECT COUNT(*)::BIGINT AS null_count FROM %I.%I WHERE %I IS NULL',
            p_schema, p_table, col_rec.col_name_alias
        );

        RETURN QUERY EXECUTE format(
            'SELECT %L AS col_name, %s::BIGINT AS total_rows, (%s) AS null_count',
            col_rec.col_name_alias, row_total, sql
        );
    END LOOP;
END;
$$ LANGUAGE plpgsql;


-- Create a function to automatically drop all tables except table_raw whenever needed
CREATE OR REPLACE FUNCTION drop_processed_tables()
RETURNS void AS $$
DECLARE
    tbl RECORD;
BEGIN
    FOR tbl IN
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = 'public'
          AND tablename LIKE 'table%'
          AND tablename <> 'table_raw'
    LOOP
        EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(tbl.tablename) || ' CASCADE;';
    END LOOP;
END;
$$ LANGUAGE plpgsql;
