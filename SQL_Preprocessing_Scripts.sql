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


----------------- SECTION 1 OF GOOGLE COLAB PREPROCESSING -----------------

--------- Section 1 just includes raw_data, so we 'll just make simple checks ---------

-- Check number of rows, columns of table_raw (has to be 119_390, 36 according to Python)
SELECT * FROM table_shape('public', 'table_raw');

-- Check for duplicates (the result should equal to 0 according to Python)
SELECT 
    (SELECT COUNT(*) FROM table_raw) 
  - (SELECT COUNT(*) FROM (SELECT DISTINCT * FROM table_raw)) 
  AS number_of_duplicates;


----------------- SECTION 2 OF GOOGLE COLAB PREPROCESSING -----------------

---------------------------- Handling Null Values ----------------------------

-- Check for nulls 
-- column country has 488 missing values, agent 16340 and company 112593 according to Python
SELECT * FROM table_summary('public', 'table2');

-- Make a copy before further preprocessing
-- table2 refers to dfdash2
CREATE TABLE table2 AS
SELECT *
FROM table_raw;

-- Fill missing values in 'children' column with the most frequent value (mode)
UPDATE table2
SET children = sub.mode_value
FROM (
    SELECT children AS mode_value
    FROM table2
    WHERE children IS NOT NULL
    GROUP BY children
    ORDER BY COUNT(*) DESC
    LIMIT 1
) AS sub
WHERE table2.children IS NULL;

-- Replace missing values in 'agent' with 0, indicating direct bookings without a travel agent
UPDATE table2
SET agent = 0
WHERE agent IS NULL;

-- Replace missing values in 'company' with 0, meaning bookings not linked to any company
UPDATE table2
SET company = 0
WHERE company IS NULL;

-- Drop all rows with missing 'country' values since location info is important for analysis
DELETE FROM table2
WHERE country IS NULL;

SELECT * FROM table_shape('public', 'table2'); -- Check shape (should be 118_902, 36)
SELECT * FROM table_summary('public', 'table2'); -- Check nulls (should be 0)












