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

---------------------------- 2.1. Handling Null Values ----------------------------

-- Check for nulls 
-- column country has 488 missing values, agent 16340 and company 112593 according to Python
SELECT * FROM table_summary('public', 'table_raw');

-- Make a copy before further preprocessing (SQL table2 corresponds to Python DataFrame dfdash2) 
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

-- Final check on this preprocessing step
SELECT * FROM table2 LIMIT 10;  -- Check table values
SELECT * FROM table_shape('public', 'table2'); -- Check shape (should be 118_902, 36)
SELECT * FROM table_summary('public', 'table2'); -- Check nulls (should be 0)


---------------------------- 2.2. Handling Date-Related Columns ----------------------------

-- Make a copy before further preprocessing (SQL table3 corresponds to Python DataFrame dfdash3) 
CREATE TABLE table3 AS
SELECT *
FROM table2;

-- Mapping month names to their corresponding numeric values
UPDATE table3
SET arrival_date_month = CASE arrival_date_month
    WHEN 'January' THEN 1
    WHEN 'February' THEN 2
    WHEN 'March' THEN 3
    WHEN 'April' THEN 4
    WHEN 'May' THEN 5
    WHEN 'June' THEN 6
    WHEN 'July' THEN 7
    WHEN 'August' THEN 8
    WHEN 'September' THEN 9
    WHEN 'October' THEN 10
    WHEN 'November' THEN 11
    WHEN 'December' THEN 12
END;

-- Make arrival_date_month INT
ALTER TABLE table3
ALTER COLUMN arrival_date_month TYPE INT USING arrival_date_month::int;

-- Add a new date column to the table
ALTER TABLE table3
ADD COLUMN arrival_date DATE;

-- Combine year, month, and day columns into a single date string in 'YYYY-MM-DD' format
UPDATE table3
SET arrival_date = MAKE_DATE(
    arrival_date_year::int,   -- cast temporarily bigint to int
    arrival_date_month,       -- already int
    arrival_date_day_of_month::int  -- cast temporarily bigint to int
);

-- Final check on this preprocessing step
SELECT * FROM table3 LIMIT 10;  -- Check table values
SELECT * FROM table_shape('public', 'table3'); -- Check shape (should be 118_902, 37)
SELECT * FROM table_summary('public', 'table3'); -- Check nulls (should be 0)
-- Check data types
SELECT column_name, data_type FROM information_schema.COLUMNS WHERE table_name = 'table3';


---------------------------- 2.3. Dropping Unimportant Columns ----------------------------


-- Make a copy before further preprocessing (SQL table4 corresponds to Python DataFrame dfdash4) 
CREATE TABLE table4 AS
SELECT *
FROM table3;

ALTER TABLE table4
DROP COLUMN name,
DROP COLUMN email,
DROP COLUMN arrival_date_month,
DROP COLUMN arrival_date_day_of_month,
DROP COLUMN "phone-number",  -- Is quoted because it contains a hyphen
DROP COLUMN credit_card,
DROP COLUMN reservation_status,
DROP COLUMN reservation_status_date,
DROP COLUMN assigned_room_type,
DROP COLUMN deposit_type,
DROP COLUMN required_car_parking_spaces,
DROP COLUMN arrival_date_week_number;

-- Final check on this preprocessing step
SELECT * FROM table4 LIMIT 10;  -- Check table values
SELECT * FROM table_shape('public', 'table4'); -- Check shape (should be 118_902, 25)

---------------------------- 2.4. Creating total_kids Column ----------------------------

-- Make a copy before further preprocessing (SQL table5 corresponds to Python DataFrame dfdash5) 
CREATE TABLE table5 AS
SELECT *
FROM table4;

-- Add a new column of type INT to the table5
ALTER TABLE table5
ADD COLUMN total_kids INT;

-- Make total_kids col to equal the sum of children + babies
UPDATE table5
SET total_kids = children::int + babies::int;

-- Drop the original 'children' and 'babies' columns after merging
ALTER TABLE table5
DROP COLUMN children,
DROP COLUMN babies;

-- Drop rows with outliers (total kids > 3) and reset index in the dashboard dataframe
DELETE FROM table5
WHERE total_kids > 3;

-- Final check on this preprocessing step
SELECT * FROM table5 LIMIT 10;  -- Check table values
SELECT * FROM table_shape('public', 'table5'); -- Check shape (should be 118_899, 24)

---------------------------- 2.5. Handling adults Column ----------------------------

-- Create a new table keeping values for adults between 1 and 4
CREATE TABLE table6 AS
SELECT * FROM table5
WHERE adults > 0 AND adults <= 4

-- Final check on this preprocessing step
SELECT * FROM table6 LIMIT 10;  -- Check table values
SELECT * FROM table_shape('public', 'table6'); -- Check shape (should be 118_490, 24)

---------------------------- 2.6. Handling meal Column ----------------------------

-- Make a copy before further preprocessing (SQL table5 corresponds to Python DataFrame dfdash5) 
CREATE TABLE table7 AS
SELECT *
FROM table6;

-- Drop rows where the 'meal' column is 'Undefined', indicating no meal choice
DELETE FROM table7
WHERE meal = 'Undefined';

-- Add a new column number_of_meals
ALTER TABLE table7 
ADD COLUMN number_of_meals INT;

-- Map meal types
UPDATE table7 
SET number_of_meals = CASE meal
	WHEN 'BB' THEN 1
	WHEN 'HB' THEN 2
	WHEN 'SC' THEN 0
	WHEN 'FB' THEN 3
END;

-- Drop the original meal column
ALTER TABLE table7
DROP COLUMN meal;

-- Final check on this preprocessing step
SELECT * FROM table7 LIMIT 10;  -- Check table values
SELECT * FROM table_shape('public', 'table7'); -- Check shape (should be 117_325, 24)
SELECT * FROM table_summary('public', 'table7'); -- Check nulls (should be 0)