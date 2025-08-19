----------------- SECTION 1 OF GOOGLE COLAB PREPROCESSING -----------------

--------- Section 1 just includes raw_data, so we 'll just make simple checks ---------

-- Check number of rows and columns of table_raw (has to be 119_390, 36 according to Python)
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
DROP COLUMN arrival_date_year,
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
SELECT * FROM table_shape('public', 'table4'); -- Check shape (should be 118_902, 24)

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
SELECT * FROM table_shape('public', 'table5'); -- Check shape (should be 118_899, 23)

---------------------------- 2.5. Handling adults Column ----------------------------

-- Create a new table keeping values for adults between 1 and 4
CREATE TABLE table6 AS  -- SQL table6 corresponds to Python DataFrame dfdash6 
SELECT * FROM table5
WHERE adults > 0 AND adults <= 4;

-- Final check on this preprocessing step
SELECT * FROM table6 LIMIT 10;  -- Check table values
SELECT * FROM table_shape('public', 'table6'); -- Check shape (should be 118_490, 23)

---------------------------- 2.6. Handling meal Column ----------------------------

-- Make a copy before further preprocessing (SQL table7 corresponds to Python DataFrame dfdash7) 
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
SELECT * FROM table_shape('public', 'table7'); -- Check shape (should be 117_325, 23)
SELECT * FROM table_summary('public', 'table7'); -- Check nulls (should be 0)

---------------------------- 2.7. Handling market_segment Column ----------------------------

-- Make a copy before further preprocessing (SQL table8 corresponds to Python DataFrame dfdash8) 
CREATE TABLE table8 AS
SELECT *
FROM table7;

-- Drop all rows where the 'market_segment' column has the category 'Undefined'
DELETE FROM table8
WHERE market_segment = 'Undefined';

-- Replace the 'Complementary' and 'Aviation' categories in the 'market_segment' column with 'Other'
UPDATE table8
SET market_segment = CASE market_segment
    WHEN 'Complementary' THEN 'Other'
    WHEN 'Aviation' THEN 'Other'
    ELSE market_segment
END;

-- Final check on this preprocessing step
SELECT * FROM table8 LIMIT 10;  -- Check table values
SELECT * FROM table_shape('public', 'table8'); -- Check shape (should be 117_323, 23)
SELECT * FROM table_summary('public', 'table8'); -- Check nulls (should be 0)

---------------------------- 2.8. Handling distribution_channel Column ----------------------------

-- Make a copy before further preprocessing (SQL table9 corresponds to Python DataFrame dfdash9) 
CREATE TABLE table9 AS
SELECT *
FROM table8;

-- Drop all rows where distribution_channel is 'Undefined'
DELETE FROM table9
WHERE distribution_channel = 'Undefined';

-- Final check on this preprocessing step
SELECT * FROM table9 LIMIT 10;  -- Check table values
SELECT * FROM table_shape('public', 'table9'); -- Check shape (should be 117_320, 23)
SELECT * FROM table_summary('public', 'table9'); -- Check nulls (should be 0)

---------------------------- 2.9. Handling reserved_room_type Column ----------------------------

-- Make a copy before further preprocessing (SQL table10 corresponds to Python DataFrame dfdash10) 
CREATE TABLE table10 AS
SELECT *
FROM table9;

-- Replace specific categories with 'Other'
UPDATE table10
SET reserved_room_type = CASE reserved_room_type
    WHEN 'C' THEN 'Other'
    WHEN 'B' THEN 'Other'
    WHEN 'H' THEN 'Other'
    WHEN 'L' THEN 'Other'
    ELSE reserved_room_type
END;

-- Final check on this preprocessing step
SELECT * FROM table10 LIMIT 10;  -- Check table values
SELECT * FROM table_shape('public', 'table10'); -- Check shape (should be 117_320, 23)

---------------------------- 2.10. Handling agent & company Columns ----------------------------

-- Make a copy before further preprocessing (SQL table11 corresponds to Python DataFrame dfdash11) 
CREATE TABLE table11 AS
SELECT *
FROM table10;

-- Add new columns with more intuitive names
ALTER TABLE table11
ADD COLUMN has_agent INT,
ADD COLUMN has_company INT;

-- Convert 'has_company' and 'has_agent' columns to binary: 1 if not 0, else 0
UPDATE table11
SET has_agent = CASE WHEN agent != 0 THEN 1 ELSE 0 END,
    has_company = CASE WHEN company != 0 THEN 1 ELSE 0 END;

-- Drop original columns
ALTER TABLE table11
DROP COLUMN agent,
DROP COLUMN company;

-- Final check on this preprocessing step
SELECT * FROM table11 LIMIT 30;  -- Check table values
SELECT * FROM table_shape('public', 'table11'); -- Check shape (should be 117_320, 23)

---------------------------- 2.11. Handling adr & lead_time Outliers ----------------------------

-- Make a copy before further preprocessing (SQL table12 corresponds to Python DataFrame dfdash12) 
CREATE TABLE table12 AS
SELECT *
FROM table11;

-- Remove adr outliers
DELETE FROM table12
WHERE adr >= 5400 OR adr < 0;

-- Remove lead_time outliers
DELETE FROM table12
WHERE lead_time >= 640;

-- Final check on this preprocessing step
SELECT * FROM table12 LIMIT 30;  -- Check table values
SELECT * FROM table_shape('public', 'table12'); -- Check shape (should be 117_316, 23)

---------------------------- 2.12. Finalizing the Dtypes ----------------------------

-- Make a copy before further preprocessing (SQL table13 corresponds to Python DataFrame dfdash13) 
CREATE TABLE table13 AS
SELECT *
FROM table12;

ALTER TABLE table13
ALTER COLUMN is_canceled TYPE INT USING is_canceled::INT,
ALTER COLUMN lead_time TYPE INT USING lead_time::INT,
ALTER COLUMN stays_in_weekend_nights TYPE INT USING stays_in_weekend_nights::INT,
ALTER COLUMN stays_in_week_nights TYPE INT USING stays_in_week_nights::INT,
ALTER COLUMN adults TYPE INT USING adults::INT,
ALTER COLUMN total_of_special_requests TYPE INT USING total_of_special_requests::INT,
ALTER COLUMN is_repeated_guest TYPE INT USING is_repeated_guest::INT,
ALTER COLUMN previous_cancellations TYPE INT USING previous_cancellations::INT,
ALTER COLUMN previous_bookings_not_canceled TYPE INT USING previous_bookings_not_canceled::INT,
ALTER COLUMN booking_changes TYPE INT USING booking_changes::INT,
ALTER COLUMN days_in_waiting_list TYPE INT USING days_in_waiting_list::INT;

---------------------------- 2.13. Final Checks on Data Integrity ----------------------------

-------------------------- THIS IS THE FINAL CLEANED TABLE: table13 --------------------------

-- Final check on this preprocessing step

SELECT * FROM table13 LIMIT 30;  -- Check table values

SELECT * FROM table_shape('public', 'table13'); -- Check shape (should be 117_316, 23)

-- Check data types
SELECT column_name, data_type FROM information_schema.COLUMNS WHERE table_name = 'table13';

-- Check for duplicates (the result should equal to 0 according to Python)
SELECT 
    (SELECT COUNT(*) FROM table13) 
  - (SELECT COUNT(*) FROM (SELECT DISTINCT * FROM table13)) 
  AS number_of_duplicates;

/* The duplicates in this dataset were generated due to simplifications during preprocessing. 
For example, all high-cardinality columns were dropped, and further preprocessing made some 
rows less unique. Only the arrival_date column remains as a high-cardinality column, 
so bookings on the same day may appear as duplicates in the final cleaned dataset, 
but all rows still represent valid, unique bookings.*/

-- Check the unique values of arrival_date (this is the only high cardinality column left in the dataset)
SELECT COUNT(DISTINCT arrival_date)
FROM table13;

-- Show dates that include 100 bookings
SELECT arrival_date, COUNT(*) AS booking_count
FROM table13 
GROUP BY arrival_date
HAVING COUNT(*) = 100;


