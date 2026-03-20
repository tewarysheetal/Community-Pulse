-- See all rows
SELECT * FROM age LIMIT 10;

-- Check what years are available
SELECT DISTINCT year FROM age ORDER BY year;

-- Query a specific table with a filter
SELECT * FROM poverty WHERE year = 2022;

-- List all columns in a table (useful given the 400+ columns)
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'age'
ORDER BY ordinal_position;
