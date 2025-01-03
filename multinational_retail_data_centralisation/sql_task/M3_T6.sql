/* Changed the columns to the required data type */

-- SELECT * FROM dim_date_times;

ALTER TABLE dim_date_times
	ALTER COLUMN month TYPE VARCHAR(2),
	ALTER COLUMN year TYPE VARCHAR(4),
	ALTER COLUMN day TYPE VARCHAR(2),
	ALTER COLUMN time_period TYPE VARCHAR(11),
	ALTER COLUMN date_uuid TYPE UUID USING CAST(date_uuid as UUID);
	
SELECT
	index, 
	day,
	month,
	year,
	timestamp, 
	time_period, 
	date_uuid
FROM dim_date_times;