/* Updated the dim_store_details and corrected the Data Type columns */

-- SELECT * FROM dim_store_details;
ALTER TABLE dim_store_details
	DROP COLUMN level_0;
	
ALTER TABLE dim_store_details
	ALTER COLUMN longitude TYPE FLOAT,
	ALTER COLUMN locality TYPE VARCHAR(255),
	ALTER COLUMN store_code TYPE VARCHAR(12),
	ALTER COLUMN staff_numbers TYPE SMALLINT,
	ALTER COLUMN opening_date TYPE DATE,
	ALTER COLUMN store_type TYPE VARCHAR(255),
	ALTER COLUMN latitude TYPE FLOAT,
	ALTER COLUMN country_code TYPE VARCHAR(2),
	ALTER COLUMN continent TYPE VARCHAR(255);

SELECT * FROM dim_store_details;