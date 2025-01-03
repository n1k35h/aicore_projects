/* Milestone 3
1. Casting the columns of the orders_table to the correct data types.*/

-- SELECT * FROM orders_table;
	
ALTER TABLE orders_table
	ALTER COLUMN date_uuid TYPE UUID USING CAST(date_uuid as UUID),
	ALTER COLUMN user_uuid TYPE UUID USING CAST(user_uuid as UUID),
	ALTER COLUMN card_number TYPE VARCHAR(19),
	ALTER COLUMN store_code TYPE VARCHAR(12),
	ALTER COLUMN product_code TYPE VARCHAR(11),
	ALTER COLUMN product_quantity TYPE SMALLINT;
	
SELECT * FROM orders_table;

/* 2. Corrected the Data Type columns in dim_users table */

-- SELECT * FROM dim_users;

ALTER TABLE dim_users
	ALTER COLUMN first_name TYPE VARCHAR(255),
	ALTER COLUMN last_name TYPE VARCHAR(255),
	ALTER COLUMN date_of_birth TYPE DATE,
	ALTER COLUMN country_code TYPE VARCHAR(2),
	ALTER COLUMN user_uuid TYPE UUID USING CAST(user_uuid as UUID),
	ALTER COLUMN join_date TYPE DATE;

SELECT * FROM dim_users;

/* 3. Updated the dim_store_details and corrected the Data Type columns */

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

/* 4. Removed pound sign '£' from the product_price and 
New column is created to allow the team to make quick decision on delivery weight_class. */

UPDATE dim_products
SET product_price = REPLACE(product_price, '£', '');

ALTER TABLE dim_products
	ADD COLUMN weight_class VARCHAR(255);

UPDATE dim_products
SET weight_class = 
	CASE
		WHEN weight < 2.0 THEN 'Light'
		WHEN weight >= 2.0 AND weight < 40.0 THEN 'Mid_Sized'
		WHEN weight >= 40.0 AND weight < 140.0 THEN 'Heavy'
		WHEN weight >= 140.0 THEN 'Truck_Required'
	END;

SELECT * FROM dim_products ORDER BY index asc;

/* 5. Renamed the 'removed' column to 'still_available' and changed the columns to the required 
datat types.*/

-- SELECT * FROM dim_products;

ALTER TABLE dim_products
	RENAME COLUMN removed TO still_available;
	
ALTER TABLE dim_products
	ALTER COLUMN product_price TYPE FLOAT USING product_price::double precision,
	ALTER COLUMN weight TYPE FLOAT,
	ALTER COLUMN "EAN" TYPE VARCHAR(18),
	ALTER COLUMN product_code TYPE VARCHAR(11),
	ALTER COLUMN date_added TYPE DATE,
	ALTER COLUMN uuid TYPE UUID USING CAST(uuid as UUID),
	ALTER COLUMN still_available TYPE BOOL USING (still_available = 'Still_available'),
    ALTER COLUMN weight_class TYPE VARCHAR(14);

SELECT * FROM dim_products ORDER BY index asc;

/* 6. Changed the columns to the required data type */

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

/* 7. Counting the length of each card_number and changed the column to the required data type */

-- SELECT * FROM dim_card_details;

ALTER TABLE dim_card_details
	ALTER COLUMN card_number TYPE VARCHAR(20),
	ALTER COLUMN expiry_date TYPE VARCHAR(5),
	ALTER COLUMN date_payment_confirmed TYPE DATE;

SELECT index,
	card_number,
	LENGTH(card_number) as length,
	card_provider,
	expiry_date,
	date_payment_confirmed
FROM dim_card_details;

/* 8. created the primary key for each of the dim tables */

ALTER TABLE dim_users
	ADD PRIMARY KEY (user_uuid);

ALTER TABLE dim_store_details
	ADD PRIMARY KEY (store_code);

ALTER TABLE dim_products
	ADD PRIMARY KEY (product_code);

ALTER TABLE dim_date_times
	ADD PRIMARY KEY (date_uuid);

ALTER TABLE dim_card_details
	ADD PRIMARY KEY (card_number);

/* 9. Data that are in orders_table but not in the dim_store_details or dim_card_details are now added
and foreign key are added to the orders_table to get the relations between the orders_table to the 
5 dim tables */

SELECT orders_table.store_code
	FROM orders_table
	LEFT JOIN dim_store_details
	ON orders_table.store_code = dim_store_details.store_code
	WHERE dim_store_details.store_code IS NULL;

INSERT INTO dim_store_details(store_code)
	SELECT DISTINCT orders_table.store_code
	FROM orders_table
	WHERE orders_table.store_code NOT IN 
		(SELECT dim_store_details.store_code
		FROM dim_store_details);

SELECT orders_table.card_number
	FROM orders_table
	LEFT JOIN dim_card_details
	ON orders_table.card_number = dim_card_details.card_number
	WHERE dim_card_details.card_number IS NULL;

INSERT INTO dim_card_details(card_number)
	SELECT DISTINCT orders_table.card_number
	FROM orders_table
	WHERE orders_table.card_number NOT IN 
		(SELECT dim_card_details.card_number
		FROM dim_card_details);
		
ALTER TABLE orders_table
	ADD FOREIGN KEY (user_uuid)
	REFERENCES dim_users(user_uuid);

ALTER TABLE orders_table
	ADD FOREIGN KEY (store_code)
	REFERENCES dim_store_details;

ALTER TABLE orders_table
	ADD FOREIGN KEY (product_code)
	REFERENCES dim_products;

ALTER TABLE orders_table
	ADD FOREIGN KEY (date_uuid)
	REFERENCES dim_date_times;
	
ALTER TABLE orders_table
	ADD FOREIGN KEY (card_number)
	REFERENCES dim_card_details;