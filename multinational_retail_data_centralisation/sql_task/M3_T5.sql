/* Renamed the 'removed' column to 'still_available' and changed the columns to the required 
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
