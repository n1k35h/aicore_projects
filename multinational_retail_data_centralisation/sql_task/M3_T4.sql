/* Removed pound sign '£' from the product_price and 
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