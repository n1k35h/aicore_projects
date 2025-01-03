/* Data that are in orders_table but not in the dim_store_details or dim_card_details are now added
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