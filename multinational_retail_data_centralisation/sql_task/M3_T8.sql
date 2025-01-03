/* created the primary key for each of the dim tables */

ALTER TABLE dim_users
	ADD PRIMARY KEY (user_uuid);
-- SELECT * FROM dim_users;

ALTER TABLE dim_store_details
	ADD PRIMARY KEY (store_code);
-- SELECT * FROM dim_store_details;

ALTER TABLE dim_products
	ADD PRIMARY KEY (product_code);
-- SELECT * FROM dim_products;

ALTER TABLE dim_date_times
	ADD PRIMARY KEY (date_uuid);
-- SELECT * FROM dim_date_times;

ALTER TABLE dim_card_details
	ADD PRIMARY KEY (card_number);
-- SELECT * FROM dim_card_details;