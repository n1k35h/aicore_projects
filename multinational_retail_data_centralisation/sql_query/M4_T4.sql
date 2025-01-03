/* The below query gives the company the information on how sales have been made online vs offline */

ALTER TABLE dim_store_details
	ADD location VARCHAR(11);

UPDATE dim_store_details
	SET location = 'Web'
	WHERE store_type = 'Web Portal';

UPDATE dim_store_details
	SET location = 'Offline'
	WHERE store_type != 'Web Portal';

WITH online_sales AS(
	SELECT * 
		FROM dim_store_details AS dsd
		INNER JOIN orders_table AS ot ON ot.store_code = dsd.store_code)
		SELECT COUNT(date_uuid) AS number_of_sales,
		SUM(product_quantity) AS product_quantity_count,
			location
		FROM online_sales
		GROUP BY location
		ORDER BY number_of_sales DESC;


-- select * from dim_store_details;