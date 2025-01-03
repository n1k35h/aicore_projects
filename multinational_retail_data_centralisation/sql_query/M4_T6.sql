/* The below query gives data on the highest month of cost sales in each year */

WITH highest_month_sales_per_year AS(
	SELECT * 
	FROM orders_table AS ot
	INNER JOIN dim_products AS dp ON dp.product_code = ot.product_code
	INNER JOIN dim_date_times AS ddt ON ddt.date_uuid = ot.date_uuid)
		SELECT 
			ROUND(CAST(SUM(product_price * product_quantity)AS NUMERIC),2) AS total_sales,
			year,
			month
		FROM highest_month_sales_per_year
		GROUP BY year, month
		ORDER BY total_sales DESC;