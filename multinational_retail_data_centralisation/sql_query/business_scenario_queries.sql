/* Milestone 4:
1. The below query will give the Operations team the information on which countries that are currently
operating and which country has the most stores. */

SELECT country_code AS country,
COUNT(store_code) AS total_no_stores
FROM dim_store_details
GROUP BY country
ORDER BY total_no_stores DESC;

/* Resulting Query 
+----------+-----------------+
| country  | total_no_stores |
+----------+-----------------+
| GB       |             264 |
| DE       |             139 |
| US       |              33 |
+----------+-----------------+
*/

/* 2. The below query will give the business stakeholders the information on which location has the
most stores. */

SELECT locality,
COUNT(store_code) AS total_no_stores
FROM dim_store_details
GROUP BY locality
ORDER BY total_no_stores DESC
LIMIT 7;

/* Resulting Query:
+-------------------+-----------------+
|     locality      | total_no_stores |
+-------------------+-----------------+
| Chapletown        |              14 |
| Belper            |              13 |
| Bushley           |              12 |
| Exeter            |              11 |
| High Wycombe      |              10 |
| Arbroath          |              10 |
| Rutherglen        |              10 |
+-------------------+-----------------+
*/

/* 3. Below query is showing the average highest monthly cost  */

WITH highest_avg_monthly_sales AS(
	SELECT *
		FROM dim_date_times AS ddt
	INNER JOIN
		orders_table AS ot ON ot.date_uuid = ddt.date_uuid
	INNER JOIN 
		dim_products AS dp ON dp.product_code = ot.product_code)
		SELECT			
			ROUND(CAST(SUM(product_quantity*product_price)AS NUMERIC), 2) AS total_sales,
			month,
			ROUND(CAST(AVG(product_quantity*product_price)AS NUMERIC), 2) AS avg_per_month
		FROM highest_avg_monthly_sales
		GROUP BY month
		ORDER BY total_sales DESC;

/* Resulting query
+-------------+-------+---------------+
| total_sales | month | ave_per_month |
+-------------+-------+---------------+
|   673295.68 |     8 |		65.85 |
|   668041.45 |     1 |		65.18 |
|   657335.84 |    10 | 	64.89 |
|   650321.43 |     5 |		64.07 |
|   645741.70 |     7 |		62.79 |
|   645463.00 |     3 |		63.79 |
+-------------+-------+---------------+
*/

/* 4. The below query gives the company the information on how sales have been made online vs offline */

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

/* Resulting query
+------------------+-------------------------+----------+
| numbers_of_sales | product_quantity_count  | location |
+------------------+-------------------------+----------+
|            26957 |                  107739 | Web      |
|            92105 |                  369900 | Offline  |
+------------------+-------------------------+----------+
*/

/* 5. The below query gives the percentage of sales that have come through each type of sales */

WITH sales_order AS (
	SELECT *
	FROM dim_store_details AS dsd
	INNER JOIN orders_table AS ot ON ot.store_code = dsd.store_code
	INNER JOIN dim_products AS dp ON dp.product_code = ot.product_code)
		SELECT store_type,
			ROUND(CAST(SUM(product_price * product_quantity)AS NUMERIC), 2) AS total_sales,
			ROUND(CAST(SUM(product_price * product_quantity) * 100.00 / 
			(SELECT SUM(product_price * product_quantity) FROM sales_order)AS NUMERIC), 2) AS percentage_total
		FROM sales_order
		GROUP BY store_type
		ORDER BY percentage_total DESC;

/* Resulting query
+-------------+-------------+---------------------+
| store_type  | total_sales | percentage_total(%) |
+-------------+-------------+---------------------+
| Local       |  3414833.09 |               44.22 |
| Web portal  |  1726547.05 |               22.36 |
| Super Store |  1210086.54 |               15.67 |
| Mall Kiosk  |   698791.61 |                9.05 |
| Outlet      |   607313.48 |                7.86 |
+-------------+-------------+---------------------+
*/

/* 6. The below query gives data on the highest month of cost sales in each year */

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

/* Resulting query
+-------------+------+-------+
| total_sales | year | month |
+-------------+------+-------+
|    27936.77 | 1994 |     3 |
|    27356.14 | 2019 |     1 |
|    27091.67 | 2009 |     8 |
|    26679.98 | 1997 |    11 |
|    26310.97 | 2018 |    12 |
|    26277.72 | 2019 |     8 |
|    26236.67 | 2017 |     9 |
|    25798.12 | 2010 |     5 |
|    25648.29 | 1996 |     8 |
|    25614.54 | 2000 |     1 |
+-------------+------+-------+
*/

/* 7. The below query gives data on overall head count in each country */

SELECT SUM(staff_numbers) AS total_staff_numbers,
	country_code
	FROM dim_store_details
	GROUP BY country_code
	ORDER BY total_staff_numbers DESC;

-- select * from dim_store_details;

/* Resulting query
+---------------------+--------------+
| total_staff_numbers | country_code |
+---------------------+--------------+
|               13132 | GB           |
|                6054 | DE           |
|                1384 | US           |
+---------------------+--------------+
*/

/* 8. The below query gives data in Germany on which store type has the most sales */

WITH german_store_sales AS (
	SELECT *
	FROM orders_table AS ot
	INNER JOIN dim_products AS dp ON dp.product_code = ot.product_code
	INNER JOIN dim_store_details AS dsd ON dsd.store_code = ot.store_code)
		SELECT 
			ROUND(CAST(SUM(product_price * product_quantity)AS NUMERIC), 2) AS total_sales,
			store_type,
			country_code
		FROM german_store_sales
		WHERE country_code = 'DE'
		GROUP BY store_type, country_code
		ORDER BY total_sales ASC;

/* Resulting query
+--------------+-------------+--------------+
| total_sales  | store_type  | country_code |
+--------------+-------------+--------------+
|   198373.57  | Outlet      | DE           |
|   247634.20  | Mall Kiosk  | DE           |
|   384625.03  | Super Store | DE           |
|  1083846.16  | Local       | DE           |
+--------------+-------------+--------------+
*/

/* 9. Below query shows how quickly the company is making sales each year based on the average time 
taken between each sales per year */

WITH company_make_sales AS(
	SELECT TO_TIMESTAMP(CONCAT(day, '-', month, '-', year, ' ', timestamp), 'DD-MM-YYYY HH24:MI:SS') AS datetime,
	year
	FROM dim_date_times
	ORDER BY datetime DESC),
	company_make_sales_2 AS(
		SELECT 
			year,
			datetime,
			LEAD(datetime, 1) OVER (ORDER BY datetime DESC) AS time_elapse FROM company_make_sales)
			SELECT
				year,
				AVG((datetime - time_elapse)) AS actual_time_taken FROM company_make_sales_2
			GROUP BY year
			ORDER BY actual_time_taken DESC 
			limit 5;

/* Resulting query
 +------+-----------------------+
 | year |  actual_time_taken    |
 +------+-----------------------+
 | 2013 |      02:17:12.300182  |
 | 1993 |      02:15:35.857327  |
 | 2002 |      02:13:50.412529  | 
 | 2022 |      02:13:06.313993  |
 | 2008 |      02:13:02.80308   |
 +------+-----------------------+
*/
