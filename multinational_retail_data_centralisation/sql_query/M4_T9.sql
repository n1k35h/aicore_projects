/* Below query shows how quickly the company is making sales each year based on the average time 
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
