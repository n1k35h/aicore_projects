# multinational-retail-data-centralisation

Scenario for this Project is to work for Multinational Retail company that sells various goods across the globe. Currently, their sales data is spread across many different data sources making it not easily accessible or analysable by current members of the team. In an effort to become more data-driven, the organisation will make the sales data accessible from one centralised location. The first goal is to produce a system that stores the current company data in a database so that it's accessed from one centralised location and act as a single source of truth for sales data. The system will then query the database to get the up-to-date metrics for the business.

The tools and languages that this project will be using are as follows:
* Tools Used:
  * AWS - Amazon Web Services (Cloud technology)
  	* Extract data source files (e.g: API, S3 Bucket, JSON)
  * VSC - Visual Studio Code
  	* writing the python code
  * pgAdmin 4 - PostgreSQL 
  	* connect to the SQL database for the use of table creation and queries

* Languages Used:
  * Python
  * Pandas
  * Numpy
  * SQL

# Milestone 2:

### Task 1:

Setting up a Sales_Data database within the PGAdmin4, where it will store all the company information after the data has been extracted and cleaned from various data sources.

### Task 2:
Created 3 classes, where each of the class carries out it's own task

* class DataExtractor - extracting data from various data sources (such as: .csv, API & s3 bucket), saved as data_extraction.py
* class DatabaseConnector - connecting and uploading the tables to the database, saved as database_utils.py
* class DataCleaning - cleaning data from each of the data sources, saved as data_cleaning.py

### Task 3: 
Extracted & Cleaned the user data
  
In this task, the User data is stored in AWS RDS database, where .yaml is created to hold the credential information but first had to install PyYAML and import yaml to allow the .yaml to work.
In DatabaseConnector a read_db_creds method is created to read the .yaml file and return a dictionary with the credential information.

	def read_db_creds(self, file):
		# read the credentials yaml file and returns a dictionary of the credentials
		with open(file, 'r') as f:
			creds_data = yaml.safe_load(f)
		return creds_data

Next method called init_db_engine is created, which generates an sqlalchemy database engine that takes the method and creates the engine with the credential provided.
After that the next method called list_db_tables is created to list all the tables in the database so that the tables can be extracted.
So that the table can be read a method in DataExtractor is created called read_db_table. This will take the Database Connector, table name and creates an engine of the table with the use of Pandas Dataframe.

Creating init_db_engine method and reading the credentials from the return of read_db_file

	def read_rds_table(self, table_name):
		conn = DatabaseConnector() 
		engine = conn.init_db_engine()
		users_data = pd.read_sql_table(table_name, engine)
		return users_data

To clean the user data a method is created called clean_user_data in DataCleaning class, which takes numpy and pandas dataframe.
First major cleaning was to remove any rows that contains NULL from the table, this will have the numpy code included

	users_df.replace('NULL', np.NaN, inplace=True)
	users_df.dropna(subset=['date_of_birth', 'email_address', 'user_uuid'], how='any', axis=0, inplace=True)

Second major cleaning was to remove any invalids dates from the date_of_birth and join_date columns, this will have the pandas dataframe to perform the function of the to_datetime.

	users_df['date_of_birth'] = pd.to_datetime(users_df['date_of_birth'], infer_datetime_format=True, errors = 'coerce')
	users_df['join_date'] = pd.to_datetime(users_df['join_date'], infer_datetime_format=True, errors ='coerce')

Third major cleaning was to remove any invalid characters from the phone number

	# removing invalid characters from the phone number
	r = "[^0-9]+."
	users_df["phone_number"] = users_df["phone_number"].str.replace(r, "")
	# Adding country extensions to the front of phone numbers
	users_df["phone_number"] = users_df["country_code"].map({"DE": "0049 ", "GB": "0044 ", "US": "001 "}).astype(str) + users_df["phone_number"]
	
Final major key cleaning was to remove any duplicates from the email_address column

	users_df = users_df.drop_duplicates(subset=['email_address'])

After performing the cleaning method, in the DatabaseConnector class a method is created called upload_to_db. This will then send the tables to the database in the SQL

	localengine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
	localengine.connect()
	df.to_sql(name=table_name, con=localengine, if_exists='replace')

After extracting and cleaning the table, the table is uploaded to the database by the dim_users.

### Task 4: Extracted Users and cleaned Card Details
  
In this task, the user card details are stored in the AWS s3 bucket, which will need to extract but first had to install tabula-py to help extract data from a pdf document.
Next method to create in DataExtractor is called retrieve_pdf_data, which will extract all pdf pages and take Pandas dataframe as argument.

	t.convert_into(pdf_data, "card_details.csv", output_format= "csv", pages= "all")
	card_df = pd.read_csv("card_details.csv")

Next method that was created in DataCleaning class is called clean_card_data.
First major cleaning was to remove any rows that contains NULL value in them by using the same type of function that was used in clean_user_data.	
Second major cleaning was to remove any invalid data in the card_number column and only leave numbers in the column

	r = "[a-zA-Z?]"
	card_df = card_df[~card_df['card_number'].str.contains(r, na=False)]
 	
  	card_df['card_number'] = card_df['card_number'].astype('int64')

Final major cleaning was to correct the invalid dates by using the same type of function that was used in clean_user_data method.

After performing the clean_card_data method, the table is uploaded to the Sales_data database by the name of dim_card_details.

### Task 5: Extracted and Cleaned the details of each store

In this task, store data information was retrieved by using the API. The API had 2 GET methods. One will return the number of stores in the business and the other to retrieve a store given a store number. To connect to the API, the API key had to be included in the method header.

In the DataExtractor class, the first GET method is created called list_number_of_stores that would return the number of stores within the .json file. In the header the get_endpoint function is included and the API key is included in the method.

	def list_number_of_stores(self, get_endpoint='https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'):
		api_dict= {'x-api-key' : 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}        
		store = r.get(get_endpoint, headers=api_dict)
		store.status_code
		number_of_stores = store.json()

After the list_number_of_stores method is performed, which will return the value of 451 stores, this will then be used to retrieve the stores information by running the second GET method, which is retrieve_stores_data method. In this method the 451 stores will be used in the for loop to extract all stores information. In the header the retrieve_endpoint function is included and the API key is included in the method.

	def retrieve_stores_data(self, retrieve_endpoint='https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details'):
		api_dict_list = []
		api_dict={'x-api-key' : 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
		for n in range(451):
			response = r.get(f'{retrieve_endpoint}/{n}', headers=api_dict)
			data = response.json()
			api_dict_list.append(data)
		response = pd.DataFrame.from_dict(api_dict_list)

To clean the store dataframe, one of the key cleaning was to remove invalid characters from staff_numbers, longitude & latitude columns. In the below code, a Lambda function was applied to clean the selected columns and take one argument of expression by converting text to numerical data. 	

	store_df[['staff_numbers', 'longitude', 'latitude']] = store_df[['staff_numbers', 'longitude', 'latitude']].apply(lambda x: round(pd.to_numeric(x, errors = 'coerce'), 1))

Another key cleaning was to drop the 'lat' column as the column did not contain any value in them

	store_df.drop(['lat'], axis = 1, inplace=True)

Another key cleaning was to fix the opening_date column using the same function that was used in previous 2 cleaning.

After performing the cleaning, the table is uploaded to the Sales_Data database by the name of dim_store_details.

### Task 6: Extract and clean the product details

In this task, each product information that the company currently sells is stored in CSV format in an s3 bucket on AWS site.

In the DataExtractor class, a method is created called extract_from_s3, which will extract the csv file.

	def extract_from_s3(self):
		product_df = pd.read_csv('s3://data-handling-public/products.csv')
		product_df.to_csv('products.csv')
		return product_df

In the DataCleaning class, a method is created to first convert the weight column of the table to the same weight unit as KG.

	# creates new column for weight amount
	product_df['amount'] = pd.to_numeric(product_df['amount'])
	product_df['amount'] = product_df['amount'].astype('float')

	# create new column for weight unit
	product_df['unit'] = product_df.weight.str.replace('[^a-zA-Z]', '')
	product_df['unit'] = product_df['unit'].astype('str')

	# converting weight to kg
	product_df.loc[product_df['unit'] == 'g', 'amount'] /= 1000
	product_df.loc[product_df['unit'] == 'ml', 'amount'] /= 1000
	product_df.loc[product_df['unit'] == 'oz', 'amount'] *= 28.34952

	# replace weight column and remove extra columns
	product_df['weight'] = product_df['amount']
	product_df['weight'] = product_df['weight'].astype('float')

	product_df.drop(['amount', 'unit'], axis=1, inplace=True)

After converting the weight units to KG another method is created called clean_products_data, which uses the date function cleaning from previous methods to clean the 'date_added' column and another cleaning was to make the EAN column to integer and dropping the 'Unnamed: 0' column.

### Task 7: Retrieve and clean the orders table

In this task, the orders table will act as a single source of truth for all orders that the company has made in the past, which is stored in a database on AWS RDS. To extract the orders_table the read_rds_table and list_db_tables was used.

	def clean_orders_data(self, order_df):	
		# removing unwanted columns from orders_table
		order_df.drop(['level_0', 'index', 'first_name', 'last_name', '1'], axis=1, inplace=True)	
		return order_df

The columns 'level_0', 'index', 'first_name', 'last_name' and '1' are removed from the orders_table. Once the colummns are removed the table named orders_table is uploaded to the Sales_Data database.

### Task 8: Retrieve and Clean the date events data:

The final extraction and cleaning was to retrieve a JSON file that stored information about the events data.

	def extract_from_s3_json(self):
		date_df = pd.read_json('https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json')
		date_df.to_csv('date_details.csv')
		return date_df

The date is cleaned by doing the similar step as previous date cleaning from previous methods

	date_df['day'] = pd.to_numeric(date_df['day'], errors='coerce')
	date_df.dropna(subset=['day'], how='any', axis=0, inplace=True)

After extracting and cleaning the table, the table is uploaded to the Sales_Data database by the name dim_date_times.

# Milestone 3: Creating Database Schema
Developing the star-based schema of the database to ensure that the data type for the required columns of each table is correctly assigned.
Here are some of the SQL task that was undertaking, whilst changing the data type to the required data type

### Task 1: Correcting the Data Type for orders_table

Changing the data type for the orders_table to the required data type

	+------------------+--------------------+--------------------+
	|   orders_table   | current data type  | required data type |
	+------------------+--------------------+--------------------+
	| date_uuid        | TEXT               | UUID               |
	| user_uuid        | TEXT               | UUID               |
	| card_number      | TEXT               | VARCHAR(?)         |
	| store_code       | TEXT               | VARCHAR(?)         |
	| product_code     | TEXT               | VARCHAR(?)         |
	| product_quantity | BIGINT             | SMALLINT           |
	+------------------+--------------------+--------------------+

The SQL query that was used to change the data type to the required data type were as follows:

	ALTER TABLE orders_table
		ALTER COLUMN date_uuid TYPE UUID USING CAST(date_uuid as UUID),
		ALTER COLUMN user_uuid TYPE UUID USING CAST(user_uuid as UUID),
		ALTER COLUMN card_number TYPE VARCHAR(20),
		ALTER COLUMN store_code TYPE VARCHAR(12),
		ALTER COLUMN product_code TYPE VARCHAR(11),
		ALTER COLUMN product_quantity TYPE SMALLINT
		;
	
	SELECT * FROM orders_table;

### Task 2: Correcting the Data Type for the dim_store_details table

Changing the data type for the dim_store_details to the required data type

	+----------------+--------------------+--------------------+
	| dim_user_table | current data type  | required data type |
	+----------------+--------------------+--------------------+
	| first_name     | TEXT               | VARCHAR(255)       |
	| last_name      | TEXT               | VARCHAR(255)       |
	| date_of_birth  | TEXT               | DATE               |
	| country_code   | TEXT               | VARCHAR(?)         |
	| user_uuid      | TEXT               | UUID               |
	| join_date      | TEXT               | DATE               |
	+----------------+--------------------+--------------------+

The SQL query are similar to the first task

### Task 3: Updating the dim_store_details table

	+---------------------+-------------------+------------------------+
	| store_details_table | current data type |   required data type   |
	+---------------------+-------------------+------------------------+
	| longitude           | TEXT              | FLOAT                  |
	| locality            | TEXT              | VARCHAR(255)           |
	| store_code          | TEXT              | VARCHAR(?)             |
	| staff_numbers       | TEXT              | SMALLINT               |
	| opening_date        | TEXT              | DATE                   |
	| store_type          | TEXT              | VARCHAR(255) NULLABLE  |
	| latitude            | TEXT              | FLOAT                  |
	| country_code        | TEXT              | VARCHAR(?)             |
	| continent           | TEXT              | VARCHAR(255)           |
	+---------------------+-------------------+------------------------+


### Task 4: Make changes to the dim_products table for the delivery team

In this task, there are 2 parts to do first was to remove the £ character using SQL and second was to add a new column called weight_class that allows the delivery team to make a quick decision on delivery weight.

First part - removing the £ character

	UPDATE dim_products
	SET product_price = REPLACE(product_price, '£', '');

Second part - adding new column weight_class to make quick decision on delivery weight

This is the method that is required

	+--------------------------+-------------------+
	| weight_class VARCHAR(?)  | weight range(kg)  |
	+--------------------------+-------------------+
	| Light                    | < 2               |
	| Mid_Sized                | >= 2 - < 40       |
	| Heavy                    | >= 40 - < 140     |
	| Truck_Required           | => 140            |
	+----------------------------+-----------------+

The SQL query that was used is as follows:

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

  
that was used to change the data type to the required data type were as follows:
 
New column weight_class is added to the dim_products table to improve human-readability to allow quick decision-making on delivery weights.

	SELECT *,
		(CASE
			WHEN weight < 2.0 THEN 'Light'
			WHEN weight >= 2.0 AND weight < 40.0 THEN 'Mid_Sized'
			WHEN weight >= 40.0 AND weight < 140.0 THEN 'Heavy'
			WHEN weight >= 140.0 THEN 'Truck_Required'
		END) AS weight_class
		FROM dim_products
		ORDER BY index asc;

### Task 5: Updating the dim_products table

Changing the data type for the dim_products to the required data type
	
	+-----------------+--------------------+--------------------+
	|  dim_products   | current data type  | required data type |
	+-----------------+--------------------+--------------------+
	| product_price   | TEXT               | FLOAT              |
	| weight          | TEXT               | FLOAT              |
	| EAN             | TEXT               | VARCHAR(?)         |
	| product_code    | TEXT               | VARCHAR(?)         |
	| date_added      | TEXT               | DATE               |
	| uuid            | TEXT               | UUID               |
	| still_available | TEXT               | BOOL               |
	| weight_class    | TEXT               | VARCHAR(?)         |
	+-----------------+--------------------+--------------------+

But first, renaming the column header for removed to still_available

	ALTER TABLE dim_products
		RENAME COLUMN removed TO still_available;

Changing the data type is similar to the previous ones, but product_price, weight and still_available are different

	ALTER COLUMN product_price TYPE FLOAT USING product_price::double precision,
 	ALTER COLUMN weight TYPE FLOAT,

The renamed still_available column, the data type is changed to boolean, which the data in the column should return True for 'Still_available' and false 'Removed'

 	ALTER COLUMN still_available TYPE BOOL USING (still_available = 'Still_available')

### Task 6: Updating the dim_date_times table

Changing the data type for the dim_date_times to the require data type

	+-----------------+-------------------+--------------------+
	| dim_date_times  | current data type | required data type |
	+-----------------+-------------------+--------------------+
	| month           | TEXT              | VARCHAR(?)         |
	| year            | TEXT              | VARCHAR(?)         |
	| day             | TEXT              | VARCHAR(?)         |
	| time_period     | TEXT              | VARCHAR(?)         |
	| date_uuid       | TEXT              | UUID               |
	+-----------------+-------------------+--------------------+

The SQL query is done the similar way as the previous ones

### Task 7: Updating the dim_card_details table

Changing the data type for the dim_card_details to the required data type

	+------------------------+-------------------+--------------------+
	|    dim_card_details    | current data type | required data type |
	+------------------------+-------------------+--------------------+
	| card_number            | TEXT              | VARCHAR(?)         |
	| expiry_date            | TEXT              | VARCHAR(?)         |
	| date_payment_confirmed | TEXT              | DATE               |
	+------------------------+-------------------+--------------------+

The SQL query is done the similar way as the previous ones

Calculating the length of the card_number, which the query goes in the SELECT statement is as follows:

	LENGTH(card_number) as length,

### Task 8: Creating the Primary Key in the dimensions tables

Completing the final stages of the star-based database schema - Primary Keys is added to the relevant columns to each of the 5 tables

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

### Task 9: Finalising the star-based schemas & adding the foreign keys to the orders_table

Below code is required to add data that are in orders_table but not in dim_store_details, which are now added to the dim_store_details.

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

Foreign Keys are added to the orders_table to get the relations between the orders_table to the other 5 dim tables.

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

For full SQL query, please see the database_schema_queries.sql

# Milestone 4: Querying the data

Here are some of the resulting queries from the SQL statements

2. Which locations currently have the most stores?

The business stakeholders would like to know which locations currently have the most stores.

	SELECT locality,
	COUNT(store_code) AS total_no_stores
	FROM dim_store_details
	GROUP BY locality
	ORDER BY total_no_stores DESC
	LIMIT 7;

Resulting query:

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

3. Which months produce the average highest cost of sales typically?

Query the database to find out which months typically have the most sales your query should return the following information:

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

Resulting query:

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

6. The below query gives data on the highest month of cost sales in each year

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

Resulting query:

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

For full resulting query, please see the business_scenario_queries.sql
