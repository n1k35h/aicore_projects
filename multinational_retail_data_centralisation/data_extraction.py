from database_utils import DatabaseConnector
import pandas as pd
import tabula as t
import requests as r

""" DataExtractor will act as a Utility class, create methods that help extract data from different
data sources such as; CSV files, an API and an S3 bucket. """
class DataExtractor:

    def read_rds_table(self, table_name):
        """ This method will extract the database table to a pandas DataFrame
        taking an instance of the DatabaseConnector class and the table name as an argument and return pandas DataFrame
        list_db_tables method take name of the table containing user data
        read_rds_table method extract the table containing user data and return a pandas DataFrame """
        conn = DatabaseConnector() 
        engine = conn.init_db_engine()
        users_data = pd.read_sql_table(table_name, engine)
        return users_data
    
    def retrieve_pdf_data(self, pdf_data):
        """ This method will extract the PDF document from AWS s3 bucket. 
        Installing a Python package called tabula-py to help extract data from PDF document. """
        t.convert_into(pdf_data, "card_details.csv", output_format= "csv", pages= "all")
        card_df = pd.read_csv("card_details.csv")
        return card_df

    def list_number_of_stores(self, get_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'):
        """ This method will return the number of stores in the business """
        api_dict= {'x-api-key' : 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}        
        store = r.get(get_endpoint, headers=api_dict)
        store.status_code
        number_of_stores = store.json()        
        return number_of_stores

    def retrieve_stores_data(self, retrieve_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details'):
        """ After the list_number_of_stores method is run, this method will then be used to retrieve the stores information by running this second GET method.
        In this method the number of stores will be used in the for loop to extract all stores information. """
        api_dict_list = []
        api_dict= {'x-api-key' : 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
        for n in range(451):
            response = r.get(f'{retrieve_endpoint}/{n}', headers=api_dict)
            data = response.json()
            api_dict_list.append(data)
        response = pd.DataFrame.from_dict(api_dict_list)
        return response
    
    def extract_from_s3(self):
        """ In this method, each product information that the company currently sells is stored in CSV format in an s3 bucket on AWS site.
        Installing boto3 python package and extracting the information that returns pandas dataframe. """
        product_df = pd.read_csv('s3://data-handling-public/products.csv')
        product_df.to_csv('products.csv')
        return product_df
    
    def extract_from_s3_json(self):
        """ In this method, the date events data is stored in s3 bucket, which is extracted in .json file. """
        date_df = pd.read_json('https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json')
        date_df.to_csv('date_details.csv')
        return date_df

if __name__ == '__main__':
    de = DataExtractor() # calls the DataExtractor class
