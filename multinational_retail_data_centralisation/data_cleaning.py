from data_extraction import DataExtractor
from database_utils import DatabaseConnector
import pandas as pd
import numpy as np

# class DataCleaning will clean data from each of the data sources
class DataCleaning:

    def clean_user_data(self, users_df):
        """ The method will clean the user data by removing the NULL values, fixing the invalid dates and
        removing unwanted characters from the table """
    
        # Drops the NULL from the table
        # users_df = de.read_rds_table('legacy_users')
        users_df.replace('NULL', np.NaN, inplace=True)
        users_df.dropna(subset=['date_of_birth', 'email_address', 'user_uuid'], how='any', axis=0, inplace=True)
        # removes invalid dates from the Table
        users_df['date_of_birth'] = pd.to_datetime(users_df['date_of_birth'], infer_datetime_format=True, errors = 'coerce')
        users_df['join_date'] = pd.to_datetime(users_df['join_date'], infer_datetime_format=True, errors ='coerce')
        users_df = users_df.dropna(subset=['join_date'])
        # changes the GGB to GB for the country code
        users_df['country_code'] = users_df['country_code'].str.replace("GGB", "GB")
        # removing invalid characters from the phone number
        r = "[^0-9]+."
        users_df["phone_number"] = users_df["phone_number"].str.replace(r, "")
        # Adding country extensions to the front of phone numbers
        users_df["phone_number"] = users_df["country_code"].map({"DE": "0049 ", "GB": "0044 ", "US": "001 "}).astype(str) + users_df["phone_number"]
        # removes the duplicates of the email address
        users_df = users_df.drop_duplicates(subset=['email_address'])
        # removes the column 0 from the table
        users_df.drop(users_df.columns[0], axis=1, inplace=True)   
        # users_df.to_csv('legacy_users.csv')        
        return users_df

    def clean_card_data(self, card_df):
        """ The method will clean the data, which will remove any erroneous values, NULL values or errors with formatting. """
        
        # removes the null values
        card_df.replace('NULL', np.NaN, inplace=True)
        card_df.dropna(subset=['card_number'], how='any', axis=0, inplace=True)
        # not containing alphabet and removes empty cells/ rows
        r = "[a-zA-Z?]"
        card_df = card_df[~card_df['card_number'].str.contains(r, na=False)]
        # correct the dates
        card_df['date_payment_confirmed'] = pd.to_datetime(card_df['date_payment_confirmed'], infer_datetime_format=True, errors = 'coerce')
        # stores large data type in the cell
        card_df['card_number'] = card_df['card_number'].astype('int64')
        # card_df.to_csv('card_details.csv')
        return card_df

    def clean_store_data(self, store_df):
        """ The method will clean the data that was retrieved from the API """
        
        # remove the 'N/A' from the first row
        store_df.replace('N/A', np.NaN, inplace=True)
        store_df[['staff_numbers', 'longitude', 'latitude']] = store_df[['staff_numbers', 'longitude', 'latitude']].apply(lambda x: round(pd.to_numeric(x, errors = 'coerce'), 1))
        store_df.dropna(subset=['staff_numbers'], inplace=True)
        store_df.astype({'staff_numbers' : 'int64'})
        # dropping the 'Lat' column as there is no value in the 'Lat'
        store_df.drop(['lat'], axis = 1, inplace=True)
        # correcting the date 
        store_df['opening_date'] = pd.to_datetime(store_df['opening_date'], infer_datetime_format=True, errors='coerce')
        # re-arrange columns
        cols = ['address', 'locality', 'country_code', 'continent', 'store_code', 'store_type', 'longitude', 'latitude', 'opening_date', 'staff_numbers']
        store_df = store_df.reindex(columns = cols)   
        return store_df
    
    def convert_product_weight(self, product_df):
        """ The method will take the products Dataframe as an argument and returns the products Dataframe.
        In the weight column, it has all different weight units ('g', 'ml' & 'oz'), the aim is to convert them into decimal 
        values using the float, which will represent the weight in kg. """
            
        #   removes the non-digits from the weight column - kg, g, oz, ml
        product_df['amount'] = product_df['weight'].str.replace(r'[^0-9.]+', '')
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
        product_df.rename(columns={'weight':'weight(kg)'}, inplace=True)
        product_df['weight(kg)'] = product_df['weight(kg)'].astype('float')
        product_df.drop(['amount', 'unit'], axis=1, inplace=True)
        return product_df        

    def clean_products_data(self, product_df):
        """ The method will clean any additional errors, like NULL values, fixing the dates, and unwanted characters from
        the table. """
            
        # Removes the Null value 
        product_df.replace('NULL', np.NaN, inplace=True)
        # correcting the date values and dropping the blank cells
        product_df['date_added'] = pd.to_datetime(product_df['date_added'], infer_datetime_format=True, errors='coerce')
        product_df.dropna(subset=['date_added'], how='any', axis=0, inplace=True)
        # removing unwanted character in the product_price
        product_df['product_price'] = product_df['product_price'].str.replace('[^0-9.]+', '')
        product_df['product_price'] = product_df['product_price'].astype('float')
        product_df['EAN'] = product_df['EAN'].astype('int64')
        # dropping the 'unnamed: 0' column as it's duplicating the index column
        product_df.drop(['Unnamed: 0'], axis=1, inplace=True) 
        return product_df
    
    def clean_orders_data(self, order_df):
        """ The method will remove the unwanted columns ('first_name', 'last_name' & '1') """
        
        # removing unwanted columns from orders_table
        order_df.drop(['level_0', 'index', 'first_name', 'last_name', '1'], axis=1, inplace=True)
        return order_df

    def clean_date_details(self, date_df):
        """ The method will clean the invalid date entries """
        
        date_df['day'] = pd.to_numeric(date_df['day'], errors='coerce')
        date_df.dropna(subset=['day'], how='any', axis=0, inplace=True)
        # reorganise the columns
        date_cols = ['day', 'month', 'year', 'timestamp', 'time_period', 'date_uuid']
        date_df = date_df.reindex(columns = date_cols)
        return date_df

if __name__ == '__main__':
    dc = DataCleaning()
    de = DataExtractor()
    conn = DatabaseConnector()
    
    users_df = de.read_rds_table('legacy_users')
    clean_user_data = dc.clean_user_data(users_df)
    # print(clean_user_data.head())
    clean_user_data.to_csv('legacy_users.csv')
    conn.upload_to_db(clean_user_data, 'dim_users')
    
    card_df = de.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
    clean_card_data = dc.clean_card_data(card_df)
    clean_card_data.to_csv('card_details.csv')
    # print(clean_card_data.head())
    conn.upload_to_db(clean_card_data, 'dim_card_details')
    
    store_df = de.retrieve_stores_data()
    clean_store_data = dc.clean_store_data(store_df)
    clean_store_data.to_csv('store_details.csv')
    # print(clean_store_data.head())
    conn.upload_to_db(store_df, 'dim_store_details')
    
    product_df = de.extract_from_s3()
    clean_products_data = dc.clean_products_data(product_df)
    dc.convert_product_weight(product_df)
    # print(clean_products_data.head())
    clean_products_data.to_csv('products.csv')
    conn.upload_to_db(product_df, 'dim_products')
    
    order_df = de.read_rds_table('orders_table')
    clean_orders_table = dc.clean_orders_data(order_df)
    clean_orders_table.to_csv('orders_table.csv')
    # print(clean_orders_table.head())
    conn.upload_to_db(order_df, 'orders_table')
    
    date_df = de.extract_from_s3_json()
    clean_date_details = dc.clean_date_details(date_df)
    # print(date_df.head())
    clean_date_details.to_csv('date_details.csv')
    conn.upload_to_db(date_df, 'dim_date_times')
