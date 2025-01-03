import yaml
import requests
from time import sleep
import random
from multiprocessing import Process
import boto3
import json
import sqlalchemy
from sqlalchemy import text

random.seed(100)

class AWSDBConnector:

    '''
    As the credential is a sensitive data a .yaml file is created, which will hold all sensitive data that other users
    cannot view.
    
    1). Create a Method that read the .yaml file that holds the credential info

    2). create_db_connector that connects to the .yaml file, which a variable is assigned to the read method.
        Instead of the sensitive data showing in the square brackets the variable in .yaml will replace the
        sensitive data. E.G:

                ['AWS_HOST']
                ['AWS_USER']
                ['AWS_PASSWORD']
                ['AWS_DATABASE']
                ['AWS_PORT']

        Then connecting the sqlalchemy engine

    '''
    
    def read_creds(self, file):

        '''
        
        .yaml file is open for the file to be read 'r'
        
        '''
        
        with open(file, 'r') as f:
            creds = yaml.safe_load(f)
        return creds

    def create_db_connector(self):

        '''
        Variable is assigned to read the .yaml file

        Sensitive data is replaced with the variables in the .yaml file 

        Returns:
        --------
        Connecting to the sqlalchemy engine with the assigned variable credential 

        '''

        # reading the credential from the yaml file
        creds = self.read_creds('upe_creds.yaml')
        HOST = creds['AWS_HOST']
        USER = creds['AWS_USER']
        PASSWORD = creds['AWS_PASSWORD']
        DATABASE = creds['AWS_DATABASE']
        PORT = creds['AWS_PORT']
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}?charset=utf8mb4")
        return engine
    
new_connector = AWSDBConnector()

class Batch_User_Posting_Emulation():

    '''

    Avoiding replicates in the code Attribute & Methods are created
    
    Attribute - result: json - data is picked from a random row in the table

    Methods - gets the data from a random row in the table

    '''
    def __init__(self, connection, random_row):

        '''

        connection should be made between the table and API server
        random_row should retrieve the data between 1 to 11000 from the table

        '''
        self.pin_result = self.post_table_data(connection, random_row, "pinterest")
        self.geo_result = self.post_table_data(connection, random_row, "geolocation")
        self.user_result = self.post_table_data(connection, random_row, "user")
        self.batch_header = {'Content-Type': 'application/vnd.kafka.json.v2+json'}

    def post_table_data(self, connection, random_row, data_name):

        '''

        reads a SQL query to select all columns from the table and return a random_row

        '''

        string = text(f"SELECT * FROM {data_name}_data LIMIT {random_row}, 1")
        selected_row = connection.execute(string)

        # Start of the loop
        for row in selected_row:
            result = dict(row._mapping)
        return result

    def batch_payload(self, structure):

        '''

        Parameter: value of the structre data from the records should be sent as a json file to the url table

        Return 
        ------
            result: dict
        
        '''
        payload = json.dumps({
            "records": [
                {
                    "value": structure
                }
            ]
        })

        return payload

    def batch_request(self, structure, data_name):

        '''
        json file is send to the url of the table to store data 
        and if response.status_code prints 200, this means success
        
        '''

        invoke_url = f"https://41mrms02f1.execute-api.us-east-1.amazonaws.com/prod/topics/0e0816526d11.{data_name}"
        payload = self.batch_payload(structure)
        response = requests.request("POST", invoke_url, headers=self.batch_header, data=payload)
        print(f"batch {data_name} data: {response.status_code}")

    def batch_data(self, pin_structure, geo_structure, user_structure):

        '''
        
        sends a request of the structure 

        '''

        self.batch_request(pin_structure, "pin")
        self.batch_request(geo_structure, "geo")
        self.batch_request(user_structure, "user")



def run_infinite_post_data_loop():

    '''
    What should happen
    ------------------

    Runs a loop of post data from the table
    
    Gets a random data from the corresponding tables (pinterest, geolocation and user)

    Invoke URL - when the code is running it sends the Data to the S3 Bucket where each of the URL stores a json file
    to their corresponding URL (pin_url, geo_url and user_url)

    The payload is created for the batch data to be sent to AWS MSK console

    Sends a POST request to the API for the batch data to be sent to the topic folder

    Code will continue to run until it gets interrupted

    Attribute:
    ----------
    pin_result into json file
        pin data gets a random row from the pinterest_data table
    geo_result into json file
        geo data gets a random row from the geolocation_data table
    user_result into json file
        user data gets a random row from the user_data table

    '''

    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:
            conn = Batch_User_Posting_Emulation(connection, random_row)

            pin_structure = {
            # Data should be send as pairs of column_name:value, with different columns separated by commas       
            "value": {"index": conn.pin_result["index"], "unique_id": conn.pin_result["unique_id"], "title": conn.pin_result["title"], "description": conn.pin_result["description"], "poster_name": conn.pin_result["poster_name"], "follower_count": conn.pin_result["follower_count"], "tag_list": conn.pin_result["tag_list"], "is_image_or_video": conn.pin_result["is_image_or_video"], "image_src": conn.pin_result["image_src"], "downloaded": conn.pin_result["downloaded"], "save_location": conn.pin_result["save_location"], "category": conn.pin_result["category"]}
            }
              
            geo_structure = {
            # Data should be send as pairs of column_name:value, with different columns separated by commas       
            "value": {"ind": conn.geo_result["ind"], "timestamp": str(conn.geo_result["timestamp" ]), "latitude": conn.geo_result["latitude"], "longitude": conn.geo_result["longitude"], "country": conn.geo_result["country"]}
            }
            
            user_structure = {
            # Data should be send as pairs of column_name:value, with different columns separated by commas       
            "value": {"ind": conn.user_result["ind"], "first_name": conn.user_result["first_name"], "last_name": conn.user_result["last_name"], "age": conn.user_result["age"], "date_joined": str(conn.user_result["date_joined"])},
            }

            conn.batch_data(pin_structure, geo_structure, user_structure)

if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')