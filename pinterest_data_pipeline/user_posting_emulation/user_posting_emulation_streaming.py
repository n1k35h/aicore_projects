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

        creds = self.read_creds('upe_creds.yaml')
        HOST = creds['AWS_HOST']
        USER = creds['AWS_USER']
        PASSWORD = creds['AWS_PASSWORD']
        DATABASE = creds['AWS_DATABASE']
        PORT = creds['AWS_PORT']
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}?charset=utf8mb4")
        return engine

new_connector = AWSDBConnector()

class Stream_User_Posting_Emulation():

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
        self.pin_streaming_result = self.put_table_data(connection, random_row, "pinterest")
        self.geo_streaming_result = self.put_table_data(connection, random_row, "geolocation")
        self.user_streaming_result = self.put_table_data(connection, random_row, "user")
        self.stream_header = {'Content-Type': 'application/json'}

    def put_table_data(self, connection, random_row, data_name):

        '''

        reads a SQL query to select all columns from the table and return a random_row

        '''

        string = text(f"SELECT * FROM {data_name}_data LIMIT {random_row}, 1")
        selected_row = connection.execute(string)

        # Start of the loop
        for row in selected_row:
            result = dict(row._mapping)
        return result

    def stream_payload(self, streaming_structure, data_name):

        '''

        Parameter: value of the structre data from the records should be sent as a json file to the url table

        Return 
        ------
            result: dict - ready to be sent to stream (AWS MSK console)
        
        '''
        payload = json.dumps({
                
            "StreamName": f"streaming-0e0816526d11-{data_name}",
            "Data": streaming_structure, 
                "PartitionKey" : f"{data_name}_streaming_partition"
        })

        return payload

    def stream_request(self, streaming_structure, data_name):

        '''
        json file is send to the url of the table to store data 
        and if response.status_code prints 200, this means success
        
        '''

        invoke_url = f"https://41mrms02f1.execute-api.us-east-1.amazonaws.com/prod2/streams/streaming-0e0816526d11-{data_name}/record"
        payload = self.stream_payload(streaming_structure, data_name)
        response = requests.request("PUT", invoke_url, headers=self.stream_header, data=payload)
        print(f"stream {data_name} data: {response.status_code}")

    def stream_data(self, pin_streaming_structure, geo_streaming_structure, user_streaming_structure):

        '''
        
        sends a request of the streaming structure to AWS MSK console

        '''

        self.stream_request(pin_streaming_structure, "pin")
        self.stream_request(geo_streaming_structure, "geo")
        self.stream_request(user_streaming_structure, "user")


def run_infinite_post_data_loop():

    '''

    Similar structure to the user_posting_emulation.py

    The payload is created for the stream data to be sent to AWS MSK console

    Sends a PUT request to the API for the stream data to be sent the stream


    '''

    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:
            conn = Stream_User_Posting_Emulation(connection, random_row)

            # Data is send as pairs of dict (column name: value) separate by a comma (,)
            pin_streaming_structure = {
                "Data" : {
                    "index": conn.pin_streaming_result["index"], "unique_id": conn.pin_streaming_result["unique_id"], "title": conn.pin_streaming_result["title"], "description": conn.pin_streaming_result["description"], "poster_name": conn.pin_streaming_result["poster_name"], "follower_count": conn.pin_streaming_result["follower_count"], "tag_list": conn.pin_streaming_result["tag_list"], "is_image_or_video": conn.pin_streaming_result["is_image_or_video"], "image_src": conn.pin_streaming_result["image_src"], "downloaded": conn.pin_streaming_result["downloaded"], "save_location": conn.pin_streaming_result["save_location"], "category": conn.pin_streaming_result["category"]
                }
            }

            # Data is send as pairs of dict (column name: value) separate by a comma (,)
            geo_streaming_structure = {
                "Data" : {
                    "ind": conn.geo_streaming_result["ind"], "timestamp": str(conn.geo_streaming_result["timestamp" ]), "latitude": conn.geo_streaming_result["latitude"], "longitude": conn.geo_streaming_result["longitude"], "country": conn.geo_streaming_result["country"]
                }
            }

            # Data is send as pairs of dict (column name: value) separate by a comma (,)
            user_streaming_structure = {
                "Data" : {
                    "ind": conn.user_streaming_result["ind"], "first_name": conn.user_streaming_result["first_name"], "last_name": conn.user_streaming_result["last_name"], "age": conn.user_streaming_result["age"], "date_joined": str(conn.user_streaming_result["date_joined"])
                }
            }

            conn.stream_data(pin_streaming_structure, geo_streaming_structure, user_streaming_structure)

if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')