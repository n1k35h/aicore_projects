import requests
import json

def api_url():
    pin_url = "https://41mrms02f1.execute-api.us-east-1.amazonaws.com/prod/topics/0e0816526d11.pin"

    pin_payload = json.dumps({
        "records": [
            {
            #Data should be send as pairs of column_name:value, with different columns separated by commas       
            "value": {"index": ["index"], "unique_id": ["unique_id"], "title": ["title"], "description": ["description"], "poster_name": ["poster_name"], "follower_count": ["follower_count"], "tag_list": ["tag_list"], "is_image_or_video": ["is_image_or_video"], "image_src": ["image_src"], "downloaded": ["download"], "save_location": ["save_location"], "category": ["category"]}
            }
        ]
    })

    geo_url = "https://41mrms02f1.execute-api.us-east-1.amazonaws.com/prod/topics/0e0816526d11.geo"

    geo_payload = json.dumps({
        "records": [
            {
            #Data should be send as pairs of column_name:value, with different columns separated by commas       
            "value": {"index": ["index"], "timestamp": ["timestamp"], "latitude": ["latitude"], "longitude": ["longitude"], "country": ["country"]}
            }
        ]
    })
    
    user_url = "https://41mrms02f1.execute-api.us-east-1.amazonaws.com/prod/topics/0e0816526d11.user"
    
    #To send JSON messages you need to follow this structure
    user_payload = json.dumps({
        "records": [
            {
            #Data should be send as pairs of column_name:value, with different columns separated by commas       
            "value": {"index": ["index"], "first_name": ["first_name"], "last_name": ["last_name"], "age": ["age"], "date_joined": ["date_joined"]},
            }
        ]
    })    

    headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
    response = requests.request("POST", pin_url, headers=headers, data=pin_payload)
    response = requests.request("POST", geo_url, headers=headers, data=geo_payload)
    response = requests.request("POST", user_url, headers=headers, data=user_payload)
    print(response.status_code)

api_url()