import json
import base64
import json,os
import boto3

def lambda_handler(event, context):
    # TODO implement
    print(event)
   
    primary_key = os.getenv('primary_key')  ### create environment variable with name primary_key and value will be list  ["user_id","product_id"]
    dynamodb = boto3.resource('dynamodb') 
    #### DynamoDB table name 
    table = dynamodb.Table('dynamodb-gsi-lsi-target') 
    for record in event["Records"] :
        coded_string = record["kinesis"]["data"]
        decoded_data  = (base64.b64decode(coded_string)).decode("utf-8").replace("'", '"')
        json_data = json.loads((decoded_data))
        event_type = json_data['eventName']
        # print(json_data)
        if event_type == "INSERT" or event_type == "MODIFY" :
            data = json_data['dynamodb']['NewImage']
            dict = {}
            for key in data.keys():
                for nested_key in (data[key]).keys():
                    data_key = key.replace("'",'"')
                    data_value =  (data[key][nested_key]).replace("'",'"')
                    dict[data_key] = data_value
            print("Inserting {0}".format(dict))
            response = table.put_item( 
               Item= dict
                ) 
            print(response)
        if event_type == "REMOVE" :
            data = json_data['dynamodb']['OldImage']
            dict = {}
            for key in data.keys():
                for nested_key in (data[key]).keys():
                    data_key = key.replace("'",'"')
                    data_value =  (data[key][nested_key]).replace("'",'"')
                    if data_key in primary_key:
                        dict[data_key] = data_value
            print("deleting {0}".format(dict))
            response = table.delete_item( Key = dict )
            print(response)
                    
                
        
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
