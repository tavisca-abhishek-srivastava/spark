import json
import boto3
import os

def lambda_handler(event, context):
    # TODO implement
    print(event)
    primary_key = os.getenv('primary_key')  ### create environment variable with name primary_key and value will be list like ["user_id","product_id"]
    dynamodb = boto3.resource('dynamodb') 
    #table name 
    table = dynamodb.Table('dynamodb-gsi-lsi-target') 
    
    for record in event["Records"] :
        if record["eventName"] == "INSERT" or record["eventName"] == "MODIFY":
            dict = {}
            for k  in (record["dynamodb"]["NewImage"]).keys() :
                #print(k)
                for nested_key in (record["dynamodb"]["NewImage"][k]).keys():
                    #print(nested_key)
                    #print(record["dynamodb"]["NewImage"][k][nested_key])
                    dict[k] = record["dynamodb"]["NewImage"][k][nested_key]
        
            print("inserting {0}".format(dict))
            #inserting values into table 
            response = table.put_item( 
               Item= dict
                ) 
            print(response)
            
            
        if record["eventName"] == "REMOVE":
            dict = {}
            for k  in (record["dynamodb"]["OldImage"]).keys() :
                #print(k)
                for nested_key in (record["dynamodb"]["OldImage"][k]).keys():
                    #print(nested_key)
                    #print(record["dynamodb"]["OldImage"][k][nested_key])
                    if k in primary_key:
                        dict[k] = record["dynamodb"]["OldImage"][k][nested_key]
        
            print("deleting {0}".format(dict))
            #deleting values from table 
            response = table.delete_item( Key = dict ) 
            print(response)
            
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }