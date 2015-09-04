import csv
import boto3
import logging

def convert_to_dynamo_item(hash_key, item_data):
    item = {
        'CarID': {'S': hash_key},
    }
    item['Tags']= {'L': []}
    for key in item_data.keys():
        if item_data[key] != '':
            if key.lower().find('tag') != -1:
                item['Tags']['L'].append({'S': item_data[key]})
            else:
                item[key] = {'S': item_data[key]}
        else:
            logging.log(logging.WARN, "Item: " + hash_key + " has no value for key " + key)
    return item

table_name = 'car_descriptions'
client = boto3.client('dynamodb')

with open('../cardb/vehicles_simpler.csv', 'rb') as csvfile:
    reader = csv.DictReader(csvfile, delimiter=',')
    for i in range(10,15):
        item_data = reader.next()
        #print(convert_to_dynamo_item(str(i), item_data))
        client.put_item(TableName=table_name, Item=convert_to_dynamo_item(str(i), item_data))



