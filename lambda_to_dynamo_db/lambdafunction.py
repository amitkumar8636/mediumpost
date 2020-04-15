import json
import boto3
import time
import decimal
from boto3.dynamodb.types import DYNAMODB_CONTEXT


"""
DynamoDB Configuration: 
DynamoDb doesn't support float_value so need to use decimal.Decimal
"""
DYNAMODB_CONTEXT.traps[decimal.Inexact] = 0
# Inhibit Rounded Exceptions
DYNAMODB_CONTEXT.traps[decimal.Rounded] = 0
dynamodb = boto3.resource('dynamodb')



def decimal_convertion(float_value):
    """ Decimal Convertion function with a precision of 5 digits with rounding up"""
    return decimal.Decimal(float_value).quantize(decimal.Decimal('0.00000'), rounding=decimal.ROUND_UP)


def insert_data(data):
    """  Method for inserting data into dynamodb """
    table = dynamodb.Table('my-table')
    for d in data:
        table.put_item(
            Item={
                'Attribute_1': d['Attribute_1'], 
                'Attribute_2': d['Attribute_2'], 
                'payload': {
                    'Attribute_3': '50', 
                    'Attribute_4': decimal_convertion(d['payload']['Attribute_4'])
                            }, 
                'timestamp': str(d['timestamp'])
                }
                )
            



def lambda_handler(event, context):
    """Lambda function invoked when data published to the Iot Core.
    Invoked by Iot Iot Core"""
    
    insert_data(event)
    return

    return {
        'statusCode': 200,
        'body': json.dumps('Data Inserted with timestamp'+event[0]['timestamp'])
    }