import json
import boto3
import decimal
from boto3.dynamodb.conditions import Key, Attr
from base64 import b64encode, b64decode

def lambda_handler(event, context):
    # Get the service resource.
    dynamodb = boto3.resource('dynamodb')
    # Instantiate a table resource object without actually
    tableUsers = dynamodb.Table('Users')
    # values will be set based on the response.
    response = tableUsers.scan(
        FilterExpression=Attr('userType').eq('user')
	)
    items = response['Items']
    count = response['Count']

    checkUsers(items)

    return response['Count']

def updateUserPhoto(payload):
     # Get the service resource.
    dynamodb = boto3.resource('dynamodb')
    # Instantiate a table resource object without actually connecting to dynamo
    tableHitches = dynamodb.Table('Users')
    # values will be set based on the response.
    response = tableHitches.put_item(Item=payload)
    # print type(hItems),hItems
    return response

def checkUsers(items):
    for i in items:
        if(i['profileImage'] is None or 'profileImage' not in i):
            print('no profile ',i['username'])
            i.update({"profileImage":"https://s3.amazonaws.com/muuwho-images/ProfileImages/profiledefault.png"})
            updateUserPhoto(i)
        if('headerImage' not in i):
            print('no header ',i['username'])
            i.update({"headerImage":"https://s3.amazonaws.com/muuwho-images/CoverImages/headerdefault.png"})
            updateUserPhoto(i)

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return float(o)
        return super(DecimalEncoder, self).default(o)
