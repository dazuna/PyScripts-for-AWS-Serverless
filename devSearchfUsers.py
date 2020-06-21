import json
import boto3
import decimal
from boto3.dynamodb.conditions import Key, Attr
from base64 import b64encode, b64decode

def lambda_handler(event, context):
    dynamodb = boto3.resource('dynamodb')
    tableUsers = dynamodb.Table('Users')
    response = tableUsers.scan()
    items = response['Items']
    count = 0

    tableHitches = dynamodb.Table('Users')
    with tableHitches.batch_writer() as batch:
        for i in items:
            if(i['userType'] == 'user'):
                searchField = i['nombre'].lower() + ' ' + i['apellidos'].lower()
            else:
                searchField = i['nombre'].lower()
            i.update({"searchField":searchField})
            count = count + 1
            batch.put_item(Item=i)
    return count

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return float(o)
        return super(DecimalEncoder, self).default(o)
