import boto3
import json
import decimal
import urllib
from boto3.dynamodb.conditions import Key, Attr
dynamo = boto3.client('dynamodb')

def respond(err, res=None):
    return {
        'statusCode': '400' if err else '200',
        'body': err.message if err else json.dumps(res,cls=DecimalEncoder),
        'headers': {
            'Content-Type': 'application/json',
        },
    }

def lambda_handler(event, context):
    body = toDict(event['body'])
    # print type(body),body
    updateUser(body)
    return respond(None, body)

def toDict(qsParam):
    qsParam = json.loads(qsParam)
    return qsParam

def updateUser(qsParam):
    dynamodb = boto3.resource('dynamodb')
    tableUsers = dynamodb.Table('Users')
    tableUsers.put_item(Item=qsParam)

# hItems = json.dumps(hItems, cls=DecimalEncoder)
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return float(o)
        return super(DecimalEncoder, self).default(o)
