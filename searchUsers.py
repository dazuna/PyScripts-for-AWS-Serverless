import boto3
import json
import decimal
from boto3.dynamodb.conditions import Key, Attr
dynamo = boto3.client('dynamodb')

def respond(err, res=None):
    return {
        'statusCode': '400' if err else '200',
        'body': json.dumps(err,cls=DecimalEncoder) if err else json.dumps(res,cls=DecimalEncoder),
        'headers': {
            'Content-Type': 'application/json',
        },
    }

def lambda_handler(event, context):
    qsParam = event['queryStringParameters']
    if('query' in qsParam):
        users = searchUsers(qsParam['query'])
        return respond(None, users)
    else:
        return respond('Wrong query parameters',None)


def searchUsers(key):
    dynamodb = boto3.resource('dynamodb')
    tableUsers = dynamodb.Table('Users')
    return tableUsers.scan(FilterExpression=Attr('searchField').contains(key.lower()))

# hItems = json.dumps(hItems, cls=DecimalEncoder)
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return float(o)
        return super(DecimalEncoder, self).default(o)
