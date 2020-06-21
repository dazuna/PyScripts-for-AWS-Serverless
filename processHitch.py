import boto3
import json
import decimal
import urllib
from boto3.dynamodb.conditions import Key, Attr
dynamo = boto3.client('dynamodb')

def respond(err, res=None):
    return {
        'statusCode': '400' if err else '200',
        'body': json.dumps(err['message']) if err else json.dumps(res,cls=DecimalEncoder),
        'headers': {
            'Content-Type': 'application/json',
        },
    }

def lambda_handler(event, context):
    body = toDict(event['body'])
    print(event)
    if('idHitch' not in body or 'accepted' not in body):
        error = {'message': 'no [idHitch] or no [accepted] in Body'}
        return respond(error,None)
    operation = updateHitch(body)
    return respond(None, operation)

def toDict(qsParam):
    qsParam = json.loads(qsParam)
    return qsParam

def updateHitch(body):
    dynamodb = boto3.resource('dynamodb')
    tableUsers = dynamodb.Table('Hitches')
    if(body['accepted']==0):
        queryAns = tableUsers.update_item(
            Key={
                'idHitch': body['idHitch']
            },
            UpdateExpression='SET enabled = :val1',
            ExpressionAttributeValues={
                ':val1': 0
            }
        )
        print('enabled to 0')
        return queryAns['ResponseMetadata']['HTTPStatusCode']
    if(body['accepted']==1):
        queryAns = tableUsers.update_item(
            Key={
                'idHitch': body['idHitch']
            },
            UpdateExpression='SET enabled = :val1, acceptedRequest = :val2',
            ExpressionAttributeValues={
                ':val1': 0,
                ':val2': 1
            }
        )
        print('enabled to 0, accepted to 1')
        return queryAns['ResponseMetadata']['HTTPStatusCode']
    return None

# hItems = json.dumps(hItems, cls=DecimalEncoder)
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return float(o)
        return super(DecimalEncoder, self).default(o)
