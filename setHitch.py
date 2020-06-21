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
    print('received body ',body)
    error = {'message': 'bad request'}
    if ("idHitch" in body and "username" in body and "idContact" in body and "userProfilePic" in body and "companyProfilePic" in body and "creationDate" in body and "company" in body and "offerCreationDate" in body and "contactName" in body and "offerReward" in body and "offerJob" in body):
        putHitch(body)
    else: return respond(error,None)
    return respond(None, body)

def toDict(qsParam):
    qsParam = json.loads(qsParam)
    return qsParam

def putHitch(qsParam):
    dynamodb = boto3.resource('dynamodb')
    tableUsers = dynamodb.Table('Hitches')
    qsParam.update({'acceptedRequest':0,'enabled':1})
    print('payload 2 dynamo ',qsParam)
    tableUsers.put_item(Item=qsParam)

# hItems = json.dumps(hItems, cls=DecimalEncoder)
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return float(o)
        return super(DecimalEncoder, self).default(o)
