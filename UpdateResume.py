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
    if('acad' in body):
        if(body['acad'] is not None):
            for a in body['acad']:
                updateAcad(a)
    if('work' in body):
        if(body['work'] is not None):
            for w in body['work']:
                updateWork(w)
    return respond(None, body)

def toDict(qsParam):
    qsParam = json.loads(qsParam)
    return qsParam

def updateAcad(qsParam):
    dynamodb = boto3.resource('dynamodb')
    tableUsers = dynamodb.Table('Academic_Info')
    if('delete' in qsParam):
        if(qsParam['delete'] == 'true'):
            tableUsers.delete_item( Key={'academicId': qsParam['academicId']} )
    else:
        tableUsers.put_item(Item=qsParam)

def updateWork(qsParam):
    dynamodb = boto3.resource('dynamodb')
    tableUsers = dynamodb.Table('Work_Experience')
    if('delete' in qsParam):
        if(qsParam['delete'] == 'true'):
            tableUsers.delete_item( Key={'workId': qsParam['workId']} )
    else:
        tableUsers.put_item(Item=qsParam)

# hItems = json.dumps(hItems, cls=DecimalEncoder)
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return float(o)
        return super(DecimalEncoder, self).default(o)
