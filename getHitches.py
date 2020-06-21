import boto3
import json
import decimal
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
    qsParam = event['queryStringParameters']
    if (qsParam is None or 'username' not in qsParam):
        error = {'message':'Bad or no query Parameters'}
        return respond(error,None)
    hitches = getOffers(qsParam)
    if (hitches == 0):
        error = {'message':'This user has no hitches'}
        return respond(error,None)
    return respond(None, hitches)

def getOffers(qsParam):
    dynamodb = boto3.resource('dynamodb')
    tableUsers = dynamodb.Table('Hitches')
    items = tableUsers.scan(
        FilterExpression=Attr('idContact').eq(qsParam['username']) &
        Attr('enabled').eq(1) &
        Attr('acceptedRequest').eq(0),
        ProjectionExpression='idHitch,username,offerJob'
        )
    print(qsParam['username'])
    print(items)
    if(items['Count']<1): return 0
    items = getNames(items['Items'])
    return items

def getNames(items):
    dynamodb = boto3.resource('dynamodb')
    tableUsers = dynamodb.Table('Users')
    for i in items:
        usr=tableUsers.scan(FilterExpression=Attr('username').eq(i['username']),
        ProjectionExpression='username,nombre,apellidos')
        i.update({'nombre':usr['Items'][0]['nombre']+' '+usr['Items'][0]['apellidos']})
    return items

# hItems = json.dumps(hItems, cls=DecimalEncoder)
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return float(o)
        return super(DecimalEncoder, self).default(o)
