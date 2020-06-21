import boto3
import json
import decimal
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

    qsParam = event['queryStringParameters']
    offers = getOffers(qsParam)
    return respond(None, offers)

def getOffers(qsParam):
    # Get the service resource.
    dynamodb = boto3.resource('dynamodb')
    # Instantiate a table resource object without actually
    tableUsers = dynamodb.Table('Offers')
    # values will be set based on the response.
    if qsParam is not None:
        if qsParam.has_key('company'): return tableUsers.scan(FilterExpression=Attr('company').eq(qsParam['company']))
        if qsParam.has_key('companyName'): return tableUsers.scan(FilterExpression=Attr('searchField').contains(qsParam['companyName']))
    else:
        return tableUsers.scan()

# hItems = json.dumps(hItems, cls=DecimalEncoder)
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return float(o)
        return super(DecimalEncoder, self).default(o)
