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
        FilterExpression=Attr('userType').eq('user'),
        ProjectionExpression = 'username',
	)
    items = response['Items']
    count = response['Count']
    for i in items:
        response = lambda_invoker(i)

    # print items
    return response, count

def lambda_invoker(usrDict):
    lmbd = boto3.client('lambda')
    try:
        response = lmbd.invoke(
            FunctionName='getProfiles',
            Payload=json.dumps(usrDict),
            InvocationType='Event'
            )
        return 'success'
    except Exception as e:
        print e

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return float(o)
        return super(DecimalEncoder, self).default(o)
