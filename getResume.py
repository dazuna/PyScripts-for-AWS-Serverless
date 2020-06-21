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

################################################### MAIN FUNCTION ###################################################
def lambda_handler(event, context):
    qsParam = event['queryStringParameters']
    isQSValid = validateQS(qsParam)
    if(isQSValid!=1):
        return respond(isQSValid,None)

    payload ={}                                 #Empty Dictionary
    acad = getAcad(qsParam)
    if(acad is not None):
        acad.sort(key=extract_time, reverse=True)
    payload.update({'acad':acad})

    work = getWork(qsParam)
    if(work is not None):
        work.sort(key=extract_time, reverse=True)
    payload.update({'work':work})

    return respond(None, payload)
################################################### MAIN FUNCTION ###################################################

def validateQS(qsParam):
    if (qsParam is None or 'username' not in qsParam):
        error = {'message':'Bad or no query Parameters'}
        return error
    else:
        return 1

def getAcad(qsParam):
    dynamodb = boto3.resource('dynamodb')
    tableUsers = dynamodb.Table('Academic_Info')
    items = tableUsers.scan( FilterExpression=Attr('username').eq(qsParam['username']) )
    if(items['Count']<1): return None
    return items['Items']

def getWork(qsParam):
    dynamodb = boto3.resource('dynamodb')
    tableUsers = dynamodb.Table('Work_Experience')
    items = tableUsers.scan( FilterExpression=Attr('username').eq(qsParam['username']) )
    if(items['Count']<1): return None
    return items['Items']

# hItems = json.dumps(hItems, cls=DecimalEncoder)
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return float(o)
        return super(DecimalEncoder, self).default(o)

def extract_time(json):
    try:
        return json['endDate']
    except KeyError:
        return 0

# lines.sort() is more efficient than lines = lines.sorted()
# lines.sort(key=extract_time, reverse=True)
