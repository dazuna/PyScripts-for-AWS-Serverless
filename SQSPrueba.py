import boto3
import json
import decimal
import hashlib
import random
from boto3.dynamodb.conditions import Key, Attr
dynamo = boto3.client('dynamodb')
sqs = boto3.resource('sqs')
queue = sqs.Queue('https://sqs.us-east-2.amazonaws.com/342196699900/InviteEmails')

def respond(err, res=None):
    return {
        'statusCode': '400' if err else '200',
        'body': json.dumps(err['message']) if err else json.dumps(res,cls=DecimalEncoder),
        'headers': {
            'Content-Type': 'application/json',
        },
    }

def lambda_handler(event, context):
    query = toDict(event['body'])
    payload = validateBody(query['Items'])
    print(payload)
    return respond(None,payload)

def validateBody(query):
    dynamodb = boto3.resource('dynamodb')
    tableUsers = dynamodb.Table('Invites_Requested')
    payload = 'payload = '
    with tableUsers.batch_writer() as batch:
        for q in query:
            if('username' in q and 'name' in q and 'contact' in q and 'date' in q):
                # payload += regInvite(q)
                idInvite = q['date']+q['username']+q['contact']+str(random.randrange(10,99))
                idInvite = hashlib.md5(idInvite.encode())
                q['idInvite'] = idInvite.hexdigest()
                q['mailSent'] = "false"
                batch.put_item(Item=q)
                payload += str(q)
    return payload

def toDict(qsParam):
    qsParam = json.loads(qsParam)
    return qsParam

# hItems = json.dumps(hItems, cls=DecimalEncoder)
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return float(o)
        return super(DecimalEncoder, self).default(o)
