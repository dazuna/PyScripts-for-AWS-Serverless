import boto3
import json
import decimal
from boto3.dynamodb.conditions import Key, Attr
from pprint import pprint
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
    print(event)
    #Validate Body existance
    if(event['body'] is not None):body = json.loads(event['body'])
    else: body = None
    #Validate qsParam and Body
    if(qsParam is not None or body is not None):
        if(qsParam is not None and 'query' in qsParam): return respond(None,searchOffers(qsParam['query'],body))
        if(body is not None):return respond(None,searchOffers(None,body))
    return respond('wrong parameters')

def searchOffers(query,body):
    dynamodb = boto3.resource('dynamodb')
    tableUsers = dynamodb.Table('Offers')
    if(query is not None):
        scanParams = Attr('searchField').contains(query.lower())
        if(body is not None):
            bodyParams = addBodyParams(body,scanParams)
            return tableUsers.scan(FilterExpression=scanParams & bodyParams)
        else: return tableUsers.scan(FilterExpression=scanParams)
    else:
        if(body is not None):
            scanParams = addBodyParams(body,None)
            return tableUsers.scan(FilterExpression=scanParams)
        return(None)

def addBodyParams(filters,botoParams):
    if('estructura' in filters):
        if(botoParams is None):
            botoParams = Attr('estructura').eq(filters['estructura'])
        else:
            botoParams = botoParams & Attr('estructura').eq(filters['estructura'])
    if('vestimenta' in filters):
        if(botoParams is None):
            botoParams = Attr('vestimenta').eq(filters['vestimenta'])
        else:
            botoParams = botoParams & Attr('vestimenta').eq(filters['vestimenta'])
    if('horario' in filters):
        if(botoParams is None):
            botoParams = Attr('horario').eq(filters['horario'])
        else:
            botoParams = botoParams & Attr('horario').eq(filters['horario'])
    if('oficina' in filters):
        if(botoParams is None):
            botoParams = Attr('oficina').eq(filters['oficina'])
        else:
            botoParams = botoParams & Attr('oficina').eq(filters['oficina'])
    if('idiomas' in filters):
        if(botoParams is None):
            botoParams = Attr('idiomas').eq(filters['idiomas'])
        else:
            botoParams = botoParams & Attr('idiomas').eq(filters['idiomas'])
    if('contrato' in filters):
        if(botoParams is None):
            botoParams = Attr('contrato').eq(filters['contrato'])
        else:
            botoParams = botoParams & Attr('contrato').eq(filters['contrato'])
    if('trabajo' in filters):
        if(botoParams is None):
            botoParams = Attr('trabajo').eq(filters['trabajo'])
        else:
            botoParams = botoParams & Attr('trabajo').eq(filters['trabajo'])
    if('local' in filters):
        if(botoParams is None):
            botoParams = Attr('local').eq(filters['local'])
        else:
            botoParams = botoParams & Attr('local').eq(filters['local'])
    if('colab' in filters):
        if(botoParams is None):
            botoParams = Attr('colab').eq(filters['colab'])
        else:
            botoParams = botoParams & Attr('colab').eq(filters['colab'])
    if('experiencia' in filters):
        if(botoParams is None):
            botoParams = Attr('experiencia').eq(filters['experiencia'])
        else:
            botoParams = botoParams & Attr('experiencia').eq(filters['experiencia'])
    if('nivel' in filters):
        if(botoParams is None):
            botoParams = Attr('nivel').eq(filters['nivel'])
        else:
            botoParams = botoParams & Attr('nivel').eq(filters['nivel'])
    if('tipocontrato' in filters):
        if(botoParams is None):
            botoParams = Attr('tipocontrato').contains(filters['tipocontrato'])
        else:
            botoParams = botoParams & Attr('tipocontrato').contains(filters['tipocontrato'])
    if('area' in filters):
        if(botoParams is None):
            botoParams = Attr('area').contains(filters['area'])
        else:
            botoParams = botoParams & Attr('area').contains(filters['area'])
    if('salarioFilter' in filters and filters['salarioFilter'] == 'true'):
        if(botoParams is None):
            botoParams = buildWageQuery(filters)
        else:
            botoParams = botoParams & buildWageQuery(filters)
    return botoParams

def buildWageQuery(filters):
    if('salarioTipo' in filters):
        if(filters['salarioTipo']=='ambos' and 'salarioMenor' in filters and 'salarioMayor' in filters):
            return(Attr('sMinimo').gte(filters['salarioMenor']) & Attr('sMinimo').lte(filters['salarioMayor']))
        if(filters['salarioTipo']=='Minimo' and 'salarioMenor' in filters):
            return(Attr('sMinimo').gte(filters['salarioMenor']))
        if(filters['salarioTipo']=='Maximo' and 'salarioMayor' in filters):
            return(Attr('sMinimo').lte(filters['salarioMayor']))
    return None

# hItems = json.dumps(hItems, cls=DecimalEncoder)
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return float(o)
        return super(DecimalEncoder, self).default(o)
