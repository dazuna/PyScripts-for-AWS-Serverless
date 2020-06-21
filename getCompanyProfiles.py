from __future__ import division
import boto3
import json
import decimal
import math
from boto3.dynamodb.conditions import Key, Attr
from datetime import datetime

def lambda_handler(event, context):

    usr = event
    user = getUserInfo(usr['username'])
    user = user[0]
    prfPercent = countExistingFields(user)
    followers = getFollowerCount(usr['username'])
    earlyBird = isEarlyBird(user['creationDate'])
    newsAmount = getNewsCount(usr['username'])

    # Prints broh
    print 'user:        ',usr['username'],'|| company:      ',user['nombre']
    print 'profile:     ',prfPercent,'%'
    print 'followers:   ',followers
    print 'cDate:       ',earlyBird
    print 'news:        ',newsAmount

    return 'All your base are belong to us'

# START getHitchTable()
# Receives username [email] of the user whose Hitch table is needed
# Returns the Hitch Table in JSON Format
def getHitchTable(usrname):
    # Get the service resource.
    dynamodb = boto3.resource('dynamodb')
    # Instantiate a table resource object without actually
    tableHitches = dynamodb.Table('Hitches')
    # values will be set based on the response.
    response = tableHitches.scan(FilterExpression=Attr('company').eq(usrname))
    hItems = response['Items']
    # print type(hItems),hItems
    return hItems
# END getHitchTable()

def getUserInfo(usrname):
    # Get the service resource.
    dynamodb = boto3.resource('dynamodb')
    # Instantiate a table resource object without actually
    tableHitches = dynamodb.Table('Users')
    # values will be set based on the response.
    response = tableHitches.scan(FilterExpression=Attr('username').eq(usrname))
    # print type(hItems),hItems
    return response['Items']

# START countExistingFields()
# Receives the email of the user, reads every column of its profile info
# Return a number in percent that represents how complete the user's profile is
def countExistingFields(usr):

    numPrcntCompl = 0

    if('about' in usr):
        if(usr['about'] is not None):numPrcntCompl +=1
    if('actividades' in usr):
        if(usr['actividades'] is not None):numPrcntCompl +=1
    if('headerImage' in usr):
        if(usr['numEmpleados'] is not None):numPrcntCompl +=1
    if('numEmpleados' in usr):
        if(usr['numEmpleados'] is not None):numPrcntCompl +=1
    if('pais' in usr):
        if(usr['pais'] is not None):numPrcntCompl +=1
    if('profileImage' in usr):
        if(usr['profileImage'] is not None):numPrcntCompl +=1
    if('sector' in usr):
        if(usr['sector'] is not None):numPrcntCompl +=1
    if('ubicacion' in usr):
        if(usr['ubicacion'] is not None):numPrcntCompl +=1
    if('web' in usr):
        if(usr['web'] is not None):numPrcntCompl +=1

    if('certificaciones' in usr):
        crt = json.loads(usr['certificaciones'])
        if ( len(crt['certificaciones']) > 0 ): numPrcntCompl += 1
    if('reclutamiento' in usr):
        rec = json.loads(usr['reclutamiento'])
        if ( len(rec['reclutamiento']) > 0 ): numPrcntCompl += 1
    if('prestaciones' in usr):
        prs = json.loads(usr['prestaciones'])
        if ( len(prs['prestaciones']) > 0 ): numPrcntCompl += 1

    return numPrcntCompl/12*100
# END countExistingFields()

# START isEarlyBird()
# Receives the username we are evaluating
# Returns if the user created it's profile before a certain date
def isEarlyBird(creatDate):
    creatDate = datetime.strptime(creatDate, "%Y/%m/%d, %H:%M:%S %Z")
    return creatDate
# END isEarlyBird()

# START processAdvancedHitches()
# Receives the hitch table of the username we are evaluating
# Returns percent of hitches that the company has advanced past phase1
def getFollowerCount(usrname):
    # Get the service resource.
    dynamodb = boto3.resource('dynamodb')
    # Instantiate a table resource object without actually
    tableHitches = dynamodb.Table('Contactos')
    # values will be set based on the response.
    response = tableHitches.scan(FilterExpression=Attr('username').eq(usrname), Select='COUNT')
    return response['Count']
# END processAdvancedHitches()

def getNewsCount(usrname):
    # Get the service resource.
    dynamodb = boto3.resource('dynamodb')
    # Instantiate a table resource object without actually
    tableHitches = dynamodb.Table('Noticias')
    # values will be set based on the response.
    now = datetime.now()
    now = '/'+str(now.month)+'/'
    response = tableHitches.scan(
        FilterExpression=Attr('company').eq(usrname) & Attr('creationDate').contains(now),
        Select='COUNT'
        )
    return response['Count']

# hItems = json.dumps(hItems, cls=DecimalEncoder)
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return float(o)
        return super(DecimalEncoder, self).default(o)
