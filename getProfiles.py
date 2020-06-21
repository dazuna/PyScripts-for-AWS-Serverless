from __future__ import division
import boto3
import json
import decimal
import math
from boto3.dynamodb.conditions import Key, Attr
from decimal import Decimal

def lambda_handler(event, context):

    usr = event
    user = getUserInfo(usr['username'])

    #unwrap user list
    user = user[0]

    #Gets profile completion percentage
    prfPercent = 0
    prfPercent += getUserAcad(user['username'])
    prfPercent += getUserWork(user['username'])
    prfPercent = countExistingFields(user,prfPercent)

    #Get User's Hitches
    hitchTable = getHitchTable(usr['username'])

    #Accepted Hitches
    hPrcntAccptd = processAcceptedHitches(hitchTable)

    #Advanced Hitches
    hPrcntAdvncd = processAdvancedHitches(hitchTable,hPrcntAccptd[0])

    # printings type(hitchTable),hitchTable[1]
    print 'profile completion = ',math.ceil(prfPercent),'%'
    print 'accepted hitches = ',math.ceil(hPrcntAccptd[1]),'%'
    print 'advanced hitches = ',math.ceil(hPrcntAdvncd),'%'

    payload = { "username":usr['username'],"profilePercent":Decimal(math.ceil(prfPercent)),"acceptedHitches":Decimal(math.ceil(hPrcntAccptd[1])),"advancedHitches":Decimal(math.ceil(hPrcntAdvncd)) }
    print payload

    updateRankUsers(payload)

    return usr

# START getHitchTable()
# Receives username [email] of the user whose Hitch table is needed
# Returns the Hitch Table in JSON Format
def getHitchTable(usrname):
    # Get the service resource.
    dynamodb = boto3.resource('dynamodb')
    # Instantiate a table resource object without actually connecting to dynamo
    tableHitches = dynamodb.Table('Hitches')
    # values will be set based on the response.
    response = tableHitches.scan(FilterExpression=Attr('username').eq(usrname))
    hItems = response['Items']
    # print type(hItems),hItems
    return hItems
# END getHitchTable()

def getUserInfo(usrname):
    # Get the service resource.
    dynamodb = boto3.resource('dynamodb')
    # Instantiate a table resource object without actually connecting to dynamo
    tableUsers = dynamodb.Table('Users')
    # values will be set based on the response.
    # response = tableUsers.scan(FilterExpression=Attr('username').eq(usrname))
    response = tableUsers.query(KeyConditionExpression=Key('username').eq(usrname))
    return response['Items']

def getUserWork(usrname):
    # Get the service resource.
    dynamodb = boto3.resource('dynamodb')
    # Instantiate a table resource object without actually connecting to dynamo
    tableUsers = dynamodb.Table('Work_Experience')
    # values will be set based on the response.
    response = tableUsers.scan(FilterExpression=Attr('username').eq(usrname))

    if (response['Count'] > 0):
        return 1
    else:
        return 0

def getUserAcad(usrname):
    # Get the service resource.
    dynamodb = boto3.resource('dynamodb')
    # Instantiate a table resource object without actually connecting to dynamo
    tableUsers = dynamodb.Table('Academic_Info')
    # values will be set based on the response.
    response = tableUsers.scan(FilterExpression=Attr('username').eq(usrname))

    if (response['Count'] > 0):
        return 1
    else:
        return 0

# START countExistingFields()
# Receives the email of the user, reads every column of its profile info
# Return a number in percent that represents how complete the user's profile is
def countExistingFields(usr, acadWork):

    numPrcntCompl = 0
    numPrcntCompl += acadWork

    # print 'acadwork: ',acadWork

    if('fechanac' in usr):
        if('Cumple' not in usr['fechanac']): numPrcntCompl += 1
    if('headerImage' in usr): numPrcntCompl += 1
    if('pais' in usr):
        if(usr['pais'] is not None): numPrcntCompl += 1
    if('personaldesc' in usr):
        if(usr['personaldesc'] is not None): numPrcntCompl += 1
    if('profileImage' in usr):
        if(usr['profileImage'] is not None): numPrcntCompl += 1
    if('telefono' in usr):
        if(len(usr['telefono']) > 5 ): numPrcntCompl += 1
    if('ubicacion' in usr):
        if(usr['ubicacion'] is not None): numPrcntCompl += 1

    if('aptitudes' in usr):
        if('{\"aptitudes\":[]}' not in usr['aptitudes']): numPrcntCompl += 1
    if('hobbies' in usr):
        if('{\"hobbies\":[]}' not in usr['hobbies']): numPrcntCompl += 1
    if('idiomas' in usr):
        if('{\"idiomas\":[{\"idioma\":[{\"nombre\":\"\"}' not in usr['idiomas']): numPrcntCompl += 1
    if('redes' in usr):
        if('{\"redes\":[{\"github\":\"\"},{\"behance\":\"\"},{\"soundcloud\":\"\"},{\"flickr\":\"\"},{\"deviantart\":\"\"}]}' not in usr['redes']): numPrcntCompl += 1
    if('sistemas' in usr):
        if('{\"sistemas\":[{\"software\":[{\"nombre\":\"\"}' not in usr['sistemas']): numPrcntCompl += 1

    return numPrcntCompl/14*100
# END countExistingFields()

# START processAcceptedHitches()
# Receives the hitch table of the username we are evaluating
# Returns percent of hitches that the "hitched" has accepted
def processAcceptedHitches(hItems):
    count = 0
    hCount = len(hItems)
    for x in hItems:
        count += x['acceptedRequest']
    if(count == 0): return [0,0]
    else:
        hPrcentAccepted = count/hCount*100
        acc_Prcnt = [count,hPrcentAccepted]
        return acc_Prcnt
# END processAcceptedHitches()

# START processAdvancedHitches()
# Receives the hitch table of the username we are evaluating and the number of accepted hitches
# Returns percent of hitches that the company has advanced past phase1
def processAdvancedHitches(hItems,accHitches):
    hAdvancedRequests = 0
    for hitch in hItems:
        hAdvancedRequests += isHitchPastPhaseOne(hitch)
    if(accHitches == 0):
        return 0
    else:
        return hAdvancedRequests/accHitches*100
# END processAdvancedHitches()

# START checkPhases()
# Receives a register from hitcTable
# Returns if the hitch has advanced phases
def isHitchPastPhaseOne(reg):
    if('fase' in reg):
        if(reg['fase'] is not None and reg['fase'] != "" and reg['fase'] > 1):
            return 1
        else:
            return 0
    else:
        return 0
# END checkPhases()

# START updateRankUsers()
# Receives a dict to be written to Table RankUsers
def updateRankUsers(payload):
     # Get the service resource.
    dynamodb = boto3.resource('dynamodb')
    # Instantiate a table resource object without actually connecting to dynamo
    tableHitches = dynamodb.Table('RankUsers')
    # values will be set based on the response.
    response = tableHitches.put_item(Item=payload)
    # print type(hItems),hItems
    return response
# END

# hItems = json.dumps(hItems, cls=DecimalEncoder)
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return float(o)
        return super(DecimalEncoder, self).default(o)
