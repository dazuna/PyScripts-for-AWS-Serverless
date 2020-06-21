# -*- coding: utf-8 -*-
import json
import boto3
from boto3.dynamodb.conditions import Key, Attr
from base64 import b64encode, b64decode

def lambda_handler(event, context):
    dynamodb = boto3.resource('dynamodb')
    tableUsers = dynamodb.Table('Offers')
    response = tableUsers.scan()
    items = response['Items']
    count = 0

    with tableUsers.batch_writer() as batch:
        for i in items:
            searchField = createSearchField(i)
            i.update({"searchField":searchField})
            count = count + 1
            print i['searchField']
            batch.put_item(Item=i)
    return count

def createSearchField(usr):
    tagString = getTags(usr)
    hierarch = getHL(usr)
    area = getFArea(usr)
    return usr['companyName'].lower()+' '+usr['puesto'].lower()+tagString+' '+usr['nivel'].lower()+' '+hierarch+area

def getTags(usr):
    tagString = ''
    tags=usr['tags']
    tags=tags.replace('  ','')
    tags=json.loads(tags)['tags']
    print(type(tags),tags)
    for t in tags:
        try:
            print type(t.keys()),t.keys()
            tagString += ' ' + t.keys()[0]
            break
        except TypeError:
            print('not a valid list')
    tagString += ' '
    return tagString

def getHL(usr):
    nivelCat={
		'Executive Level':'ejecutivo ',
		'Management Level':'gerente ',
		'Professional Level':'profesionista ',
		'Entry Level':''
    }
    return nivelCat.get(usr['nivel'],'')

def getFArea(cadena):
	fACat = {
		'Not Specified' : '',
		'Administrative/Secretarial':'administrativo/ secretarial '+cadena['area'].lower(),
		'Advisory':'asesoria '+cadena['area'].lower(),
		'Consulting':'consultoria '+cadena['area'].lower(),
		'Corporate Affairs/Communication':'asuntos corporativos / comunicacion '+cadena['area'].lower(),
		'Customer Service':'servicio al cliente '+cadena['area'].lower(),
		'Facilities/Fleet Management':'instalaciones / gestion de flotillas '+cadena['area'].lower(),
		'Finance':'finanzas '+cadena['area'].lower(),
		'General Management':'administracion general '+cadena['area'].lower(),
		'Health, Safety & Environment':'salud, seguridad y medio ambiente '+cadena['area'].lower(),
		'Human Resources':'recursos humanos '+cadena['area'].lower(),
		'Information Technology':'tecnologias de la informacion '+cadena['area'].lower(),
		'Legal & Compliance':'cumplimiento legal '+cadena['area'].lower(),
		'Marketing':'mercadotecnia '+cadena['area'].lower(),
		'Operations':'operaciones '+cadena['area'].lower(),
		'Procurement':'compras / adquisiciones '+cadena['area'].lower(),
		'Project Management':'gestion de proyectos '+cadena['area'].lower(),
		'Public Relations/Investor Relations':'relaciones publicas / relaciones con inversionistas '+cadena['area'].lower(),
		'Research & Analytics':'investigacion y analisis '+cadena['area'].lower(),
		'Sales':'ventas '+cadena['area'].lower(),
		'Strategy/Planning/Corporate Development':'estrategia / planificacion / desarrollo corporativo '+cadena['area'].lower(),
		'Supply Chain Logistics':'logistica en la cadena de suministros '+cadena['area'].lower()
	}
	return fACat.get(cadena['area'],'')
