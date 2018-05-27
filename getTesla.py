
#!/usr/bin/python
# -*- encoding: utf-8 -*-
# https://pypi.python.org/pypi/geocoder/
from __future__ import unicode_literals
import os
import teslajson
import geocoder
import datetime
import time
import json
import sys
import MySQLdb
from pymongo import MongoClient
from pprint import pprint


def logwrite(logentry):
	now = datetime.datetime.utcnow()
	logentry=now.strftime("%Y-%m-%d %H:%M:%S") + " " + logentry + "\n"
	try:
		fh = open(logfile,'a')
		fh.write(logentry)
		fh.close
	except:
		print "Logfile kann nicht geöffnet werden"


def poicheck(latitude, longitude, poiwindow=0.01):
	'''Hier wird geprüft ob das Auto in einem Point of Interest ist'''
	poi = False
	try:
		with open (poilistfile, 'r') as f:
			poilist = json.load(f)
	except:
		print "Es existiert keine poilist.json"
		logwrite("Es existiert keine poilist.json")
		return False

	for p in poilist['poi']:
		plongitude = float(p['longitude'])
		platidute = float(p['latitude'])
        '''Check'''https://github.com/Duecki/getTesla.git
        longitude < (plongitude+0.01) and longitude > (plongitude-0.01) and latitude < (platidute+0.01) and latitude > (platidute-0.01):
			if debugmode:
				print "setzen des POI Flags"
			logwrite("setzte POI Flag")
			poi = True
			return poi
	if simulatepoi:
		poi = True
	return poi


def write2mysql():
	#connect zur Datenbank
	try:
		db = MySQLdb.connect(connect_timeout=10,host=DB_SERVER,user=DB_USER,passwd=DB_PWD,db=DB_DATENBANK)
		cursor = db.cursor()
	except:
		print "Verbindung zum mysql Server fehlgeschlagen"
		logwrite("Verbindung zum mysql Server fehlgeschlagen")

	#Datenbankeintrag
	for elem in data:
		try:
			cursor.execute("""INSERT INTO TeslaLogDB.teslatracking
				(vehicle_id,user_id,
				charging_state,battery_level,
				charger_voltage,charger_power,
				gps_as_of,speed,
				car_version,longitude,
				latitude,date,kmstand)
				VALUES (%s,%s,
				%s,%s,
				%s,%s,
				%s,%s,
				%s,%s,
				%s,%s,%s)""",[elem['vehicle_id'], TESLA_EMAIL,
					elem['charging_state'], elem['battery_level'], elem['charger_voltage'], elem['charger_power'], elem['gps_as_of'], elem['speed'], elem['car_version'], elem['longitude'], elem['latitude'], elem['messZeit'], elem['KMstand']])
			db.commit()
		except:
			print "Fehler beim SQL Statement"
			db.close()
	db.close()


def write2mongo():
	#schreiben der Testdaten
#	client = MongoClient(username=mongoDB_USER, password=mongoDB_PWD)
#	client = MongoClient('mongodb://%s:%s@%s' % (mongoDB_USER, mongoDB_PWD, mongoDB_SERVER))
	mongoconnect = MongoClient(mongoDB_SERVER, mongoDB_PORT)
	mdb = mongoconnect.TeslaLog
	mdb.authenticate(mongoDB_USER,mongoDB_PWD)
	posts = mdb.posts
	post_id = posts.insert_many(data)
	if debugmode:
		print "Eintrag in MongoDB"
		print "Rückgabewert nach Eintrag post_id:",post_id


def gettesla():
	funktionsstart = time.time()
	if debugmode:
		print "Starte Funktion gettesla"
	try:
		vehicle = connection.vehicles[0]
		result = vehicle.data_request("drive_state")
		charge = vehicle.data_request("charge_state")
		climate = vehicle.data_request("climate_state")
		vehicle_state = vehicle.data_request("vehicle_state")
	except:
		print "Probleme beim Aufbau der Verbindung zu Tesla. Programm wird abgebrochen."
		logwrite("Probleme beim Aufbau der Verbindung zu Tesla. Programm wird abgebrochen.")
		sys.exit(1)

	now = datetime.datetime.utcnow()

	tesladata = {
		'messZeit':datetime.datetime.utcnow(),
		'KMstand':'{:06.2f}'.format(vehicle_state['odometer'] * 1.609344),
		'poi':poicheck(result['latitude'],result['longitude'])
	}
	tesladata.update(vehicle)
	tesladata.update(result)
	tesladata.update(charge)
	tesladata.update(climate)
	tesladata.update(vehicle_state)

	try:
		g = geocoder.google([result['latitude'] , result['longitude']], method='reverse')
		geo = {
			'City':g.city.encode("utf-8"),
			'Street':g.street_long.encode("utf-8"),
			'Housenumber':g.housenumber
		}
		tesladata.update(geo)
	except:
		if debugmode:
			print "Fehler bei der Adressauflösung"



	km = vehicle_state['odometer'] * 1.609344
	jetzt = now.strftime("%Y-%m-%d %H:%M:%S")
	kmstand='{:06.0f}'.format(km)
	milestand='{:06.0f}'.format(vehicle_state['odometer'])
	poi = poicheck(result['latitude'],result['longitude'])
	if debugmode:
		debugout = open("debugout.txt","a")
		debugout.write(str(result['longitude']) + ";" + str(result['latitude']) + ";" + str(poi) + "\n")

	if debugmode:
		print "getteslalaufzeit: ",round(time.time()-funktionsstart,2)
	return tesladata


'''Begin des Main programms!'''
startzeit = time.time()
laufzeit = 0
data = []


poi = 0
i = 0
debugmode = True
dauerlauf = False		#Lässt die Abfrage in einer Dauerschleife laufen. Nur für Debug zwecke
simulatepoi = False		#Zum simulieren einer POI Position
writemysql = True		#Schalter zum schreiben in die mysql DB
writemongo = True		#Schalter zum schreiben in die MongoDB



if True:
	#Einlesen der Configuration
	if debugmode:
		print "einlesen der Config"
	try:
		with open ('/home/ubuntu/tesla/getTeslaconfig.json', 'r') as f:
			getTeslaconf = json.load(f)
	except:
		print "configfile kann nicht geöffnet werden"
		sys.exit(1)
	TESLA_EMAIL = getTeslaconf['TESLA_EMAIL']
	TESLA_PASSWORD = getTeslaconf['TESLA_PASSWORD']
	DB_SERVER = getTeslaconf['DB_SERVER']
	DB_USER = getTeslaconf['DB_USER']
	DB_PWD = getTeslaconf['DB_PWD']
	DB_DATENBANK = getTeslaconf['DB_DATENBANK']
	mongoDB_SERVER = getTeslaconf['mongoDB_SERVER']
	mongoDB_DATENBANK = getTeslaconf['mongoDB_DATENBANK']
	mongoDB_USER = getTeslaconf['mongoDB_USER']
	mongoDB_PWD = getTeslaconf['mongoDB_PWD']
	mongoDB_PORT = int(getTeslaconf['mongoDB_PORT'])
	MAX_RUNTIME = int(getTeslaconf['MAX_RUNTIME'])
	logfile = getTeslaconf['logfile']
	poilistfile = getTeslaconf['poilistfile']




if debugmode:
	'''Ausgeben der Programm startzeit'''
	now = datetime.datetime.now()
	jetzt = now.strftime("%Y-%m-%d %H:%M:%S")
	os.system('clear')
	print "Programmstart:",jetzt




if debugmode:
	print "Starte Verbindung zu Tesla:",round(time.time()-startzeit,2)
'''Verbindung zum TeslaServer'''
try:
	connection = teslajson.Connection(TESLA_EMAIL, TESLA_PASSWORD)
except:
	print "Fehler beim Verbindungsaufbau zu Tesla"
	sys.exit(1)

logwrite("Start")

run = True
while run:
	'''Hier läuft der Hauptaufruf'''
	tesladaten = gettesla()
	run = tesladaten['poi']
	data.append(tesladaten)

	laufzeit = time.time() - startzeit
	if debugmode and run:
		print "Reslaufzeit:", MAX_RUNTIME-laufzeit
	if laufzeit > MAX_RUNTIME:
		run = False

if False:
	with open('data.txt', 'wa') as outfile:
	    json.dump(data, outfile)
	with open('data.txt', 'a') as outfile:
		outfile.write("\n")


if writemysql:
	write2mysql()

if writemongo:
	write2mongo()

if dauerlauf:
	print "Starte Dauerschleife"
	while dauerlauf:
		gettesla()




'''Programm exit'''
if debugmode:
	now = datetime.datetime.now()
	jetzt = now.strftime("%Y-%m-%d %H:%M:%S")
	print "Programende:",jetzt
	print "Programlaufzeit",round(time.time()-startzeit,2)


sys.exit(0)









'''
Fehlercode Tabelle
1 = Fehler beim Verbindungsaufbau zu Tesla

'''
