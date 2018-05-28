#!/usr/bin/python
# -*- encoding: utf-8 -*-
# https://pypi.python.org/pypi/geocoder/
from pymongo import MongoClient
import os
import json
import time
import pprint


def mongoconnect():
    start = time.time()
    mongoconnect = MongoClient(mongoDB_SERVER, mongoDB_PORT)
    mdb = mongoconnect.TeslaLog
    mdb.authenticate(mongoDB_USER,mongoDB_PWD)
    posts = mdb.posts
    print "Mongoconnect:",time.time() - start
    return(posts)



debugmode = False


#Einlesen der Configuration
if debugmode:
    print "einlesen der Config"
#	try:
homedir = os.environ['HOME'] + "/.getTeslaconfig.json"
try:
    with open (homedir, 'r') as f:
        getTeslaconf = json.load(f)
except:
    print "configfile error. Expect:",homedir


DB_SERVER = getTeslaconf['DB_SERVER']
DB_USER = getTeslaconf['DB_USER']
DB_PWD = getTeslaconf['DB_PWD']
DB_DATENBANK = getTeslaconf['DB_DATENBANK']
mongoDB_SERVER = getTeslaconf['mongoDB_SERVER']
mongoDB_DATENBANK = getTeslaconf['mongoDB_DATENBANK']
mongoDB_USER = getTeslaconf['mongoDB_USER']
mongoDB_PWD = getTeslaconf['mongoDB_PWD']
mongoDB_PORT = int(getTeslaconf['mongoDB_PORT'])


posts = mongoconnect()
#mongorequest = m

#ausgabe = posts.find({"messZeit": {"$gt": "ISODate"("2018-05-27T22:00:00.000Z")}}, {"City": 1, "Street": 1, "messZeit": 1}).sort({"messZeit": -1})
ausgabe = posts.find({}, {"City": 1, "Street": 1, "messZeit": 1, "poi": 1})


print ausgabe
for i in ausgabe:
#    print ":",i['messZeit'],":",i['poi']
    print ":",i,type(i)


#ausgabe = posts.find({"City":{"$exists":False}},{"_id":1,"longitude":1,"latitude":1,"messZeit":1,"City":1}).limit(100)
#pprint (ausgabe)
