#!/usr/bin/env python
#Author: Warwick Louw

## All things probe related: Fetching probes, updating probe details etc..

import os
import json
import urllib2
import sqlite3
import Database

## Return a list containing the id of all ipv6 probes with status 1
def ret_ipv6_probes():
	try:
		response = urllib2.urlopen("https://atlas.ripe.net/api/v1/probe/?prefix_v6=::/0&limit=0").read()
		response = json.loads(response)
		probes = [probe['id'] for probe in [probes for probes in response['objects'] if probes['status'] == 1]]
		return probes
	except Exception e:
		print ("There was and error:"+e)
		## Use: if None, there was an error
		return None

#def Update_probes_status():
	## **First insert probes that are not in the database then update.
	## Build list of probes from the internet.
	## Build list of probes in our table
	## Connect to the database
	## Connection is required to insert into DB and read the table schema.
	#con = Database.ret_con()
	## Get a cursor object from the connection
	#cursor = con.cursor()
	## Get the schema of the databse
	#tbl_schem = mes_write.get_tbl_schema("tbl_probes")
	##Get all probes
	#req = urllib2.urlopen("https://atlas.ripe.net/api/v1/probe/?limit=0").read()
	#req = json.loads(req)
	#for obj in req['objects']:
		## Update status for each probe
		
		#db_spec = tbl_schem
		#for obj_k,obj_v in obj.iteritems():
			#for db_k,db_v in db_spec.iteritems():
				#if obj['status'] == 1 or 2:
					#if obj_k == db_k and obj_v != 'null':
						#db_spec[db_k] = obj_v
		#try:
			#insert_into_db("tbl_probes",db_spec)
		#except sqlite3.DatabaseError, e:
			##Do not add anything more if an error is encountered
			#conn.rollback()
			#print e
			#pass
		#except Exception, e:
			#print e
	#con.commit()
	#con.close()

## ~ Consider removal and use DB_writer ~
## DB insert method
def insert_into_db(tbl,tbl_schem):
	keys = ""
	vals = ""
	for k,v in tbl_schem.iteritems():
		keys += "'"+(k)+"',"
		vals += "'"+str(v)+"',"
	keys = keys[:-1]
	vals = vals[:-1]
	cursor.execute("insert into "+tbl+"("+(keys)+") values("+vals+")")
