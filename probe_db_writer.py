#!/usr/bin/env python
import os
import json
import urllib2
import sqlite3
import db_creator
import Measurement_db_writer as mes_write

def insert_into_db(tbl,tbl_schem):
	keys = ""
	vals = ""
	for k,v in tbl_schem.iteritems():
		keys += "'"+(k)+"',"
		vals += "'"+str(v)+"',"
	keys = keys[:-1]
	vals = vals[:-1]
	cursor.execute("insert into "+tbl+"("+(keys)+") values("+vals+")")

if __name__ == "__main__":
	conn = sqlite3.connect('Measurements.db')
	cursor = conn.cursor()
	tbl_schem = mes_write.get_tbl_schema("tbl_probes")
	##Get all probes
	req = urllib2.urlopen("https://atlas.ripe.net/api/v1/probe/?limit=0").read()
	req = json.loads(req)
	for obj in req['objects']:
		db_spec = tbl_schem
		for obj_k,obj_v in obj.iteritems():
			for db_k,db_v in db_spec.iteritems():
				if obj['status'] == 1 or 2:
					if obj_k == db_k and obj_v != 'null':
						db_spec[db_k] = obj_v
		try:
			insert_into_db("tbl_probes",db_spec)
		except sqlite3.DatabaseError, e:
			##Do not add anything more if an error is encountered
			conn.rollback()
			print e
			pass
		except Exception, e:
			print e
	conn.commit()
	conn.close()
	
