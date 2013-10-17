#!/usr/bin/env python
#Author Warwick Louw

import sqlite3
import Atlas_Query

def connect():
	conn = sqlite3.connect('Measurements.db')
	cursor = conn.cursor()
	return cursor

def chunks(lis,num):
	for i in xrange(0, len(lis), num):
		yield lis[i:i+num]

def baseline_ping_all():
	##
	all_probes = ""
	cursor = connect()
	##
	cursor.execute("select id from tbl_probes where status == 1 and address_v4 is not null;")
	probes = cursor.fetchall()
	probes = [x[0] for x in probes]
	c = chunks(probes,499)
	for chunk in c:
		all_probes = ','.join(map(str, chunk))
		print "Number of probes: ",len(chunk)
		key = Atlas_Query.get_Key()
		req = Atlas_Query.get_Req(key)
		skeleton = Atlas_Query.get_type('ping')
		skeleton = Atlas_Query.get_probes(skeleton, 1)
		defs = {"target" : '2001:7b8:40:1:16:aff:fe8f:223',"packets":1, "size" : 1500, "is_public":True, "is_oneoff":True, "type" : 'ping',"description" : "bulk ping","af" : 6}
		que_probs = [{"requested": len(chunk), "type": "probes", "value":all_probes}]
		q = [defs,que_probs]
		Query = Atlas_Query.ext_Query_Builder(skeleton, q)
		print Query
		measurement = Atlas_Query.send_Query(req, Query)
		if len( str(measurement) ) != None or len( str(measurement) ) != 0:
			Atlas_Query.write_Measurement(measurement,'ping')

if __name__ == "__main__":
	baseline_ping_all()
