#!/usr/bin/env python
#Author Warwick Louw

#|===========================================
#|Notes:
#|	Still to come (for now):
#|		*include measurement_reader
#|		*scheduler
#|		*some form of web display
#|		*read my own measuremets(not public)
#|===========================================

from random import randint
from datetime import date
import Atlas_Query
import sys
import time
import datetime

def ret_defs(t):
	if t == "ping4":
		defs = {"target" : '213.136.31.100',"packets":3, "size" : 1280, "is_public":True, "is_oneoff":True, "type" : 'ping',"description" : t,"af" : 4}
		return defs
	elif t == "ping6":
		defs = {"target" : '2a02:c0:203:0:ffff:0:57ee:30f3',"packets":3, "is_public":True, "is_oneoff":True, "size" : 1400, "type" : 'ping',"description" : "Ping6","af" : 6}
		return defs
	elif t == "dns4":
		defs = {"target" : "192.134.1.25", "type": "dns", "description": t, "is_public":True, "is_oneoff": True, "query_argument": "nl", "query_class": "IN", "query_type":"SOA","af":4}
		return defs
	elif t == "dns6":
		defs = {"target" : "2001:67c:2b0:3d:92b1:1cff:fe1a:9145", "type": "dns", "is_public":True, "description": t, "is_oneoff": True, "query_argument": "nl", "query_class": "IN", "query_type":"SOA","af":6}
		return defs
	elif t == 'traceroute4':
		defs = {"target" : '213.136.31.100',"is_oneoff":True, "is_public":True,"type" : 'traceroute',"description" : t,"af" : 4,"protocol":"ICMP","size":1200,"paris":5}
		return defs
	elif t == 'traceroute6':
		defs = {"target" : '2001:1488:ffff::20',"is_oneoff":True, "is_public":True, "type" : 'traceroute',"description" : t,"af" : 6,"protocol":"ICMP","size":1400,"paris":5}
		return defs
	else:
		sys.exit(0)
	
def ret_probes( num ):
	pro = []
	i = 1
	c = [ "GB", "DE", "FR", 'CA', 'US' ] #Some countries that have probes
	for i in range( num ):
		pro.append( {"requested": randint( 1, 10 ), "type": "country", "value": str(c[ randint( 0, 4 ) ]) }) #Random number of probes (1-10), from a random country (0-4)
	return pro

def write_Measurement(measurement,t):
	f = open('Measurements.txt','a')
	strr = str(date.today())+'|'+str(datetime.datetime.now().time())[:-7] + '|' + t + '|' +  str( measurement ) + "\n"
	f.writelines(strr)
	f.close()

if __name__ == "__main__":               
	#tests = [ 'ping4', 'ping6', 'dns4', 'dns6', 'traceroute4', 'traceroute6' ]
	tests = ['ping4']
	probeNum = randint( 1, 3 ) #Random number of probe groups
	for test in tests:
		key = Atlas_Query.get_Key()
		req = Atlas_Query.get_Req(key)
		skeleton = Atlas_Query.get_type(test[:-1])
		skeleton = Atlas_Query.get_probes(skeleton, probeNum)
		defs = ret_defs(test)
		probes = ret_probes(probeNum)
		q = [defs , probes]
		Query = Atlas_Query.ext_Query_Builder(skeleton, q)
		print Query
		try:
			measurement = Atlas_Query.send_Query(req, Query)
			if len(str(measurement)) != None:
				write_Measurement(measurement,test)
				print measurement
		except:
			print "There was an error"
			pass

## set Vim tabs for viewing
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
