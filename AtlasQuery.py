import optparse
from urllib2 import *
from urllib2 import Request
import urllib2
import urllib
import json
import argparse
import sys
from datetime import date
import time
import datetime
import getpass

def get_Key():
	f = open('/home/'+getpass.getuser()+'/Sec/AtlasKey','r')
	key = f.readline()
	return key

def get_Req(key):
	url = "https://atlas.ripe.net/api/v1/measurement/"
	request = urllib2.Request("%s?key=%s" % (url, key))
	request.add_header("Content-Type", "application/json")
	request.add_header("Accept", "application/json")
	return request

def Query_Builder(opts,skel):
	for x in skel['definitions']:
		for y in opts:
			if (y[:4] == "def_") or (y[:3] == opts['def_type'][:3]):
				if(y[4:] in x) and opts[y] != None:
					skel['definitions'][0][y[4:]] = opts[y]
	for x in skel['probes']:
		for y in opts:
			if(y[:5] == "prob_"):
				if(y[5:] in x) and opts[y] != None:
					skel['probes'][0][y[5:]] = opts[y]
	return skel

#~ --Not yet--
#~ def sanitizeQuery(Query):
	#~ for x in Query['']:
		

def get_Time():
	#Unix time stamps end = now + 5ms
	start_time = time.time()
	stop_time = time.time() + 500

def get_type(t):
	if t == 'ping':
		ping = {"definitions": [{ "target":None, "description":None,"is_oneoff":None,"type":None,"af":None,"packets":None,"size":None}]}
		return ping
	elif t == 'traceroute': #Not tested
		trace = {"definitions": [{ "target":None, "description":None,"packets":None,"is_public":None,"paris":None,"interval":None,"firsthop":None,"resolve_on_probe":None,"is_oneoff":True,"type":None,"protocol":None, "af":None}] }
		return trace
	elif t == 'dns':
		dns = {"definitions": [{ "target": None, "query_argument": None, "query_class": None, "query_type": None, "description": None, "type": None, "af": None, "is_oneoff": None}] }
		return dns
		
def get_probes(d):
	d['probes'] = [{"requested":None,"type":None,"value":None}]
	return d

def send_Query(request,Query):
	try:
		print request, json.dumps(Query)
		conn = urllib2.urlopen(request, json.dumps(Query))
		results = json.load(conn)
		measurement = int(results["measurements"][0])
		print "Measurement ",(measurement)
		return measurement
	except Exception,e:
		print 'No dice: ',e

def write_Measurement(measurement):
	f = open('AQR.txt','a')
	strr = str(date.today())+' '+str(datetime.datetime.now().time())[:-7]+' Measurement: '+str(measurement)+"\n"
	print strr
	f.write(strr)
	f.close()

if __name__ == "__main__":
	
	#Create argparser object
	parser = argparse.ArgumentParser()

	#-----------------------------------------
	#	General
	#-----------------------------------------
	parser.add_argument("-def_target",help="|String(-)| Target",required=True)
	parser.add_argument("-def_type",help="|String(-)| Type of action ",required=True)
	parser.add_argument("-def_is_oneoff",help="|boolean(4)| is this a Oneoff measurement",type=bool)
	parser.add_argument("-def_interval",help="|Int(3)| Interval",type=int)
	parser.add_argument("-def_af",help="|Standard|Int(1)| AF either 4 or 6",type=int)
	parser.add_argument("-def_description",help="|String(-)| Description")
	parser.add_argument("-def_resolve_on_probe",help="|boolean(4)| Resolve on probe",type=bool)
	parser.add_argument("-def_can_visualise",help="|boolean(4)| Can visualise the results",type=bool)
	parser.add_argument("-def_is_public",help="|boolean(4)| Is the measurement public or not",type=bool)

	#-----------------------------------------
	#	Probe specific
	#-----------------------------------------
	parser.add_argument("-prob_requested",help="|int(-)| Number of probes requested",type=int)
	parser.add_argument("-prob_type",help="|String(-)| probe type eg: WW (world wide)")
	parser.add_argument("-prob_value",help="|String(-)| value corresponding to the type")

	#-----------------------------------------
	#	Mostly ping specific
	#-----------------------------------------
	parser.add_argument("-pin_packets",help="|int(2)|Number of packets to send",type=int)
	parser.add_argument("-pin_size",help="|int(4)| Size of packet Max size: 2048",type=int)

	#-----------------------------------------
	#	TraceRoute specific - Not tested
	#-----------------------------------------
	parser.add_argument("-tra_dontfrag",help="|boolean(4)| Don't fragment",type=bool)
	parser.add_argument("-tra_paris",help="|int(2)| Use Paris value from 1 to 16",type=int)

	#-----------------------------------------
	#	DNS specific
	#-----------------------------------------
	parser.add_argument("-dns_use_probe_resolver",help="|boolean(4)|",type=bool)
	parser.add_argument("-dns_use_NSID",help="||",type=bool)
	parser.add_argument("-dns_query_class",help="|String(5)| Either IN or CHAOS")
	parser.add_argument("-dns_query_type",help="|String|")
	parser.add_argument("-dns_query_argument",help="|String|")	
	parser.add_argument("-dns_recursion_desired",help="|boolean|",type=bool)
	parser.add_argument("-dns_use_tcp",help="|boolean|",type=bool)
	parser.add_argument("-dns_udp_payload_size",help="|int(3)|",type=int)
	
	args = parser.parse_args()
	opts = vars(args)
	key = get_Key()
	req = get_Req(key)
	#~ getTime() #Not yet, used in start and stop times
	skeleton = get_type(args.def_type)
	skeleton = get_probes(skeleton)
	Query = Query_Builder(opts,skeleton)
	#~ Query = sanitizeQuery(Query) #Not yet
	measurement = send_Query(req,Query)
	#~ write_Measurement(measurement)
