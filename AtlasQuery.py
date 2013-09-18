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

def main():
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
	#	Probe stuff
	#-----------------------------------------
	parser.add_argument("-prob_requested",help="|int(-)| Number of probes requested",type=int)
	parser.add_argument("-prob_type",help="|String(-)| probe type eg: WW (world wide)")
	parser.add_argument("-prob_value",help="|String(-)| value corresponding to the type")

	#-----------------------------------------
	#	Mostly ping specific
	#-----------------------------------------
	parser.add_argument("-ping_packets",help="|int(2)|Number of packets to send",type=int)
	parser.add_argument("-ping_size",help="|int(4)| Size of packet Max size: 2048",type=int)

	#-----------------------------------------
	#	TraceRoute specific
	#-----------------------------------------
	#parser.add_argument("-trc_Frag",help="|boolean(4)| Don't fragment",type=bool)
	#parser.add_argument("-trc_aris",help="|int(2)| Use Paris value from 1 to 16",type=int)

	#-----------------------------------------
	#	Begin
	#-----------------------------------------	
	args = parser.parse_args()
	opts = vars(args)
	key = getKey()
	req = getReq(key)
	#getTime() #Not yet, used to start and stop times
	skeleton = getType(args.def_type)
	Query = QBuild(opts,skeleton)
	#Query = sanitizeQuery(Query) #Not yet
	measurement = sendQ(req,Query)
	writeMeasurement(measurement)

def getKey():
	f = open('/home/'+getpass.getuser()+'/Sec/AtlasKey','r')
	key = f.readline()
	print key
	return key

def getReq(key):
	url = "https://atlas.ripe.net/api/v1/measurement/"
	request = urllib2.Request("%s?key=%s" % (url, key))
	request.add_header("Content-Type", "application/json")
	request.add_header("Accept", "application/json")
	return request

def QBuild(opts,skel):
	for x in skel['definitions']:
		for y in opts:
			if (y[0:4] == "def_"):
				if(y[4:] in x) and opts[y] != None:
					skel['definitions'][0][y[4:]] = opts[y]
	for x in skel['probes']:
		for y in opts:
			if(y[0:5] == "prob_"):
				if(y[5:] in x) and opts[y] != None:
					skel['probes'][0][y[5:]] = opts[y]
	return skel

#~ --Not yet--
#~ def sanitizeQuery(Query):
	#~ for x in Query['']:
		

def getTime():
	#Unix time stamps end = now + 5ms
	start_time = time.time()
	stop_time = time.time() + 500

def getType(t):
	if t == 'ping':
		ping = {"definitions": [{ "target":None, "description":None,"is_oneoff":None,"type":None,"af":None}],"probes": [{"requested":None,"type":None,"value":None}]}
		return ping

def sendQ(request,Query):
	try:
		conn = urllib2.urlopen(request, json.dumps(Query))
		results = json.load(conn)
		measurement = int(results["measurements"][0])
		print "Measurement ",(measurement)
		return measurement
	except:
		print 'No dice'

def writeMeasurement(measurement):
	f = open('AQR.txt','a')
	strr = str(date.today())+' '+str(datetime.datetime.now().time())[:-7]+' Measurement: '+str(measurement)+"\n"
	print strr
	f.write(strr)
	f.close()

main()

