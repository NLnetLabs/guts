#!/usr/bin/env python
#Author Warwick Louw

## Imports
import os
import sys
import time
import json
import getpass
import urllib2
import argparse
import datetime
from urllib2 import *
from urllib2 import Request
from datetime import date
##

def get_Key():
	## to add: Some kind of global config file
	directory = '/home/'+getpass.getuser()+'/Sec/AtlasKey'
	f = open(directory,'r')
	if not f:
		print "Aborting. reason: Atlas Key file "+directory+" could not be found."
		print "please specify the directory"
		sys.exit(0)
	key = f.readline()
	if not (len(key) > 35):
		print 'Aborting. reason: Key could not be found in file'
		sys.exit(0)
	return key

## Add header information
def get_Req(key):
	url = "https://atlas.ripe.net/api/v1/measurement/"
	request = urllib2.Request("%s?key=%s" % (url, key))
	request.add_header("Content-Type", "application/json")
	request.add_header("Accept", "application/json")
	return request

## Chunk a list into smaller chunks
## Code sauce: StackOverFlow
def ret_chunks(lis,num):
	for i in xrange(0, len(lis), num):
		yield lis[i:i+num]

def base_line_dns(target="2001:7b8:206:1::1",probe_list=None):
	if not probe_list:
		probe_list = probes_ipv6()
	measurements = []
	probe_string = ""
	chunks = ret_chunks(probe_list,499)
	skeleton = Atlas_Query.get_type('dns')
	skeleton = Atlas_Query.get_probes(skeleton, 1)
	defaults = {"target" : target, "type": "dns", "af":6, "description": "ipv6 baseline", "is_oneoff": True}
	defaults.update('udp_payload_size':512,"interval":None, "use_probe_resolver":False, "query_type":"AAAA"})
	defaults.update({"query_argument": "www.nlnetlabs.nl", "query_class": "IN", "is_public":True)
	for chunk in chunks:
		probe_string = ','.join(map(str, chunk))
		probe_que = [{"requested": len(chunk), "type": "probes", "value":probe_string}]
		Query = Atlas_Query.ext_Query_Builder(skeleton, [defs,probe_que])
		Query.update(Atlas_Query.get_Time(1200))
		measurement = Atlas_Query.send_Query(Query)
		if measurement:
			print "Measurement:",measurement
			measurements.append(measurement)
			Atlas_Query.write_Measurement(measurement,'dns baseline')
	if measurements:
		return measurements
	else:
		return None

## Query builder for external scripts, a skeleton(list of dicts) is parsed as well as the query(list of dicts)
## This method will match the keys with the corresponding values, query -> skeleton. to flesh out the skeleton
def ext_Query_Builder(skeleton,q):
	## Declarations
	defs 	= q[0]
	probes 	= q[1]
	##
	## Definitions:
	for definition in skeleton['definitions']:
		## Code sauce: Willem Toorop
		definition.update(dict([(k, defs.get(k, None)) for k, v in definition.items() if k in defs]))
	## Probes:
	for probe in range(len(probes)):
		for k,v in skeleton['probes'][probe].iteritems():
			val = probes[probe].get(k,None)
			if val:
				skeleton['probes'][probe][k] = val
	return skeleton

def get_Time(end_time):
	## Unix time stamps
	#start_time = time.time()
	stop_time = int(time.time() + end_time)
	return {"stop_time":stop_time}

def get_type(t):## to add: default values for measurement type, these can change based on the input
	if t == 'ping':
		ping = 	{"definitions":	[{ "target":None, "description":None, "is_oneoff":None, "type":None, "af":None, "packets":None, "size":None,"is_public":None}]}
		return ping
	elif t == 'traceroute':
		trace =	{"definitions": [{ "target":None, "description":None, "is_public":None, "paris":None, "interval":None, "firsthop":None, "is_oneoff":True, "type":None, "protocol":None, "af":None,"is_public":None}] }
		return trace
	elif t == 'dns':
		dns =	{"definitions":	[{"target": None, "query_argument": None, "query_class": None, "query_type": None, "description": None, "type": None, "af": None, "is_oneoff": None,"use_probe_resolver": None, "recursion_desired": None, 'udp_payload_size':None, "interval":None}]}
		return dns
	else:
		return None

def get_probes(skeleton,probeNum):
	skeleton['probes'] = []
	x = 1
	for x in range(probeNum):
		skeleton['probes'].append({"requested":None,"type":None,"value":None})
	return skeleton

def send_Query(Query):
	## Get key and request
	key = get_Key()
	req = get_Req(key)
	try:
		conn = urllib2.urlopen(req, json.dumps(Query))## conn missleading name, might be confused with a db connection
		 ## If no waiting time is given, queries might fail in succession of one another.
		 ## Learned the hard way...
		 ## Now for a very loose quote:
		 ## "First thou lobbest the query.Then thou must count to three.
		 ## Three shall be the number of the counting and the number of the counting shall be three.
		 ## Four shalt thou not count, neither shalt thou count two, excepting that thou then proceedeth to three.
		 ## Five is right out. Once the number three, being the number of the counting, be reached, then lobbest
		 ## thou the query in the direction of thine target, who, being naughty in my sight, shall snuff it."
		 ## -- Monty Python, "Monty Python and the Holy Grail"
		print "Waiting..."
		time.sleep(3)
		results = json.load(conn)
		measurement = int(results["measurements"][0])
		return measurement
	## code sauce: Willem Toorop
	except HTTPError, e:
		print e.read()
		return None

## Considering moving to utilies to have a global filewriter
def write_Measurement(measurement,t):
	f = open('Measurements.txt','a')
	strr = str(date.today())+'|'+str(datetime.datetime.now().time())[:-7] + '|' + t + '|' +  str( measurement ) + "\n"
	f.writelines(strr)
	f.close()

## set Vim tabs for viewing
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
