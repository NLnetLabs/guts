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

def get_Req(key):
	url = "https://atlas.ripe.net/api/v1/measurement/"
	request = urllib2.Request("%s?key=%s" % (url, key))
	request.add_header("Content-Type", "application/json")
	request.add_header("Accept", "application/json")
	return request

def main_Query_Builder(opts,skel):
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

## Query builder for external scripts, a skeleton(list of dicts) is parsed as well as the query(list of dicts)
## This method will match the keys with the corresponding values, query -> skeleton. to flesh out the skeleton
def ext_Query_Builder(skeleton,q):
	## Declarations
	defs 	= q[0]
	probes 	= q[1]
	##
	## Definitions:
	for definition in range(len(skeleton['definitions'])):
		for k,v in skeleton['definitions'][definition].iteritems():
			val = defs.get(k,None)
			if val or val == 0:
				skeleton['definitions'][definition][k] = val
	## Probes:
	for probe in range(len(probes)):
		for k,v in skeleton['probes'][probe].iteritems():
			val = probes[probe].get(k,None)
			if val:
				skeleton['probes'][probe][k] = val
	return skeleton

def get_Time():
	## Unix time stamps end = now + 5mins
	start_time = time.time()
	stop_time = time.time() + 300

def get_type(t):
	if t == 'ping':
		ping = {"definitions": [{ "target":None, "description":None, "is_oneoff":None, "type":None, "af":None, "packets":None, "size":None,"is_public":None}]}
		return ping
	elif t == 'traceroute':
		trace = {"definitions": [{ "target":None, "description":None, "is_public":None, "paris":None, "interval":None, "firsthop":None, "is_oneoff":True, "type":None, "protocol":None, "af":None,"is_public":None}] }
		return trace
	elif t == 'dns':
		dns = {"definitions": [{ "target": None, "query_argument": None, "query_class": None, "query_type": None, "description": None, "type": None, "af": None, "is_oneoff": None}] }
		return dns

def get_probes(skeleton,probeNum):
	skeleton['probes'] = []
	x = 1
	for x in range(probeNum):
		skeleton['probes'].append({"requested":None,"type":None,"value":None})
	return skeleton

def send_Query(request,Query):
	try:
		conn = urllib2.urlopen(request, json.dumps(Query))
		time.sleep(5) ## Waiting time
		results = json.load(conn)
		measurement = int(results["measurements"][0])
		print "Measurement ",(measurement)
		return measurement
	except HTTPError, e: # <- code source: Willem Toorop
		print e.read()

def write_Measurement(measurement,t):
	f = open('Measurements.txt','a')
	strr = str(date.today())+'|'+str(datetime.datetime.now().time())[:-7] + '|' + t + '|' +  str( measurement ) + "\n"
	f.writelines(strr)
	f.close()

## set Vim tabs for viewing
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
