#!/usr/bin/env python
#Author Warwick Louw

import json
import urllib2
import getpass
import Atlas_Query
##
from Atlas_Query import *
from urllib2 import Request

## Return the id of all connected ipv6 probes
def probes_ipv6():
	probes = []
	req = urllib2.urlopen("https://atlas.ripe.net/api/v1/probe/?prefix_v6=::/0&limit=0").read()
	req = json.loads(req)
	## AWW YISS!! list comprehension xD
	probes = [probe['id'] for probe in [probes for probes in req['objects'] if probes['status'] == 1]]
	return probes

## Chunk a list into smaller chunks
## Code sauce: StackOverFlow
def ret_chunks(lis,num):
	for i in xrange(0, len(lis), num):
		yield lis[i:i+num]

## Return results from the internet
def get_results(measurement):
	try:
		response = urllib2.urlopen("https://atlas.ripe.net/api/v1/measurement/"+str(measurement)+"/result/")
		response = json.load(response)
		return response
	## Code sauce: Willem Toorop
	## Sauce is used in a joking term, as used on the internet.
	except urllib2.HTTPError, e:
		print (e.read())
		return None

## Returns the measurement info of the given measurement id
def measurement_info(measurement):
	try:
		response = urllib2.urlopen("https://atlas.ripe.net/api/v1/measurement/"+str(measurement))
		response = json.load(response)
		return response
	## Code sauce: Willem Toorop
	except urllib2.HTTPError, e:
		print (e.read())
		return None

## Given the results of a measurement it will return the id of all probes which failed at least once
def which_failed(response,verbose=0):
	if len(response) >= 1:
		## Build a list of all that fail and all that succeed, then combine to see which never succeeded
		fail = set()
		succeed = set()
		## In the case of ping, if the avg has a value of -1 then the Host is unreachable(request timed out)
		fail.update([result['prb_id'] for result in [results for results in response if 'avg' in results and results['avg'] is -1]])
		succeed.update([result['prb_id'] for result in [results for results in response if 'avg' in results and results['avg'] is not -1]])
		## In the case of an error, in DNS the 'error' key will be in the main dict
		fail.update([result['prb_id'] for result in [results for results in response if 'error' in results]])
		succeed.update([result['prb_id'] for result in [results for results in response if 'error' not in results]])
		## Look for errors in the result sets contained in the results of each line
		## Pings and traceroutes will contain errors in their result sets but DNS will not.
		## Code sauce: Willem Toorop
		fail.update([results['prb_id'] for results in response if 'result' in result for result in results['result'] if 'error' in result])
		succeed.update([results['prb_id'] for results in response if 'result' in result for result in results['result'] if 'error' not in result])
		fail.update([probe for probe in fail if probe not in succeed])
		if verbose:
			## print the results
			print "Of a total:",(len(succeed)+len(fail)),"probes"
			print "Had :",len(fail),"Failure(s) and :",(len(succeed)),"Successful results"
		## return the list containing the id of all probes which failed at least once.
		return fail
	else:
		print "There are no results in that response. It could be that the measurement has not began"
		return None

## Given a result set find the probes which succeeded at least once
#def which_succeeded_once(result_set):

## Will return a list of a all probes involved in a measurement
def id_of_all_probes(response):
	return set([results['prb_id'] for results in response])

## Do the baseline measurement for probes provided or if none are provided, use all connected ipv6 probes
def do_ipv6_baseline(probe_list=None):
	measurements = []
	if not probe_list:
		probe_list = probes_ipv6()
	probe_string = ""
	chunks = ret_chunks(probe_list,499)
	for chunk in chunks:
		probe_string = ','.join(map(str, chunk))
		## ~ Consider moving to Atals_Query
		key = Atlas_Query.get_Key()
		req = Atlas_Query.get_Req(key)
		## ~
		skeleton = Atlas_Query.get_type('dns')
		skeleton = Atlas_Query.get_probes(skeleton, 1)
		defs = {"target" : "2001:7b8:206:1::1", "type": "dns", "is_public":True, "description": "ipv6 baseline", "is_oneoff": True, "query_argument": "www.nlnetlabs.nl", "query_class": "IN", "query_type":"AAAA","af":6,'udp_payload_size':256}
		probe_que = [{"requested": len(chunk), "type": "probes", "value":probe_list}]
		Query = Atlas_Query.ext_Query_Builder(skeleton, [defs,probe_que])
		measurement = Atlas_Query.send_Query(req, Query)
		if measurement:
			print "Measurement:",measurement
			measurements.append(measurement)
			Atlas_Query.write_Measurement(measurement,'dns baseline')
	if measurements:
		return measurements
	else:
		return None

if __name__ == "__main__":
	#measurements = [1034087,1034088,1034089]
	measurements = [1031937,1031969]
	for measurement in measurements:
		mes_info = measurement_info(measurement)
		response = get_results(measurement)
		if response:
			num_probes = len(id_of_all_probes(response))
			fail_list = which_failed(response,1)
