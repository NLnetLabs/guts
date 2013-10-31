#!/usr/bin/env python
#Author Warwick Louw

import json
import urllib2
import getpass
import Atlas_Query
##
from Atlas_Query import *
from urllib2 import Request

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

## Given the results of a measurement it will return the id of all probes which succeed
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
		return fail,succeeded
	else:
		print "There are no results in that response. It could be that the measurement has not began"
		return None

## ~ Consider moving to Probes ~
## Will return a list of a all probes involved in a measurement
def id_of_all_probes(response):
	return set([results['prb_id'] for results in response])

if __name__ == "__main__":
	#measurements = Atals_Query.baseline_dns()
	#print measurements
	mes_in = measurement_info(1034089)
	print mes_in['stop_time']
	#measurements = [1034087,1034088,1034089]
	#measurements = [1031937,1031969]
	#for measurement in measurements:
		#mes_info = measurement_info(measurement)
		#response = get_results(measurement)
		#if response:
			#num_probes = len(id_of_all_probes(response))
			#fail_list = which_failed(response,1)
