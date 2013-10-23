#!/usr/bin/env python
#Author Warwick Louw

import json
import urllib2
from urllib2 import Request

def get_results(measurement):
	try:
		response = urllib2.urlopen("https://atlas.ripe.net/api/v1/measurement/"+str(measurement)+"/result/")
		response = json.load(response)
		return response
	## Code sauce: Willem Toorop
	except urllib2.HTTPError, e:
		print (e.read())

## Given the results of a measurement it will return the id of all probes which failed
def which_failed(response):
	if len(response) >= 1:
		fails = set()
		## In the case of ping, if the avg has a value of -1 then the Host is unreachable(request timed out)
		fails.update([result['prb_id'] for result in [results for results in response if 'avg' in results and results['avg'] == -1]])
		## In the case of an error, in DNS the 'error' key will be in the main dict
		fails.update([result['prb_id'] for result in [results for results in response if 'error' in results]])
		## Look for errors in the result sets contained in the results of each line
		## Pings and traceroutes will contain errors in their result sets
		## Code sauce: Willem Toorop
		fails.update([results['prb_id'] for results in response for result in results['result'] if 'error' in result])
		return fails
	else:
		print "There are no results in that response. There could be a fault in the query"
		print "Are you sure the measurement has stopped? If not please try again later"
		return None

## Determine which probes have successfully completed the baseline measurement and are on the IPv6 network
#def all_ipv6_probes_capable():

if __name__ == "__main__":
	measurements = [1033447,1033784,1033781,1033785]
	for measurement in measurements:
		response = get_results(measurement)
		fail_list = which_failed(response)
		print "Measurement Number:",measurement,"of the total:",len(response)
		print "Had :",len(fail_list),"Failure(s) and :",(len(response) - len(fail_list)),"Successful results"
	## Answer later
	#all_ipv6_probes_capable()
