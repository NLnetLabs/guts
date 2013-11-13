## Timeless Scheduler

import time
import Probes
import Database
import itertools
import Processing
import Atals_Query
import Database_Handler as DB_Handler

class Scheduled:
	def __init__(self,prop):
		self.propety = prop
	#def submit(self,query,probes): ## This is where Atlas query comes in. Maybe improve Atlas_Query
	def busy_probes(self):
		## (returned as a list of dictionaries)
		busy_probes = DB_Handler.query_table("tbl_Measurements",'targeted',' "finished" != 1')[0]['targeted']
		if not busy_probes:
			return []
		else:
			busy_probes = str(busy_probes)[1:-1].split(",")
			return busy_probes
	def lazy_probes(self): ## Needs to be tested.
		## Select probes that were targeted but did not respond in the last 7 days
		now = int(time.time())
		time_period = (int(now) - 7 * 24 * 60 * 60) ## time period of 1 week
		rows = DB_Handler.query_table("tbl_Measurements",None,' "timestamp" < '+str(now)+' and "timestamp" > '+ str(time_period))
		targeted_probes = set([int(probe) for probes in [row['targeted_probes'][1:-1].replace(" ","").split(",") for row in rows] for probe in probes])
		if not targeted_probes:
			return [] ## cannot subtract a None from a list**
		responsive_probes = set([probes for probes in [row['succeeded_probes'] for row in rows] + [row['failed_probes'] for row in rows]])
		lazy_probes = set([ probe for probe in targeted_probes if probe not in responsive_probes])
		return lazy_probes

class Scheduled_IPv6_Capable(Scheduled):
	def __init__(self,propety):
		Scheduled.__init__(self,propety)
	def process_results(self,measurements):
		## This will assume that the field targeted has alrady been filled in.
		## With that said, let's get some results.
		for measurement in measurements:
			## Get results from the web
			response = Processing.get_response(measurement)
			## Determine which probes failed and which succeeded
			failed_probes, succeeded_probes = Processing.failed_succeeded(response)
			## Get the targeted probes from the database (returned as a list of dictionaries)
			targeted_probes = DB_Handler.query_table("tbl_Measurements",'"Targeted"',' "measurement_id" == '+str(measurement))
			incapable_probes = [probe for probe in targeted_probes.split(",") if probe not in failed_probes and probe not in succeeded_probes]
			updates = {"failed":failed_probes,"suceeded":succeeded_probes,"incapable":incapable_probes}
			ret = DB_Handler.list_update("tbl_Measurements",updates,' "measurement_id" == '+str(measurement))
			if ret:
				print ("Update measurement:"+str(measurement)+" successful")
	def run(self):
		## routine here
		p =  Probes.ipv6()
		p -= self.busy_probes()
		p -= self.lazy_probes()
		## Until I imporve Atlas_Query, this will have to do.
		Atlas_Query.baseline_dns(p)

if __name__ == "__main__":
	ipv6sched = Scheduled_IPv6_Capable("ipv6Capable")
	print ipv6sched.run()
