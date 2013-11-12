## Timeless Scheduler

import Probes
import Processing
import Database_Handler as DB_Handler

class Scheduled:
	def __init__(self,prop):
		self.propety = prop
	#def submit(self,query,probes):
	def busy_probes(self):
		## (returned as a list of dictionaries)
		busy_probes = DB_Handler.query_table("tbl_Measurements",'"targeted"',' "finished" != 1')[0]['"targeted"']
		busy_probes = str(busy_probes)[3:-2].split(",")
		return busy_probes
	#def lazy_probes(self):

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
			DB_Handler.list_update("tbl_Measurements",updates)
	def run(self):
		## routine here
		p =  Probes.ipv6()
		p -= self.busy_probes()
		print p

if __name__ == "__main__":
	ipv6sched = Scheduled_IPv6_Capable("ipv6Capable")
	print ipv6sched.run()
