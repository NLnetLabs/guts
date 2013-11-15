## Timeless Scheduler

import time
import Probes
import Database
import itertools
import Processing
import Atlas_Query
import Database_Handler as DB_Handler

class Scheduled:
	def __init__(self,prop):
		self.propety = prop
	#def submit(self,query,probes): ## This is where Atlas query comes in. Maybe improve Atlas_Query
	def busy_probes(self): ## Needs to be tested.
		## (returned as a list of dictionaries)
		cursor = Database.get_con().cursor()
		rows = cursor.execute('Select "targeted_probes" from tbl_Measurements where "finished" = 0 and "network_prop" = "'+str(self.propety)+'"').fetchall()
		if not rows:
			return set([])
		else:
			busy_probes = set([int(probe) for probes in [row[0][1:-1].replace(" ","").split(",") for row in rows] for probe in probes])
			print busy_probes
			return busy_probes
	def lazy_probes(self):
		## Select probes that were targeted but did not respond in the last 7 days
		cursor = Database.get_con().cursor()
		now = int(time.time())
		time_period = (int(now) - 7 * 24 * 60 * 60) ## time period of 1 week
		rows = cursor.execute('Select "targeted_probes","succeeded_probes","failed_probes" from tbl_Measurements where "timestamp" < '+str(now)+' and "timestamp" > '+ str(time_period)).fetchall()
		targeted_probes = set([int(probe) for probes in [row[0][1:-1].replace(" ","").split(",") for row in rows] for probe in probes])
		if not targeted_probes:
			return set([]) ## cannot subtract a None from a list**
		responsive_probes = set([probes for probes in [row[1] for row in rows] + [row[2] for row in rows]])
		lazy_probes = set([ probe for probe in targeted_probes if probe not in responsive_probes])
		return lazy_probes

class Scheduled_IPv6_Capable(Scheduled):
	def __init__(self,propety):
		Scheduled.__init__(self,propety)
	def process_results(self,measurements):
		## This will assume that the field targeted has already been filled in.
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
			ret = DB_Handler.batch_update("tbl_Measurements",updates,' "measurement_id" == '+str(measurement))
			if ret:
				print ("Update measurement:"+str(measurement)+" successful")
	def run(self):
		## routine here
		## (Thanks Willem Toorop)
		p = Probes.ipv6()
		p -= self.busy_probes()
		p -= self.lazy_probes()
		## Until I imporve Atlas_Query, this will have to do.
		## Submit
		measurements = Atlas_Query.baseline_dns(p)
		## Temp store targeted probes into the database, this will be done in Atlas_Query
		if not measurements:
			print "No measurements were correctly submitted."
			return False
		con = Database.get_con()
		cursor = con.cursor()
		for measurement in measurements:
			q =  'insert into tbl_Measurements("measurement_id","network_prop","timestamp","finished","targeted_probes")' 
			q += 'Values('+measurement+','+self.propety+','+int(time.time())+','+0+','+p+')'
			try:
				cursor.execute(q)
			except Exception,e:
				print (e)
				con.close()
				return
		con.commit()
		con.close()
		print ("Updating measurement:"+str(measurement)+", successful.")
		return True

if __name__ == "__main__":
	ipv6sched = Scheduled_IPv6_Capable("ipv6Capable")
	#ipv6sched.process_results()
	ret = ipv6sched.run()
	if ret:
		print ("ipv6 Capable run was successful.")
	else:
		print ("ipv6 Capable run was unsuccessful.")
