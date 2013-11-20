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

	def busy_probes(self):
		cursor = Database.get_con().cursor()
		now = int(time.time())
		time_period = (int(now) - 7 * 24 * 60 * 60) ## 1 week ago
		q = """ SELECT probe_id FROM tbl_Scheduled,tbl_Targeted
				WHERE tbl_Scheduled.submitted > {}
				AND tbl_Scheduled.finished = "None"
				AND tbl_Scheduled.network_propety = '{}'
				AND tbl_Scheduled.measurement_id = tbl_Targeted.measurement_id
			""".format(time_period,self.propety)
		rows = cursor.execute(q).fetchall()
		if not rows:
			print("No busy probes")
			return set([])
		else:
			busy_probes = set([probe[0] for probe in rows])
			return busy_probes

	def lazy_probes(self): ## This needs testing because it relies on processed data.
		## Select probes that were targeted but did not respond in the last 7 days
		cursor = Database.get_con().cursor()
		now = int(time.time())
		time_period = (int(now) - 7 * 24 * 60 * 60) ## 1 week ago
		q = """ SELECT tbl_Targeted.probe_id FROM tbl_Targeted, tbl_Scheduled, tbl_Results
				WHERE tbl_Scheduled.submitted > {}
				AND tbl_Scheduled.network_propety = "{}"
				AND tbl_Targeted.probe_id = tbl_Results.probe_id
				AND tbl_Targeted.measurement_id = tbl_Results.measurement_id
				AND tbl_Results.good = 1
			""".format(time_period,self.propety)
		rows = cursor.execute(q).fetchall()
		if not rows:
			print("No lazy probes")
			return set([])
		else:
			lazy_probes = set([probe[0] for probe in rows])
			return lazy_probes

	def run(self):
		self.measure()
		#self.process()

class Scheduled_IPv6_Capable(Scheduled):

	def __init__(self,propety):
		Scheduled.__init__(self,propety)

	def measure(self):
		## routine here
		## (Thanks Willem Toorop)
		p = Probes.ipv6()
		p -= self.busy_probes()
		p -= self.lazy_probes()
		measurements = Atlas_Query.baseline_dns(p)
		#if not measurements:
			#print "No measurements were correctly submitted."
			#return False
		#con = Database.get_con()
		#cursor = con.cursor()
		#for measurement in measurements:
			#q = """
				#""".format()
			#try:
				#cursor.execute(q)
			#except Exception,e:
				#print (e)
				#con.close()
				#return
		#con.commit()
		#con.close()
		#print ("measurement: {} successfully updated.".format(measurement))
		#return True

	def process(self):	## rewrite in progress
		## Get all measurement ids that are less than a week old and are ready to process.
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

if __name__ == "__main__":
	## List of network propeties
	net_props = ["ipv6Capable"]
	for prop in net_props:
		if prop == "ipv6Capable":
			sch = Scheduled_IPv6_Capable(prop)
		try:
			sch.run()
		except Exception,e:
			print ("There was and error: {}".format(e))
			pass
	print("fin.")
