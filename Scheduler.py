#!/usr/bin/env python
## Timeless Scheduler

import dpkt
import base64
import Database
from atlas import *

class Scheduler:
	def __init__(self):
		pass

	def chunker(self,lis,num): ## A long list and the size of the chunks
		for i in xrange(0, len(lis), num):
			yield lis[i:i+num]

	def probes(self): ## for now, no args.
		p = set([probe['id'] for probe in atlas.probe(prefix_v6 = '::/0', limit = 0) if probe['status'] == 1])
		return p

	def busy_probes(self):
		cursor = Database.get_con().cursor()
		time_period = (time() - 7 * 24 * 60 * 60) ## 1 week ago
		q = """ SELECT probe_id FROM Measurements,Targeted
				WHERE Measurements.submitted > {week}
				AND Measurements.finished = 0
				AND Measurements.network_propety = '{prop}'
				AND Measurements.measurement_id = Targeted.measurement_id
			""".format(week = int(time_period),prop = self.get_propety_name())
		rows = cursor.execute(q).fetchall()

		if not rows:
			print ("No busy probes")
			return set([])

		busy_probes = set([probe[0] for probe in rows])
		return busy_probes

	def lazy_probes(self): ## This needs testing because it relies on processed data.
		## Select probes that were targeted but did not respond in the last 7 days
		cursor = Database.get_con().cursor()
		time_period = (time() - 7 * 24 * 60 * 60) ## 1 week ago
		appearance = 5 ## The number of times a probe should appear in Targeted and have no result to be a lazy probe
		q = """ SELECT Targeted.probe_id FROM Targeted, Measurements, Results
				WHERE Measurements.submitted > {week}
				AND Measurements.network_propety = '{prop}'
				AND Measurements.measurement_id = Results.measurement_id
				AND Targeted.probe_id NOT IN
				(SELECT Results.probe_id from Results, Measurements
				WHERE Measurements.submitted > {week}
				AND Measurements.measurement_id = Results.measurement_id
				AND Measurements.network_propety = '{prop}')
				GROUP BY Targeted.probe_id
				HAVING COUNT(Targeted.probe_id) > {appr}
			""".format(week = int(time_period), prop = self.get_propety_name(), appr = appearance)
		rows = cursor.execute(q).fetchall()
		if not rows:
			print("No lazy probes")
			return set([])

		lazy_probes = set([probe[0] for probe in rows])
		return lazy_probes

	def done_probes(self):
		cursor = Database.get_con().cursor()
		time_period = (time() - 7 * 24 * 60 * 60) ## 1 week ago
		appearance = 5 ## The number of times a probe should appear in results for the last week
		q = """ SELECT Results.probe_id FROM Results, Measurements
				WHERE Measurements.submitted > {week}
				AND Measurements.measurement_id = Results.measurement_id
				AND Measurements.network_propety = '{prop}'
				GROUP BY Results.probe_id
				HAVING COUNT(*) > {appr}
			""".format(week = int(time_period), prop = self.get_propety_name(), appr = appearance)
		rows = cursor.execute(q).fetchall()
		if not rows:
			print("No finished probes")
			return set([])

		done_probes = set([probe[0] for probe in rows])
		return done_probes

	def run(self):
		#self.measure()
		self.process()

class Scheduler_IPv6_Capable(Scheduler):

	def __init__(self,propety):
		self.propety_name = propety
		Scheduler.__init__(self)

	def get_propety_name(self):
		return self.propety_name

	def measure(self):
		## Get database connection
		con = Database.get_con()
		cursor = con.cursor()
		p = self.probes()
		p -= self.busy_probes()
		p -= self.lazy_probes()
		p -= self.done_probes()
		defs = dns6('nl','AAAA','2001:7b8:40:1:d0e1::1')
		probes = list(p) ## Convert to list to be used in atlas
		## Need to handle chunks here.
		for chunk in self.chunker(probes,500):
			response = atlas.create(defs,chunk)
			measurements = response['measurements']
			if not measurements:
				print("No measurements were correctly submitted.")
				return
			for measurement in measurements:
				q = """ INSERT INTO Measurements(measurement_id,network_propety,submitted,finished)
						VALUES({msm},"{prop}",{now},0)
					""".format(msm = measurement,prop = self.get_propety_name(), now = int(time()))
				try:
					cursor.execute(q)
					print ("measurement: {} successfully inserted.".format(measurement))
					for probe in chunk:
						pq = "INSERT INTO Targeted(measurement_id,probe_id) VALUES({},{})".format(measurement,probe)
						try:
							cursor.execute(pq)
						except Exception,e:
							print("Error inserting probe: {}, reason: {}.".format(probe,e))
				except Exception,e:
					print ("Error inserting measurement: {}, reason: {}".format(measurement,e))
		con.commit()
		con.close()
		return

	def process(self):
		## Get all measurement ids that are less than a week old and are ready to process.
		con = Database.get_con()
		cursor = con.cursor()
		time_period = (time() - 7 * 24 * 60 * 60) ## 1 week ago
		q = """ SELECT measurement_id FROM Measurements
				WHERE submitted > {week}
				AND network_propety = '{prop}'
				AND finished = 0
			""".format(week = int(time_period), prop = self.get_propety_name())
		rows = cursor.execute(q).fetchall()
		if not rows:
			print("There are no measurements to process")
			return
		measurements = set([measurement[0] for measurement in rows])
		## Determine (below) which measurement from the list of measurements have stopped and are ready to process.
		measurements = [x for y in [[measurement['msm_id'] for measurement in atlas.measurement(measurement) if measurement['status']['name'] == 'Stopped'] for measurement in measurements] for x in y]
		for measurement in measurements:
			response = [x for y in atlas.result(measurement) for x in y]
			measurement_header = [x for y in atlas.measurement(measurement) for x in y]
			for result in response:
				probe_id = result['prb_id']
				## This only works for DNS.
				good = 0 if result.get('error',None) else 1
				## When checking the answer from DNS we exclude those that do not have an answer.
				if good:
					dns = dpkt.dns.DNS(base64.b64decode(result['result']['abuf']))
					good = 0 if (len(dns.an) == 0) else 1
				q = 'insert into Results(measurement_id,probe_id,good,json) values({},{},{},"{}")'.format(measurement,probe_id,good,result)
				cursor.execute(q)
			q = 'UPDATE Measurements SET finished = 1,json="{}" WHERE measurement_id = {}'.format(measurement_header,measurement)
			cursor.execute(q)
		con.commit()
		print("Results processed")
		return

if __name__ == "__main__":
	## List of network propeties
	## testing purposes
	sch = Scheduler_IPv6_Capable("ipv6Capable")
	sch.run()
	#net_props = ["ipv6Capable"]
	#for prop in net_props:
		#if prop == "ipv6Capable":
			#sch = Scheduler_IPv6_Capable(prop)
		#try:
			#sch.run()
		#except Exception,e:
			#print ("There was and error: {}".format(e))
			#pass
	#print("fin.")
