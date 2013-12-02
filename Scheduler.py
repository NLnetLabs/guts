#!/usr/bin/env python
## Timeless Scheduler

import Database
from atlas import *

class Scheduler:

	def __init__(self):
		pass

	def chunker(self,lis,chunk_size):
		for i in xrange(0, len(lis), chunk_size):
			yield lis[i:i+num]

	def ipv6_probes(self): ## for now, no args.
		p = set([probe['id'] for probe in atlas.probe(prefix_v6 = '::/0', limit = 0) if probe['status'] == 1])
		return p

	def ipv4_probes(self): ## for now, no args.
		p = set([probe['id'] for probe in atlas.probe(prefix_v4 = '0.0.0.0/0', limit = 0) if probe['status'] == 1])
		return p

	## These are probes that are busy being measured for the current network propety.
	def busy_probes(self):
		cursor = Database.get_con().cursor()
		time_period = (time() - 7 * 24 * 60 * 60) ## 1 week ago
		q = """ SELECT probe_id FROM Measurements,Targeted
				WHERE Measurements.submitted > {week}
				AND Measurements.finished = 0
				AND Measurements.network_propety = '{prop}'
				AND Measurements.measurement_id = Targeted.measurement_id
			""".format(week = int(time_period),prop = self.get_propety_name())
		print q
		rows = cursor.execute(q).fetchall()

		if rows:
			busy_probes = set([probe[0] for probe in rows])
		else:
			busy_probes = set([])

		#print ("{}: busy_probes: {}".format(self.get_propety_name(),len(busy_probes)))
		return busy_probes

	## These are probes that were targeted but did not respond in the last 7 days.
	def lazy_probes(self): ## This needs testing because it relies on processed data.
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

		if rows:
			lazy_probes = set([probe[0] for probe in rows])
		else:
			lazy_probes = set([])

		#print ("{}: lazy_probes: {}".format(self.get_propety_name(),len(lazy_probes)))
		return lazy_probes

	## These are probes that have been targeted and have a certain number of results.(For the most part they co-operated.)
	## This does not mean these probes have good or bad results.
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

		if rows:
			done_probes = set([probe[0] for probe in rows])
		else:
			done_probes = set([])

		#print ("{}: done_probes: {}".format(self.get_propety_name(),len(done_probes)))
		return done_probes

	## These are probes which have executed the measurements but have failed the entire week.
	## These are probes which cannot perform this measurement.
	def incapable_probes(self): ## This needs testing.
		cursor = Database.get_con().cursor()
		time_period = (time() - 7 * 24 * 60 * 60) ## 1 week ago
		q = """
			SELECT probe_id FROM Targeted, Measurements, Results
			WHERE Measurements.submitted > {week}
			AND Measurements.measurement_id = Results.measurement_id
			AND Measurements.network_propety = '{prop}'
			AND (SELECT COUNT(good) from results,Measurements,Targeted
				WHERE Measurements.submitted > {week}
				AND Measurements.measurement_id = Results.measurement_id
				AND Measurements.network_propety = '{prop}'
				AND Results.good = 0
				GROUP BY Targeted.probe_id)
				=
				(SELECT COUNT(good) from results,Measurements,Targeted
				WHERE Measurements.submitted > {week}
				AND Measurements.measurement_id = Results.measurement_id
				AND Measurements.network_propety = '{prop}'
				GROUP BY Targeted.probe_id)
			""".format(week = int(time_period), prop = self.get_propety_name())
		rows = cursor.execute(q).fetchall()

		if rows:
			incapable_probes = set([probe[0] for probe in rows])
		else:
			incapable_probes = set([])

		return incapable_probes

	def print_status(self):
		print ("{}: busy_probes: {}".format(self.get_propety_name(),len(self.busy_probes())))
		print ("{}: lazy_probes: {}".format(self.get_propety_name(),len(self.lazy_probes())))
		print ("{}: done_probes: {}".format(self.get_propety_name(),len(self.done_probes())))

	def return_status(self):## return a list containing the status of the network propeties
		stat = []
		stat.append("{}: busy_probes: {}".format(self.get_propety_name(),len(self.busy_probes())))
		stat.append("{}: lazy_probes: {}".format(self.get_propety_name(),len(self.lazy_probes())))
		stat.append("{}: done_probes: {}".format(self.get_propety_name(),len(self.done_probes())))
		return stat

	def run(self):
		self.measure()
		self.process()
		self.print_status()

class Scheduler_IPv6_dns_Capable(Scheduler):

	def __init__(self,propety):
		self.propety_name = propety
		Scheduler.__init__(self)

	def get_propety_name(self):
		return self.propety_name

	def measure(self):
		con = Database.get_con()
		cursor = con.cursor()
		p  = self.ipv6_probes()
		p -= self.busy_probes()
		p -= self.lazy_probes()
		p -= self.done_probes()
		defs = dns6('nl','AAAA','2001:7b8:40:1:d0e1::1')
		probes = list(p)
		for chunk in self.chunker(probes,500):
			try: ## If creating the measurement fails then nothing more must be done for this chunk.
				measurements = atlas.create(defs,probes)
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
							except Exception as e:
								print("Error inserting probe: {}, reason: {}.".format(probe,e))
					except Exception as e:
						print ("Error inserting measurement: {}, reason: {}".format(measurement,e))
				con.commit()
			except Exception as e:
				print ("There was an error submitting that query, reason: {}".format(e))
				pass
		con.close()
		return

	## Get all measurement ids that are less than a week old and are ready to process.
	def process(self):
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
		## Determine (below) which measurement from the list of measurements have stopped and are ready for processing.
		measurements = [x for y in [[measurement['msm_id'] for measurement in atlas.measurement(measurement) if measurement['status']['name'] == 'Stopped'] for measurement in measurements] for x in y]
		for measurement in measurements:
			try:
				response = [x for y in atlas.result(measurement) for x in y]
				measurement_header = [x for y in atlas.measurement(measurement) for x in y]
				for result in response:
					probe_id = result['prb_id']
					## This only works for DNS.
					good = 0 if result.get('error',None) else 1
					## When checking the answer from DNS we exclude those that do not have an answer.
					if good:
						## Since the query has returned a result we will now check that result.
						good = 0 if (result['ANCOUNT'] == 0) else 1
					q = 'insert into Results(measurement_id,probe_id,good,json) values({},{},{},"{}")'.format(measurement,probe_id,good,result)
					cursor.execute(q)
				q = 'UPDATE Measurements SET finished = 1,json="{}" WHERE measurement_id = {}'.format(measurement_header,measurement)
				cursor.execute(q)
				print("Results processed")
				con.commit()
			except Exception as e:
				print("There was an error processing measurement: {}, reason: {}".format(measurement,e))
				pass
		con.close()
		return

class Scheduler_IPv6_ping_Capable(Scheduler):

	def __init__(self,propety):
		self.propety_name = propety
		Scheduler.__init__(self)

	def get_propety_name(self):
		return self.propety_name

	def measure(self):
		con = Database.get_con()
		cursor = con.cursor()
		p  = self.ipv6_probes()
		p -= self.busy_probes()
		p -= self.lazy_probes()
		p -= self.done_probes()
		defs = ping6('2001:7b8:40:1:d0e1::1')
		probes = list(p) ## Convert to list to be used in atlas
		## Need to handle chunks here.
		for chunk in self.chunker(probes,500):
			try: ## If creating the measurement fails then nothing more must be done for this chunk.
				measurements = atlas.create(defs,probes)
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
							except Exception as e:
								print("Error inserting probe: {}, reason: {}.".format(probe,e))
					except Exception as e:
						print ("Error inserting measurement: {}, reason: {}".format(measurement,e))
				con.commit()
			except Exception as e:
				print ("There was an error submitting that query, reason: {}".format(e))
				pass
		con.close()
		return

	## Get all measurement ids that are less than a week old and are ready to process.
	def process(self):
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
		## Determine (below) which measurement from the list of measurements have stopped and are ready for processing.
		measurements = [x for y in [[measurement['msm_id'] for measurement in atlas.measurement(measurement) if measurement['status']['name'] == 'Stopped'] for measurement in measurements] for x in y]
		for measurement in measurements:
			try:
				response = [x for y in atlas.result(measurement) for x in y]
				measurement_header = [x for y in atlas.measurement(measurement) for x in y]
				for result in response:
					probe_id = result['prb_id']
					## Looking for "error" in result.(This will also work for traceroute)
					good = 0 if result['result'].get('error',None) else 1
					q = 'insert into Results(measurement_id,probe_id,good,json) values({},{},{},"{}")'.format(measurement,probe_id,good,result)
					cursor.execute(q)
				q = 'UPDATE Measurements SET finished = 1,json="{}" WHERE measurement_id = {}'.format(measurement_header,measurement)
				cursor.execute(q)
				print("Results processed")
				con.commit()
			except Exception as e:
				print("There was an error processing measurement: {}, reason: {}".format(measurement,e))
				pass
		con.close()
		return

class Scheduler_IPv4_ping_Capable(Scheduler):

	def __init__(self,propety):
		self.propety_name = propety
		Scheduler.__init__(self)

	def get_propety_name(self):
		return self.propety_name

	def measure(self):
		p  = self.ipv4_probes()
		p -= busy_probes()
		p -= lazy_probes()
		p -= done_probes()
		defs = ping4("213.136.31.100")
		probes = list(p)
		try:
			measurements = atlas.create(defs,probes)
			for chunk in chunker(probes,500):
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
							except Exception as e:
								print("Error inserting probe: {}, reason: {}.".format(probe,e))
					except Exception as e:
						print ("Error inserting measurement: {}, reason: {}".format(measurement,e))
				con.commit()
		except Exception as e:
			print ("There was an error submitting that query, reason: {}".format(e))
			pass
		con.close()
		return

	def process(self):
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
		## Determine (below) which measurement from the list of measurements have stopped and are ready for processing.
		measurements = [x for y in [[measurement['msm_id'] for measurement in atlas.measurement(measurement) if measurement['status']['name'] == 'Stopped'] for measurement in measurements] for x in y]
		for measurement in measurements:
			try:
				response = [x for y in atlas.result(measurement) for x in y]
				measurement_header = [x for y in atlas.measurement(measurement) for x in y]
				for result in response:
					probe_id = result['prb_id']
					## Looking for "error" in result.(This will also work for traceroute)
					good = 0 if result['result'].get('error',None) else 1
					q = 'insert into Results(measurement_id,probe_id,good,json) values({},{},{},"{}")'.format(measurement,probe_id,good,result)
					cursor.execute(q)
				q = 'UPDATE Measurements SET finished = 1,json="{}" WHERE measurement_id = {}'.format(measurement_header,measurement)
				cursor.execute(q)
				print("Results processed")
				con.commit()
			except Exception as e:
				print("There was an error processing measurement: {}, reason: {}".format(measurement,e))
				pass
		con.close()
		return

class Scheduler_IPv6_Capable_Resolver(Scheduler):

	def __init__(self,propety):
		self.propety_name = propety
		Scheduler.__init__(self)

	def get_propety_name(self):
		return self.propety_name

	def measure(self):
		con = Database.get_con()
		cursor = con.cursor()
		## Get all probes which have succesfully completed ipv6 dns Capability.
		## I.E. all done IPv6 Capable probes.
		p = Scheduler_IPv6_dns_Capable("IPv6_dns_Capable").done_probes()
		probes = list(p)
		defs = dns6('ripe67.nlnetlabs.nl','AAAA')
		for chunk in chunker(probes,500):
			try: ## If creating the measurement fails then nothing more must be done for this chunk.
				measurements = atlas.create(defs,probes)
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
							except Exception as e:
								print("Error inserting probe: {}, reason: {}.".format(probe,e))
					except Exception as e:
						print ("Error inserting measurement: {}, reason: {}".format(measurement,e))
				con.commit()
			except Exception as e:
				print ("There was an error submitting that query, reason: {}".format(e))
				pass
		con.close()
		return

	## Get all measurement ids that are less than a week old and are ready to process.	
	def process(self):
		con = Database.get_con()
		cursor = con.cursor()
		time_period = (time() - 7 * 24 * 60 * 60) ## 1 week ago
		q = """
			SELECT measurement_id from Measurements
			WHERE finished = 0
			AND network_propety = {prop}
			AND submitted > {time_period}
			""".format(prop = self.get_propety_name(),time_period = int(time_period))
		rows = cursor.execute(q).fetchall()

		if not rows:
			print("There are no measurements to process")
			return

		measurements = set([measurement[0] for measurement in rows])
		## Determine (below) which measurement from the list of measurements have stopped and are ready for processing.
		measurements = [x for y in [[measurement['msm_id'] for measurement in atlas.measurement(measurement) if measurement['status']['name'] == 'Stopped'] for measurement in measurements] for x in y]
		for measurement in measurements:
			try:
				response = [x for y in atlas.result(measurement) for x in y]
				measurement_header = [x for y in atlas.measurement(measurement) for x in y]
				for result in response:
					probe_id = result['prb_id']
					## This only works for DNS.
					good = 0 if result.get('error',None) else 1
					## When checking the answer from DNS we exclude those that do not have an answer.
					if good:
						## Since the query has returned a result we will now check that result.
						good = 0 if (result['ANCOUNT'] == 0) else 1
					q = 'insert into Results(measurement_id,probe_id,good,json) values({},{},{},"{}")'.format(measurement,probe_id,good,result)
					cursor.execute(q)
				q = 'UPDATE Measurements SET finished = 1,json="{}" WHERE measurement_id = {}'.format(measurement_header,measurement)
				cursor.execute(q)
				print("Results processed")
				con.commit()
			except Exception as e:
				print("There was an error processing measurement: {}, reason: {}".format(measurement,e))
				pass
		con.close()
		return

class Scheduler_DNSSEC_resolver(Scheduler):

	def __init__(self,propety):
		self.propety_name = propety
		Scheduler.__init__(self)

	def get_propety_name(self):
		return self.propety_name

class Scheduler_MTU(Scheduler): ## Needs testing.

	def __init__(self,propety,size):
		self.propety_name = propety
		self.mtu_size = size
		Scheduler.__init__(self)

	def get_mtu_size(self):
		return self.mtu_size

	def get_propety_name(self):
		return self.propety_name

	def measure(self):
		## Start with 1500 mtu, see if they are able to do it.
		## Then use those which attempted but failed this measurement.
		## Repeat until 512.
		ipv = str(self.get_propety_name()[3:4])
		if self.get_mtu_size() == 1500:
			p = self.get_probes()
		elif self.get_mtu_size() == 1280:
			p = Scheduler_MTU("IPv{}_MTU_{}".format(ipv,1500)).incapable_probes()
		elif self.get_mtu_size() == 512:
			p = Scheduler_MTU("IPv{}_MTU_{}".format(ipv,1280)).incapable_probes()
		else:
			return
		p -= self.busy_probes()
		p -= self.lazy_probes()
		p -= self.done_probes()
		if ipv == "4":
			defs = traceroute4("213.136.31.100",size=int(str(self.get_propety_name())[9:]))
		elif ipv == "6":
			defs = traceroute6("2001:7b8:40:1:702c:29ff:fec7:ee03",size=int(str(self.get_propety_name())[9:]))
		else:
			print("Cannot continue, reason: IP version is not clear.")
			return
		probes = list(p)
		for chunk in chunker(probes,500):
			try: ## If creating the measurement fails then nothing more must be done for this chunk.
				measurements = atlas.create(defs,probes)
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
							except Exception as e:
								print("Error inserting probe: {}, reason: {}.".format(probe,e))
					except Exception as e:
						print ("Error inserting measurement: {}, reason: {}".format(measurement,e))
				con.commit()
			except Exception as e:
				print ("There was an error submitting that query, reason: {}".format(e))
				pass
		con.close()
		return

	## Get all measurement ids that are less than a week old and are ready to process.	
	def process(self):
		con = Database.get_con()
		cursor = con.cursor()
		time_period = (time() - 7 * 24 * 60 * 60) ## 1 week ago
		q = """
			SELECT measurement_id from Measurements
			WHERE finished = 0
			AND network_propety = {prop}
			AND submitted > {time_period}
			""".format(prop = self.get_propety_name(),time_period = int(time_period))
		rows = cursor.execute(q).fetchall()

		if not rows:
			print("There are no measurements to process")
			return

		measurements = set([measurement[0] for measurement in rows])
		## Determine (below) which measurement from the list of measurements have stopped and are ready for processing.
		measurements = [x for y in [[measurement['msm_id'] for measurement in atlas.measurement(measurement) if measurement['status']['name'] == 'Stopped'] for measurement in measurements] for x in y]
		for measurement in measurements:
			try:
				response = [x for y in atlas.result(measurement) for x in y]
				measurement_header = [x for y in atlas.measurement(measurement) for x in y]
				for result in response:
					probe_id = result['prb_id']
					good = 0 if result['result'].get('error',None) else 1
					q = 'insert into Results(measurement_id,probe_id,good,json) values({},{},{},"{}")'.format(measurement,probe_id,good,result)
					cursor.execute(q)
				q = 'UPDATE Measurements SET finished = 1,json="{}" WHERE measurement_id = {}'.format(measurement_header,measurement)
				cursor.execute(q)
				print("Results processed")
				con.commit()
			except Exception as e:
				print("There was an error processing measurement: {}, reason: {}".format(measurement,e))
				pass
		con.close()
		return
 
if __name__ == "__main__":
	#sch = Scheduler_IPv6_dns_Capable("IPv6_dns_Capable")
	#sch = Scheduler_IPv6_ping_Capable("IPv6_ping_Capable")
	#sch = Scheduler_IPv4_ping_Capable("IPv4_ping_Capable")
	sch = Scheduler_MTU("IPv6_MTU",1500)
	#sch = Scheduler_MTU("IPv6_MTU",1280)
	#sch = Scheduler_MTU("IPv6_MTU",512)
	#sch = Scheduler_MTU("IPv4_MTU",1500)
	#sch = Scheduler_MTU("IPv4_MTU",1280)
	#sch = Scheduler_MTU("IPv4_MTU",512)
	sch.run()
	## List of network propeties
	#network_propeties = ["IPv6_dns_Capable","IPv6_ping_Capable","IPv4_ping_Capable"]
	#for propety in network_propeties:
		#if propety == "Ipv6_dns_Capable":
			#sch = Scheduler_IPv6_dns_Capable(propety)
		#elif propety == "IPv6_ping_Capable":
			#sch = Scheduler_IPv6_ping_Capable(propety)
		#elif propety == "IPv4_ping_Capable":
			#sch = Scheduler_IPv4_ping_Capable(propety)
		#else:
			#sch = None
		#if sch:
			#try:
				#sch.run()
			#except Exception as e:
				#print ("There was and error running propety: {}, reason: {}".format(propety,e))
				#pass
	#print("fin.")
