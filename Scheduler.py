#!/usr/bin/env python
## Timeless Scheduler

import Database
from atlas import *
import urllib2 ## Used to explain submitting errors

class Scheduler:

	def __init__(self): ## Nothing to do here..
		pass

	## Chunk lists of elements into smaller lists.
	def chunker(self,lis,chunk_size):
		for i in xrange(0, len(lis), chunk_size):
			yield lis[i:i+chunk_size]

	def probes(self, ipv4 = None, ipv6 = None):
		if (ipv4 == None and ipv6 == None):
			prefix_v6 = 1 ## Default to ipv6, most interesing and least number of probes. Everyone wins.
		p = set([])
		if ipv4:
			p.update(set([probe['id'] for probe in atlas.probe(prefix_v4 = '0.0.0.0/0', limit = 0) if probe['status'] == 1]))
		if ipv6:
			p.update(set([probe['id'] for probe in atlas.probe(prefix_v6 = '::/0', limit = 0) if probe['status'] == 1]))
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
		rows = cursor.execute(q).fetchall()

		if rows:
			busy_probes = set([probe[0] for probe in rows])
		else:
			busy_probes = set([])
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
		return done_probes

	## These are probes which have executed the measurements but have failed the entire week and have a certain number of results.
	## These are probes which cannot perform this measurement.
	def incapable_probes(self): ## This needs testing.
		cursor = Database.get_con().cursor()
		time_period = (time() - 7 * 24 * 60 * 60) ## 1 week ago
		appearance = 5
		q = """
			SELECT Targeted.probe_id FROM Targeted, Measurements, Results
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
			AND	(SELECT COUNT(*) from results,Measurements,Targeted
				WHERE Measurements.submitted > {week}
				AND Measurements.measurement_id = Results.measurement_id
				AND Measurements.network_propety = '{prop}'
				GROUP BY Targeted.probe_id) > {appr}
			""".format(week = int(time_period), prop = self.get_propety_name(),appr = appearance)
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
		#print ("{}: Incapable_probes: {}".format(self.get_propety_name(),len(self.incapable_probes())))

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

## Test which ipv6 probes have dns capability.
class Scheduler_IPv6_dns_Capable(Scheduler):

	def __init__(self,propety):
		self.propety_name = propety
		Scheduler.__init__(self)

	def get_propety_name(self):
		return self.propety_name

	def measure(self):
		con = Database.get_con()
		cursor = con.cursor()
		p  = self.probes(ipv6 = 1)
		p -= self.busy_probes()
		p -= self.lazy_probes()
		p -= self.done_probes()
		defs = dns6('nl','AAAA')
		probes = list(p)
		for chunk in self.chunker(probes,500):
			try: ## If creating the measurement fails then nothing more must be done for this chunk.
				measurements = atlas.create(defs,chunk)['measurements']
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
			except urllib2.HTTPError as e:
				print ("There was an error submitting that query, reason: {}".format(e))
			except Exception as e:
				print ("There was an error submitting that query, reason: {}".format(e))
		con.close()

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
				measurement_header = list(atlas.measurement(measurement))[0]
				for result in response:
					probe_id = result['prb_id']
					## This only works for DNS.
					good = 0 if result.get('error',None) else 1
					## When checking the answer from DNS we exclude those that do not have an answer.
					if good:
						## Since the query has returned a result we will now check that result.
						good = 0 if (result['result']['ANCOUNT'] == 0) else 1
					q = "insert into Results(measurement_id,probe_id,good,json) values({},{},{},'{}')".format(measurement,probe_id,good,json.dumps(result))
					cursor.execute(q)
				q = "UPDATE Measurements SET finished = 1,json='{}' WHERE measurement_id = {}".format(json.dumps(measurement_header),measurement)
				cursor.execute(q)
				print("Results for measurement: {} processed".format(measurement))
				con.commit()
			except Exception as e:
				print("There was an error processing measurement: {}, reason: {}".format(measurement,e))
				pass
		con.close()

class Scheduler_IPv6_ping_Capable(Scheduler):

	def __init__(self,propety):
		self.propety_name = propety
		Scheduler.__init__(self)

	def get_propety_name(self):
		return self.propety_name

	def measure(self):
		con = Database.get_con()
		cursor = con.cursor()
		p  = self.probes(ipv6 = 1)
		p -= self.busy_probes()
		p -= self.lazy_probes()
		p -= self.done_probes()
		defs = ping6('2001:7b8:40:1:d0e1::1')
		probes = list(p) ## Convert to list to be used in atlas
		## Need to handle chunks here.
		for chunk in self.chunker(probes,500):
			try: ## If creating the measurement fails then nothing more must be done for this chunk.
				measurements = atlas.create(defs,chunk)['measurements']
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
			except urllib2.HTTPError as e:
				print ("There was an error submitting that query, reason: {}".format(e))
			except Exception as e:
				print ("There was an error submitting that query, reason: {}".format(e))
		con.close()

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
				measurement_header = list(atlas.measurement(measurement))[0]
				for result in response:
					probe_id = result['prb_id']
					## Looking for "error" in result.(This will also work for traceroute)
					good = 0 if result['result'].get('error',None) else 1
					q = "insert into Results(measurement_id,probe_id,good,json) values({},{},{},'{}')".format(measurement,probe_id,good,json.dumps(result))
					cursor.execute(q)
				q = "UPDATE Measurements SET finished = 1,json='{}' WHERE measurement_id = {}".format(json.dumps(measurement_header),measurement)
				cursor.execute(q)
				print("Results for measurement: {} processed".format(measurement))
				con.commit()
			except Exception as e:
				print("There was an error processing measurement: {}, reason: {}".format(measurement,e))
				pass
		con.close()

class Scheduler_IPv4_ping_Capable(Scheduler):

	def __init__(self,propety):
		self.propety_name = propety
		Scheduler.__init__(self)

	def get_propety_name(self):
		return self.propety_name

	def measure(self):
		p  = self.probes(ipv4 = 1)
		p -= self.busy_probes()
		p -= lazy_probes()
		p -= done_probes()
		defs = ping4("213.136.31.100")
		probes = list(p)
		for chunk in chunker(probes,500):
			try:
				measurements = atlas.create(defs,chunk)['measurements']
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
			except urllib2.HTTPError as e:
				print ("There was an error submitting that query, reason: {}".format(e))
			except Exception as e:
				print ("There was an error submitting that query, reason: {}".format(e))
		con.close()

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
				measurement_header = list(atlas.measurement(measurement))[0]
				for result in response:
					probe_id = result['prb_id']
					## Looking for "error" in result.
					good = 0 if result['result'].get('error',None) else 1
					q = "insert into Results(measurement_id,probe_id,good,json) values({},{},{},'{}')".format(measurement,probe_id,good,json.dumps(result))
					cursor.execute(q)
				q = "UPDATE Measurements SET finished = 1,json='{}' WHERE measurement_id = {}".format(json.dumps(measurement_header),measurement)
				print("Results for measurement: {} processed".format(measurement))
				con.commit()
			except Exception as e:
				print("There was an error processing measurement: {}, reason: {}".format(measurement,e))
				pass
		con.close()

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
		for chunk in self.chunker(probes,500):
			try: ## If creating the measurement fails then nothing more must be done for this chunk.
				measurements = atlas.create(defs,chunk)['measurements']
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
			except urllib2.HTTPError as e:
				print ("There was an error submitting that query, reason: {}".format(e))
			except Exception as e:
				print ("There was an error submitting that query, reason: {}".format(e))
		con.close()

	## Get all measurement ids that are less than a week old and are ready to process.	
	def process(self):
		con = Database.get_con()
		cursor = con.cursor()
		time_period = (time() - 7 * 24 * 60 * 60) ## 1 week ago
		q = """
			SELECT measurement_id from Measurements
			WHERE finished = 0
			AND network_propety = '{prop}'
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
				measurement_header = list(atlas.measurement(measurement))[0]
				for result in response:
					probe_id = result['prb_id']
					## This only works for DNS.
					good = 0 if result.get('error',None) else 1
					## When checking the answer from DNS we exclude those that do not have an answer.
					if good:
						## Since the query has returned a result we will now check that result.
						good = 0 if (result['ANCOUNT'] == 0) else 1
					q = "insert into Results(measurement_id,probe_id,good,json) values({},{},{},'{}')".format(measurement,probe_id,good,json.dumps(result))
					cursor.execute(q)
				q = "UPDATE Measurements SET finished = 1,json='{}' WHERE measurement_id = {}".format(json.dumps(measurement_header),measurement)
				cursor.execute(q)
				print("Results for measurement: {} processed".format(measurement))
				con.commit()
			except Exception as e:
				print("There was an error processing measurement: {}, reason: {}.".format(measurement,e))
				pass
		con.close()

## I feel this should not be a Scheduler but instead something that will be processed by the web ui
class Scheduler_Probes_resolver(Scheduler):

	def __init__(self,propety):
		self.propety_name = propety
		Scheduler.__init__(self)

	def get_propety_name(self):
		return self.propety_name

	def measure(self):
		## Could use the results of ipv6_dns_capable.
		## Otherwise: Use the probes which were successful at ipv6_dns_capable

		## for now I will implement the former.
		pass

	def process(self):
		con = Database.get_con()
		cursor = con.cursor()
		time_period = (time() - 7 * 24 * 60 * 60) ## 1 week ago
		q = """
			SELECT Results.json from Results, Measurements
			WHERE Measurements.Submitted > {week}
			AND Results.measurement_id = Measurements.measurement_id
			AND Measurements.network_propety = '{prop}'
			AND Results.good = 1
			""".format(prop = "IPv6_dns_Capable",week = int(time_period))
		rows = cursor.execute(q).fetchall()

		if not rows:
			print("There are no results from IPv6_dns_Capable to get results from within the last week.")
			return

		jss = [json.loads(js[0]) for js in rows]
		resolvers = {}
		for probe in jss:
			key = probe['src_addr']
			if key not in resolvers:
				resolvers.update({key : []})
			resolvers[key].append(probe['prb_id'])
		for resolver, probes in resolvers.iteritems():
			print resolver,len(probes)

class Scheduler_DNSSEC_resolver(Scheduler):

	def __init__(self,propety):
		self.propety_name = propety
		Scheduler.__init__(self)

	def get_propety_name(self):
		return self.propety_name

	def measure(self):
		con = Database.get_con()
		cursor = con.cursor()
		p  = self.probes(ipv6 = 1)
		p -= self.busy_probes()
		p -= self.lazy_probes()
		p -= self.done_probes()
		probes = list(p)
		batches = 2
		defs = dns6("bogus.nlnetlabs.nl","TXT")
		for batch in range(batches):
			if batch == 0:
				defs.update(do=True)
				propety = str(self.get_propety_name())+"_dnssec"
			else:
				propety = str(self.get_propety_name())+"_nosec"
			for chunk in self.chunker(probes,500):
				try: ## If creating the measurement fails then nothing more must be done for this chunk.
					measurements = atlas.create(defs,chunk)['measurements']
					for measurement in measurements:
						q = """ INSERT INTO Measurements(measurement_id,network_propety,submitted,finished)
								VALUES({msm},"{prop}",{now},0)
							""".format(msm = measurement,prop = propety, now = int(time()))
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
				except urllib2.HTTPError as e:
					print ("There was an error submitting that query, reason: {}".format(e))
				except Exception as e:
					print ("There was an error submitting that query, reason: {}".format(e))
		con.close()

	def process(self):
		## First dnssec
		con = Database.get_con()
		cursor = con.cursor()
		time_period = (time() - 7 * 24 * 60 * 60) ## 1 week ago
		q = """
			SELECT measurement_id from Measurements
			WHERE finished = 0
			AND network_propety = '{prop}'
			AND submitted > {week}
			""".format(prop = str(self.get_propety_name())+"_dnssec",week = int(time_period))
		dnssec_rows = cursor.execute(q).fetchall()

		if not dnssec_rows:
			print("Cannot continue. Reason: there are no {}_dnssec results to process.".format(self.get_propety_name()))
			return

		## Second nosec
		con = Database.get_con()
		cursor = con.cursor()
		time_period = (time() - 7 * 24 * 60 * 60) ## 1 week ago
		q =	"""
			SELECT measurement_id from Measurements
			WHERE finished = 0
			AND network_propety = '{prop}'
			AND submitted > {week}
			""".format(prop = str(self.get_propety_name())+"_nosec",week = int(time_period))
		nossec_rows = cursor.execute(q).fetchall()

		if not nossec_rows:
			print ("Cannot continue. Reason: there are no {}_nosec results to process.".format(self.get_propety_name()))
			return

		## Hoping for symetry.
		if len(dnssec_rows) != len(nossec_rows):
			print ("Cannot continue. Reason: No Symetry between dnssec and non dnsec, maintained meaning we cannot cross reference the results.")
			return

		dnssec_mes = [row[0] for row in dnssec_rows]
		nossec_mes = [row[0] for row in nossec_rows]
		for measurement in range(len(dnssec_rows)):
			try:
				measurement_header = list(atlas.measurement(measurement))[0]
				sec_results = {x['prb_id']: x for x in list(atlas.result(dnssec_mes[measurement]))[0]} ## AWWW YISS.. DICTIONARY COMPREHENSION!! >.<
				nos_results = {x['prb_id']: x for x in list(atlas.result(nossec_mes[measurement]))[0]}
				results = [(sec_results[p], nos_results[p]) for p in (set(sec_results.keys()) & set(nos_results.keys()))]
				for result in results:## if 'answers' exists in the probes results then it recieved bogus message A simple test.
					good = 0 if ('answers' in result[0]['result'] and 'answers' in result[1]['result']) else 1
					q = """INSERT INTO Results(measurement_id,probe_id,good,json) VALUES({},{},{},'{}')
						""".format(dnssec_mes[measurement],result[0]['prb_id'],good,(json.dumps(json.dumps(result[0]),result[1])))
					cursor.execute(q)
				q = "UPDATE Measurements SET finished = 1 WHERE measurement_id={}".format(dnssec_mes[measurement])
				cursor.execute(q)
				q = "UPDATE Measurements SET finished = 1 json = '{}' WHERE measurement_id={}".format(json.dumps(measurement_header),nossec_mes[measurement])
				cursor.execute(q)
				print("Results for measurements: {} processed".format((dnssec_mes[measurement],nossec_mes[measurement])))
				con.commit()
			except urllib2.HTTPError as e:
				print ("There was an error submitting that query, reason: {}".format(e))
				pass
			except Exception as e:
				print ("Could not continue, reason: {}".format(e))
				pass
		con.close()

class Scheduler_MTU(Scheduler): ## Needs testing.

	def __init__(self,propety,ip,size):
		self.propety_name = propety
		self.ipv = ip
		self.mtu_size = size
		Scheduler.__init__(self)

	def get_mtu_size(self):
		return self.mtu_size

	def get_ipv(self):
		return self.ipv

	def get_propety_name(self):
		return self.propety_name

	def measure(self):
		## Start with 1500 mtu, see if they are able to do it.
		## Then use those which attempted but failed this measurement.
		## Repeat until 512.
		mtu = self.get_mtu_size()
		ipv = self.get_ipv()
		con = Database.get_con()
		cursor = con.cursor()
		if   mtu == 1500:
			if ipv == 4:
				p  = self.probes(ipv4 = 1)
			elif ipv == 6:
				p  = self.probes(ipv6 = 1)
			else:
				print ("Cannot continue, reason: IP version is unknown.")
				return
		elif mtu == 1280:
			p = Scheduler_MTU("IPv{}_MTU_{}".format(ipv,1500),ipv,1500).incapable_probes()
		elif mtu == 512:
			p = Scheduler_MTU("IPv{}_MTU_{}".format(ipv,1280),ipv,1280).incapable_probes()
		else:
			print("Cannot continue, reason: The chosen MTU size is not supported.")
			return

		if not p:
			return

		p -= self.busy_probes()
		p -= self.lazy_probes()
		p -= self.done_probes()
		if ipv == 4:
			defs = ping("213.136.31.100",size=mtu)
		elif ipv == 6:
			defs = ping6("2001:7b8:40:1:702c:29ff:fec7:ee03",size=mtu)
		else:
			print("Cannot continue, reason: IP version is not clear.")
			return
		probes = list(p)
		for chunk in self.chunker(probes,500):
			try: ## If creating the measurement fails then nothing more must be done for this chunk.
				measurements = atlas.create(defs,chunk)['measurements']
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
			except urllib2.HTTPError as e:
				print ("There was an error submitting that query, reason: {}".format(e))
			except Exception as e:
				print ("There was an error submitting that query, reason: {}".format(e))
		con.close()

	## Get all measurement ids that are less than a week old and are ready to process.	
	def process(self):
		con = Database.get_con()
		cursor = con.cursor()
		time_period = (time() - 7 * 24 * 60 * 60) ## 1 week ago
		q = """
			SELECT measurement_id from Measurements
			WHERE finished = 0
			AND network_propety = '{prop}'
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
				measurement_header = list(atlas.measurement(measurement))[0]
				for result in response:
					good = 0 if ('error' in (results for results in result['result']) or (result['avg'] == -1)) else 1
					q = "insert into Results(measurement_id,probe_id,good,json) values({},{},{},'{}')".format(measurement,result['prb_id'],good,json.dumps(result))
					cursor.execute(q)
				q = "UPDATE Measurements SET finished = 1,json='{}' WHERE measurement_id = {}".format(json.dumps(measurement_header),measurement)
				cursor.execute(q)
				print("Results for measurement: {} processed".format(measurement))
				con.commit()
			except Exception as e:
				print("There was an error processing measurement: {}, reason: {}".format(measurement,e))
				pass
		con.close()

if __name__ == "__main__":
	#sch = Scheduler_IPv6_dns_Capable("IPv6_dns_Capable")
	#sch = Scheduler_IPv6_ping_Capable("IPv6_ping_Capable")
	#sch = Scheduler_IPv4_ping_Capable("IPv4_ping_Capable")
	#sch = Scheduler_DNSSEC_resolver("DNSSEC_resolver")
	#sch = Scheduler_IPv6_Capable_Resolver("IPv6_Capable_Resolver")
	sch = Scheduler_MTU("IPv6_MTU",6,1500)
	#sch = Scheduler_MTU("IPv6_MTU",1280)
	#sch = Scheduler_MTU("IPv6_MTU",512)
	#sch = Scheduler_MTU("IPv4_MTU",1500)
	#sch = Scheduler_MTU("IPv4_MTU",1280)
	#sch = Scheduler_MTU("IPv4_MTU",512)
	#sch = Scheduler_Probes_resolver("Probes_resolver")
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
