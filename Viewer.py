#!/usr/bin/env python
import time
import Database
import Scheduler

## View the number of probes using a certain resolver.
def show_resolvers():
	con = Database.get_con()
	cursor = con.cursor()
	time_period = (time.time() - 7 * 24 * 60 * 60) ## 1 week ago
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

if __name__ == "__main__":
	show_resolvers()
