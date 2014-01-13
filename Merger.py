#!/usr/bin/env python

## The Merger allows the user to merg data with others from the internet
## or to merge data from one database to another (provided there is access to that db.)

from atlas import *
import Database
import json
import os

if __name__ == "__main__": ## Needs testing.
	con = Database.get_con()
	cursor = con.cursor()

	print ("Please select an option:")
	print ("1) merge web results")
	print ("2) merge database results")

	choice = raw_input()
	if choice == 1:
		print ("Please insert the measurement id or list of measurement ids:")
		measurements = raw_input()
		## This way we can handle a list of measurements and a single measurement with one block of code.
		measurements = list(measurements) if type(measurements) is type(int) else measurements
		if type(measurements) is type(list):
			for measurement in measurements:
				## Handle measurement header here.
				measurement_info = atlas.measurement(measurement)
				sub = measurement_info['creation_time']
				prop = measurement_info['description'] ## This will not work if there is not valid description.
				if len(prop) == 0:
					print ("Invalid description length for measurement: {}.".format(measurement))
					break ## AWW hell nawww, it's all messed up.
				q = """ INSERT INTO Measurements(measurement_id,network_propety,submitted,finished,json)
						VALUES({msm},"{prop}",{sub},0,'{json}')
					""".format(msm = measurement,prop = prop, sub = sub,json.dumps(measurement_info))
				cursor.execute(q)
				results = list(atlas.result(measurement))[0]
				## Look for individual probes within the results
				probes = set([probe['prb_id'] for probe in [result for result in results]])
				## Good now add those probes into targeted. This is not a true reflection of the original measurement.
				for probe in probes:
					pq = "INSERT INTO Targeted(measurement_id,probe_id) VALUES({},{})".format(measurement,probe)
					cursor.execute(pq)
				## Ok now this measurement is in the database, it will be processed as long as the appropriate Scheduler is run.
				print ("Measurement {} has been successfully inserted into the database.".format(measurement))
			con.commit()
		else:
			## not list or int, back away slowly... RUN!
			print ("Give me a list of ints or an int bruh..")
			return
	else:
		print ("Please specify the database from which to copy:")
		db_in = raw_input()
		if not os.path.exists(filename):
			## You said you would provide a valid database, that fact that you haven't determines that was a lie.
			print ("Invalid database or location.")
			return
		con = Database.get_con() ## This is the second db(the one being copied to.).
		curs_in = sqlite3.connect(db_in).con_in.cursor() ## As in; i_b_curs_in >.<
		curs_out = con.cursor()
		print ("Please insert the measurement id or list of measurement ids:")
		measurements = raw_input()
		## This way we can handle a list of measurements and a single measurement with one block of code.
		measurements = list(measurements) if type(measurements) is type(int) else measurements
		if type(measurements) is type(list):
			for measurement in measurements:
				## Handle Measurments info here
				q = "SELECT * FROM Measreuements WHERE measurement_id = {}".format(measurement)
				res = list(curs_in.execute(q))[0]
				if not res:
					break ## This measurement does not exist in the current database.
				q = """ INSERT INTO Measurements(measurement_id,network_propety,submitted,finished,json)
						Values({},'{}',{},{},'{}')
					""".format(res[0],res[1],res[2],res[3],res[4])
					curs_out.execute(q)
				## Handle Targeted probes here
				q = "SELECT * FROM Targeted WHERE measurement_id = {}".format(measurement)
				rows = list(curs_in.execute(q))
				for row in rows:
					q = "INSERT INTO Targeted(measurement_id,probe_id) VALUES({},{})".format(measurement,res[row][1])
					curs_out.execute(q)
				## Handle Results here
				q = "SELECT * FROM Targeted WHERE measurement_id = {}".format(measurement)
				rows = list(curs_in.execute(q))
				for row in rows:
				q = """ INSERT INTO Results(measurement_id,probe_id,good,json) VALUES({},{},{},'{}')
					""".format(res[row][0],res[row][1],res[row][2],res[row][3])
					curs_out.execute(q)
				print ("Measurement {} has been successfully inserted into the database.".format(measurement))
				con.commit()
