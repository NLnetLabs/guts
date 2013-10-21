#!/usr/bin/env python
#Author Warwick Louw
## imports
import os
import sys
import json
import time
import copy
import urllib
import pprint
import urllib2
import sqlite3
import datetime
import Database
from datetime import date
import db_creator
##
## The following script is seperated into two:

## Auxiliary methods:
## These methds are used to complete tasks for members such as return rows, ids, processing etc..
## These are then seperated: Database specific, Measurement specific, processing and other

## Caller methods:
## These methds are what should be called by the user or script

## ---------------------------------------------------------------------------------------------------------
## Database specific auxiliary methods.
## ---------------------------------------------------------------------------------------------------------

## return a connection to the database
def ret_con():
	con = Database.ret_con()
	return con

## Return all the measurement ids stored in the database
def get_all_measurement_ids():
	ids = []
	cursor.execute("select mes_msm_id from tbl_Measurements")
	c = cursor.fetchall()
	for i in c:
		ids.append(i[0])
	return ids

## ~Deprecated~ Keeping for keeps sake
## Return the last row in a table
def get_last_row(tbl):
	cursor.execute('select * from '+tbl+' where oid = (select max(oid) from '+tbl+');')
	return cursor.fetchone()

## Remove all keys where the values are None
def sanitize_dict(d):
	delkeys = []
	for k,v in d.iteritems():
		if v is None:
			delkeys.append(k)
	for i in range(len(delkeys)):
		del d[delkeys[i]]
	return d

## Return the last error id so that we can assign the next one, if there is another error :?
def get_last_err_id():
	cursor.execute("Select max(res_err_id) from tbl_results")
	row = cursor.fetchone()
	if row[0] is None:
		return 0
	else:
		return row[0]

## Return the last result id so that we can assign the next one
def ret_max_res_id():
	cursor.execute("Select max(res_set_id) from tbl_results")
	row = cursor.fetchone()
	if row[0] is None:
		return 0
	else:
		return row[0]

## view all items on the specified table
def view_tbl(tbl):
	cursor = Database.ret_con.cursor()## returns a connection which we can then assign a cursor to.
	rows = cursor.execute("Select * from "+tbl)
	for row in rows:## to add: include column names and tab everything into place to imporve readability
		print row

## Inserts data into the database, a tuple(table name, values) is parsed
def insert_tuple_in_db(inserts):
	if type(inserts) != type(tuple):
		print "Inserts should be in the form of a tuple. tuple : (table name, values)"
		print 'example: ("tbl_Measurements",(111111,"date","127.0.0.1",4,"ICMP","Ping"))'
		return
	## to include: split executes larger than 30 000
	for insert in inserts:
		## Declarations
		tbl = insert[0]
		que = insert[1]
		##
		que = sanitize_dict(que)
		if len(que) >= 1:
			keys = ", ".join(que.keys())
			vals = ", ".join('?' * len(que))
			query = "insert into "+tbl+"({}) values({})".format(keys,vals)
			cursor.execute(query,que.values())

## Return the table schema as a dict({column name : None})
def get_tbl_schema(tbl):
	d = {}
	for r in cursor.execute("PRAGMA table_info("+tbl+");"):
		d.update({str(r[1]):None})
	return d

## ---------------------------------------------------------------------------------------------------------
## Measurement specific auxiliary methods.
## ---------------------------------------------------------------------------------------------------------

## Return the data from the measurements file
def get_measurements_file(fileName):
	if len( fileName ) > 0:
		if os.path.exists(fileName):
			f = open(fileName,'r')
			mes = f.readlines()
			f.close()
			return mes
	else:
		print ("Could not find the measurements file: "+str(filename)+" or the file is empty")
		return None
		#sys.exit(0)## ~Deprecated~ deleted in next commit

## Add measurements info to insert list. return tuple for tuple insert.
def insert_measurement_info(mes_info):
	tbl_Measurements = get_tbl_schema("tbl_Measurements")
	for k,v in tbl_Measurements.iteritems():
		val = mes_info.get(k[:4],None)
		if val:
			tbl_Measurements[k] = val
	return ("tbl_Measurements",tbl_Measurements)

## Return the results of a specific measurement
def get_measurements_web(measurement):
	try:
		urls = "https://atlas.ripe.net/api/v1/measurement/"+str(measurement)+"/result/"
		req = urllib2.urlopen(urls)
		req = json.load(req)
		return req
	except urllib2.HTTPError, e: ## <- Idea from Willem Toorop
		print (e.read())

## View measurement info
def view_measurement_info(measurement):
	response = measurement_info(measurement)
	if response != None:
		## For now, this will do.
		for k,v in response.iteritems():
			print k,":",v

## Return the time stamp in the format yyyy-mm-dd/hh-mm-ss
## Useful for converting a start_time and end_time of a measurement
## Found on Stackoverflow
def ret_timestamp_to_dt(ts):
	dt = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d/%H:%M:%S')
	return dt

## Gather Measurements info
def get_measurement_info(measurement):
	try:
		response = urllib2.urlopen("https://atlas.ripe.net/api/v1/measurement/"+str(measurement)).read()
		response = json.loads(response)
		return response
	except urllib2.HTTPError, e: ## <- Idea from Willem Toorop
		print (e.read())
		return None

## ---------------------------------------------------------------------------------------------------------
## Processing for storage.
## ---------------------------------------------------------------------------------------------------------
## Place the data from the web into the database
def proc_result(results):
	try:
		inserts = []
		if (type(results) == list) and (len(results) != 0):
			## Remove the measurements table from the list of tables since it has been dealt with.
			tbls = ['tbl_Measurements', 'tbl_results', 'tbl_result_set', 'tbl_traceroute_mpls']
			for result in results:
				res_id = ret_max_res_id() + 1
				err_id = get_last_err_id() + 1
				## Handle each type seperately
				if result['type'] == 'dns':
					proc_dns(line,tbls,res_id,err_id,inserts)
				elif result['type'] == 'ping':
					proc_ping(line,tbls,res_id,err_id,inserts)
				elif result['type'] == 'traceroute':
					proc_trace(line,tbls,res_id,err_id,inserts)
				else:
					print ('The type of measurement is either not supported or it is unidentifiable')
			return inserts
	except KeyError, e:
		print (e)
	except Exception, e:
		print ("Error: ",e)
		pass

## Process DNS
def proc_dns(line,tbls,res_id,err_id,inserts):## It's still ugly but works, faster
	## Declarations
	has_result = False
	tbl_res = get_tbl_schema(tbls[0])
	##
	if exists_in_dict("result",line):
		has_result = True
	for k,v in tbl_res.iteritems():
		val = line.get(str(k[4:]),None)
		if val or val == 0:
			tbl_res[k] = val
	if has_result:
		tbl_res['res_set_id'] = res_id
		for k,v in tbl_res.iteritems():
			val = line['result'].get(k[4:],None)
			if val or val == 0:
				tbl_res[k] = val
	else:## the result was an error
		for ek,ev in line['error'].iteritems():
			## Set table only needed if there is an error
			tbl_res['res_set_id'] = res_id
			tbl_set = get_tbl_schema(tbls[1])
			tbl_set['set_err_id'] = err_id
			tbl_set['set_err_reason'] = ek
			tbl_set['set_err_value'] = ev
	## Append the results to a list to insert values later
		inserts.append((tbls[1],tbl_set))
	inserts.append((tbls[0],tbl_res))

## Process Ping
def proc_ping(line,tbls,res_id,err_id,inserts):## It's still ugly but works, faster
	## Declarations
	has_result = False
	tbl_res = get_tbl_schema(tbls[0])
	tbl_set = get_tbl_schema(tbls[1])
	##
	if exists_in_dict("result",line):
		has_result = True
		tbl_res['res_set_id'] = res_id
	else:
		tbl_res['res_err_id'] = err_id
	for k,v in tbl_res.iteritems():
			val = line.get(k[4:],None)
			if val or val == 0:
				tbl_res[k] = val
	## Handle result set with in the result.
	for line_result in line['result']:
		## A deep copy of the schema of the table (set)
		tbl_set_copy = copy.deepcopy(tbl_set) ## Can improve speed, maybe see insert_into_db
		if has_result:
			for k,v in tbl_set_copy.iteritems():
				tbl_set_copy['set_set_id'] = res_id
				val = line_result.get(k[4:],None)
				if val or val == 0:
					tbl_set_copy[k] = val
		else:
			tbl_set_copy['set_err_id'] = err_id
			for ek,ev in line['error']:
				tbl_set_copy['set_err_reason'] = ek
				tbl_set_copy['set_err_value'] = ev
		inserts.append((tbls[1],tbl_set_copy))
	inserts.append((tbls[0],tbl_res))

##	Process Traceroute
def proc_trace(line,tbls,res_id,err_id,inserts): ## Tested, ugly but it works
	## Declarations
	has_result = False
	tbl_res = get_tbl_schema(tbls[0])
	tbl_set = get_tbl_schema(tbls[1])
	tbl_mpl = get_tbl_schema(tbls[2])
	##
	## Handle errors or results
	if exists_in_dict('result',line):
		if exists_in_dict('result',line['result'][0]):
			has_result = True
			tbl_res['res_set_id'] = res_id
		elif exists_in_dict('error',line['result'][0]):
			tbl_res['res_err_id'] = err_id
	## Set values in the tbl_res schema
	for k,v in tbl_res.iteritems():
		val = line.get(k[4:],None)
		if val or val == 0:
			tbl_res[k] = val
	inserts.append((tbls[0],tbl_res))

	## Handle the results within the results within the results.
	## It's like a bunch of grapes within a packet within a fruit bin within the supermarket. So deep :P
	has_icmp = False
	for res in line['result']:
		if has_result:
			for res_set in res['result']:
				##
				tbl_set_copy = copy.deepcopy(tbl_set)
				tbl_set_copy['set_set_id'] = res_id
				tbl_set_copy['set_hop'] = res['hop']
				if exists_in_dict('icmpext',res_set):## <- This is why we can't have nice things
					has_icmp = True
					tbl_set['set_icmpext_mpls_id'] = get_last_icmpext_id()
				##
				for k,v in tbl_set_copy.iteritems():
					val = res_set.get(k[4:],None)
					if val or val == 0:
						tbl_set_copy[k] = val
				inserts.append((tbls[1],tbl_set_copy))
				## icmpext has been removed since I'm not sure if it is necessary
		else:
			tbl_set_copy = copy.deepcopy(tbl_set)
			tbl_set_copy['set_err_id'] = err_id
			for ek,ev in line['result']:
				tbl_set_copy['set_err_reason'] = ek
				tbl_set_copy['set_err_value'] = ev
		inserts.append((tbls[1],tbl_set))

## ---------------------------------------------------------------------------------------------------------
## Other:
## ---------------------------------------------------------------------------------------------------------

## Anon func to determine whether a key is in a dict.
## Lengthly name to ensure it does not clash.
## ~Deprecated~ delete after next commit
#exists_in_dict = lambda x,d: True if x in d else False

## function to return whether a key exists within the dict
def exist_in_dict(x,d):
	return x in d

## Returns mesurement ids of measurements which do not exist in the database
## Useful to determine which of a set of ids need to be added
def check_if_updated(measurements,mes_ids):
	fi_mes = []
	to_add = []
	for measurement in measurements:
		fi_mes.append(int(str(measurement).split('|')[3].replace("\n","")))
	for i in range(len(fi_mes)):
		if fi_mes[i] not in mes_ids : to_add.append(fi_mes[i])
	return to_add

## ---------------------------------------------------------------------------------------------------------
## Caller methods:
## ---------------------------------------------------------------------------------------------------------
## These methods should be called by the user which will incorperate the use of auxiliary methods

## Method to add a single measurement to database and returns success
## Optional parameter means the calling member can indicate whether to insert the 
## query now or wait, in the case of batch queries
def add_single_measurement_to_db(measurement,insert_measurement=1):
	if len(str(measurement)) > 0:
		mes_info = measurement_info(measurement)
		if "Stopped" in mes_info['status'].values():
			## call all inserts
			con = ret_con()
			cursor = con.cursor()
			inserts = []
			## Deal with measurements info
			mes_info = get_measurement_info(measurement)
			inserts.append(("tbl_Measurements",insert_measurement_info(mes_info)))
			## Deal with results
			results = get_measurements_web(measurement)
			inserts.append(db_place(results))
			## Judge whether to insert the query now or rather pass the insert back to the caller
			if not insert_measurement:
				return inserts
			else:
				for insert in inserts:
					insert_tuple_in_db(inserts)
				con.commit()
				con.close()
				return True ## Success
		## default return is false meaning the insert did not happen for various reasons
		else:
			print "This measurement has not yet stopped, please try again later"
			print "May I recommend viewing the measurements info or adding the insert to  the schedule"
			return False ## Failure

## Method to insert measurements from file to database to inserts list
def insert_measurements_from_file(measurements):
	insert = []
	return insert

if __name__ == "__main__":	
	#conn.commit()
	print "Fin"
	conn.close()

## set Vim tabs for viewing
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

