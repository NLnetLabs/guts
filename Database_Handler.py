#!/usr/bin/env python
#Author Warwick Louw

## This program will handle all database reading/writing.

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
import Processing
from datetime import date

## ---------------------------------------------------------------------------------------------------------
## Database specific auxiliary methods.
## ---------------------------------------------------------------------------------------------------------

## return a connection to the database
def get_con():
	con = Database.get_con()
	return con

## Return all the measurement ids stored in the database
def get_all_measurement_ids():
	ids = []
	cursor.execute("select mes_msm_id from tbl_Measurements")
	c = cursor.fetchall()
	for i in c:
		ids.append(i[0])
	return ids

## Remove all keys where the values are None
def sanitize_dict(d):
	delkeys = []
	for k,v in d.iteritems():
		if v is None:
			delkeys.append(k)
	for i in range(len(delkeys)):
		del d[delkeys[i]]
	return d

## This can be chaged to return the last id of the last measurement or even all the info of the last measurement
def get_max_something():
	## change the query below to match the use of this method.
	cursor.execute("Select max(table) from database")
	row = cursor.fetchone()
	if row[0] is None:
		return 0
	else:
		return row[0]

## view all items on the specified table
def view_tbl(tbl):
	cursor = Database.ret_con().cursor()## returns a connection which we can then assign a cursor to.
	rows = cursor.execute("Select * from "+tbl)
	for row in rows:## to add: include column names and tab everything into place to imporve readability
		print row

## Consider removal in favour of list insert.
## Inserts data into the database, a tuple(table name, values) is parsed
def insert_tuple_in_db(inserts):
	if type(inserts) != type(tuple):
		print "Inserts should be in the form of a tuple. tuple : (table name, values)"
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

## Much simpler way of inserting values into the database
def list_insert(tbl,insert):
	con = Database.get_con()
	cursor = con.cursor()
	if not insert:
		return
	## Since a list is parsed the "[" "]" need to be removed. To do this it is parsed as a string, the first and last chars are removed.
	query = "insert into "+tbl+"({}) values({})".format(str(insert[0])[1:-1],str(insert[1])[1:-1])
	try:
		print query ## Testing purposes
		cursor.execute(query)
		con.commit()
		cursor.close()
		con.close()
		return True
	except:
		cursor.close()
		con.close()
		return False

## Return the table schema as a dict({column name : None})
def get_tbl_schema(tbl):
	d = {}
	cursor = Database.ret_con().cursor()## returns a connection which we can then assign a cursor to.
	for r in cursor.execute("PRAGMA table_info("+tbl+");"):
		d.update({str(r[1]):None})
	return d

## Return the columns of a table in a list.
def get_tbl_columns(tbl):
	cursor = Database.get_con().cursor()## returns a connection which we can then assign a cursor to.
	results = [str(r[1]) for r in cursor.execute("PRAGMA table_info("+tbl+");")]
	cursor.close()
	return results

## Return the rows and columns of a table in a list of ditcionaries.
## Dictionaries make it easier since all the data is tagged eg: {"completed": yes}
def get_table(tbl,cols = None,args = None):
	## returns a connection which we can then assign a cursor to.
	cursor = Database.get_con().cursor()
	columns = get_tbl_columns(tbl)
	if not args and not cols: ## Specified neither columns nor arguments
		rows = [dict(zip(columns,  row)) for row in [list(row) for row in cursor.execute('Select * from '+str(tbl)).fetchall()]]
		#rows = [list(row) for row in cursor.execute('Select * from '+str(tbl)).fetchall()] ## ~depreciated~ list approach.
	elif cols and not args: ## Specified columns but no argument
		rows = [dict(zip(cols.split(","),row)) for row in [list(row) for row in cursor.execute('Select '+( str(cols) )+' from '+str(tbl)).fetchall()]]
		#rows = [list(row) for row in cursor.execute('Select '+( str(cols) )+' from '+str(tbl)).fetchall()] ## ~depreciated~ list approach.
	elif args and not cols: ## Specified argument but not columns
		rows = [dict(zip(columns,  row)) for row in [list(row) for row in cursor.execute('Select * from '+str(tbl)+' where '+str(args)).fetchall()]]
		#rows = [list(row) for row in cursor.execute('Select * from '+str(tbl)+' where '+str(args)).fetchall()] ## ~depreciated~ list approach.
	else: ## Specified columns and argument
		rows = [dict(zip(cols.split(","),row)) for row in [list(row) for row in cursor.execute('Select '+( str(cols) )+' from '+str(tbl)+' where '+str(args)).fetchall()]]
		#rows = cursor.execute('Select '+( str(cols) )+' from '+str(tbl)+' where '+str(args)).fetchall()[0] ## ~depreciated~ list approach.
	## Close up.
	Database.get_con().close()
	cursor.close()
	return rows

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
def get_timestamp_to_dt(ts):
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
## Replaced by the Processing script
## This recieves many measurement ids and processes the results then stores them.
## Rather dumb method because it just takes those which failed and stores them, then stores those which succeeded
def process_and_store(measurements):
	for measurement in measurements:
		Probes_total = Processing.id_of_all_probes(measurement)
		Probes_failed, Probes_succeeded = Processing.which_failed(measurement)

## ---------------------------------------------------------------------------------------------------------
## Other:
## ---------------------------------------------------------------------------------------------------------

## function to return whether a key exists within the dict
def exist_in_dict(x,d):
	return x in d

## ~ Could still be used, this can see whether a certain measurements info is in the database
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

## Useful if database is deleted or migrated, we could repopulate the databaes using this.
## Method to insert measurements from file to database to inserts list
def insert_measurements_from_file(measurements):
	insert = []
	return insert

## set Vim tabs for viewing
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

