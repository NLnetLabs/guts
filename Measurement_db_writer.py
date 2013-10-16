#!/usr/bin/env python

import os
import sys
import json
import time
import copy
import urllib
import urllib2
import sqlite3
import datetime
from datetime import date
import db_creator

conn = sqlite3.connect('Measurements.db')
cursor = conn.cursor()
## Check whether the database exists, if it doesn't create it
## If it does, connect to it.
def db_conn():
	db_filename = 'Measurements.db'
	db_is_new = not os.path.exists(db_filename)
	conn = sqlite3.connect(db_filename)
	cursor = conn.cursor()
	if db_is_new:
		db_creator.create_db()
	return conn

## Anon func to determine whether a key is in a dict.
## Lengthly name to ensure it does not clash.
exists_in_dict = lambda x,d: True if x in d else False

## Place the data from the web into the database
def db_place(lines,tbls):
	try:
		inserts = []
		tbl_mes = get_tbl_schema(tbls[0])
		if (type(lines) == list) and (len(lines) != 0):
			## Add to measurements table since the data is generic for all measurment types
			for k,v in tbl_mes.iteritems():
				val = lines[0].get(k[4:],None)
				if val or val == 0:
					tbl_mes[k] = val
			tbl_mes['mes_msm_date'] = str(ret_timestamp_to_dt(lines[0]['timestamp']))
			inserts.append((tbls[0],tbl_mes))
			## Remove the measurements table from the list of tables since it has been dealt with.
			tbls = tbls[1:]
			for line in lines:
				res_id = ret_max_res_id() + 1
				err_id = get_last_err_id() + 1
				## Handle each type seperately
				if line['type'] == 'dns':
					proc_dns(line,tbls,res_id,err_id,inserts)
				elif line['type'] == 'ping':
					proc_ping(line,tbls,res_id,err_id,inserts)
				elif line['type'] == 'traceroute':
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

## Return the last icmpext id so that we can assign the next one
def get_last_icmpext_id():
	cursor.execute("Select max(set_icmpext_mpls_id) from tbl_result_set")
	row = cursor.fetchone()
	if row[0] is None:
		return 0
	else:
		return row[0]

## Return the last icmpext id so that we can assign the next one
def get_last_icmpext_mpls_id():
	cursor.execute("Select max(mpl_icmptext_mpls_id) from tbl_traceroute_mpls")
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

## Inserts data into the database.
def insert_into_db(tbl,tbl_schem):
	#1: remove all None values
	#2: create a string of all viable(non Null) keys, same with values
	#3: insert info into db (columns,values)
	keys = ""
	vals = ""
	tmp = tbl_schem
	tbl_schem = sanitize_dict(tbl_schem)
	for k,v in tbl_schem.iteritems():
		keys += "'"+(k)+"',"
		vals += "'"+str(v)+"',"
	keys = keys[:-1]
	vals = vals[:-1]
	cursor.execute("insert into "+tbl+"("+(keys)+") values("+vals+")")
	
def insert_tuples_in_db(inserts):
	## to include: split executes larger than 10000
	for insert in inserts:
		##
		keys = ""
		vals = ""
		tbl = insert[0]
		que = insert[1]
		##
		que = sanitize_dict(que)
		if len(que) >= 1:
			for k,v in que.iteritems():
				keys += "'"+(k)+"',"
				vals += "'"+str(v)+"',"
			keys = keys[:-1]
			vals = vals[:-1]
			cursor.execute("insert into "+tbl+"("+(keys)+") values("+vals+")")

## Return the results of a specific measurement
def get_measurements_web(measurement):
	try:
		urls = "https://atlas.ripe.net/api/v1/measurement/"+str(measurement)+"/result/"
		req = urllib2.urlopen(urls)
		req = json.load(req)
		return req
	except urllib2.HTTPError, e: ## <- Idea from Willem Toorop
		print (e.read())

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
		sys.exit(0)

## Return the time stamp in the format yyyy-mm-dd/hh-mm-ss
def ret_timestamp_to_dt(ts):
	dt = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d/%H:%M:%S')
	return dt

## Return the last row in a table
def get_last_row(tbl): ## Not used
	cursor.execute('select * from '+tbl+' where oid = (select max(oid) from '+tbl+');')
	return cursor.fetchone()

## Return all items from a table
def view_tbl(tbl):
	row = cursor.execute("Select * from "+tbl)
	return row.fetchall()

## Return a dict(column name:None)
def get_tbl_schema(tbl):
	d = {}
	for r in cursor.execute("PRAGMA table_info("+tbl+");"):
		d.update({str(r[1]):None})
	return d

## Return whether the database is up to date or not
def check_if_updated(measurements,mes_ids):
	fi_mes = []
	to_add = []
	for measurement in measurements:
		fi_mes.append(int(str(measurement).split('|')[3].replace("\n","")))
	for i in range(len(fi_mes)):
		if fi_mes[i] not in mes_ids : to_add.append(fi_mes[i])
	return to_add

## Return all the measurement ids
def get_all_measurement_ids():
	ids = []
	cursor.execute("select mes_msm_id from tbl_Measurements")
	c = cursor.fetchall()
	for i in c:
		ids.append(i[0])
	return ids

if __name__ == "__main__":
	db_conn()
	fi = 'Measurements.txt'
	tbls = ['tbl_Measurements', 'tbl_results', 'tbl_result_set', 'tbl_traceroute_mpls']
	mes_ids = get_all_measurement_ids()
	insert_list = []
	measurements = get_measurements_file(fi)
	to_add = check_if_updated(measurements,mes_ids)
	if to_add != None or len(to_add) != 0:
		for measurement in to_add:
			req = get_measurements_web(measurement)
			ins = db_place(req,tbls)
			insert_list.append(ins)
	else:
		print "The database is up to date according to the measurements file"
	print len(insert_list)
	for x in range(len(insert_list)):
		print len(insert_list[x])
		insert_tuples_in_db(insert_list[x])
	conn.commit()
	print "Fin"
	conn.close()

## set Vim tabs for viewing
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4