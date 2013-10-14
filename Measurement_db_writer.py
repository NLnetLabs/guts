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

def db_conn():
	db_filename = 'Measurements.db'
	db_is_new = not os.path.exists(db_filename)
	conn = sqlite3.connect(db_filename)
	cursor = conn.cursor()
	if db_is_new:
		db_creator.create_db()
	return conn

##	Anon func to determine whether a key is in a dict.
##	Lengthly name to ensure it does not clash.
exists_in_dict = lambda x,d: True if x in d else False

##	Place the data from the web into the database
def db_place(lines,tbls):
	#try:
		##	Start processing the data into a dict made from the schema of the table
	mes_schema = get_tbl_schema(tbls[0])
	if type(lines) == list:
		##	Add to measurements table since the data is generic for all measurment types
		for k1,v1 in lines[0].iteritems():
			for k2,v2 in mes_schema.iteritems():
				if k1 == k2[4:]:
					mes_schema[k2] = v1
				mes_schema['mes_msm_date'] = str(ret_timestamp_to_dt(lines[0]['timestamp']))
		insert_into_db(tbls[0],mes_schema)
		##	Remove the measurements table from the list of tables since it has been dealt with.
		tbls = tbls[1:]
		for line in lines:
			res_id = ret_max_res_id() + 1
			err_id = get_last_err_id() + 1
			##	Handle each type seperately
			if line['type'] == 'dns':
				proc_dns(line,tbls,res_id,err_id)
			elif line['type'] == 'ping':
				proc_ping(line,tbls,res_id,err_id)
			elif line['type'] == 'traceroute':
				proc_trace(line,tbls,res_id,err_id)
			else:
				print ('The type of measurement is either not supported or it is unidentifiable')				
	#except KeyError, e:
		#print (e)
	#except sqlite3.IntegrityError:
		#pass
	#except Exception, e:
		#print ("Error: ",e)
		#pass

##	Process DNS
def proc_dns(line,tbls,res_id,err_id):##	Tested and working
	tbl_schem = []
	has_result = False
	res_table = get_tbl_schema(tbls[0])
	for sk,sv in res_table.iteritems():
		for lk,lv in line.iteritems():
			if lk == 'result':
				has_result = True
			elif lk == 'error' and sk == 'res_err_id':
				res_table[sk] = err_id
			elif str(sk)[4:] == lk:
				res_table[sk] = lv
	if not has_result:	##	You not has good engrish skill leik muh!
		set_table = get_tbl_schema(tbls[1])
		set_table['set_err_id'] = err_id
		for ek,ev in line['error'].iteritems():
			set_table['set_err_reason'] = ek
			set_table['set_err_value'] = ev
		insert_into_db(tbls[1],set_table)
	insert_into_db(tbls[0],res_table)

##	Process Ping
def proc_ping(line,tbls,res_id,err_id):##	Working only on error untested
	tbl_schem = []
	has_result = False
	res_table = get_tbl_schema(tbls[0])
	for sk,sv in res_table.iteritems():
		for lk,lv in line.iteritems():
			if lk == 'result' and sk == 'res_set_id':
				res_table[sk] = res_id
				has_result = True
			if lk == 'error' and sk == 'res_err_id':
				res_table[sk] = err_id
			elif str(sk)[4:] == lk:
				res_table[sk] = lv
	insert_into_db(tbls[0],res_table)
	##	Handle result set with in the result.
	for line_result in line['result']:
		set_table = get_tbl_schema(tbls[1])
		if has_result:
			for sk,sv in set_table.iteritems():
				for lk,lv in line_result.iteritems():
					set_table['set_set_id'] = res_id
					if str(sk)[4:] == lk:
						set_table[sk] = lv
		else:##	Not tested
			set_table['set_err_id'] = err_id
			for ek,ev in line['error']:
				set_table['set_err_reason'] = ek
				set_table['set_err_value'] = ev
		insert_into_db(tbls[1],set_table)

##	Process Traceroute
def proc_trace(line,tbls,res_id,err_id):
	##
	tbl_res = get_tbl_schema(tbls[0])
	tbl_set = get_tbl_schema(tbls[1])
	tbl_mpl = get_tbl_schema(tbls[2])
	##
	tbl_schem = []
	has_result = False
	res_table = tbl_res
	for sk,sv in res_table.iteritems():
		for lk,lv in line.iteritems():
			if type(lv) == list:
				if 'result' in lv[0] and lk == 'result' and sk == 'res_set_id':
					res_table[sk] = res_id
					has_result = True
				elif 'error' in lv[0] and sk == 'res_err_id':
					has_result = False
					res_table[sk] = err_id
			elif str(sk)[4:] == lk:
				res_table[sk] = lv
	insert_into_db(tbls[0],res_table)

	##	Handle the results within the results within the results.
	##	It's like a bunch of grapes within a packet within a fruit bin within the supermarket.
	has_icmp = False
	for res in line['result']:
		if has_result:
			for res_set in res['result']:
				for k,v in res_set.iteritems():
					set_table = copy.deepcopy(tbl_set)
					set_table['set_set_id'] = res_id
					set_table['set_hop'] = res['hop']
					for k_set,v_set in set_table.iteritems():
						if k == k_set[4:]:
							set_table[k_set] = v
						elif k == 'icmpext':##<- This is why we can't have nice things
							set_table['set_icmpext_mpls_id'] = get_last_icmpext_id()
							has_icmp = True
				insert_into_db(tbls[1],set_table)
#
			##	Hard coding this part, not sure if it is necessary ##
				if has_icmp:
					mpls_table = copy.deepcopy(tbl_mpl)
					for icm_k,icm_v in res_set.iteritems():
						if icm_k == k_set[12:]:
							set_table[k_set] = icm_v
						elif icm_k == 'obj':
							for icm_objs in icm_v:
								if exists_in_dict('class',icm_objs) : set_table['set_icmpext_class'] = icm_objs['class']
								if exists_in_dict('type', icm_objs) : set_table['set_icmpext_type'] = icm_objs['type']
								for mpl in icm_objs['mpls']:
									mpls_table['mpl_icmptext_mpls_id'] = get_last_icmpext_mpls_id()
									if exists_in_dict('label', mpl) : 	mpls_table['mpl_label'] = mpl['label']
									if exists_in_dict('exp', mpl) : 	mpls_table['mpl_exp'] = mpl['exp']
									if exists_in_dict('s', mpl) : 		mpls_table['mpl_s'] = mpl['s']
									if exists_in_dict('ttl', mpl) : 	mpls_table['mpl_ttl'] = mpl['ttl']
									insert_into_db(tbls[2],mpls_table)
		else:
			set_table = tbl_set
			set_table['set_err_id'] = err_id
			for ek,ev in line['result']:
				set_table['set_err_reason'] = ek
				set_table['set_err_value'] = ev
		insert_into_db(tbls[1],set_table)

##	Remove all keys where the values are None
def sanitize_dict(d):
	delkeys = []
	for k,v in d.iteritems():
		if v is None:
			delkeys.append(k)
	for i in range(len(delkeys)):
		del d[delkeys[i]]
	return d

##	Return the last error id so that we can assign the next one, if there is another error :?
def get_last_err_id():
	cursor.execute("Select max(res_err_id) from tbl_results")
	row = cursor.fetchone()
	if row[0] is None:
		return 0
	else:
		return row[0]

##	Return the last icmpext id so that we can assign the next one
def get_last_icmpext_id():
	cursor.execute("Select max(set_icmpext_mpls_id) from tbl_result_set")
	row = cursor.fetchone()
	if row[0] is None:
		return 0
	else:
		return row[0]
		
##	Return the last icmpext id so that we can assign the next one
def get_last_icmpext_mpls_id():
	cursor.execute("Select max(mpl_icmptext_mpls_id) from tbl_traceroute_mpls")
	row = cursor.fetchone()
	if row[0] is None:
		return 0
	else:
		return row[0]

##	Return the last result id so that we can assign the next one
def ret_max_res_id():
	cursor.execute("Select max(res_set_id) from tbl_results")
	row = cursor.fetchone()
	if row[0] is None:
		return 0
	else:
		return row[0]

##	Inserts data into the database.
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

##	Return the results of a specific measurement
def get_measurements_web(measurement):
	try:
		urls = "https://atlas.ripe.net/api/v1/measurement/"+str(measurement)+"/result/"
		req = urllib2.urlopen(urls)
		req = json.load(req)
		return req
	except urllib2.HTTPError, e: ## <- Idea from Willem Toorop
		print (e.read())

##	Return the data from the measurements file
def get_measurements_file(fileName):
	if len( fileName ) > 0:
		if os.path.exists(fileName):
			f = open(fileName,'r')
			mes = f.readlines()
			f.close()
			return mes
	else:
		print ("Could not find file: "+str(filename))
		sys.exit(0)

##	Return the time stamp in the format yyyy-mm-dd/hh-mm-ss
def ret_timestamp_to_dt(ts):
	dt = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d/%H:%M:%S')
	return dt

##	Return the last row in a table
def get_last_row(tbl): ## Not used
	cursor.execute('select * from '+tbl+' where oid = (select max(oid) from '+tbl+');')
	return cursor.fetchone()

##	Return all items from a table
def view_tbl(tbl):
	row = cursor.execute("Select * from "+tbl)
	return row.fetchall()

##	Return a dict(column name:None)
def get_tbl_schema(tbl):
	d = {}
	for r in cursor.execute("PRAGMA table_info("+tbl+");"):
		d.update({str(r[1]):None})
	return d

##	Return whether the database is up to date or not
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
	measurements = get_measurements_file(fi)
	to_add = check_if_updated(measurements,mes_ids)
	if to_add != None or len(to_add) != 0:
		for measurement in to_add:
			req = get_measurements_web(measurement)
			db_place(req,tbls)
	else:
		print "The database is up to date according to the measurements file"
	conn.commit()
	print "Fin"
	conn.close()
