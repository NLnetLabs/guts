#!/usr/bin/env python
#Author Warwick Louw

import os
import sqlite3

## return a connection to the database
def ret_con():
	filename = 'Atlas.db'
	if not os.path.exists(filename):
		create_db()
	con = sqlite3.connect(filename)
	return con

## Pretty obvious hat this does, if not:
## Create the database based on the given spec
def create_db():
	db_spec = []
	db_spec.append("""
			create table tbl_Measurements(
				Measurement_id			integer		primary key,
				Measurement_date		text,
				Target					text,
				description				int,
				successful_probes		blob,
				unsuccessful_probes		blob,
				total_probes			int
			);
			""")
	db_spec.append("""
			create table tbl_Schedule(
				Sched_num			integer		primary key 	auto_increment,
				name    			text,
				time_stamp			text,
				datetime			text,
				timestamp			text,
				task				text,
				persistent			text,
				completed			text
			);
			""")
	db_spec.append("""
			create table tbl_Schedule_state(
				pkey				int			primary key,
				msm_Total			blob,
				msm_Results			blob,
				probes				blob
			);
			""")
	db_spec.append("""
			create table tbl_Probes(
				probe_id			int			primary key,
				status				int,
				country				text,
				last_result			text,
				measurements		blob
			);
			""")
	for x in db_spec:
		cursor.execute(x)
	conn.commit()

## set Vim tabs for viewing
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
