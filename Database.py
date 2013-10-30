#!/usr/bin/env python
#Author Warwick Louw

import os
import sqlite3

## return a connection to the database
def ret_con():
	filename = 'Measurements.db'
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
	for x in db_spec:
		cursor.execute(x)
	conn.commit()

## set Vim tabs for viewing
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
