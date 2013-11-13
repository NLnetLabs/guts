#!/usr/bin/env python
#Author Warwick Louw

import os
import sqlite3

## return a connection to the database
def get_con():
	filename = 'Atlas.db'
	if not os.path.exists(filename):
		con = sqlite3.connect(filename)
		create_db(con)
	con = sqlite3.connect(filename)
	return con

## Create the database based on the given spec
def create_db(con):
	cursor = con.cursor()
	db_spec = []
	db_spec.append("""
			create table tbl_Measurements(
				measurement_id		integer		primary key,
				network_prop		text,
				timestamp			text,
				finished  			int,
				targeted_probes		blob,
				succeeded_probes	blob,
				failed_probes		blob,
				incapable_probes	blob,
				total_probes		int
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
	con.commit()
	
if __name__ == "__main__":
	get_con()

## set Vim tabs for viewing
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
