#!/usr/bin/env python

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
			create table Measurements(
				measurement_id		integer		primary key,
				network_propety		text,
				submitted			int,
				finished  			int,
				json				blob
			);
			""")
	db_spec.append("""
			create table Targeted(
				measurement_id		int,
				probe_id			int
			);
			""")
	db_spec.append("""
			create table Results(
				measurement_id		int,
				probe_id			int,
				good				int,
				json				blob
			);
			""")
	for x in db_spec:
		cursor.execute(x)
	con.commit()
	
if __name__ == "__main__":
	get_con()

## set Vim tabs for viewing
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
