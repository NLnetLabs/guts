#!/usr/bin/env python

import os
import json

## return a connection to the database
def get_con():
	if os.path.exists("config.json"):
		json_data = open("config.json")
		js = json.load(json_data)
		if js['DBE'] == "sqlite":
			import sqlite3
			if not os.path.exists(js['DB_location']):
				con = sqlite3.connect(js['DB_location'])
				create_db(con)
			con = sqlite3.connect(js['DB_location'])
		else:
			import MySQLdb
			location, user, password, db = js['DB_location'].split(',')
			con = MySQLdb.connect( location, user, password, db)
	else:
		print ("Cannot find config.json, please run Config.py")
		return
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
			)
			""")
	db_spec.append("""
			create table Targeted(
				measurement_id		int,
				probe_id			int
			)
			""")
	db_spec.append("""
			create table Results(
				measurement_id		int,
				probe_id			int,
				good				int,
				json				blob
			)
			""")
	for x in db_spec:
		cursor.execute(x)
	con.commit()
	
if __name__ == "__main__":
	get_con()

## set Vim tabs for viewing
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
