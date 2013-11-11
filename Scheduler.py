#!/usr/bin/env python
#Author Warwick Louw

## This program will be called by an external scheduler
## Scheduler orcastrates the flow

import json
import time
import sched
import Probes
import urllib2
import Database
import threading
import Processing
import Atlas_Query
import Database_Handler as DB_Handle

## Scheduler class
class Scheduler:
	## Here we will initialize the values
	## Additional values could be added for specific propeties.
	def __init__(self):
		self.propety = "" ## The network propety this scheduler will represent
		self.targeted = []	#List of targeted probes
		self.results = []	#List of results
	## Get the last state
	#def get_propety():
	#def get_Targeted():
	#def get_results():

	## Update the current state
	#def Update_Targeted():
	#def Update_Results():
	
	## Set the state in the database
	#def set_Targeted():
	#def set_Results():

## Start
def start():
	#add_task() ## Testing.
	## Get scheduled tasks
	#tasks = get_tasks()
	#if not tasks:
		#print "There are no tasks that need to be done now."
		#return
	#thread_list = []
	#for task in tasks:
		#task_name = task["task"]
		#print task_name, task_args
		## Here we can filter out the task and move them in the right direction.
		## For instance if the tasks is to process measurements then, create a new thread and assign it to process the measurements.
		#if task_name == "process_measurements":
			#thread_list.append(threading.Thread(target=process_results, args=((task),)))
			## task_completed(task)
	#for thread in thread_list:
		#thread.start()
		## fetch state, build state, fly
		#Scheduler = Scheduler()

	## Now that the tasks that needed to be done now are done.
	## We will see which tasks need to be scheduled.
	## Get all persistent tasks.
	per_tasks = DB_Handle.query_table("tbl_Routines",None,' "persistent" == "yes"')
	## Get task which are currently scheduled
	cur_tasks = DB_Handle.query_table("tbl_Schedule",None,' "completed" == "No" or "no"')
	## Now we have all the tasks that are persistent and all the tasks that are scheduled.
	new_tasks = [task for task in per_tasks if task['routine_name'] not in [cur_task['task'] for cur_task in cur_tasks]]
	## we need to schedule the tasks.
	for task in new_tasks:
		schedule_task(str(task['routine_name']),int(time.time() + task['interval']))

## See what we gotta do.
def get_tasks():
	## Get the contents of the table tbl_Schedule and provide the argument: if not completed
	rows = DB_Handle.query_table("tbl_Schedule",None,'"completed" == "No" or "no"')
	if not rows:
		print "There are no incomplete tasks scheduled."
		return None
	else:
		now = int(time.time()) ## Get current time
		## filter out the tasks that do not need to be done now.
		rows = [row for row in rows if row["timestamp"] < now]
		if rows:
			return rows
		else:
			return None

## Drop the baseline:
## Maybe parse the type of baseline measurement type like: dns_ipv6.
def do_baseline():
## Check for probes in the scheduler state
	probe_list = Scheduler.targeted
	if not probe_list:
		## Get all IPv6 probes with status 1
		probe_list = Probes.ret_ipv6_probes()
	## Atals_Query.base_line_dns returns a lits of ids of meassurements that were sucessful
	Measurements = Atlas_Query.baseline_dns(probe_list)
	## Task - What needs to be done after this has finished.
	task = "read_measurements"
	## stop time
	stop_time = Processing.measurement_info(measurements[len(measurements)-1])['stop_time'] ## Unix timestamp here
	## Whether the task is persistent like conducting measurements on a regular basis
	schedule_task(task,stop_time,str(measurements))

## This method is for testing purposes only.
def add_task():
	measurements = [1034087,1034088,1034089] ## temp list of measurements
	task = "process_measurements"
	stop_time = Processing.measurement_info(measurements[len(measurements)-1])['stop_time'] ## Unix timestamp here
	schedule_task(task,stop_time,str(measurements))

def schedule_task(task,stop_time,arguments=None):
## The task will be stored in the database.
## First gather all the information
	## stop time in a date/time format (for humans)
	date_time = DB_Handle.get_timestamp_to_dt(stop_time)
	## Completed
	completed = "No" ## This will always be no since it is inserted now.
	## Get the columns
	columns = DB_Handle.get_tbl_columns("tbl_Schedule")[1:] ## Exclude the sched_num autonumbered column
	## Now insert.
	## A true will be returned if the task was successfully inserted, false if it wasn't.
	print columns
	if not arguments:
		columns = columns[:1] + columns[2:]
		ret = DB_Handle.list_insert("tbl_Schedule",(columns,[task,date_time,stop_time,completed]))
	else:
		ret = DB_Handle.list_insert("tbl_Schedule",(columns,[task,arguments,date_time,stop_time,completed]))	
	if ret:
		print ("Task scheduled.")
	else:
		print ("Error scheduling that task.")

def task_completed(task):
	## Temp must replace ['Sched_num'] with ['sched_num']
	DB_Handle.list_update("tbl_Schedule",['"completed"',"'Yes'",' "Sched_num" == '+str(task['Sched_num'])])

## What we will do here is process the results then store them.
def process_results(task):
	measurements = task["argument"][1:-1].replace(" ","").split(",")
## Now get the results of the measurements.
	for measurement in measurements:
		response = Processing.get_results(measurement)
		if response:
			## Get measurement header info:
			header = Processing.measurement_info(measurement)
			date = DB_Handle.get_timestamp_to_dt(header['stop_time'])
			desc = header['description']
			target = header['dst_addr']
			fail_list, success_list = Processing.failed_succeeded(response,1)
			columns = DB_Handle.get_tbl_columns("tbl_Measurements")
			inserts = [str(measurement),str(date),str(target),str(desc),str(success_list)[5:-2],str(fail_list)[5:-2],str((len(success_list)+ len(fail_list)))]
			ret = DB_Handle.list_insert("tbl_Measurements",(columns,inserts))
			if ret:
				print ("Results inserted.")
				task_completed(task)
			else:
				print ("Error inserting results for measurement: "+str(measurement))

if __name__ == "__main__":
	start()
