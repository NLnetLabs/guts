#!/usr/bin/env python
#Author Warwick Louw

## This program will be called by an external scheduler
## Scheduler orcastrates the flow

import sched
import Probes
import Database
import Processing
import Atlas_Query
import Database_Handler as DB_Handle

## Scheduler class
#class Scheduler:
## Here we will initialize the values
	#targeted = []	#List of targeted probes
	#results = []	#List of results
## Get the last state
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
## Get scheduled tasks
## insert some schedule data
	do_baseline()
	#tasks = get_tasks()
## fetch state, build state, fly
	#Scheduler = Scheduler()
## call do_baseline

## See what we gotta do.
def get_tasks():
	columns, rows = DB_Handle.ret_table("tbl_schedule",'completed == "no" or "No"')
	if not rows:
		print "There are no incomplete tasks scheduled."
		return None
	else:
		print columns
		i = columns.index("timestamp")
		for row in rows:
			print i,row[i]

## Drop the baseline:
## Maybe parse the type of baseline measurement like: dns_ipv6.
def do_baseline():
## Check for probes in the scheduler state
	#probe_list = Scheduler.targeted
	#if not probe_list:
		## Get all IPv6 probes with status 1
		#probe_list = Probes.ret_ipv6_probes()
	## Atals_Query.base_line_dns returns a lits of ids of meassurements that were sucessful
	#Measurements = Atlas_Query.baseline_dns(probe_list)
	measurements = [1034087,1034088,1034089] ## temp list of measurements
	## Task - What needs to be done after this has finished.
	task = "read_measurements"
	## stop time
	stop_time = Processing.measurement_info(measurements[len(measurements)-1])['stop_time'] ## Unix timestamp here
	## Whether the task is persistent like conducting measurements on a regular basis
	persistent = "No" # reading measurements is not persistent
	schedule_task(task,stop_time,str(measurements),persistent)

def schedule_task(task,stop_time,arguments,persistent):
## The task will be stored in the database.
## First gather all the information
	## stop time in a date/time format
	date_time = DB_Handle.get_timestamp_to_dt(stop_time)
	## Completed
	completed = "No" ## This will always be no since it is inserted now.
	## Get the columns
	columns = DB_Handle.get_tbl_columns("tbl_Schedule")[1:] ## Exclude the sched_num autonumbered column
	## Now insert.
	#print (columns,[task_name,stop_time,date_time,task_persistent,completed])
	ret = DB_Handle.list_insert("tbl_Schedule",(columns,[task,arguments,date_time,stop_time,persistent,completed]))
	if ret:
		print ("Success.")
	else:
		print ("Error scheduling that task.")

## What we will do here is process the results then store them.
#def write_results():
## Now get the results of the measurements.
	#for measurement in measurements:
	#	fail_list, success_list = Processing.failed_succeeded(measurement)
## Update the scheduler state here
# 		Scheduler.Update_Targeted()
# 		Scheduler.Update_Results()

##Things scheduler will do:
##	**The output should be probes that were able to perform the baseline measurement in the last 7 days.
##		*(T) Keep track of probes that have been targeted for measurement in the past 7 days but did not participate in the measurement.
##		*(R)Keep track of the results of these measurements
##		*Remove probes that have been in more than 5 measurements within the last 7 days
## 		*Remove from probes list which have a successful result in R
##		*Remove from probes list which have 5 or more result sets in R (Can't do it.)
##		*Perform baseline measurement with the remaining probes, process results, update T and R
##	**Do not remove probes that have been in the list for more than 7 days
##	*Ipv6 capable resolvers
##	*DNSSEC capable resolvers
##	*Determine the resolver used by each probe
##	*probes with PMTU of 1500
##	*Probes with PMTU greater than 1280 but less than 1500
##	*etc..

if __name__ == "__main__":
	start()
