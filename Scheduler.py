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
	tasks = get_tasks()
## fetch state, build state, fly
	#Scheduler = Scheduler()
## call do_baseline

## See what we gotta do.
def get_tasks():
	results = DB_Handle.ret_table("tbl_schedule")

## Drop the baseline:
def do_baseline():
## Check for probes in the scheduler state
	#probe_list = Scheduler.targeted
	if not probe_list:
## Get all IPv6 probes with status 1
		probe_list = Probes.ret_ipv6_probes()
## Baseline Measurement DNS with all probes
## Use the results from the above list
	size = 512 #(Smallest allowed)
## Atals_Query.base_line_dns returns a lits of measurement ids that were sucessful
	Measurements = Atlas_Query.baseline_dns(probe_list)
## <- Schedule the reading of the measurements
## First we need to get the stop time of the measurements, we will use the stop time of the last measurement.
## This will be a safe method since the measurements are in sucession of one another.
	stop_time = measurement_info(mesurements[:len(Measurements)-1])['stop_time'] ## Unix timestamp here
## <- schedule reading the measurements

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
##		*(T)Keep track of how many times each measurement has been done in the past 7 days
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
