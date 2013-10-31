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
class Scheduler:
## Get the last state
## Total is the number of times a measurement has run in the past 7 days
## Results are the results of those measurements
	def get_Total():
	def get_results():
## Update the current state
	def Update_Total():
	def Update_Results():
## Set the state in the database
	def set_Total():
	def set_Results():

## Start
def start():
## fetch state, build state, fly
Scheduler = Scheduler()

## Drop the baseline:
def do_baseline():

## Get all IPv6 probes with status 1
	probe_list = Probes.ret_ipv6_probes()
## Or use the probes from the scheduler databse
## <- Maybe a scheduler fetch state method here

## Baseline Measurement DNS with all probes
## Use the results from the above list
	size = 512 #(Smallest allowed)
## Atals_Query.base_line_dns returns a lits of measurement ids that were sucessful
	Measurements = Atlas_Query.base_line_dns(probe_list)
## <- Schedule the reading of the measurements
## First we need to get the stop time of the measurements, we will use the stop time of the last measurement.
## This will be a safe method since the measurements are in sucession of one another.
	stop_time = measurement_info(mesurements[:len(Measurements)-1])['stop_time']
## The problem with the scheduler is that there is no way to get the values returned by the methods called.
## So.. We will need to store the results into the database, fetch them then proceed
	shd.enterabs(stop_time,1,DB_Handle.process_and_store,(measurements,))
## The program will now halt here until the stop time.
## If anything else needs to be done now would be a good time to start a thread and do it. The thread can be killed after. Just a note.
	print ("The program will now halt until the measurements have stopped")
	shd.run()
## Now get the results of the measurements.
	fail_list = []
	success_list = []
	for measurement in measurements:
## Update the scheduler state here
# 		Scheduler.Update_Total()
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

#if __name__ == "__main__":
# 	Start()
