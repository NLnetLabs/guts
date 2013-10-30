import time
import sched
import socket
import threading
import Processing

def listening(thread_list):
	sch = sched.scheduler(time.time, time.sleep)
	host = 'localhost'
	port = 9999
	addr = (host,port)
	server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	server_sock.setblocking(1) # Set blocking so that connections do not interfere with one another.
	server_sock.bind(addr)
	server_sock.listen(10)
	try:
		while 1:
			print 'waiting for connection...'
			client_sock, client_addr = server_sock.accept()
			print '...connected from:',client_addr
			threading.Thread(target=sched_something, args = ((client_sock,client_addr),sch),).start()
	except:
		try:
			client_sock.close()
		except:
			pass
		server_sock.close()

def pretty_time():
	dt = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d/%H:%M:%S')
	return dt

## decypher the callers intention
## ~ Name will be changed at some point ~
def sched_something(sock,shed):
	data = sock[0].recv(512)
	#print len(str(data))
	#print type(data)
	#print data
	if str(data)[:5] == "sched":
		event = data.split(" ")[1:]
		## Do ipv6 baseline then store the results
		if event[0] == "do_ipv6_baseline":
			## Do the baseline with all availible probes.
			## delay, priority, function, args
			shed.enter(,,Processing.do_ipv6_baseline, ())
			sock[0].send("Sent queries")
			## Store the results of a specific measurement when it's done
		if event[0] == "write_measurement":
			measurement = event[1]
			measurement_info = Processing.measurement_info(measurement)
			measurement_stop = measurement_info['stop_time']
			## Add measurement_writer here
			shed.enterabs(measurement_stop,1,, ())
			## Run the scheduler on this thread
			shed.run()
			## Once this is sent the caller should close the connection on their side
			sock[0].send("Measurement set to write at: {}".format(str(pretty_time(measurement_stop)))))
	else:
		## There is no stored proceedure to schedule matching the request, try again.
		sock[0].send("Nothing to do here.")
	## Close the socket
	sock[0].close()
	
if __name__ == "__main__":
	thread_list = []
	listening(thread_list)
