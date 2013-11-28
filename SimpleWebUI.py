#!/usr/bin/env python
## A quick dirty web ui

import Scheduler

def write_lines(status):
	for stat in status:
		f.writelines("\t\t\t<p>{}</p>\n".format(stat))

if __name__ == "__main__":
	f = open('Status.html','w')
	f.writelines('<html>')
	f.writelines('\n\t<head>')
	f.writelines('\n\t\t<title>Guts status</title>')
	f.writelines('\n\t\t<h1>Guts status</h1>')
	f.writelines('\n\t</head>')
	f.writelines('\n\t<body>\n')
	sch1 = Scheduler.Scheduler_IPv6_dns_Capable("IPv6_dns_Capable")
	sch2 = Scheduler.Scheduler_IPv6_ping_Capable("IPv6_ping_Capable")
	sch3 = Scheduler.Scheduler_IPv4_ping_Capable("IPv4_ping_Capable")
	write_lines(sch1.return_status())
	write_lines(sch2.return_status())
	write_lines(sch3.return_status())
	f.writelines('\n</body>')
	f.writelines("\n</html>")
	f.close()
