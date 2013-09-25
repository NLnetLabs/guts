#============================================
#=#	Only works with public measurements		=
#=#	for private measurements include -  	=
#=#	mechanize.								=
#=#	code:									=
#=     import mechanize					 	=
#=											=
#=		br = mechanize.Browser()           	=
#=		br.open("https://access.ripe.net") 	=
#=		br.select_form(nr=1)               	=
#=		br['username'] = '<your email>'    	=
#=		br['password'] = '<your password>' 	=
#=                                        	=
#=		br.submit()							=
#=#C-sauce: https://atlas.ripe.net/doc/code	=
#============================================

import urllib
import urllib2
import json

def json_writer(st):
	f = open('measurements.txt','a')
	f.writelines(st)
	f.close()

def get_measurement(m):
	me = urllib2.urlopen().read()

def get_json(url):
	req = urllib2.urlopen(url).read()
	return req
#----------------------------------
def ret_json_as_text(js): # AAWW YISS!! SWEET RECURSION(AWW YISS!! SWEET RECURSION(...))!!!!
	if type(js) == list:
		ret_json_list(js)
	elif (type(js) == dict):
		ret_json_dict(js)
	else:
		print 'panic..',js
#----------------------------------
#Values need to be halted until the tails of all lists are found.
#That is why there are external methods to find these.
#----------------------------------
def ret_json_list(lis):
	for x in lis:
		if type(x) == list:
			ret_json_list(x)
		elif type(x) == dict:
			ret_json_dict(x)

def ret_json_dict(dic):
	for k,v in dic.iteritems():
		if type(v) == list:
			print k,":{",
			ret_json_list(v)
		else:
			print "{",k,":",v,"},"

#----------------------------------
if __name__ == "__main__":
	mes = []
	mes.append( 1027576 ) #Longer request
	#~ mes = 1000228 # <- Private measurement, will not work without auth(see top).
	#~ mes.append( 1027585 ) #Even longer request
	#~ mes.append( 1026850 )
	for x in mes:
		url = "https://atlas.ripe.net/api/v1/measurement/"+str(x)+"/result/"
		s = ""
		req = get_json(url)
		req = json.loads(req)
		print json.dumps(req,indent=2,sort_keys=True)
		ret_json_as_text(req)
