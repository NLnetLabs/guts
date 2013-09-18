import urllib
import urllib2
import json

def main():
	jsonList = []
	lis = country()
	f = open('probes.txt','a')
	for x in lis:
		req = urllib2.urlopen("https://stat.ripe.net/data/country-resource-list/data.json?resource="+x).read()
		js = json.loads(req)
		for x in js['data']['resources']['asn']:
			f.writelines(str(x)+"\n")

def country():
	countryList = []
	page = urllib2.urlopen('http://countrycode.org/').read()
	page = str(page)
	for x in page.split('<td align="center">'):
		for y in str(x).split(' / '):
			if (len(y) == 2):
				countryList.append(y)
	return countryList
main()
