## In the end we want the info for each anchor
import urllib2

def scout():
	url = "https://atlas.ripe.net/anchors/list/?page="
	data = True
	pages = 0
	while data:
		try:
			r = urllib2.urlopen(url+str(pages + 1)).read()
			pages = pages + 1
		except urllib2.HTTPError as e:
			data = False
	return pages

def AnchorList():
	url = "https://atlas.ripe.net/anchors/list/?page="
	pages = scout()
	anchor_list = []
	for page in range(1,pages+1):
		r = urllib2.urlopen("https://atlas.ripe.net/anchors/list/?page={}".format(page)).read()
		r = r.split("<tbody>")[1].split("</tbody>")[0] ## Get to where the magic is at.
		data_list = r.split('<td class="hostname">')[1:]## Remove begining stuff.
		for an in data_list:## Iterate through this beast to get what we want.
			an = [x.replace("&#32;","").replace("<td>","") for x in an.split("</td>")]## remove extra chars
			anchor = {"hostname":an[0]}
			for i in range(1,7):
				key = an[i].split('"')[1]
				val = an[i][an[i].index(">")+1:]
				anchor[key]=val
			anchor_list.append(anchor)
	return anchor_list ## return the list of anchor dictionaries.

if __name__ == "__main__":
	anchors = AnchorList()
	for anch in anchors:
		print anch
