#!/usr/bin/env python
## Configuration creator

import sys
import json

class config:
	
	def __init__(self):
		if sys.platform == "linux" or sys.platform == "linux2":
			 self.os = "Linux"
		elif sys.platform == "darwin":
			self.os = "OS X"
		elif sys.platform == "win32":
			self.os = "Windows"
		self.version = float(sys.version.split("(")[0][:3])
		
	def set_key_location(self,loc):
		self.key_location = loc

	def set_DBE(self,db):
		self.DBE = db

	def set_DB_location(self,loc):
		self.DB_location = loc

	def get_os(self):
		return self.os

	def get_version(self):
		return self.version

	def get_key_location(self):
		return self.key_location

	def get_DBE(self):
		return self.DBE

	def get_DB_location(self):
		return self.DB_location

if __name__ == "__main__":
	conf = config()
	banner = 	" _______    \t\t\t________\t________\n"
	banner += 	"|\t    \t|\t|\t   |   \t\t|\n"
	banner += 	"|\t    \t|\t|\t   |   \t\t|\n"
	banner += 	"|    ___\t|\t|\t   |   \t\t|________\n"
	banner += 	"|      |\t|\t|\t   |   \t\t\t|\n"
	banner += 	"|      |\t|\t|\t   |   \t\t\t|\n"
	banner += 	"|______|\t|_______|\t   |\t\t________|\n"
	banner +=	"\t\t\tNLnetLabs"
	print (banner) ## banner is: GUTS \n NLnetLabs
	print ("Hello and welcome!\nYou must be new here.")
	print ("Would you like to use the default settings?(Y/N)")
	if raw_input().lower() == 'y':
		if conf.os == "Linux" or conf.os == "Linux2" or conf.os == "OS X":
			conf.set_key_location("~/.atlas/auth")
		else:
			conf.set_key_location("C:/Atlas/key/auth.txt")
		conf.DBE = "sqlite"
		conf.DB_location = "Atlas.db"
	else:
		print ("Please enter the location of your api key file")
		conf.set_key_location(raw_input())
		print ("Please selece your favoured choice of database:")
		print ("1) SQLite\n2) MySQL")
		if int(raw_input()) == 1:
			conf.DBE = "sqlite"
			print ("Please specify the location of the database")
			conf.set_DB_location(raw_input())
		else:
			conf.DBE = "mysql"
			conf.set_DB_location("localhost")
	js = [{"os":conf.get_os(),"key_location":conf.get_key_location(),"version":conf.get_version(),"DBE":conf.get_DBE(),"DB_location":conf.get_DB_location()}]
	with open('config.json','w') as outfile:
		json.dump(js,outfile)
	
