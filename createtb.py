#!/usr/bin/env python
import sys
import os
import socket
#import xlrd
import csv
import numpy as np
import re
import string
import MySQLdb
import datetime
####################All Global Variables###########################
csv.register_dialect('piper', delimiter='|', quoting=csv.QUOTE_NONE)
curscript = os.path.realpath(__file__)
scriptname = os.path.basename(__file__)
home = '/ascsg/pythonproject/'
hmepy = home+'pyscripts/ascweb/'
hmeconf = hmepy+'conf/'
logdir = hmepy+'logdir/'
resfile = logdir+scriptname+'res-file.txt' 
resf = open(resfile,'w')
errlog = logdir+scriptname+'err-file.txt'
text_file = open(errlog,'w')
dbconnectfl = hmeconf+'dbconnector.cf'
srvip = socket.gethostbyname(socket.gethostname())
srvname = socket.gethostname()
dte='{:%d-%b-%Y|%H-%M-%S}'.format(datetime.datetime.now())
########## Variables in Main function ##########
tbexistindb = 'false'
dbname = ' '
tbname = ' '
tbcfname = ' '
########## Variables in Check_file function ##########
filename = ' '
tbfexist = 'false'
tbfsize = 'Not Null'
########## Variables in conn_details ##########
global usr, sec

########## Variables in tb_print_real function ##########
dbexists = 'false'


def displayout(text,option,logfile):
	if logfile == errlog:
		if option == 'b':
			print text, '\n'
			text_file.write(format(text) + "\n")
		elif option == 'o':
			print text, '\n'
		else:
			text_file.write(format(text) + "\n")
			print text, '\n'
	elif logfile == resfile:
		resf.write(format(text) + "\n")
	else:
		print text, '\n'

def query_yes_no(question):
	yes = {'yes','y', 'ye'}
	no = {'no','n','q', 'quit'}
	while True:
		sys.stdout.write(question)
		choice = raw_input().lower()
		if choice in yes:
			return True
		elif choice in no:
			quit()
		else:
			txt = 'The given choice "' + choice + '" doesnt match any available option'
			sys.stdout.write(txt)		

def check_file(filename):
	global tbfexist,tbfsize
	if filename != "":
		fileexists = os.path.isfile (filename)
		if fileexists:
			txt = dte+'|The table definition file '+ filename + ' exists.'
			displayout(txt,'b',errlog)
			tbfexist = 'true'
			filesize = os.path.getsize (filename)
			if  not filesize > 0:
				txt = dte+'|The table definition file '+ filename + ' size is 0. Quit'
				displayout(txt,'b',errlog)
				tbfsize = 'Null'
		else:
			txt = dte+'|The table definition file '+ filename +' does not exists.'
			displayout(txt,'b',errlog)
			tbfexist = 'false'
			exit()
	else:
		txt = 'The table configuration file name is invalid'
		displayout(txt,'b',errlog)

def create_table_from_file(filename):
	column_str = ''
	tbconflh = open(filename, 'r')
	for line in tbconflh:
		if not line.lstrip().startswith('#'):
			column_attr = line.replace('|',' ').replace ("\n",'')
			column_str = column_str + column_attr + ', '
	length = len(column_str)
	create_qry = 'CREATE TABLE ' + tbname + "(" + column_str[0:(length-2)] + ")"
	txt = 'Table '+tbname+' with '+ 'column and attribes as below will be created \n' + create_qry
	displayout(txt,'b',errlog)   
	txt = '\nFor table creation, Please respond with "yes|y|ye to proceed" or "no|q|quit to quit" : '
	query_yes_no (txt)
	connection = MySQLdb.connect(host = srvip,user = usr,passwd = sec,db = dbname)
	cur = connection.cursor()
	cur.execute(create_qry)
	

def conn_details(ip, tbname, dbname):
	global usr, sec, cur
	if ip != "":
		dbconflh = open(dbconnectfl, 'r')
		mtchstr = ip+'|'
		for line in dbconflh:
			match_res = re.match(mtchstr,line)
			if match_res.group(0) != "":
				# Get the DB name, user and credential from DB connector file.
				ip, usr, sec, db_name = line.split('|')
				txt = dte + '|At IP-' + ip + ', DB name=' + db_name + 'and DB user=' + usr
				#displayout(txt,'b',errlog)
                txt = 'Database connection in progress..'
                displayout(txt,'b',errlog)
                connection = MySQLdb.connect(host = ip ,user = usr,passwd = sec)
                cur = connection.cursor()
                #tb_print_real(ip, usr, sec, tbname, dbname)
				#return tbexistindb
					
def tb_print_real(ip, tbname, dbname):
    global dbexists,tbexistindb
    conn_details(ip, tbname, dbname)
    if ip != "":
		if usr != "":
			txt = 'Database connection in progress..'
			displayout(txt,'b',errlog)
			if sec != "":
				txt = dte + '|Connecting to databases at IP=' + ip + ' as User=' + usr
				displayout(txt,'b',errlog)
				#connection = MySQLdb.connect(host = ip ,user = usr,passwd = sec)
				#cur = connection.cursor()
				cur.execute("SHOW databases")
				dbes = cur.fetchall()
				dbesout0 = re.sub(r"[(,\']","",str(dbes))
				dbesout = str(re.sub(r"[)]","|",dbesout0))[:-1]
				txt = "Databases in this server are :" + dbesout
				displayout(txt,'b',errlog)
				## Start of for loop for checking the tables existence in specific DB#####
				for i in dbes: 
					if dbname == i[0]:
						txt = dbname + ' is part of db`s' 
						displayout(txt,'b',errlog)
						dbexists = 'true'
					#else:
					#	if dbexists == 'false':
					#	txt = dbname + ' is NOT part of db`s '
					#	displayout(txt,'b',errlog)
					qry1 = 'USE '+ i[0]
					cur.execute(qry1)
					cur.execute("SHOW tables")
					tables = cur.fetchall ()
					#print tables
					for j in tables:
						txt = i[0] + '\t' + j[0] + '\t'
						displayout(txt,'b',resfile)
						if dbname == i[0] and tbname == j[0]:
							txt = tbname + ' is part of tables in ' + dbname
							displayout(txt,'b',errlog)
							tbexistindb = 'true'
							#return tbexistind
						## End of for loop for checking the tables existence in specific DB#####
			else:
				txt = 'sec not found in db configuration file.'
				displayout(txt,'b',errlog)
		else:
			txt = 'No User specified'
			displayout(txt,'b',errlog)
    else:
		txt = 'No IP specified'
		displayout(txt,'b',errlog)




def main():
    global tbname, dbname
    txt = dte + '===============***********=============='
    displayout(txt,'b',errlog)
    txt = dte + '|Start of script' + curscript
    displayout(txt,'b',errlog)
    if  len(sys.argv) == 3:
	dbname = sys.argv[1]
	tbname = sys.argv[2]
	txt = dte + '|Given Table Name file is : ' +tbname
	displayout(txt,'b',errlog)
	txt = dte + '|Given Database Name is : '+ dbname
	displayout(txt,'b',errlog)
	tbcfname = hmeconf+tbname
	txt = dte + '|Table Map file name :'+ tbcfname
	displayout(txt,'b',errlog)
	check_file(tbcfname)
	txt = dte + "|Existing DB's and Tables at " + srvip + 'is listed in '+ resfile
	displayout(txt,'b',errlog)
        tb_print_real(srvip, tbname, dbname)
        txt = dte + '\t|DB Exsist=' + dbexists + '\t |Table exist in DB='+ tbexistindb + '\t |Table file exists=' + tbfexist
        displayout(txt,'b',errlog)
        if tbexistindb == 'true':
            txt = tbname + ' is part of tables in DB ' + dbname + '. Quit! '
            displayout(txt,'b',errlog)
        elif dbexists == 'false' and tbexistindb == 'true':
            txt = dbname + ' does not exist. Quit! '
            displayout(txt,'b',errlog)
        elif dbexists == 'false' and tbexistindb == 'false':
            txt = dbname + ' does not exist. Quit! '
            displayout(txt,'b',errlog)
        elif tbfexist == 'false':
            txt =  'Table definition file|'+ tbcfname +' does not exist. Quit! '
            displayout(txt,'b',errlog)
        elif tbfsize == 'Null':
            txt =  'Table definition file|'+ tbcfname +' is null. Quit! '
            displayout(txt,'b',errlog)
        else:
            txt =  'Proceeding to create '+ tbname + ' in '+ dbname + ' using the file '+ tbcfname
            displayout(txt,'b',errlog)
            create_table_from_file(tbcfname)

    else:
        #conn_details(srvip)
        txt = dte + '|Table not created as DB Name and  Table Name not specified'
        displayout(txt,'b',errlog)
        txt = dte + '|Usage ' + curscript + ' [DB name] [tb name]'
        displayout(txt,'b',errlog)

main()
