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
import shutil
####################All Global Variables###########################
####################All Global Variables###########################
curscript = os.path.realpath(__file__)
scriptname = os.path.basename(__file__)
home = '/ascsg/pythonproject/'
hmepy = home+'pyscripts/ascweb/'
hmeconf = hmepy+'conf/'
hmeinf = hmepy+'inputfiles/'
logdir = hmepy+'logdir/'
dte='{:%d-%b-%Y_%H-%M-%S}'.format(datetime.datetime.now())
dtet='{:%d-%b-%Y|%H-%M-%S}'.format(datetime.datetime.now())
resfile = logdir+scriptname+'-'+dte+'-res-file.txt'
resf = open(resfile,'w')
errlog = logdir+scriptname+'-'+dte+'-err-file.txt'
text_file = open(errlog,'w')
dbconnectfl = hmeconf+'dbconnector.cf'
srvip = socket.gethostbyname(socket.gethostname())
srvname = socket.gethostname()
global fname, rows_per_csv, nofiles, targetflloc
########## Variables in Main function ##########
fexist = 'Not Null'
fsize = 'Not Null'
dbexists = 'Not Null'
tbexistindb = 'Not Null'
tbfexist = 'Not Null'
########## Variables in readheaders function ##########
rowcol = 0
csvFileArray = []
########## Variables in readheaders function ##########
########## Variables in get_tb_columns ##########
rowcol = 0
dbTableArray = []
########## Variables in get_tb_columns ##########


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
	global fexist, fsize
        if filename != "":
                fileexists = os.path.isfile (filename)
                if fileexists:
                        txt = dtet+'|The CSV file '+ filename + ' exists.'
                        displayout(txt,'b',errlog)
                        fexist = 'true'
                        filesize = os.path.getsize (filename)
                        if  filesize > 0:
                            txt = dtet+'|The CSV file '+ filename + ' size is not 0'
                            displayout(txt,'b',errlog)
                            fsize = 'true'
                else:
                        txt = dtet+'|The CSV file '+ filename +' does not exists.'
                        displayout(txt,'b',errlog)
                        fexist = 'false'
                        exit()
        else:
                txt = 'The CSV file name is invalid'
                displayout(txt,'b',errlog)

def splitfiles(filename):
    file_name = targetflloc +'/'+ fname.replace('.csv','')
    totfiles = str(nofiles)
    
    with open(filename) as infile:
        reader = csv.DictReader(infile)
        header = reader.fieldnames
        rows = [row for row in reader]
        pages = []
        row_count = nofiles * rows_per_csv
        print row_count, rows_per_csv
        start_index = 0
        while start_index < row_count:
            pages.append(rows[start_index: start_index+rows_per_csv])
            start_index += rows_per_csv

            for i, page in enumerate(pages):
                with open('{}_{}.csv'.format(file_name, i+1), 'w+') as outfile:
                    writer = csv.DictWriter(outfile, fieldnames=header)
                    writer.writeheader()
                    for row in page:
                        writer.writerow(row)

        txt = dtet + '|Done splitting ' + filename + ' into ' + totfiles + ' files. The test files resides at ' + targetflloc + '. Enjoy testing!'
        displayout(txt,'b',errlog)
        
    
def main():
        global fname, rows_per_csv, nofiles, targetflloc
        txt = dtet + '===============***********=============='
        displayout(txt,'b',errlog)
        txt = dtet + '|Start of script' + curscript
        displayout(txt,'b',errlog)
        if len(sys.argv) == 4:
            fname = os.path.basename(sys.argv[1])
            absfilename = os.path.realpath(sys.argv[1])
            check_file(absfilename)
            targetdir = absfilename.replace(fname,'')
            targetdrname = fname.replace('.csv','')
            targetflloc = targetdir + 'test-files-' + targetdrname
            direxists = os.path.isdir(targetflloc)
            if direxists:
                shutil.rmtree(targetflloc)
                os.mkdir(targetflloc)
            else:
                os.mkdir(targetflloc)
            
            rows_per_csv = int(sys.argv[2])
            nofiles = int(sys.argv[3])
            splitfiles(absfilename)
        else:
            txt = dtet + '|Syntax error| Not enough arguments'
            displayout(txt,'b',errlog)
            txt = dtet + '|Usage ' + curscript + ' [CSV filename] [No. of rows] [No of files to create]'
            displayout(txt,'b',errlog)
            exit()

            
main()
