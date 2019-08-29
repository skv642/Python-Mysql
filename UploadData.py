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
####################All Global Variables###########################
csv.register_dialect('piper', delimiter='|', quoting=csv.QUOTE_NONE)
curscript = os.path.realpath(__file__)
scriptname = os.path.basename(__file__)
dtet='{:%d-%b-%Y|%H-%M-%S}'.format(datetime.datetime.now())
dte='{:%d-%b-%Y_%H}'.format(datetime.datetime.now())
home = '/ascsg/pythonproject/'
hmepy = home+'pyscripts/ascweb/'
hmeconf = hmepy+'conf/'
hmeinf = hmepy+'inputfiles/'
logdir = hmepy+'logdir/'
resfile = logdir+scriptname+'-'+dte+'-res-file.txt'
resf = open(resfile,'w')
errlog = logdir+scriptname+'-'+dte+'-err-file.txt'
text_file = open(errlog,'w')
dbconnectfl = hmeconf+'dbconnector.cf'
srvip = socket.gethostbyname(socket.gethostname())
srvname = socket.gethostname()
uniquec = 0
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

def conn_details(ip, tbname, dbname):
	global usr, sec, cur, connection
	if ip != "":
		dbconflh = open(dbconnectfl, 'r')
		mtchstr = srvip+'|'
		for line in dbconflh:
			match_res = re.match(mtchstr,line)
			if match_res.group(0) != "":
				# Get the DB name, user and credential from DB connector file.
				ip, usr, sec, db_name = line.split('|')
				txt = dtet + '|At IP-' + ip + ', DB name=' + db_name + 'and DB user=' + usr
				displayout(txt,'b',errlog)
                txt = 'Database connection in progress..'
                displayout(txt,'b',errlog)
                if ip != "":
                    connection = MySQLdb.connect(host = ip ,user = usr,passwd = sec)
                    cur = connection.cursor()
                else:
                    txt = 'No IP specified'
                    displayout(txt,'b',errlog)

def tb_print_real(ip, tbname, dbname):
        global dbexists,tbexistindb,tbfexist,dbTableArray,cur
        conn_details(ip, tbname, dbname)
        if ip != "":
            if usr != "":
			#txt = 'Database connection in progress..'
			#displayout(txt,'b',errlog)
                if sec != "":
                    txt = dtet + '|Connecting to databases at IP=' + ip + ' as User=' + usr
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
                                #qry2 = 'USE '+ dbname
                                #cur.execute(qry1)
                                qry3 = 'SHOW columns FROM '+ tbname
                                cur.execute(qry3)
                                dbTableArray = list(cur.fetchall ())
                            #else:
                            #    tbexistindb = 'false'
                            #return tbexistind
                            ## End of for loop for checking the tables existence in specific DB#####
                else:
                    txt = 'sec not found in db configuration file.'
                    displayout(txt,'b',errlog)
            else:
                txt = 'No User specified'
                displayout(txt,'b',errlog)




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

def readheaders(filename):
    global rowcol, csvFileArray
    txt = dtet + '===============***********=============='
    displayout(txt,'b',errlog)
    txt = dtet + '|Start of script' + curscript
    displayout(txt,'b',errlog)
    if len(sys.argv) != 0:
        fname = filename
		#rowcol = int(sys.argv[2])
        rowcol = 100
        txt = dtet + '|Given CSV file Name : ' +fname
        displayout(txt,'b',errlog)
        check_file(fname)
        with open(fname) as csvfname:
            csvFileArray = list(csv.reader(csvfname, delimiter=','))
    else:
        txt = dtet + '|Maybe you missed the file ?'
        displayout(txt,'b',errlog)
        txt = dtet + '|Usage ' + curscript + ' [CSV filename]'+ '[Number of rows to print]'
        displayout(txt,'b',errlog)

def upload_content_from_file(dbn, tbn, fnam):
    fname = fnam
    dname = dbn
    tbname = tbn
    uniquel = []
    uniquev = []
    up_res_filen = '/ascsg/pythonproject/pyscripts/ascweb/logdir/'+tbname+'-up-res-file-'+dte
    up_err_filen = '/ascsg/pythonproject/pyscripts/ascweb/logdir/'+tbname+'-up-err-file-'+dte
    up_res_fileh = open(up_res_filen, 'w')
    up_err_fileh = open(up_err_filen, 'w')
    csvFileObject = csv.reader(fname)
    with open(fname) as csvfname:
        csvFileArray = list(csv.reader(csvfname, delimiter=','))
    csvrowcount = len(csvFileArray)-1
    text = 'No of records to be uploaded = '+str(csvrowcount)
    up_res_fileh.write(format(text) + "\n")
    ca = "','"
    dq = '"'
    sq = "'"
    slash = '\\'
    fslash = '/'
    atr = '@'
    qr1 = 'USE '+ dbname;
    cur.execute(qr1)
    cur.execute ('SHOW columns FROM '+ tbname)
    dbTableArray = list(cur.fetchall ())
    header = ''
    cno = 1
    dlist = ['100','101']
    #dtestr = sq+'%m'+fslash+'%d'+fslash+'%Y'+sq
    dtestr = sq+'%Y'+'-'+'%m'+'-'+'%d'+sq
    setword = ''

    for line in dbTableArray:
        if cno in dlist:
            header = header + atr + line[0] + ','
            #print cno
            setword = setword + line[0] + ' = ' + 'STR_TO_DATE(@' + line[0]+ ',' + dtestr + ')' + ','
        else:
            header = header + line[0] + ','
            #print cno 
        cno = cno + 1
    header  = header.rstrip(',')
    setword = setword.rstrip(',')
    query1 = 'LOAD DATA INFILE '+ sq + fname + sq + ' IGNORE INTO TABLE '+ tbname +  ' CHARACTER SET latin1 FIELDS TERMINATED BY '+ ca + ' OPTIONALLY ENCLOSED BY ' + sq + dq + sq + ' LINES TERMINATED BY ' + sq + slash + 'n' + sq + ' IGNORE 1 LINES'

    if cno in dlist:
        print 'ending here'
        query2 = query1 + ' (' + header + ') ' +   'SET ' + setword + ';'
    else:
        query2 = query1 + ' (' + header + ')'+';'
    
    tmpstderr = sys.stderr
    sys.stderr = up_err_fileh
    cur.execute(qr1)
    cur.execute(query2)
    connection.commit()
    rows_uploaded = str(cur.rowcount)
    sys.stderr = tmpstderr
    errline = open(up_err_filen).read()
    rows_rejected = errline.count('Duplicate')
    text = 'No. of rows uploaded|affected = '+ rows_uploaded
    up_res_fileh.write(format(text) + "\n")
    text = 'No. of rows rejected due to duplicate = '+ str(rows_rejected)
    up_res_fileh.write(format(text) + "\n")
    if rows_rejected > 0 :
        text = 'Certain records were not uploaded. The record and reason for failure is mentioned in file '+ up_err_filen
        up_res_fileh.write(format(text) + "\n")
    
    for row in csvFileArray:
        uniquel.append(row[(uniquec-1)])
    
    ss = [" ".join(re.findall("[a-zA-A0-9]+",s)) for s in uniquel]
        #print ss
    uniquev = "".join("{0}|".format(w) for w in ss)
    uniquev = sq+uniquev.rstrip('|')+sq
        #uniquev = uniquev[4:len(uniquev)]
    #print uniquev
    query2 = 'SELECT ' + dbTableArray[(uniquec-1)][0] + ' FROM ' + tbname + ' WHERE ' + dbTableArray[(uniquec-1)][0] + ' REGEXP ' + uniquev + ';'
    #print query2
    cur.execute(query2)
    fr = cur.fetchall()
    valfrl = []
    for valfr in fr:
        valfr = " ".join(re.findall("[a-zA-A0-9]+",valfr[0]))
        valfrl.append(valfr)

    checkpresence = 0
    for val in uniquel:
        val = " ".join(re.findall("[a-zA-A0-9]+",val))
        #print val
        if val not in valfrl:
            text = 'Record with unique ID = '+ val+' not in table'
            up_err_fileh.write(format(text) + "\n")
        else:
            checkpresence +=1
            text = 'Record with unique ID = ' + val + ' present at |' + str(valfrl.index(val))
            up_res_fileh.write(format(text) + "\n")

    querycount = str(len(valfrl))
    csvarraycount = str(csvrowcount)
    text = 'Number of records with unique ID present in the table ' + tbname + ' = ' + querycount
    up_res_fileh.write(format(text) + "\n")
    #up_res_fileh.close()
    #up_err_fileh.close()
    #up_res_fileh = open(up_res_filen, 'w+')
    #up_err_fileh = open(up_err_filen, 'w')
    if querycount == csvarraycount:
        text = 'All uniqie records in CSV['+csvarraycount+']'+fname+' is uniquely present in '+tbname+'['+querycount+']'
        up_res_fileh.write(format(text) + "\n")
    elif csvarraycount == str(checkpresence):
        text = 'All records in CSV['+csvarraycount+']'+fname+' is present in '+tbname+'['+str(checkpresence)+']'
        up_res_fileh.write(format(text) + "\n")
    else:
        diff = int(csvarraycount) - checkpresence
        text = str(diff) +' records in CSV ' + fname + ' is not found in table '+tbname
        up_res_fileh.write(format(text) + "\n")
        text = 'No. of records in CSV =' + csvarraycount + "\n" + 'No. of records of CSV found in table =' + querycount +"\n" + 'No. of records of CSV passed checking ='+str(checkpresence)
        up_res_fileh.write(format(text) + "\n")
    up_res_fileh.close()
    up_err_fileh.close()
   
def main():
        global tbname, dbname, fname, uniquec
        txt = dtet + '===============***********=============='
        displayout(txt,'b',errlog)
        txt = dtet + '|Start of script' + curscript
        displayout(txt,'b',errlog)
        if len(sys.argv) == 5:
            dbname = sys.argv[1]
            tbname = sys.argv[2]
            fnamevar = sys.argv[3]
            uniquec = int(sys.argv[4])
            fname = hmeinf+fnamevar
            txt = dtet + '|Given Table Name file is : ' +tbname
            displayout(txt,'b',errlog)
            txt = dtet + '|Given Database Name is : '+ dbname
            displayout(txt,'b',errlog)
            tbcfname = hmeconf+tbname+'-tb.cf'
            txt = dtet + '|CSV upload file name :'+ fname
            displayout(txt,'b',errlog)
            check_file(fname)
            txt = dtet + "|Existing DB's and Tables at " + srvip + 'is listed in '+ resfile
            displayout(txt,'b',errlog)
            #conn_details(srvip, tbname, dbname)
            tb_print_real(srvip, tbname, dbname)
            txt = dtet + '\t|DB Exsist=' + dbexists + '\t |Table exist in DB='+ tbexistindb + '\t |CSV file exists=' + fexist + '\t |CSV file size=' + fsize
            displayout(txt,'b',errlog)
            if tbexistindb == 'Not Null':
                txt = tbname + ' is not part of tables in DB ' + dbname + ' May be different TB name?'
                displayout(txt,'b',errlog)
            elif dbexists == 'false' and tbexistindb == 'true':
                txt = dbname + ' does not exist. Quit! '
                displayout(txt,'b',errlog)
            elif dbexists == 'false' and tbexistindb == 'false':
                txt = dbname + ' does not exist. Quit! '
                displayout(txt,'b',errlog)
            elif fexist == 'false':
                txt =  'CSV upload file|'+ fname +' does not exist. Quit! '
                displayout(txt,'b',errlog)
            elif fsize == 'Null':
                txt =  'Table definition file|'+ tbcfname +' is null. Quit! '
                displayout(txt,'b',errlog)
            else:
                if tbexistindb == 'true' and fexist == 'true' and fsize == 'true':
                    readheaders(fname)
                    csvheader = []
                    for j in  csvFileArray[0]:
                        cname2 = j.replace(" ","")
                        cname = cname2.replace("/","")
                        csvheader.append(cname)
                    csvhdct = 0 
                    headererr = 'false'
                    for i in dbTableArray:
                        if (i[0] == csvheader[csvhdct]):
                            txt = dtet+'|Header '+ i[0] + ' from Table '+ tbname + 'is same as '+ csvheader[csvhdct] + ' from csv upload file.'
                            displayout(txt,'b',errlog)
                        else:
                            print 'Headers of CSV doesnt match the table'
                            headererr = 'true'
                        csvhdct += 1
                    if (headererr == 'false'):
                        txt =  dtet+'|Proceeding to upload '+ fname + ' in '+ ' the table '+tbname+' residing in '+dbname
                        displayout(txt,'b',errlog)
                        txt = '\nFor table Upload, Please respond with "yes|y|ye to proceed" or "no|q|quit to quit" : '
                        query_yes_no (txt)
                        txt =  dtet+'|Proceeding to upload '+ fname + ' in '+ ' the table '+tbname+' residing in '+dbname
                        upload_content_from_file(dbname, tbname, fname)
                    else:
                        txt =  'Headers doesnt match. Quit'
                        displayout(txt,'b',errlog)
                        exit
                        
                else: 
                    txt =  'Unknown Error '+ fname +' Failing despite the best efforts, Quit! '
                    displayout(txt,'b',errlog)                
        else:
            #conn_details(srvip)
            txt = dtet + '|Syntax error| Not enough arguments'
            displayout(txt,'b',errlog)
            txt = dtet + '|Usage ' + curscript + ' [DB name] [tb name] [csv filename] [Unique Column index]'
            displayout(txt,'b',errlog)
            exit()
        connection.close()
main()
