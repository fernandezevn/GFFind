from __future__ import print_function
from sqlite3 import connect
import sys
import os

def help():
    info = """
**********************
	GFFind
**********************

GFFind is a GFF3 file parser that allows users to parse their file into a 
SQLite database and query the data. The results of the query are written to 
a tab delimited text file with the default name gff_search_results.txt.


Parse Mode:
	Args:
	infile: GFF3 file to be parsed
	
	Returns:
	None. Builds SQLite database but nothing is returned until Query
	Mode.

	--pr | --parse <file.gff>
	In the current version, Query Mode is the default and thus, Parse 
	Mode must be specified using the -pr flag. 

	Options:
	--bd | --build <tableName>
	The --bd option specifies the name of the database being built in 
	SQLite for the GFF3 file in the same command. This can be useful to 
	help keep track of the databases when parsing multiple files to 
	query from however, in the current version of GFFind there is no way 
	to parse multiple files in one command. Each file must be parsed 
	separately. 

	Example:
	GFFind.py --pr <path to GFF3 file> [--bd <table name>]


Query Mode:
	Query Mode is the default setting for GFFind so it has no specifying 
	flag. Because the data is stored in a SQLite database, Query Mode 
	commands follow a similar method of logic to SQLite queries (i.e. 
	where these things are true return these pieces of information).

	Args:
	table: SQLite table to be queried
	How many things count as an argument? Options too?

	Returns:
	Tab delimited table of results in a GFF3-like format.

	Options:
	--na | -name <tableName>
	Specify the database to be queried.
	-o | -output <outputFileName>
	Specify the name of the output file.
	
	--id | --seqid <chromosome number>
	--so | --source <sourceName>
	--ty | --type <featureType>
	--sc | --score <featureScore>
	--st | --strand <+ | - | . | ?>
	--ph | --phase <0 | 1 | 2>
	Most of the options for Query Mode follow the standards for the GFF3 
	file format as described here (http://gmod.org/wiki/GFF3).
	
	--at | --attribute <attr1=attr1value;attr2=attr2value>
	Selecting for a specific attribute also follows GFF3 file format 
	standards and requires that the user input their desired selection 
	in the form of a string (encased in single quotes) of semicolon 
	delimited attributes. This requires that the user know what 
	attributes are available for them to query before they query the 
	database. 
	--a | --all 
	Output the entire database (NOTE: this option is specific to the 
	RETURN portion of the latter half of the query). 
	
	The region options are specific to either the INPUT or RETURN 
	portion of the query. 
	
		INPUT:
		--rc | --regionContained 
		Elements contained within this region.
		--rb | --regonBeginning
		Elements that begin within this region. 
		--re | --regionEnd
		Elements that end within this region.
		--rs | --regionSpan
		Elements that span this region.

		Example:
		-rx <1234-5678>

		RETURN:
		-rg | region
		The region of each element matching the input criteria.
		-bg | -beginning
		The start point of each element matching the input criteria.
		-en | -end
		The end point of each element matching the input criteria.
		
		
Example:
GFFind.py [-na <tableName>] [-o <outputFileName>] [-id <idValue>] 
[-so <sourceName>] [-ty <featureType>] [-sc <featureScore>] 
[-st <+ | - | . | ?>] [-ph <0 | 1 | 2>] 
[-at <'at1=value1;at2=value2;at3=value3'>] [-rx <1234-5678>] return 
[-id] [-so] [-ty] [-sc] [-st] [-ph ] [-rg] [-bg] [-en] [-a] 
"""
    return info


def parseFile(infile,table='gff'):
    added = []
    gff = open(infile, 'r')
    conn = connect('gffdb.sqlite')
    curs = conn.cursor()
    #See if table exists and do naming
    name = createTableName(table)
    sql = 'Create table ' + name + '(seqid TEXT DEFAULT ".",'
    sql += 'source TEXT DEFAULT ".",type TEXT DEFAULT ".",'
    sql += 'beginning INTEGER DEFAULT ".",end INTEGER DEFAULT ".",'
    sql += 'score REAL DEFAULT ".",strand TEXTDEFAULT ".",'
    sql += 'phase INTEGER DEFAULT ".")'
    
    curs.execute(sql)
    conn.commit()
    num_rows = 0
    #Go through each line in the file and add it to SQLite db
    for line in gff:
        #Make sure it's not a comment line 
        if(not line.startswith('#')):
            num_rows = num_rows + 1
            line = line.strip()
            #Split on tab since gff is tab delim
            row = line.split("\t")
            #Initiate vars
            ref = {}
            attr_cols = ''
            keys = []
            vs = []
            #Split up the attributes column and prepare for SQLite
            for attr in row[8].split(";"):
                if('=' in attr):
                    key = attr.split("=",1)[0]
                    val = attr.split("=",1)[1]
                    keys.append(key)
                    vs.append(val)
                    ref[key] = val
                    #Make sure the column isn't already in table before adding
                    if(not key in added):
                        #Add column to table
                        curs.execute('ALTER TABLE ' + name + ' ADD ' + key + ' TEXT DEFAULT "."')
                        conn.commit()
                        added.append(key)
                        
                        
            attr_cols = attr_cols + ','.join(keys)
                #Need to quote strings in sql statement!!
            vals = ''
            for i in xrange(0,8):
                #Creates the insert statement section for required columns.
                #Makes sure that strings are enclosed in quotes and numerics are not
                if(row[i] == '.'):
                    vals = vals + '"' + row[i] + '"'+ ','
                else:
                    if(i == 3 or i==4 or i==5 or i==7):
                        vals = vals + row[i] + ','
                    else:
                        vals = vals + '"' + row[i] + '"'+ ','
            vals = vals.rstrip(',')
            
            #Create part of statement for attributes 
            attrs = ','
            for curr in vs:
                attrs = attrs + '"' + curr + '"' + ','
            attrs = attrs.rstrip(',')
            
            if(attr_cols): attr_cols = ',' + attr_cols
            
            #Combine parts into one statement
            ins = 'insert into ' + name + ' (seqid,source,type,beginning,end,score,strand,phase' + attr_cols + ')' + ' values(' + vals + attrs + ')'
            #Send to SQLite and commit 
            curs.execute(ins)
            conn.commit()
    gff.close()
            
        #Should this return anything? Maybe a table name?
        #Maybe return number of rows parsed so program can print as confirmation similar to SAS
        
    return 'Created Table ' + table + ' with ' + str(num_rows) + ' Entries.'

def getInAndOut():
    ret = 0
    out = []
    inp = []
    for curr in sys.argv:
        if(curr == 'GFFind.py'): continue
        elif(curr == '--return' or curr == '--rt'): 
            ret = 1
        elif(ret):
            out.append(curr)   
        else:
            inp.append(curr)
   
    return parseArgs(inp), parseArgs(out,1)



def parseArgs(passedArr,ret=0):
    user_input = {}
    opt = 0
    att = 0
    tmp = ''
    #Go through each argument given by the user
    #Keep track of what the argument before it was
    #If the arg before starts with -, then the previous argument has no needed text
    for curr in passedArr:
        #Skip the arg for the name of the program
        if(curr == 'GFFind.py'): continue 
        elif(curr.startswith('-')):
            if(not curr in opts.keys()):
                print('Invalid Argument: ' + curr)
            elif(opt):
                if(tmp):
                    #set tmp to ''
                    user_input[opts[tmp]] = ''
                #set curr to ''
                user_input[opts[curr]] = ''
                opt = 0
            else:
                tmp = curr
                opt = 1
        elif ret:
            if(att):
                user_input['attribute'] = curr
                att = 0
            elif(opts['--'+curr] == 'attribute'):
                att = 1
            else:
                user_input[opts['--'+curr]] = ''
        else:
            #set tmp to curr
            user_input[opts[tmp]] = curr
            opt = 0
            tmp = ''
    if(tmp):
        user_input[opts[tmp]] = ''
    return user_input



def searchDB(output,select,where='',table='gff'):
    statement = 'select ' + select
    statement += ' from ' + table
    if(where):
        statement += ' where ' + where
    
    conn = connect('gffdb.sqlite')
    conn.row_factory = dict_factory
    curs = conn.cursor()
    res = curs.execute(statement)
    #Have this go through outputting too but use a different function
    #At the end, have it return a success statement saying something like
    ##"File gff_search_results.txt Created"
    #Which means the output function should return the filename
    
    return "File " + writeOutput(getHeader(res),parseSQLOutput(res.fetchall()),output) + ' Created.'

def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

def parseSQLOutput(output):
    #The output should be in the same order a gff file would be
    #Attributes will be in output the standard way for a gff
    #Should I use the header function to get the header, or is it better to do it in here?
    order = ['seqid','source','type','beginning','end','score','strand','phase']
    statement = ''
    for item in output:
        for curr in order:
            if(curr in item.keys()):
                statement = statement + str(item[curr]) + '\t'
                item.pop(curr)
        if(len(item)>0):
            for att in item.keys():
                statement = statement + att + '=' + item[att] + ';'
            statement = statement + '\n'
        statement = statement.rstrip('\t') + '\n'
    return statement.rstrip('\n')

def tableNames():
    conn = connect('gffdb.sqlite')
    curs = conn.cursor()
    curs.execute('select name from sqlite_master where type="table"')
    conn.commit()
    names = curs.fetchall()
    tables = []
    for curr in names:
        tables.append(curr[0])
    return tables

def createTableName(name='gff'):
    return getNewName(name,tableNames())

def getNewName(want,names):
    name = want
    i = 1
    found = 0
    while not found:
        if name in names:
            i+=1
            name = want + str(i)
            
        else:
            found = 1
            
    return name

def removeTable(table):
    conn = connect('gffdb.sqlite')
    curs = conn.cursor()
    curs.execute('drop table ' + table)
    conn.commit()
    curs.close()
    return 'Deleted table ' + table

def writeOutput(header,results,want):
    #Get the output name that you'll use
    files = [f for f in os.listdir('.') if os.path.isfile(f)]
    adj_files = []
    for curr in files:
        adj_files.append(curr.rstrip('.txt'))
    name = getNewName(want,adj_files) + '.txt'
    
    fil = open(name, 'w')
    fil.write('#' + header + '\n' + results)
    fil.close()
    
    return name

def createSelection(srch):
    #Will be done with key work return
    statement = ''
    options = ['seqid','source','type','region','beginning','end','score','strand','phase','attribute']
    for curr in srch:
       if(curr in options):
            if(curr == 'attribute'):
               for atr in srch[curr].split(';'):
                   if(atr):
                       statement = statement + atr + ','
            elif(curr == 'region'):
                statement = statement + 'beginning,end,'
            else:
                statement = statement  + curr + ','
    statement = statement.rstrip(',')
    if(not statement):
        statement = '*'
    return statement

def createWhere(srch):
    options = ['seqid','source','type','regionContained','regionBeginning', 'regionEnd','regionSpan','score','strand','phase','attribute']
    statement = ''

    #Need to make special cases for regions and attributes
    for curr in srch.iterkeys():
        if(curr in options):
            if(curr == 'regionContained'):
                beg = srch[curr].split('-')[0]
                end = srch[curr].split('-')[1]
                statement = statement + 'beginning > ' + beg + ' AND end < ' + end + ' AND '
    
            elif(curr == 'regionBeginning'):
                beg = srch[curr].split('-')[0]
                end = srch[curr].split('-')[1]
                statement = statement + 'beginning > ' + beg + ' AND beginning < ' + end + ' AND end > ' + end + ' AND '
                
            elif(curr == 'regionEnd'):
                beg = srch[curr].split('-')[0]
                end = srch[curr].split('-')[1]
                statement = statement + 'beginning < ' + beg + ' AND end < ' + end + ' AND end > ' + beg + ' AND '
                
            elif(curr == 'regionSpan'):
                beg = srch[curr].split('-')[0]
                end = srch[curr].split('-')[1]
                statement = statement + 'beginning < ' + beg + ' AND end > ' + end + ' AND '
                
            elif(curr == 'attribute'):
                for atr in srch[curr].split(';'):
                    if(atr):
                        statement = statement + atr.split('=')[0] + ' = "' + atr.split('=')[1] + '" AND '
                
            else:
                statement = statement + curr + ' = "' + srch[curr] + '" AND ' 
    #Return statement with last AND removed
    return statement.rstrip(' AND ')

def getHeader(cursor):
    '''Makes a header row from the cursor descriptoion. It is tab delimited.
    Arguments:
        cursor: a cursor from a select query
    Returns:
        string: A string consisiting of the column names sseparated by tabs, no new line'''
    #Use this function to print header to file
    options = ['seqid','source','type','beginning','end','score','strand','phase']
    header = []
    att = 0
    for opt in options:
        for curr in cursor.description:
            if(curr[0] == opt):
                header.append(curr[0])
            elif(not curr[0] in options):
                att = 1
    if(att): header.append('Attributes')
    return '\t'.join(header)




#Dictionary to parse user input
opts = {
#List tables
'--ta':'tables','--tables':'tables',
#Remove table
'--rm':'remove','--remove':'remove',
#Parses given file
'--pr':'parse', '--parse':'parse',
#Name for table
'--bd':'build', '--build':'build',
#Name for table to search
'--na':'name', '--name':'name',
'--id':'seqid', '--seqid':'seqid',
'--so':'source', '--source':'source',
'--ty':'type', '--type':'type',
'--rc':'regionContained', '--regionContained':'regionContained',
'--rb':'regionBeginning', '--regionBeginning':'regionBeginning',
'--re':'regionEnd', '--regionEnd':'regionEnd',
'--rs':'regionSpan', '--regionSpan':'regionSpan',
'--sc':'score', '--score':'score',
'--st':'strand', '--strand':'strand',
'--ph':'phase', '--phase':'phase',
'--at':'attribute', '--attribute':'attribute',
#Specific cases for select
'--rt':'return', '--return':'return',
#One for all
'--rg':'region','--region':'region',
#One of start
'--bg':'beginning','--beginning':'beginning',
#One for end
'--en':'end','--end':'end',
#Option to output the entire file
'-a':'all','--all':'all',
#Option for help
'-h':'help','--help':'help',
'-o':'output','--output':'output'
}

#Get user input and make it useable
inp, out = getInAndOut()

#Check for help
if(inp.has_key('help')):
    print(help())

elif(inp.has_key('tables')):
    tables = tableNames()
    print('Tables built: ')
    for curr in tables:
        print(curr)

#If no help requested
else:
    parse = 0
    remove = 0
    if(inp.has_key('parse')): 
        parse = 1
        if(inp.has_key('build')): 
            print(parseFile(inp['parse'],inp['build']))
            inp.pop('build')
        else: 
            print(parseFile(inp['parse']))
        inp.pop('parse')
        
        
    #Also check to see if table is being removed
    if(inp.has_key('remove')):
        print(removeTable(inp['remove']))
        remove = 1
        
    output = 'gff_search_results'
    if(inp.has_key('output')):
        output = inp['output']
        
    #Then search table according to other commands
    #Don't wnat to search if the user is just parsing
    #Unless the user specified all
    if(((parse or not remove) and len(inp)>0) or inp.has_key('all')):
        select = createSelection(out)
        where = createWhere(inp)
        if(inp.has_key('name')):
            print(searchDB(output,select,where,inp['name']))
        else:
            print(searchDB(output,select,where))
    
