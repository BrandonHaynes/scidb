#!/usr/bin/python
# BEGIN_COPYRIGHT
#
# This file is part of SciDB.
# Copyright (C) 2008-2014 SciDB, Inc.
#
# SciDB is free software: you can redistribute it and/or modify
# it under the terms of the AFFERO GNU General Public License as published by
# the Free Software Foundation.
#
# SciDB is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
# INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
# NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
# the AFFERO GNU General Public License for the complete license terms.
#
# You should have received a copy of the AFFERO GNU General Public License
# along with SciDB.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
#
# END_COPYRIGHT
#
import csv
import sys

def usage():
  print ""
  print "\t Usage: python pythonsample4.py <fullstatementfilepath> <fullfilepath> <fullpythonlibdiri>"
  print ""
  print "Use this script to read and execute scidb statements contained"
  print "in a file" 
  print ""
  print "fullfilepath:"
  print "   FULL PATH to a containing a file of SciDB statements."
  print ""
  print "fullpythonlibdir:"
  print "   FULL PATH for directory libscidbpython.py"
  print ""
  print "Example: python pythonsample4.py /home/me/pythonexamples/myscript.ini /opt/scidb-1.0/lib"
  print "Example: python pythonsample4.py `pwd`/myscript.txt /opt/scidb-1.0/lib"
  print ""
  sys.exit(2)

def q_execute(query, myzz):
        if(query[0].strip().lower() == 'aql'):
        	queryType = 0
        else:
		queryType = 1
        print "%s" % query[2]
        try: 
		myzz.executeQuery(query[2], queryType, qr, myhandle)
        except Exception, inst:
                print "Exception occured during statement execution attempt:"
                print "     Exception Type: %s" % type(inst)     # the exception instance
                print "     %s" % inst
                print "Program will proceed to next statement in file."
                return False 
        return True 

def print_result(qr):
	mydesc = qr.array.getArrayDesc()
	mydims = mydesc.getDimensions()
	myattributes = mydesc.getAttributes()
	out_file.write( "\n Printing dimension information" )
	numberofdimensions = mydims.size()
	out_file.write( "\n      Number of dimensions: %d" % numberofdimensions )
	for i in range (0, numberofdimensions ):
		out_file.write( "\n      Dimension vector index %d contains a dimension object with:" % i )
		out_file.write( "\n           Name = %s " % mydims[i].getBaseName() )
		out_file.write( "\n           Maximum end for array index = %d" % mydims[i].getEndMax() )
		out_file.write( "\n           Interval = %d " % mydims[i].getChunkInterval() )
		out_file.write( "\n           Overlap = %d " % mydims[i].getChunkOverlap() )
		out_file.write( "\n _____________________________________________________________________________________" )
	out_file.write( "\n Printing attribute information" )
	numberofattributes = myattributes.size()
	out_file.write( "\n      Number of attributes: %d" % numberofattributes )
	for i in range (0, numberofattributes):
		out_file.write( "\n      Attribute vector index %d contains a attribute object with:" % i )
		out_file.write( "\n           ID: %d" % ( myattributes[i].getId()) )
		out_file.write( "\n           Name: '%s'" % ( myattributes[i].getName()) )
		out_file.write( "\n           Type: %s" % myattributes[i].getType() )
	out_file.write( "\n _____________________________________________________________________________________" )
	out_file.write( "\n Getting array iterators."  )
	arrayiterators = libscidbpython.constarrayiterator(numberofattributes) 
	out_file.write( "\n      Number of arrayiterators: %d" % numberofattributes )
	for i in range (0, numberofattributes): 
		attributeid = myattributes[i].getId() 
		out_file.write( "\n      Attributeid = %d" % attributeid )
		arrayiterators[i] = qr.array.getConstIterator( attributeid )
		out_file.write( "\n      Iterator %d loaded" % i )
	out_file.write( "_____________________________________________________________________________________" )
	out_file.write( "\n Getting chunk iterators and displaying data" )
	numberofchunks = 0
	chunkiterators = libscidbpython.constchunkiterator(numberofattributes)
	while not arrayiterators[0].end():
		out_file.write( "\n      Getting chunk iterators for chunk %d." % numberofchunks )
		out_file.write( "\n      Number of chunkiterators: %d" % numberofattributes )
		for i in range (0, numberofattributes):
			currentchunk = arrayiterators[i].getChunk()
			chunkiterators[i] = currentchunk.getConstIterator((libscidbpython.ConstChunkIterator.IGNORE_OVERLAPS))
			out_file.write( "\n      Chunk iterator %d loaded." % i )
		out_file.write( "\n      ____________________________________________" )
		out_file.write( "\n      Printing data of chunk %d." % numberofchunks )
		while not chunkiterators[0].end():
			for i in range (0, numberofattributes):
                        	if not chunkiterators[i].isEmpty(): #Determine if the attribute contains data.
                                                                    #This test is not necessary if currentchunk.getConstIterator is called
                                                                    #with the paramater libscidbpython.ConstChunkIterator.IGNORE_OVERLAPS 
                                                                    # is or'ed(|) with
                                                                    #libscidbpython.ConstChunkIterator.IGNORE_EMPTY_CELLS 
					dataitem = chunkiterators[i].getItem()
					#out_file.write( "\n           Data: %s" % libscidbpython.ValueToString(myattributes[i].getType(),dataitem) )
 					if myattributes[i].getType() == "string":
 						out_file.write( "\n           Data: %s" % dataitem.getString() )
 					elif myattributes[i].getType() == "char":
 						out_file.write( "\n           Data: %s" % dataitem.getChar() )
 					elif myattributes[i].getType() == "int32":
 						out_file.write( "\n           Data: %d" % dataitem.getUint32() )
 					elif myattributes[i].getType() == "int64":
 						out_file.write( "\n           Data: %d" % dataitem.getInt64() )
                                	elif myattributes[i].getType() == "uint64":
                                         	out_file.write( "\n           Data: %d" % dataitem.getUint64() )
                                 	elif myattributes[i].getType() == "bool":
                                         	out_file.write( "\n           Data: %s" % dataitem.getBool() )
                                 	elif myattributes[i].getType() == "datetime":
                                         	out_file.write( "\n           Data: %s" % dataitem.getDateTime() )
                                 	elif myattributes[i].getType() == "double":
                                         	out_file.write( "\n           Data: %d" % dataitem.getDouble() )
                                 	elif myattributes[i].getType() == "float":
                                         	out_file.write( "\n           Data: %s" % dataitem.getFloat() )
                                 	elif myattributes[i].getType() == "indicator":
                                         	out_file.write( "\n           Indicator: Returned attributes matched predicate criteria(e.g. where...)")
                                        #note: Scidb will return an extra synthesized attribute for every cell containing data that matches predicate criteria. 
                                        #      iquery shows the value of this synthized attribute as "true".  So if a cell has 5 attributes and a select statement
                                        #      returned all the attributes because the predicate criteria where matched then a total of 6 attributes will be 
                                        #      returned. The last attribute will be the indicator. When the array shape is maintained in the result set the 
                                        #      indicator can be used to identify cells with data. 
                                 	elif myattributes[i].getType() == "int16":
                                         	out_file.write( "\n           Data: %s" % dataitem.getInt16() )
                                 	elif myattributes[i].getType() == "uint16":
                                         	out_file.write( "\n           Data: %s" % dataitem.getUint16() )
                                 	elif myattributes[i].getType() == "int8":
                                         	out_file.write( "\n           Data: %s" % dataitem.getInt8() )
                                 	elif myattributes[i].getType() == "uint8":
                                         	out_file.write( "\n           Data: %s" % dataitem.getUint8() )
                                 	elif myattributes[i].getType() == "void":
                                         	out_file.write( "\n           Data: %s" % dataitem.getVoid() )
 					else:
 						out_file.write( "\n Please write function for datatype '%s' in your python script" % myattributes[i].getType())
						sys.exit(2)
			   	chunkiterators[i].increment_to_next()
                      	if not chunkiterators[i].isEmpty():
                       		out_file.write( "\n           ______________________________________" )

		for j in range (0, numberofattributes):
			arrayiterators[j].increment_to_next();                
		numberofchunks += 1;
	out_file.write( "\n           End of data" )
	out_file.write( "\n _____________________________________________________________________________________" )


if __name__ == "__main__":
        import sys
        if len(sys.argv) != 3:
           usage()
        #Set the path to the data file used to load our new array.
        fullfilepath = sys.argv[1]
        #Set the path to the Python API runtime library.  This path contains the file libscidbpython.py. 
        #libscidbpython.py loads the actual runtime library _libscidbpython.so
        #libscidbpython.py contains the python interface code to _libscidbpython.so. It must be imported
        #into your python application.
        sys.path.append(sys.argv[2])
	files = [fullfilepath]
	print "-----"
	import libscidbpython
	zz = libscidbpython.getSciDB()
	myhandle = zz.connect("localhost",1239)
	for ifile in files:
		stmt = []
		qno = 0
		print ""
		print "Reading file %s" % ifile
		#fileHandle = csv.reader(open(ifile, 'rb'), delimiter=',', quotechar='|')
		fileHandle = open(ifile,'r')
		for row in fileHandle:
			#print ' <-> '.join(row)
			stmt.append(row)
		#for query in stmt:
		  #if( len(query) == 2 ):
		for stmt_str in stmt:
		  query = stmt_str.partition(',')
		  if( len(query[2].strip()) > 3 ):
                        qno+=1
			print "Executing query no (%d): %s" % (qno, query[2])
                        out_file=open('%s_statement_py4%d' % (ifile, qno), 'w')
			#out_file=open('query_%s_%d' % (ifile, qno), 'w')
			out_file.write( "\n Executing query : %s" % query[2] )
                        qr = libscidbpython.QueryResult()
			success = q_execute(query, zz)
			if success:
				if(qr.selective):
					print_result(qr)
				else:
					out_file.write( "\n Query was executed successfully" )
			out_file.close()

	print "Done!"
	sys.exit(0) #success

