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

def usage():
  print ""
  print "\t Usage: python pythonsample2.py <fulldatadirpath> <fullpythonlibdir"
  print ""
  print "Use this script to execute several simple scidb statements using python."
  print ""
  print "fulldatadirpath:"
  print "   FULL PATH for directory containing file simplearray.data."
  print "   The data file MUST be on the same instance as the server!"
  print "" 
  print "fullpythonlibdir:"
  print "   FULL PATH for directory libscidbpython.py"
  print ""
  print "Example: python pythonsample2.py /home/me/pythonexamples /opt/scidb-1.0/lib"
  print "Example: python pythonsample2.py `pwd` /opt/scidb-1.0/lib"
  print "" 
  print "This program uses an ODBC/JDBC like interface to connect to the SciDB server"
  print "and execute statements. The functions are: connect, disconnect, executeQuery,"
  print "completeQuery and cancelQuery." 
  print ""
  print "Data retrieval is performed through a set of methods attached to objects describing"
  print "the result set. The root object for data retrieval is the Array object. From this"
  print "object information can be discovered about the array dimensions and attributes. The"
  print "Array object also allows the retrieval of the iterators needed to retrieve the actual"
  print "data."  
  sys.exit(2)


if __name__ == "__main__":
        import time
        import sys
	if len(sys.argv) != 3:
           usage()
        #Set the path to the data file used to load our new array.
        fulldatadirpath = sys.argv[1]
        #Set the path to the Python API runtime library.  This path contains the file libscidbpython.py. 
        #libscidbpython.py loads the actual runtime library _libscidbpython.so
        #libscidbpython.py contains the python interface code to _libscidbpython.so. It must be imported
        #into your python application.
        sys.path.append(sys.argv[2])
        #Now import libscidbpython.py
        import libscidbpython
	print "_____________________________________________________________________________________"
        print "Executing script %s" % sys.argv[0]

        droparraystring = "drop array simplearray"
        createarraystring = "CREATE ARRAY simplearray < COL000N: int32,COL001C: char,COL002S: string > [a=0:99,10,0,b=0:9,10,0]"
        loaddatafile = "'" + fulldatadirpath + "/simplearray.data" + "'"
        loadcommandstring = "load simplearray from " + loaddatafile  
        #Get an object to access the connect, disconnect, executeQuery, completeQuery and cancelQuery methods of 
        #the python API.
	zz = libscidbpython.getSciDB()

        #Connect to a SciDB server.  This examples requires the client to be on the same machine as the 
        #server.  This is necessary because the server loads data from the accessable file system on 
        #the instance that it executes. A full path is neccessary to find the file. 
        try: 
		myhandle = zz.connect("localhost",1239)
        except Exception,  inst: 
                print "Exception occured during connection attempt:"
        	print "     Exception Type: %s" % type(inst)     # the exception instance
		print "     %s" % inst 
                print "     Program exiting, check that server is up and accepting connections"    
                exit(2) 
        #Allocate a QueryResult structure for use in processing result sets.
        #The QueryResult structure contains boolean attribute name "selective".  The programmer can
        #determine the type of command initiated by testing the boolean. If the boolean is true the 
        #a data retriveal command was executed (if qr.selective). This boolean is useful when building
        #a general purpose statement execution program that does not have prior knowledge of commands 
        #to be executed.  

        qr4=libscidbpython.QueryResult()
        try:
            zz.executeQuery(droparraystring, libscidbpython.AQL, qr4, myhandle)
            zz.completeQuery(qr4.queryID, myhandle)
        except Exception, inst:
            print "     Exception Type: %s" % type(inst)     # the exception instance
            print "     %s" % inst
            print " a failure above is normal when it can't clean up an old array that isn't there"
            # don't exit here, its normal for this to fail when there is no array

 
        #NOTE: QueryResult structures may NOT be reused. Create a new one for each statement executed. 
	qr3= libscidbpython.QueryResult()

        #Now create the array, load the array, and execute the query "select * from simplearray" 
        print "_____________________________________________________________________________________"
        print "Creat array statement: %s" % createarraystring 
        try:
         	zz.executeQuery(createarraystring, libscidbpython.AQL, qr3, myhandle)
                zz.completeQuery(qr3.queryID, myhandle)
        except Exception,  inst:
                print "Exception occured during array creation:"
                print "     Exception Type: %s" % type(inst)     # the exception instance
                print "     %s" % inst
                print "     Program exiting, check that server is up and accepting connections"
                exit(2)

        print "Load array statement: %s" % loadcommandstring 
        qr2 = libscidbpython.QueryResult()
        try: 
        	zz.executeQuery(loadcommandstring, libscidbpython.AQL, qr2, myhandle)
                zz.completeQuery(qr2.queryID, myhandle)
        except Exception,  inst:
                print "Exception occured during data load attempt:"
                print "     Exception Type: %s" % type(inst)     # the exception instance
                print "     %s" % inst
                print "     Program exiting, check that server is up and accepting connections"
                exit(2)

        print "Executing: select * from simplearray"
        qr = libscidbpython.QueryResult()
        try: 
		zz.executeQuery("select * from simplearray", libscidbpython.AQL ,qr,myhandle)
        except Exception,  inst:
                print "Exception occured during select attempt:"
                print "     Exception Type: %s" % type(inst)     # the exception instance
                print "     %s" % inst
                print "     Program exiting, check that server is up and accepting connections"
                exit(2)

        #Now we will start the process to retrieve data from the server.
        #First, get the array descriptor. The QueryResult structure, qr, contains the returned array.  
        #The returned array, qr.array, contains access information needed for retrieving data. 
	mydesc = qr.array.getArrayDesc()
        #Get the dimensions of the array. In this example the dimenions are not used for processing the 
        #result set.  Other programs may need this information to retrieve selected parts of the array. 
	mydims = mydesc.getDimensions()
        #Get the attributes of the array, attributes are the fields contained in each cell. 
        #This information contains information such as attribute type and name. 
	myattributes = mydesc.getAttributes()

	print "_____________________________________________________________________________________"
	print "Printing dimension information"
        #Discover the number of dimensions of the result set, iterator through them and print 
        #information about the dimensions.  This result set has two dimensions, [a=0:99,10,0,b=0:9,10,0].
	numberofdimensions = mydims.size()
	print "     Number of dimensions: %d" % numberofdimensions
	for i in range (0, numberofdimensions ):
		print "     Dimension vector index %d contains a dimension object with:" % i
		print "          Maximum end for array index = %d" % mydims[i].getEndMax()
		print "          Interval = %d " % mydims[i].getChunkInterval()
		print "          Overlap = %d " % mydims[i].getChunkOverlap()

        print "_____________________________________________________________________________________"
	print "Printing attribute information" 
	numberofattributes = myattributes.size()
        #Discover the number of attributes of the result set, iterator through them and print 
        #information about each attribute.  This result set has three attributes: 
        #< COL000N: int32,COL001C: char,COL002S: string >
	print "     Number of attributes: %d" % numberofattributes 
 	for i in range (0, numberofattributes):
 		print "     Attribute vector index %d contains a attribute object with:" % i
 		print "          ID: %d" % ( myattributes[i].getId()) 
 		print "          Name: '%s'" % ( myattributes[i].getName())
 		print "          Type: %s" % myattributes[i].getType()

 	print "_____________________________________________________________________________________"
        print "Getting array iterators."  
        #The next two steps get iterators necessary to retrieve the actual data. These iterators
        #are READ-ONLY. Use of the python help commands - e.g. help (arrayiterator) - will show methods
        #that can modify the iterators or the data chunks.  Since the underlying C++ implementation 
        #exposed to python are C++ contant definitions of iterators and chunk objects it is not possible 
        #to execute any methods that modify iterators or data chunks. ALL DATA MODIFICATION ACTIVITIES
        #MUST BE PERFORMED THROUGH THE executeQuery interface. 
	#First retrive contant array iterators.   
    # attr iter
    # attr iter
    # attr iter
        arrayiterators = libscidbpython.constarrayiterator(numberofattributes) 
        print "     Number of arrayiterators: %d" % numberofattributes
        for i in range (0, numberofattributes): 
                attributeid = myattributes[i].getId() 
                print "     Attributeid = %d" % attributeid 
                arrayiterators[i] = qr.array.getConstIterator( attributeid )
                print "     Iterator %d loaded" % i

        print "_____________________________________________________________________________________"
	print "Getting chunk iterators and displaying data"
        numberofchunks = 0
        #Now retrieve the constant chunk iterators. The constant chunk iterators are retrieved from 
        #the data chunks.  
    # chunk iter
    # chunk iter
    # chunk iter
        chunkiterators = libscidbpython.constchunkiterator(numberofattributes)
        while not arrayiterators[0].end():
                print "     Getting chunk iterators for chunk %d." % numberofchunks
                print "     Number of chunkiterators: %d" % numberofattributes
                for i in range (0, numberofattributes):
                      currentchunk = arrayiterators[i].getChunk()
                      chunkiterators[i] = currentchunk.getConstIterator((libscidbpython.ConstChunkIterator.IGNORE_EMPTY_CELLS | libscidbpython.ConstChunkIterator.IGNORE_OVERLAPS))
                      print "     Chunk iterator %d loaded." % i
                 
                print "     ____________________________________________"
                print "     Printing data of chunk %d." % numberofchunks
                #Now use the constant chunk and array iterators to print the data. 
                #Note that the getItem method of the chunkiterator object returns an object 
                #of type Value (dataitem).  A Value object method must be called to 
                #retrieve the actual data.  In this case getString, getChar, and getUint32 
                #methods are used. 
  		while not chunkiterators[0].end():
                      for i in range (0, numberofattributes):
                                dataitem = chunkiterators[i].getItem()
                                if myattributes[i].getType() == "string":
                                	print "          Data: %s" % dataitem.getString()
                                elif myattributes[i].getType() == "char":
                                        print "          Data: %s" % dataitem.getChar()
                                elif myattributes[i].getType() == "int32":
                                        print "          Data: %d" % dataitem.getUint32()
                                else:
                                   print "Invalid type for array simplearray was returned!"
                                   sys.exit(2)
 			 	chunkiterators[i].increment_to_next()
                      print "          ______________________________________"
                for j in range (0, numberofattributes):
                      arrayiterators[j].increment_to_next();                
                numberofchunks += 1;
        print "          End of data"

        print "_____________________________________________________________________________________"


        zz.completeQuery(qr.queryID, myhandle)
        
        print "Dropping simplearray"
        #Drop the table
        qr4 = libscidbpython.QueryResult()
        try:
        	zz.executeQuery("drop array simplearray", libscidbpython.AQL ,qr4,myhandle)
                zz.completeQuery(qr4.queryID, myhandle)
        except Exception,  inst:
                print "Exception occured during drop array attempt:"
                print "     Exception Type: %s" % type(inst)     # the exception instance
                print "     %s" % inst
                print "     Program exiting, check that server is up and accepting connections"
                exit(2)

        print "_____________________________________________________________________________________"
        print "Disconnecting from the server"
        #Disconnect from the SciDB server. 
        zz.disconnect(myhandle) 
        print "_____________________________________________________________________________________"

	print "Done!"
	sys.exit(0) #success

