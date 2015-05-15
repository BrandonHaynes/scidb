#!/usr/bin/python
#
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
from scidbTestCase import testCase
from pushPopCleaners import ArrayCleaner
from pushPopCleaners import DataDirCleaner
from scidbIqueryLib import scidbIquery
from queryMakerLib import defaultBuildQuery
import functools
import sys
import time
import os
import re

# Consume query template
CONSUME_QUERY = 'consume(<query>)'

# Message contained in case of an Error
ERROR_MSG = 'Error id:'

# List of Data Types
DATA_TYPES = ['bool', 'double', 'float', 'int', 'string', 'uint']

# List to contain array names and their schemas
arrList = []


def getMoreArrays(dbq,dType):
    # Generate arrays with different attributes, dimensions and overlaps
    # The set of arrays is used for each data type mentioned in the variable DATA_TYPES 

    global arrList

    a1d1 = dbq.schema(a=['at1'],t=['int8'],d=['d1'],r=[(0,8)],c=[2])
    a1d2 = dbq.schema(a=['at1'],t=['int16'],d=['d1','d2'],r=[(0,8)],c=[3,2])
    a1d3 = dbq.schema(a=['at1'],t=['int32'],d=['d1','d2','d3'],r=[(0,8)],c=[1,3,2])
    a2d1 = dbq.schema(a=['at1','at2'],t=['int16','int64'],d=['d1'],r=[(-2,2)],c=[3])
    a2d2 = dbq.schema(a=['at1','at2'],t=['int8','int64'],d=['d1','d2'],r=[(-2,2),(-1,2)],c=[3])
    a2d3 = dbq.schema(a=['at1','at2'],t=['int64','int64'],d=['d1','d2','d3'],r=[(-2,2),(-1,2),(0,2)],c=[3])
    a3d1 = dbq.schema(a=['at1','at2','at3'],t=['int64','int16','int32'],d=['d1'],r=[(2,7)],c=[3])
    a3d2 = dbq.schema(a=['at1','at2','at3'],t=['int16','int8','int16'],d=['d1','d2'],r=[(2,2),(1,2)],c=[3])
    a3d3 = dbq.schema(a=['at1','at2','at3'],t=['int32','int64','int32'],d=['d1','d2','d3'],r=[(2,2),(1,2),(0,2)],c=[3])
    a1d1o = dbq.schema(a=['at1'],t=['int8'],d=['d1'],r=[(0,8)],c=[2],o=[1])
    a1d2o = dbq.schema(a=['at1'],t=['int16'],d=['d1','d2'],r=[(0,8)],c=[3,2],o=[0,2])
    a1d3o = dbq.schema(a=['at1'],t=['int32'],d=['d1','d2','d3'],r=[(0,8)],c=[1,3,2],o=[0,2,1])
    a2d1o = dbq.schema(a=['at1','at2'],t=['int16','int64'],d=['d1'],r=[(-2,2)],c=[3],o=[2])
    a2d2o = dbq.schema(a=['at1','at2'],t=['int8','int64'],d=['d1','d2'],r=[(-2,2),(-1,2)],c=[3],o=[1,0])
    a2d3o = dbq.schema(a=['at1','at2'],t=['int64','int64'],d=['d1','d2','d3'],r=[(-2,2),(-1,2),(0,2)],c=[3],o=[1,2,1])
    a3d1o = dbq.schema(a=['at1','at2','at3'],t=['int64','int16','int32'],d=['d1'],r=[(2,7)],c=[3],o=[3])
    a3d2o = dbq.schema(a=['at1','at2','at3'],t=['int16','int8','int16'],d=['d1','d2'],r=[(2,2),(1,2)],c=[3],o=[1,2])
    a3d3o = dbq.schema(a=['at1','at2','at3'],t=['int32','int64','int32'],d=['d1','d2','d3'],r=[(2,2),(1,2),(0,2)],c=[3],o=[2,0,1])

    arrList = [[a1d1,'a1d1'],[a1d2,'a1d2'],[a1d3,'a1d3'],[a2d1,'a2d1'],[a2d2,'a2d2'],[a2d3,'a2d3'],[a3d1,'a3d1'],[a3d2,'a3d2'],[a3d3,'a3d3'],[a1d1o,'a1d1o'],[a1d2o,'a1d2o'],[a1d3o,'a1d3o'],[a2d1o,'a2d1o'],[a2d2o,'a2d2o'],[a2d3o,'a2d3o'],[a3d1o,'a3d1o'],[a3d2o,'a3d2o'],[a3d3o,'a3d3o']]

    if dType == 'uint':    # Replace data type int preserving the size
        arrList = [ [a.replace('int', dType), b] for a,b in arrList]
    elif dType != 'int':   # Replace entire data type
        arrList = [ [re.sub(r"int[1-9]+", dType, a), b] for a,b in arrList]

    ARRAY_CREATE_QUERIES = []
    for arr in arrList:
        ARRAY_CREATE_QUERIES += ['create array '+arr[1]+' '+arr[0]]

    return ARRAY_CREATE_QUERIES


def getArrayBuildQueries():
    # Build array data and store values in the arrays

    ARRAY_BUILD_QUERIES = []

    for arr in arrList:
        dimIndex = arr[0].find('[')
        attrList = arr[0][1:dimIndex-1].split(',')
        buildQuery = 'build(<'+attrList[0]+'>'+arr[0][dimIndex:]+',random())'
        if len(attrList) > 1:
            buildQuery = 'join('+buildQuery+',build(<'+attrList[1]+'>'+arr[0][dimIndex:]+',random()))'
        if len(attrList) > 2:
            buildQuery = 'join('+buildQuery+',build(<'+attrList[2]+'>'+arr[0][dimIndex:]+',random()))'

        ARRAY_BUILD_QUERIES += ['store('+buildQuery+','+arr[1]+')']

    return ARRAY_BUILD_QUERIES


def getMoreArrayQueries():
    MORE_ARRAY_QUERIES = []

    for arr in arrList:

        eval_attr = 'at' + arr[1][1]  # select the attribute for use in the operator

        MORE_ARRAY_QUERIES += [
            'aggregate(' + arr[1] + ',count(*))',
            'allversions(' + arr[1] + ')',
            'attributes(' + arr[1] + ')',
            'analyze(' + arr[1] + ')',
            'apply(' + arr[1] + ',z,d1*2)',
            'attribute_rename(' + arr[1] + ',' + eval_attr + ',y)',
            'avg_rank(' + arr[1] + ')',
            'bernoulli(' + arr[1] + ',0.3)',
            'cross_join(' + arr[1] + ',' + arr[1] + ')',
            'dimensions(' + arr[1] + ')',
            'join(' + arr[1] + ',' + arr[1] + ')',
            'merge(filter(' + arr[1] + ',d1<2), filter(' + arr[1] + ',d1>2))',
            'quantile(' + arr[1] + ',2)',
            'rank(' + arr[1] + ',' + eval_attr + ')',
            'reverse(' + arr[1] + ')',
            'sort(' + arr[1] + ')',
            'unpack(' + arr[1] + ',z)'
        ]

        if (arr[0][5:9]!='bool') and (len(arr[1]) == 4):   # Array dimension does Not have overlaps and data type is not bool
            MORE_ARRAY_QUERIES += [ 'cumulate(' + arr[1] + ',sum(at1),d1)'  ]

        if (arr[0][5:11]=='double') and (arr[1][3] == '1'):   # Array is 1-D
            MORE_ARRAY_QUERIES += [ 'normalize(project(' + arr[1] + ',at1))' ]

    return MORE_ARRAY_QUERIES


# Test case class.
class myTestCase(testCase):
    #-------------------------------------------------------------------------
    # Constructor:
    def __init__(self,queries,stopOnError=False):
        super(myTestCase,self).__init__() # Important: call the superclass constructor.
        self.registerCleaner(ArrayCleaner()) # Register the cleaner class to remove arrays
                                             # created by this test.
        self.queries = queries
        self.stopOnError = stopOnError

        try:
            dataPath = os.path.join(
                os.environ['SCIDB_DATA_PATH'],
                '000',
                '0'
                )
            # Register the cleaner that will remove any data files created by this test.
            self.registerCleaner(DataDirCleaner(dataPath))
        except:
            pass # Somehow, we could not get to the scidb data folder: 
                 # we will leave some junk files in there.
            
        self.__iquery = scidbIquery() # Iquery wrapper class.
        self.exitCode = 0 # Exit code for test harness
        
    #-------------------------------------------------------------------------
    # runQueries: executes a list of queries
    def runQueries(self,queries,stopOnError=False):
        exitStatus = 0
        for q in queries:
            queryException = False
            queryOk = False
            startTime = time.time()
            
            try:
                exitCode,\
                stdoutData,\
                stderrData = self.__iquery.runQuery(q)
                queryOk = True
            except Exception as e:
                queryException = True
                queryOk = False
            endTime = time.time()
            queryTime = time.strftime('%H:%M:%S',time.gmtime(endTime-startTime))

            if ((exitCode != 0) or (ERROR_MSG in stderrData) or (ERROR_MSG in stdoutData)):
                msg =  'Error during query: {0}'.format(q)
                if (queryException):
                    msg =  'Exception during query: {0}'.format(q)
                print msg
                print stdoutData
                print stderrData
                exitStatus = 1
                queryOk = False
                
            if ((not queryOk) and stopOnError):
                break
                
            if (queryOk):
                print 'Time = {0}, sucess: {1}'.format(queryTime,q)
        return exitStatus
    #-------------------------------------------------------------------------
    # test: main entry point into the test.
    # Important: this function gets called by the superclass function runTest.
    # RunTest wraps this function in a "try: except: finally:" clause to
    # ensure the cleanup function is called.  Cleanup performs all of the 
    # actions of the registered cleaner classes (see constructor).
    def test(self):
        
        # Default build query and schema "roller": makes the strings.
        # The default build query (called as is - dbq.build()), will produce this:
        #  'build(<attr1:double>[i=0:9,4,0,j=0:9,4,0],random())'
        # The default schema (if called as is - dbq.schema()), will produce this:
        # '<attr1:double>[i=0:9,4,0,j=0:9,4,0]'
        # See definition of the class on explanation of parameters.
        dbq = defaultBuildQuery()
        
        self.exitCode = self.runQueries(self.queries,self.stopOnError)


def generateQueries(dType):
    QUERIES = []
    stopOnError = False
    dbq = defaultBuildQuery()

    if dType in DATA_TYPES:
        QUERIES = getMoreArrays(dbq,dType) + \
            [CONSUME_QUERY.replace('<query>',x) for x in getArrayBuildQueries()] + \
            [CONSUME_QUERY.replace('<query>',x) for x in getMoreArrayQueries()]
        stopOnError = True

    return [QUERIES, stopOnError]


#-----------------------------------------------------------------------------
# Main entry point into the script.
if __name__ == '__main__':

    for dType in DATA_TYPES:
        print '\n\n*** Running "' + dType + '" queries ***\n'
        it = myTestCase(*generateQueries(dType))
        it.runTest()
        if (it.exitCode != 0):
            print 'Error(s) occurred.'
            sys.exit(it.exitCode) # Signal to test harness pass or failure.
    print 'All done.'

