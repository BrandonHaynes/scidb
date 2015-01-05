#!/usr/bin/python

# Initialize, start and stop scidb. 
# Supports single instance and cluster configurations. 
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

import subprocess
import sys
import time
import os
import string
import signal
import errno
import socket
import fcntl
import struct
import array
from ConfigParser import *
from ConfigParser import RawConfigParser
from glob import glob

def usage():
  print ""
  print "\t Usage: cumsum_tests.py"
  print ""
  print " run a whole set of cumulative sum tests"
  sys.exit(2)

# class to define a test
class SciDBTest:

    def __init__(self, arrayFunction, testFunction, expectedFunction, expectedResult="[(0)]\n", isSparse=False, isOnlySquare=False, isNeverSquare=False, isEmpty=False):
        self.arrayFunction = arrayFunction
        self.testFunction = testFunction
        self.expectedFunction = expectedFunction
        self.cleanupMatrixNameList = ["T", "E", "R"]
        self.sparseFlag = isSparse
        self.onlySquareFlag = isOnlySquare
        self.neverSquareFlag = isNeverSquare
        self.emptyFlag = isEmpty
        self.expectedResult = expectedResult
        self.sourceDim = "[ i=1:%d,%d,0, j=1:%d,%d,0 ]"
        self.setResultDim(2)  # standard 2D matrix
        self.setResultDataType(None)
        self.setSparseFunction(None)
        self.resultIsSparse = isSparse
        self.resultIsNullable = isSparse

    #
    # set expected dimensions for results
    def setResultDim(self, number, resDim='i'):
        if number == 0:
           self.resultDim = ["[ i=0:0,1,0 ]", 0]
           self.resultIsSparse = False
        elif number == 1:
           self.resultIsSparse = False
           if resDim =='i':
               self.resultDim = ["[ i=1:%d,%d,0 ]", 1, 0]
           else:
               self.resultDim = ["[ j=1:%d,%d,0 ]", 1, 1]
        elif resDim =='i':
           self.resultDim = ["[ i=1:%d,%d,0, j=1:%d,%d,0 ]", 2, 0]
        else:    
           self.resultDim = ["[ i=1:%d,%d,0, j=1:%d,%d,0 ]", 2, 1]
    
    # workaround for not allowing non-nullable results in nullable arrays
    def setNullableResult(self, n):
        self.resultIsNullable = n
    
    # sometimes, results have different data types than sources
    def setResultDataType(self,dataType):
        self.resultDataType = dataType
                              
    def setSparseFunction(self,sf):
        if sf:
            self.sparseFunction=","+sf
        else:
            self.sparseFunction=""
    
    def getCreateCommands(self, dataType, dim, createFlags=""):
        cc = []
        createTemplate = "create %s array %s < x: %s > "
        if len(dim)>2:
            iChunkSize = dim[2]
        else:
            iChunkSize=100 
        if len(dim)>3:
            jChunkSize = dim[3]
        else:
            jChunkSize=100
        
        # T may be rectangular
        createCommand = createTemplate + self.sourceDim
        if self.isSparse():
            cf = createFlags[0]+' '+createFlags[1]
        else:
            cf = createFlags[0]
        cc.append(createCommand%(cf,"T",dataType, dim[0], iChunkSize, dim[1], jChunkSize))
        
        # check for different resultDataType
        if self.resultDataType:
            rdt = self.resultDataType
        else:
            rdt = dataType

        # check for nullable result array
        if self.resultIsNullable:
            rdt += " null"
            
        # Result & Expected Result arrays 
        createCommand = createTemplate + self.resultDim[0]
        if self.resultIsSparse:
            cf = createFlags[0]+' '+createFlags[1]
        else:
            cf = createFlags[0]
            
        if self.resultDim[1] == 2:
            cc.append(createCommand%(cf,"E",rdt, dim[self.resultDim[2]], iChunkSize, dim[self.resultDim[2]], iChunkSize))
            cc.append(createCommand%(cf,"R",rdt, dim[self.resultDim[2]], iChunkSize, dim[self.resultDim[2]], iChunkSize))
        elif self.resultDim[1] == 1:
            cc.append(createCommand%(cf,"E",rdt, dim[self.resultDim[2]], iChunkSize))
            cc.append(createCommand%(cf,"R",rdt, dim[self.resultDim[2]], iChunkSize))
        else:
            cc.append(createCommand%(cf,"E",rdt))
            cc.append(createCommand%(cf,"R",rdt))
            
        return cc
    
    def getFillCommands(self,iSize,jSize, emptyFlag=False):
        if self.isSparse():
            bf = "build_sparse"
        else:
            bf = "build"
        cc = ["store(%s(T,%s%s),T)"%(bf,self.arrayFunction,self.sparseFunction)]
        
        if(emptyFlag):
            empty = 1
        else:
            empty = 0  
        ef = self.expectedFunction%{"J":jSize, "I":iSize, "EMPTY":empty}

        if self.resultIsSparse:
            cc.append("store(build_sparse(E,%s%s),E)"%(ef,self.sparseFunction))
        else:
            cc.append("store(build(E,%s),E)"%ef)
        return cc

    def getTestCommand(self):
        cc = ["store(repart(%s,R),R)"%(self.testFunction)]
        return cc
    
    def getComparisonCommand(self):
        eps = 0.0000001
        #eps = 0.000001
        return "SELECT count(*) FROM R , E WHERE abs((R.x-E.x)/iif(E.x=0,1,E.x)) > %g"%eps
    
    def getExpectedResult(self):
        return self.expectedResult

    def getCleanupCommands(self):
        cc = []
        for m in self.cleanupMatrixNameList:
            cc.append("remove(%s)"%m)
        return cc
    
    def getArrayFunction(self):
        return self.arrayFunction
    
    def isSparse(self):
        return self.sparseFlag
    
    def isOnlySquare(self):
        return self.onlySquareFlag
    
    def isNeverSquare(self):
        return self.neverSquareFlag
    
    def isEmpty(self):
        return self.emptyFlag

    def testName(self):
        if self.isSparse():
            return "%s(%s)"%(self.arrayFunction,"sparse")
        else:
            return self.arrayFunction
    
# execute the statement
def executeCmd(cmd,flags, echo=False):
    if echo:
       print "iquery %s '%s'" %(flags, cmd)
    #p = subprocess.Popen(['iquery','-p','7239',flags,cmd], shell=False,stdout=subprocess.PIPE, stderr=subprocess.PIPE) 
    p = subprocess.Popen(['iquery',flags,cmd], shell=False,stdout=subprocess.PIPE, stderr=subprocess.PIPE) 
    #p = subprocess.Popen(['echo',flags,cmd], shell=False,stdout=subprocess.PIPE, stderr=subprocess.PIPE) 
    return p.communicate()
#
# piece the test from various pieces and run it with all sizes
def runTest(t, matrixNames, matrixSizes, dataTypes):
    if t.isSparse():
        creationFlags = [ ["","empty"],["immutable",""], ["immutable" ,"empty" ], ["",""]]
    else:
        creationFlags = [["",""],["immutable", ""]]
        
    for d in dataTypes:
        for i in matrixSizes:
            # skip rectangular data sets for tests that are defined to be square
            if i[0]<>i[1] and t.isOnlySquare():
               continue

            # skip square data sets for tests that are defined to be not square
            if i[0]==i[1] and t.isNeverSquare():
                continue
            
            for cf in creationFlags:
                # initial cleanup
                for cmd in t.getCleanupCommands():
                    r = executeCmd(cmd,'-aq')
                #
                # create the matrices
                #
                for cmd in t.getCreateCommands(d,i,cf):
                    r = executeCmd(cmd,'-q', True)
                    if r[1]:
                        print "FAIL: cmd %s failed, reported %s"%(cmd,r[1])
            
                # the test and expected matrices
                for cmd in t.getFillCommands(i[0],i[1],cf[1]=="empty"):
                    r = executeCmd(cmd,'-anq', True)
                    if r[1]:
                        print "FAIL: cmd %s failed, reported %s"%(cmd,r[1])
                    
                # the operation result
                for cmd in t.getTestCommand():
                    r = executeCmd(cmd,'-anq', True)
                    if r[1]:
                        print "FAIL: cmd %s failed, reported %s"%(cmd,r[1])
                
                # and now test the result
                r = executeCmd(t.getComparisonCommand(), '-q', True)
                if r[1]:
                    print "FAIL: cmd %s failed, reported %s"%(cmd,r[1])
                elif r[0] != t.getExpectedResult():
                    print "FAIL: Test %s %s %s %s %dx%d %s failed - reported %s"%(t.testFunction,cf[0],cf[1],d,i[0],i[1],t.testName(),r[0])
                else:
                    print "Test %s %s %s %s %dx%d %s succeeded - reported %s"%(t.testFunction,cf[0],cf[1],d,i[0],i[1],t.testName(),r[0])
                        
                # final cleanup
                for cmd in t.getCleanupCommands():
                    executeCmd(cmd,'-aq')            
            
                sys.stdout.flush()
