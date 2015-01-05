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
from SciDBTest import SciDBTest, runTest

def usage():
  print ""
  print "\t Usage: l_tests.py"
  print ""
  print " run a whole set of cumulative sum tests"
  sys.exit(2)

# class to define a test
    
# dictionaries filled with commands
matrixNames = ["T", "E", "R"]
matrixSizes = [ [3,4,2,3], [13,21,13,21], [13,21,11,17], [29,29], [29,29,13,12], [1000,200], [1001,201], [1000,1000], [1024,1024,1024,1024], [1024,1024,256,256], [1050,1050], [1001,1001], [1001,999,101,99] ]
#matrixSizes = [ [5,2], [3,3,2,1] ]
#matrixSizes = [ [5,2] ]

#matrixSizes = [ [3,4,2,3], [13,21,13,21], [13,21,11,17], [1000,200], [1001,201], [1000,1000], [1024,1024,1024,1024], [1024,1024,256,256], [1050,1050], [1001,1001], [1001,999,101,99] ]
#matrixSizes = [ [1024,1024,256,256], [1050,1050], [1001,1001], [1001,999,101,99] ]
#matrixSizes = [ [5,5] ]
#matrixSizes = [ [13,21], [1001,201], [200,200] ]
#dataTypes   = ["double", "int64", "int32", "int16", "uint64", "float" ] 
#dataTypes   = ["double"] 
dataTypes   = ["double", "int64"] 
#dataTypes   = ["float"] 
successString = "Command completed successfully"
# each test is a sequence of create, fill and comparison (and, of course, cleanup)
tests = []

t = SciDBTest("i+j","aggregate(T, count(x))", "iif(%(EMPTY)d=1, 3*%(I)d-2, %(J)d*%(I)d)",isSparse=True, isOnlySquare=True)
t.setResultDim(0)
t.setSparseFunction("j-2<i and i<j+2")
t.setResultDataType("uint64")
tests.append(t)

t = SciDBTest("i+j","aggregate(T, count(x))", "iif(%(EMPTY)d=1,iif(%(I)d>%(J)d,3*%(J)d-1,3*%(I)d-1),%(J)d*%(I)d)",isSparse=True, isNeverSquare=True)
t.setResultDim(0)
t.setSparseFunction("j-2<i and i<j+2")
t.setResultDataType("uint64")
tests.append(t)

t = SciDBTest("i+j","aggregate(T, sum(x))", "(3*%(I)d-2)*(%(I)d+1)",isSparse=True, isOnlySquare=True)
t.setResultDim(0)
t.setSparseFunction("j-2<i and i<j+2")
#t.setNullableResult(False)
tests.append(t)

t = SciDBTest("i+j","aggregate(T, sum(x))", "iif(%(I)d>%(J)d, 3*%(J)d*(%(J)d+1)-1, 3*%(I)d*(%(I)d+1)-1)",isSparse=True, isNeverSquare=True)
t.setResultDim(0)
t.setSparseFunction("j-2<i and i<j+2")
#t.setNullableResult(False)
tests.append(t)

#mine t = SciDBTest("i+j","avg(T)", "iif(%(EMPTY)d=1, (%(I)d+1.), (3*%(I)d-2.)*(%(I)d+1.)/(%(I)d*%(I)d) )",isSparse=True, isOnlySquare=True)
t = SciDBTest("i+j","aggregate(T, avg(x))", "iif(%(EMPTY)d=1, (%(I)d+1), (3.*%(I)d-2)*(%(I)d+1)/(%(I)d*%(I)d) )",isSparse=True, isOnlySquare=True)
t.setResultDim(0)
t.setSparseFunction("j-2<i and i<j+2")
t.setResultDataType("double")
tests.append(t)

# mine t = SciDBTest("i+j","avg(T)", "iif(%(EMPTY)d=1 ,iif(%(I)d>%(J)d, (3.*%(J)d*(%(J)d+1)-1.)/(3.*%(J)d-1.),(3.*%(I)d*(%(I)d+1.)-1.)/(3.*%(I)d-1.)), iif(%(I)d>%(J)d, (3.*%(J)d*(%(J)d+1.)-1.)/(%(J)d*%(I)d),(3.*%(I)d*(%(I)d+1.)-1.)/(%(I)d*%(J)d)) )",isSparse=True, isNeverSquare=True)
t = SciDBTest("i+j","aggregate(T, avg(x))", "iif(%(EMPTY)d=1 ,iif(%(I)d>%(J)d, (3.*%(J)d*(%(J)d+1)-1)/(3*%(J)d-1),(3.*%(I)d*(%(I)d+1)-1)/(3*%(I)d-1)), iif(%(I)d>%(J)d, (3.*%(J)d*(%(J)d+1)-1)/(%(J)d*%(I)d),(3.*%(I)d*(%(I)d+1)-1)/(%(I)d*%(J)d)) )",isSparse=True, isNeverSquare=True)
t.setResultDim(0)
t.setSparseFunction("j-2<i and i<j+2")
t.setResultDataType("double")
tests.append(t)

t = SciDBTest("i+j","aggregate(T, min(x))", "iif(%(EMPTY)d=1, 2, 0 )",isSparse=True)
t.setResultDim(0)
t.setSparseFunction("j-2<i and i<j+2")
tests.append(t)

t = SciDBTest("i+j","aggregate(T, min(x), i)", "iif(%(EMPTY)d=1, iif(i=1, 2, 2*i-1),0 )",isSparse=True, isOnlySquare=True)
t.setResultDim(1)
t.setSparseFunction("j-2<i and i<j+2")
tests.append(t)

t = SciDBTest("i+j","aggregate(T, min(x), j)", "iif(%(EMPTY)d=1, iif(j=1, 2, 2*j-1),0 )",isSparse=True, isOnlySquare=True)
t.setResultDim(1,"j")
t.setSparseFunction("j-2<i and i<j+2")
tests.append(t)

t = SciDBTest("i+j","aggregate(T, max(x))", "iif(%(I)d=%(J)d, 2*%(I)d, iif(%(I)d>%(J)d, 2*%(J)d+1, 2*%(I)d+1) )",isSparse=True)
t.setResultDim(0)
t.setSparseFunction("j-2<i and i<j+2")
tests.append(t)

t = SciDBTest("i+j","aggregate(T, max(x), i)", "iif(i<%(I)d, 2*i+1, 2*i)",isSparse=True, isOnlySquare=True)
t.setResultDim(1)
t.setSparseFunction("j-2<i and i<j+2")
tests.append(t)

t = SciDBTest("i+j","aggregate(T, max(x), j)", "iif(j<%(J)d, 2*j+1, 2*j)",isSparse=True, isOnlySquare=True)
t.setResultDim(1,"j")
t.setSparseFunction("j-2<i and i<j+2")
tests.append(t)

# need to add: var
            
for t in tests:
    runTest(t, matrixNames, matrixSizes, dataTypes)
