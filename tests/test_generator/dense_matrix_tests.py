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
matrixSizes = [ [3,4,2,3], [13,21,13,21], [13,21,11,17], [1000,200], [1001,201], [1000,1000], [1024,1024,1024,1024], [1024,1024,256,256], [1050,1050], [1001,1001], [1001,999,101,99] ]
#matrixSizes = [ [1000,200], [1001,201], [1000,1000], [1050,1050], [1001,1001] ]
#matrixSizes = [ [5,2], [3,3] ]
#matrixSizes = [ [13,21], [301,301], [201,201], [200,200] ]
#dataTypes   = ["double", "int64", "int32", "int16", "uint64", "float" ] 
#dataTypes   = ["double"] 
dataTypes   = ["double", "int64"] 
#dataTypes   = ["float"] 
successString = "Command completed successfully"
# each test is a sequence of create, fill and comparison (and, of course, cleanup)
tests = []
t = SciDBTest("i+j","multiply(T,transpose(T))", "i*j*%(J)d + (i+j)*(%(J)d*(%(J)d +1)/2)+%(J)d*(%(J)d+1)*(2*%(J)d+1)/6")
tests.append(t)


t = SciDBTest("i+j","aggregate(T, count(x))", "%(I)d*%(J)d")
t.setResultDim(0)
t.setResultDataType("uint64")
t.setNullableResult(True)
tests.append(t)

t = SciDBTest("i+j","aggregate(T, count(x), i)", "%(J)d")
t.setResultDim(1, "i")
t.setResultDataType("uint64")
t.setNullableResult(True)
tests.append(t)

t = SciDBTest("i+j","aggregate(T, count(x), j)", "%(I)d")
t.setResultDim(1, "j")
t.setResultDataType("uint64")
t.setNullableResult(True)
tests.append(t)

t = SciDBTest("i+j","aggregate(T,avg(x),i)", "i+(%(J)d+1.)/2.")
t.setResultDim(1)
t.setResultDataType("double")
t.setNullableResult(True)
tests.append(t)

t = SciDBTest("i+j","aggregate(T,avg(x),j)", "j+(%(I)d+1.)/2.")
t.setResultDim(1,'j')
t.setResultDataType("double")
t.setNullableResult(True)
tests.append(t)

t = SciDBTest("i+j","aggregate(T, sum(x))", "%(I)d*%(J)d*(%(I)d+%(J)d+2)/2")
t.setResultDim(0)
t.setNullableResult(True)
tests.append(t)

t = SciDBTest("i+j","aggregate(T,sum(x),i)", "i*%(J)d + %(J)d*(%(J)d+1)/2")
t.setResultDim(1)
t.setNullableResult(True)
tests.append(t)

t = SciDBTest("i+j","aggregate(T,sum(x),j)", "j*%(I)d + %(I)d*(%(I)d+1)/2")
t.setResultDim(1, 'j')
t.setNullableResult(True)
tests.append(t)

t = SciDBTest("i+j","aggregate(T, min(x))", "2")
t.setNullableResult(True)
t.setResultDim(0)
t.setNullableResult(True)
tests.append(t)

t = SciDBTest("i+j","aggregate(T,min(x),i)", "i+1")
t.setNullableResult(True)
t.setResultDim(1)
t.setNullableResult(True)
tests.append(t)

t = SciDBTest("i+j","aggregate(T,min(x),j)", "j+1")
t.setNullableResult(True)
t.setResultDim(1,"j")
t.setNullableResult(True)
tests.append(t)

t = SciDBTest("i+j","aggregate(T, max(x))", "%(I)d+%(J)d")
t.setNullableResult(True)
t.setResultDim(0)
t.setNullableResult(True)
tests.append(t)

t = SciDBTest("i+j","aggregate(T,max(x),i)", "%(J)d+i")
t.setResultDim(1)
t.setNullableResult(True)
tests.append(t)

t = SciDBTest("i+j","aggregate(T,max(x),j)", "j+%(I)d")
t.setResultDim(1,"j")
t.setNullableResult(True)
tests.append(t)

t = SciDBTest("i-j","aggregate(T, sum(x))", "%(I)d * %(J)d * (%(I)d - %(J)d)/2")
t.setResultDim(0)
t.setNullableResult(True)
tests.append(t)

t = SciDBTest("i-j","aggregate(T,sum(x),i)", "i*%(J)d - %(J)d*(%(J)d+1)/2")
t.setResultDim(1)
t.setNullableResult(True)
tests.append(t)

t = SciDBTest("i-j","aggregate(T,sum(x),j)", "%(I)d*(%(I)d+1)/2 - j*%(I)d")
t.setResultDim(1, 'j')
t.setNullableResult(True)
tests.append(t)

#t = SciDBTest("i+j","var(T)", "%(I)d * %(J)d /6.", isOnlySquare=True)
t = SciDBTest("i+j","aggregate(T, var(x))", "%(I)d * %(J)d * (%(I)d * %(I)d + %(J)d * %(J)d -2.)/(12.*(%(I)d * %(J)d -1.))")
t.setResultDim(0)
t.setResultDataType("double")
t.setNullableResult(True)
tests.append(t)

            
#t = SciDBTest("i+j","stdev(T)", "sqrt(%(I)d * %(J)d /6.)", isOnlySquare=True)
t = SciDBTest("i+j","aggregate(T, stdev(x))", "sqrt( %(I)d * %(J)d * (%(I)d * %(I)d + %(J)d * %(J)d -2.)/(12.*(%(I)d * %(J)d -1.)))")
t.setResultDim(0)
t.setResultDataType("double")
t.setNullableResult(True)
tests.append(t)

for t in tests:
    runTest(t, matrixNames, matrixSizes, dataTypes)
print "Completed"

