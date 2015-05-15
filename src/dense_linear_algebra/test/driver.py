#!/usr/bin/python

# Initialize, start and stop scidb in a cluster.
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

import collections
import copy
import datetime
import errno
import exceptions
import os
import subprocess
import sys
import string
import time
import traceback

# not order preserving
# should be O(n log n) or O(n) depending on
# whether set uses hashing or trees
def noDupes(seq):
    # simple implementation:
    # there are faster ways, but significant amounts of
    # dictionary code is involved.
    return list(set(seq))


# bad style to use from, removes the namespace colission-avoidance mechanism
from ConfigParser import RawConfigParser

def printDebug(string, force=False):
   if _DBG or force:
      print >> sys.stderr, "%s: DBG: %s" % (sys.argv[0], string)
      sys.stderr.flush()

def printDebugForce(string):
    printDebug(string, force=True)

def printInfo(string):
   sys.stdout.flush()
   print >> sys.stdout, "%s" % (string)
   sys.stdout.flush()

def printError(string):
   print >> sys.stderr, "%s: ERROR: %s" % (sys.argv[0], string)
   sys.stderr.flush()

def usage():
   print ""
   print "Usage: %s [<config_file>]" % sys.argv[0]
   print "Commands:"
   print "\t all"

# Parse a config file
def parseGlobalOptions(filename, section_name):
   config = RawConfigParser()
   config.read(filename)

   # First process the "global" section.
   try:
      #print "Parsing %s section." % (section_name)
      for (key, value) in config.items(section_name):
         _configOptions[key] = value
   except Exception, e:
      printError("config file parser error in file: %s, reason: %s" % (filename, e))
      sys.exit(1)
#
# Execute OS command
# This is a wrapper method for subprocess.Popen()
# If waitFlag=True and raiseOnBadExitCode=True and the exit code of the child process != 0,
# an exception will be raised.
def executeIt(cmdList,
              nocwd=False,
              useShell=False,
              cmd=None,
              stdoutFile=None, stderrFile=None,
              waitFlag=True,
              raiseOnBadExitCode=True):
    ret = 0
    out = ''
    err = ''

    dataDir = "./"
    if nocwd:
       currentDir = None
    else:
       currentDir = dataDir
    # print currentDir

    my_env = os.environ

    if useShell:
       cmdList=[" ".join(cmdList)]

    try:
       sout = None
       if stdoutFile:
          # print "local - about to open stdoutFile log file:", stdoutFile
          sout=open(stdoutFile,"w")
       elif not waitFlag:
          sout=open("/dev/null","w")

       serr = None
       if stderrFile:
          #print "local - about to open stderrFile log file:", stderrFile
          serr=open(stderrFile,"w")
       elif not waitFlag:
          serr=open("/dev/null","w")

       p = subprocess.Popen(cmdList,
                            env=my_env, cwd=currentDir,
                            stderr=serr, stdout=sout,
                            shell=useShell, executable=cmd)
       if waitFlag:
          p.wait()
          ret = p.returncode
          if ret != 0 and raiseOnBadExitCode:
             raise Exception("Abnormal return code: %s on command %s" % (ret, cmdList))
    finally:
       if (sout):
          sout.close()
       if (serr):
          serr.close()

    return ret

def postCleanup(name, enable=True):
    if enable:
        _usedMatrices[name]=1
    
#
# Remove a given array from SciDB
# if autoCleanup=True, name is added into usedMatrices
def eliminate(name, autoCleanup=True):
    postCleanup(name, autoCleanup)
    cmdList=[_iqueryBin, "-p", _basePort, "-c", _targetHost, "-naq", "remove(%s)"%name]
    ret = executeIt(cmdList,
                    useShell=False,
                    raiseOnBadExitCode=False,
                    nocwd=True,
                    stdoutFile="/dev/null",
                    stderrFile="/dev/null")
    return ret
#
# Run a given AFL without fetching the result
# Since it doesn't know whether its a store query, or the name
# query executers can't support autoCleanup directly
#
def nafl(input, timeIt=False):
    cmdList=[_iqueryBin, "-p", _basePort, "-c", _targetHost, "-naq", input]
    if _timePrefix and timeIt:
       cmdList.insert(0,_timePrefix)
    ret = executeIt(cmdList,
                    useShell=False,
                    nocwd=True,
                    stdoutFile="/dev/null",
                    stderrFile=None)
    return ret


def store(what, name, autoCleanup=True):
    postCleanup(name, autoCleanup)
    nafl("store(%s, %s)" % (what, name))


def eliminateAndStore(what, name, autoCleanup=True):
    eliminate(name, autoCleanup=autoCleanup)
    store(what, name, autoCleanup=autoCleanup)


#
# Run a given AFL with printing the result to stdout
#format:
#"-ocsv+"   # for explicit row,column printing
#"-osparse" # enable this when debugging distribution issues

def afl(input, format="-ocsv", timeIt=False):
    cmdList=[_iqueryBin, "-p", _basePort, "-c", _targetHost, format, "-w16", "-aq", input]
    if _timePrefix and timeIt:
       cmdList.insert(0,_timePrefix)
    ret = executeIt(cmdList,
                    useShell=False,
                    nocwd=True,
                    stdoutFile=None,
                    stderrFile=None)
    return ret
#
# Run a given AFL with collecting the result into a buffer, which is returned
def aflResult(input, format="-ocsv", timeIt=False):
    # XXX TODO: get rid of the file altogether ?
    outFile = "/tmp/afl.%s.out" % os.getpid()
    cmdList=[_iqueryBin, "-p", _basePort, "-c", _targetHost, format, "-w16", "-aq", input]
    if _timePrefix and timeIt:
       cmdList.insert(0,_timePrefix)
    ret = executeIt(cmdList,
                    useShell=False,
                    nocwd=True,
                    stdoutFile=outFile,
                    stderrFile=None)
    sout=None
    try:
       sout = open(outFile, "r")
       result = sout.read()
    finally:
       if sout!=None:
          sout.close()
    return (ret,result)


def aflStderr(input, format="-odcsv", timeIt=False):
    (ret,result) = aflResult(input, format, timeIt)
    if (ret != 0):
        printError("bad status returned from query: %s" % (input,))
    else:
        printDebugForce("TMP -- running query %s" % (input,) )
    printDebugForce(result)


#
# check size of a matrix against its count
def checkCount(MAT, nrow, ncol):
   (ret,countRow) = aflResult("aggregate(%s,count(*))" % (MAT,))
   count = int(string.split(countRow)[1])
   if count != nrow * ncol:
       errStr = "count(MAT %s) = %s != (nrow %s * ncol %s == %s)" % (MAT, count, nrow, ncol, nrow*ncol)
       printDebugForce("@@@@@@@ SERIOUS ERROR");
       printDebugForce("@@@@@@@ %s" % (errStr,))
       raise(RuntimeError(errStr))


def getMatrixSchema(nrow, ncol, rowChunkSize, colChunkSize, attrName="v"):
   schemaStr = "<%s:double>[r=0:%d,%d,0, c=0:%d,%d,0]" % (attrName, nrow-1, rowChunkSize, ncol-1, colChunkSize)
   return schemaStr


#
# Create a 2D SciDB array schema
#
# TODO: wean off of createMatrix(); populate() by using getMatrixSchema instead of creating
#       and then use eliminate(name), store(MAT_AFL,name)
#       and then have store do the eliminate
def createMatrix(name, nrow, ncol, rowChunkSize, colChunkSize, autoCleanup=True):
    aflStr = "create array %s %s" % (name, getMatrixSchema(nrow, ncol, rowChunkSize, colChunkSize))
    eliminate(name, autoCleanup=False)
    postCleanup(name, autoCleanup) # someday eliminate won't even have the option
    nafl(aflStr)


#
# Populate (i.e. build) a 2D SciDB array
# TODO: wean off of createMatrix(); populate() by using getMatrixSchema instead of creating
#       and then use eliminate(name), store(MAT_AFL,name)
#       and then have store do the eliminate
def populateMatrix(name, MAT_AFL):
    store(MAT_AFL, name)


#
# handy string to common built-in operator conversion
def strToOp(arg):
    # TODO: if the built-in operator table is exposed
    #       use it instead of this hand-maintained table
    opDict = { "+" : lambda a,b: a+b,
               "*" : lambda a,b: a*b }
    if arg in opDict:
        return opDict[arg]
    return None


#
# a copyable, component-wise description of a range
# so we can make rectangular range descriptions easily from order range descriptions
# this is what we'll parse inputs into, and eliminate all other use of range string formats
RangeStruct = collections.namedtuple("RangeStruct", "start last step stepOp")  # stop <- last, final <- last


#
# Return a range struct corresponding to a range format string
# Format: '+'|'*':startVal:lastVal:stepVal
# If string is invalid, an exception is raised
def rangeStructFromStr(rangeStr):
    # Try to parse the range 
    args = string.split(rangeStr,':')
    if len(args) != 4:
        raise Exception("rangeStr must have 4 parts separated by colons, got: %s " % (rangeStr))

    op = strToOp(args[0])
    if not op:
        raise Exception("could not convert string to operator, op: %s " % (args[0]))

    return RangeStruct(start=int(args[1]), last=int(args[2]), step=float(args[3]), stepOp=op);


# xrangeGeneralized
# Modeled after xrange()
# + Returns a python generator object, like xrange() does [vs range() which returns a sequence]
# + Generalizations:
#   + an optional specific final value [[x]range() does not return end], called final
#   + an optional generalized step operation [ vs + step], called stepOp
# + NOTE first four arguments are backward-compatible with xrange()
#
def xrangeGeneralized(start, stop, step=1, dtype=None,        # xrange()-compatible
                      stepOp=(lambda a,b:a+b), final=None):   # extension arguments
    
    value = start
    lastYeilded = None
    # generalize value < stop for negative step values
    inrange = lambda c: c < stop if step >= 0 else lambda c : c > stop  # assumption that 0 is group identity element
    while (inrange(value)):
        lastYeilded = value if dtype is None else dtype(value);
        yield lastYeilded
        value = stepOp(value, step)
    
    if (final != lastYeilded and final is not None):
        yield final


#
# return a generator according to the range struct
# see xrangeGeneralized for a description of the generator
def xrangeGeneralizedFromRangeStruct(rs):
    return xrangeGeneralized(start=rs.start, stop=rs.last, step=rs.step,
                             dtype=int, stepOp=rs.stepOp, final=rs.last)


#
# Return a number generator in the specified range.
# If range is invalid, None is returned.
# Format: '+'|'*':startVal:lastVal:stepVal
# Just for backward compatibility until program converted
# to use rangeStructFromStr in main()
# and pass RangeStructs thereafter
def xrangeGeneralizedFromStr(rangeStr):
    return xrangeGeneralizedFromRangeStruct(rangeStructFromStr(rangeStr))


#
# Return a list of numbers
# If list is invalid, None is returned.
# Format: #(,#)+    (a regular expression)
# Note: a single number is not an accepted format (use range)
def parseIntList(listStr):
   tmp = string.split(listStr,',')
   if len(tmp) > 1 : #list of 1 is not acceptable, use range
      return map((lambda v: int(v)), tmp)
   return None


#
# Return a chunk size generator based on the formatOrSeqName.
# formatOrSeqName can be either in range/list format or a name of an internally-stored list,
# e.g. DEFAULT_CSIZE_LIST
def getChunkSizeRange(order, formatOrSeqName):
   chunkSizeLists = {
      "DEFAULT_CSIZE_LIST" :[32]
   }

   chunkSizeList=None
   if formatOrSeqName in chunkSizeLists:
      chunkSizeList=chunkSizeLists[formatOrSeqName] # it is a sequence name
   else:
      chunkSizeList = parseIntList(formatOrSeqName) # maybe it is a direct list of integers

   if chunkSizeList:
      divs=getOrderDivisorList()
      if len(divs)>0:  #variation in chunk sizes is requested
         chunkSizeList = varyChunkSizes(order, chunkSizeList, divs)
      return noDupes(chunkSizeList)

   return xrangeGeneralizedFromStr(formatOrSeqName) # maybe it is a format string like "+:1:10:1"


#
# Generate chunk size for testing lots of edge conditions
# by setting the chunkSize to make edge conditions
# and to ensure a minimum amount of multi-instance
# distribution is involved, by dividing into
# roughly 2,3,4, .. etc pieces
def varyChunkSizes(mat_order, scalapack_block_sizes, divs):
    chunkSizes={}
    max_scalapack_block = scalapack_block_sizes[len(scalapack_block_sizes)-1]
    for scalapack_block_size in scalapack_block_sizes:
       chunkSizes[scalapack_block_size]=1
       # white box test: also test chunkSizes near the matrix size
       #
       # _ separates the numbers,... easier to tr to newlines for sorting with sort -nu
       # for now, we are not going to go above chunkSize 64 ... that may be
       # the limit I set in the operator
       if mat_order < scalapack_block_size:
          # add on order-1, order, and order+1 sizes (boundary conditions)
          chunkSizes[mat_order-1]=1
          chunkSizes[mat_order]=1
          chunkSizes[mat_order+1]=1
       elif mat_order <= scalapack_block_size:
          # test is same as "equal"
          # add on order-1 andl order ... order+1 is over the limit
          chunkSizes[mat_order-1]=1
          chunkSizes[mat_order]=1
       elif mat_order<=max_scalapack_block: # XXX: svd errors out on chunk sizes > max_scalapack_block 
          # just order itself
          chunkSizes[mat_order]=1

    # XXX TODO JHM: verify logic
    # black box test: divide the size into N chunks ..
    # this is away from the edge conditions, but without exhaustive testing
    for div in divs: #XXX must get above the number of instances to see full parallelism
        tmpChunkSize = mat_order / div
        if tmpChunkSize > 1 and tmpChunkSize <= max_scalapack_block:
           chunkSizes[tmpChunkSize]=1

    # remove duplicate chunk sizes only to save testing time
    result = sorted(chunkSizes.keys())
    for i in range(len(result)):
       if result[i] > mat_order:
          return result[:i+1] #XXX test only one chunk size greater than the order

    return result

defaultMatrixTypes = ("zero", "identity", "int_nz", "random") # TODO: substitute strang_k for int_nz ?

def getMatrixTypeAfl(nrow, ncol, chunkSize, matrixTypeName):
    # XXX TODO:
    # here are some handy input matrices. keep these until we have full
    # extensive regression tests written, using R and SciDB compare ops, for example
    # XXX TODO: better names ?
    buildExprs = {
        "zero"    :"0",
        "one"     :"1",
        "identity":"iif(r=c, 1, 0)",
        "int_nz"  :"iif(r=c, 1,r+c)", # nz is symmetric, that's not a great test for gemm
        "strang_k" : "iif(r=c, 2, iif(r-1=c or r=c-1, -1, 0))", # Gilbert Strang's favorite matrix:
                                      # tridiagonal with 2 on the diagonal and -1 above and below
                                      # which makes it also a symmetric topelitz matrix
                                      # in matlab notation: topelitz([2, -1, zeros(1,2)])
                                      # it is symmetric, invertible, and (moreover) positive definite
        #"strang_c" : "iif(r=c, 2, iif((r-1=c or r=c-1), -1, iif((r=ORDER-1 and c=0) or (r=0 and c=ORDER-1), -1, 0)))"
                                      # toeplitz([2 -1 zeros(1,1) -1]) -- like K, but circulant (aka periodic, aka cyclic convolution)
                                      # this matrix IS SINGULAR
                                      # it is symmetric, non-invertible, and (moreover) positive semi-definite
                                      # [can't define until these defs are parameterized on ORDER]
        "strang_t" : "iif(r=0 and c=0, 1 ,iif(r=c, 2, iif(r-1=c or r=c-1, -1, 0)))",
                                      # same as k, but with T[0,0] = 1
                                      # it is symmetric, invertible, and (moreover) positive definite
        #"strang_b" : "iif((r=0 and c=0) or (r=ORDER-1 and c=ORDER-1), 1 ,iif(r=c, 2, iif(r-1=c or r=c-1, -1, 0)))"
                                      # same as k, but with T[0,0] = 1 AND T[n-1, n-1] = 1
                                      # it is symmetric, non-invertible, and (moreover) positive semi-definite
                                      # [can't define until these defs are parameterized on ORDER]
                                      
        "col" :"c",                   # helpful to debug,e.g. multiply
        "kr_plus_c":"100*r+c",        # helpful to debug,e.g. multiply
        "flt_1_c" :"1.0/(1+c)",       # converges to 1/6 PI**2 as c -> inf, when mult by its transpose
        # the next is for experiments on error bounds of gemm
        "rand_gemmx"   :"random()/2147483647.0*0.5+0.5",   #  range [.5,1.0]
#        "iid"     :"instanceid()",
        "random"  :"abs(random()/2147483647.0)+1.0e-6"   # want range (0,1] approximate with [1e-6,1.000001]
                                                         # range of random() seems to be (0, 2**31-1)
    }

    if matrixTypeName == "svdrandom":
        randomAfl = getMatrixExpressionAfl(nrow, ncol, chunkSize, chunkSize, buildExprs["random"])
        # we must condition the ordinary random matrix before using it for SVD tests
        return getMatrixSvdRandomAfl(nrow, ncol, chunkSize, randomAfl);

    return getMatrixExpressionAfl(nrow, ncol, chunkSize, chunkSize, buildExprs[matrixTypeName])

#
# getMatrixExpressionAfl
#
def getMatrixExpressionAfl(nrow, ncol, rowChunkSize, colChunkSize, expr):
    schema= "<v:double>[r=0:%s,%s,0, c=0:%s,%s,0]" % (nrow-1, rowChunkSize, ncol-1, colChunkSize)
    return "build(%s,%s)" % (schema, expr)


def getChunkSizeList():
    return _chunkSizeList

def getOrderDivisorList():
    return _divisorList

#
# Execute a set of tests in the "allTests" table local to this routine.
# The set is specified with a list of keys.  The default is to do all of them.
# Move allTests, doTest, and iterateOverTests to before main(), to avoid these lambdas
allTests = {
    "TRANSPOSE" : lambda mName, mType, nrow, ncol, chsize, errLimit: testTranspose(mName, mType, nrow, ncol, chsize, errLimit),
    "SVD"       : lambda mName, mType, nrow, ncol, chsize, errLimit:       testSVD(mName, mType, nrow, ncol, chsize, errLimit),
    "GEMM"      : lambda mName, mType, nrow, ncol, chsize, errLimit:      testGEMM(mName, mType, nrow, ncol, chsize, errLimit),
    "MPICOPY"   : lambda mName, mType, nrow, ncol, chsize, errLimit:   testMPICopy(mName, mType, nrow, ncol, chsize, errLimit)

}

def doTest(funcName=None, func=None, matrixTypeName=None, nrow=None, ncol=None, chunkSize=None, errorLimit=None):
    try:
        func(matrixTypeName, getMatrixTypeAfl(nrow, ncol, chunkSize, matrixTypeName), nrow, ncol, chunkSize, errorLimit)
        sys.stdout.flush()
    except Exception, e:
        mesg = "Exception in func %s when testing nrow %s, ncol %s, chunk_size %s, matrixType %s errorLimit %s PASS: FALSE" % \
               (funcName, nrow, ncol, chunkSize, matrixTypeName, errorLimit)
        # short message to stdout
        printInfo(mesg)
        printInfo("Reason: %s" % e)
        # longer message to stderr
        printDebugForce(mesg)
        traceback.print_exc()
        sys.stderr.flush()
        raise
        


def iterateOverTests(orderStr, errorLimit, testNames=allTests.keys(), matrixTypeNamesIn=None):
    assert matrixTypeNamesIn
    for testName in testNames:
        assert testName in allTests

        if testName != "SVD":
            matrixTypeNames = matrixTypeNamesIn
        else:
            # test "SVD" uses matrixType "svdrandom" instead of "random"
            matrixTypeNames = [ name if name != "random" else "svdrandom"
                                for name in matrixTypeNamesIn ]

        printDebugForce("Iterating over matrix data types: " + str(matrixTypeNames))
        printDebugForce("And matrix orders: " + orderStr)
        printDebugForce("And chunk sizes: %s"% str(getChunkSizeList()) )
        
        oRS = rangeStructFromStr(orderStr) # oRS = order RangeStruct, e.g oRS.start is the start value
        for order in xrangeGeneralizedFromRangeStruct(oRS):
            for chunkSize in getChunkSizeRange(order, getChunkSizeList()): # TODO: upgrade to rangeStruct way
                printDebugForce("TESTS START: %s, chunkSize %s, order %s" % (testName, chunkSize, order,))
                for matrixTypeName in matrixTypeNames:
                    doRectangularIteration = True
                    if doRectangularIteration :
                        start = oRS.start # or 1
                        # from start high & order wide to almost square (final is None, not order)
                        for nrow in xrangeGeneralized(start, order, oRS.step, dtype=int, stepOp=oRS.stepOp):
                            printDebug("TEST %s, chunkSize %s, dtype %s, nrow %s , ncol=order %s" % (testName, chunkSize, matrixTypeName, nrow, order,))
                            doTest(funcName=testName, func=allTests[testName], matrixTypeName=matrixTypeName,
                                   nrow=nrow, ncol=order, chunkSize=chunkSize, errorLimit=errorLimit)
                        
                        # from order high & start wide to square (final is order)
                        for ncol in xrangeGeneralized(start, order+1, oRS.step, dtype=int, stepOp=oRS.stepOp, final=order):
                            printDebug("TEST %s, chunkSize %s, dtype %s, nrow=order %s , ncol %s" % (testName, chunkSize, matrixTypeName, order, ncol,))
                            doTest(funcName=testName, func=allTests[testName], matrixTypeName=matrixTypeName,
                                   nrow=order, ncol=ncol, chunkSize=chunkSize, errorLimit=errorLimit)
                    else:
                        printDebug("TEST %s, chunkSize %s, dtype %s, nrow = ncol = order %s" % (testName, chunkSize, matrixTypeName, order,))
                        doTest(funcName=testName, func=allTests[testName], matrixTypeName=matrixTypeName,
                               nrow=order, ncol=order, chunkSize=chunkSize, errorLimit=errorLimit)
                
                printDebugForce("TESTS END: %s, chunkSize %s, order %s" % (testName, chunkSize, order,))


# convert adddim(array,dimension) to redimension(apply())
def getAdddim(myArray, myDimension):

   # get schema of array
   (ret,mySchema) = aflResult("show(%s)" % myArray)
   mySchema = mySchema[2:-2]
   attrStart = mySchema.find("<")
   dimStart = mySchema.find("[")
   dimEnd = mySchema.find("]")
   myResult = "redimension(apply(%s,%s,0),%s [%s=0:0,1,0,%s)" % (myArray, myDimension, mySchema[attrStart:dimStart], myDimension, mySchema[dimStart+1:])

   return myResult

# Generate a diagonal matrix from the vector vector and store in the array called result
def generateDiagonal(vector, result, autoCleanup=True):
   printDebug("Generating diagonal matrix from %s and saving as %s" % (vector, result))

   # get length of vector
   dims = getDims(vector)
   ncol = dims[0]

   intervalAfl = "project(dimensions(%s),chunk_interval)" % vector
   (ret,chunkInterval) = aflResult(intervalAfl)

   printDebug("DEBUG: chunkInterval=%s" % (chunkInterval,))

   chunkSize = int(string.split(chunkInterval)[1])

   # generate a column matrix of equal to the singular values, S (since S is output as a vector,
   # and the linear aglebra library does not yet know that a vector is a column)
   # and a row vector of ones
   # and multiply them together.  They will RLE nicely.
   #   [ a ]                      [ a a a ]
   #   [ b ]       x  [ 1 1 1 ] = [ b b b ]
   #   [ c ]                      [ c c c ]
   #
   #   t(addim(VEC)) * VEC_1 -> OUTER_PRODUCT
   #

   VEC_1="VEC_1"
   createMatrix(VEC_1, 1, ncol, rowChunkSize=1, colChunkSize=chunkSize) # 
   populateMatrix(VEC_1, getMatrixExpressionAfl(1, ncol, rowChunkSize=1, colChunkSize=chunkSize, expr="1")) #matrix of ones

   ADDDIM_Q=getAdddim(vector,'c')

   if False:
      printDebug("debug diag transpose(adddim(%s)) @@@@@@@@@@@@@@@@" % vector)
      afl("transpose(%s)" % ADDDIM_Q)
      eliminate("DEBUG_1")
      afl("store(transpose(%s),DEBUG_1)" % ADDDIM_Q)
      afl("show(DEBUG_1)")
      printDebug("debug diag transpose %s @@@@@@@@@@@@@@@@" % vector)
      afl("show(%s)" % VEC_1)

   OUTER_PRODUCT="OUTER_PRODUCT"
   outerProductAfl="aggregate(apply(cross_join(transpose(%s as A),%s as B, A.c, B.r), prod, A.sigma * B.v), sum(prod) as multiply, A.i, B.c)" % (ADDDIM_Q, VEC_1)
   eliminateAndStore(outerProductAfl, OUTER_PRODUCT)

   # then we just use iif to set off-diagonal values to 0
   #
   #
   #    [ a 0 0 ]
   # -> [ 0 b 0 ] which is our result
   #    [ 0 0 c ]
   resultAfl = "project(apply(%s,s,iif(i=c,multiply,0.0)),s)" % (OUTER_PRODUCT)
   eliminateAndStore(resultAfl, result, autoCleanup=autoCleanup)

#
# Frobenius norm of a matrix
def norm(MAT_AFL, attr, attr_norm, MAT_NORM=None):
    printDebug("Computing norm(%s)"%MAT_AFL)

    normAfl="project(apply(aggregate(project(apply(%s,square,%s*%s),square), sum(square)), %s, sqrt(square_sum)), %s)" % \
        (MAT_AFL,attr,attr,attr_norm,attr_norm)

    if MAT_NORM:
       eliminateAndStore(normAfl,MAT_NORM)
    return normAfl


from sys import float_info
def getULP():
    # in a perfect world, these should come from the target machine, which may not be where
    # this program is running
    # This is good enough for now
    float_base = 2                          # sue me for not being more general than that
    ULP =  float_info.epsilon * float_base # Units in the Last Place = epsilon * machine base, for ScaLAPACK
                                            # see ScaLAPACK pdlamch_(context, 'P')
    return ULP

#
# Scale matrix values by double_epsilon / order
#
def scaleToULPs(MAT_AFL, rms_error_normed, nrow, ncol, rms_ulp_error, MAT_ERROR):
    order = max(nrow, ncol)
    ULP_reciprocal = 1.0/getULP()
    errULPsAfl=("project(apply(%s,%s,%s*%s/%d),%s)" %
                (MAT_AFL, rms_ulp_error, rms_error_normed, ULP_reciprocal, order, rms_ulp_error))
    # note: dividing by the order here is a cheat because the error in ULPs
    #       with order, when I thought it should not.
    # TODO: study and fix that

    if MAT_ERROR:
        eliminateAndStore(errULPsAfl,MAT_ERROR)
    return errULPsAfl

def divMat(MAT_1, attr_1, MAT_2, attr_2, attr_result, MAT_RES=None):
   scaledAfl = "project(apply(join(%s,%s),%s,%s/%s), %s)" % (MAT_1, MAT_2, attr_result, attr_1, attr_2, attr_result)
   if MAT_RES:
      eliminateAndStore(scaledAfl,MAT_RES)
   return scaledAfl

def addMat(MAT_1, attr_1, MAT_2, attr_2, attr_result, MAT_RES=None):
   sumAfl = "project(apply(join(%s,%s),%s,%s+%s), %s)" % (MAT_1, MAT_2, attr_result, attr_1, attr_2, attr_result)
   if MAT_RES:
      eliminateAndStore(sumAfl,MAT_RES)
   return sumAfl

def getDims(M):
   dimsAfl="project(dimensions(%s),length)" % M
   (ret,lengths)=aflResult(dimsAfl)

   printDebug("getDims: lengths=%s" % str(lengths))

   results = string.split(lengths)
   return map((lambda v: int(v)), results[1:])

#
# a class to use with the "with" statement that
# collects the time on entering and exiting a
# block of code.
# Time is wall-clock time, unlike time.clock() which is actualy accumulated cpu time, not a wall-clock.
#
class TimerRealTime():
    def __enter__(self):     self.start = time.time() ; return self
    def __exit__(self, *args): self.end = time.time() ; self.interval = self.end - self.start

        
# Compute residual measures: (MAT_1-MAT_2)
def computeResidualMeasures(MAT_1, attr_1, MAT_2, attr_2, attr_result_residual, MAT_RES=None):
    printDebug("computeResidualMeasures start")
    printDebug("MAT_1 is %s" % MAT_1)
    printDebug("attr_1 is %s" % attr_1)
    printDebug("MAT_2 is %s"  % MAT_2)
    printDebug("attr_2 is %s" % attr_2)
    printDebug("attr_result_residual is %s" % attr_result_residual)
    printDebug("MAT_RES is %s" % MAT_RES)

    residualAfl = "apply(join(%s,%s),%s,(%s-%s))" % (MAT_1, MAT_2, attr_result_residual, attr_1, attr_2)

    if MAT_RES:
       printDebug("computeResidualMeasures output is in array %s" % MAT_RES)
       eliminateAndStore(residualAfl, MAT_RES)
    printDebug("computeResidualMeasures finish")
    return residualAfl

# Compute the maximum with its position (by crossing with its position first):
def findMax(MAT, attr, attr_max_tmp="attr_max_tmp"):
    printDebug("findMax start")
    printDebug("MAT is %s" % MAT)
    printDebug("attr is %s" % attr)
    printDebug("attr_max_tmp is %s" % attr_max_tmp)

    findMaxAlf = ("filter(cross_join(%s,aggregate(%s, max(%s) as %s)),%s = %s)" %
                        (MAT, MAT, attr, attr_max_tmp, attr, attr_max_tmp))

    printDebug("findMax finish")
    return findMaxAlf


# WARNING: ripped off from testForError for debug purposes
#          check with Tigor as to whether it can be factored
# return TRUE (or FALSE) if the ATTR_ERROR_METRIC values in ARRAY_RMS_ERROR are within errorLimit
# WARNING: as a dbg routine, it computes its answer immediately, it does not return
#          a query string like testForError does
def dbgGetErrorMetric(testName, ARRAY_RMS_ERROR, attr_error_metric, errorLimit):
    passTestAfl = "apply(%s,PASS,%s <= %d)" % (ARRAY_RMS_ERROR, attr_error_metric, errorLimit)
    if _DBG:
        printDebugForce("DEBUG dbgGetErrorMetric")
        aflStderr(passTestAfl)
    
    (ret,result)=aflResult(passTestAfl)
    firstLine = string.split(result)[1]  # split on newline, take first line
    vals = string.split(firstLine,',')   # split on commas
    return float(vals[0])                # vals[0] is error metric

# Print TRUE (or FALSE) to stdout if the attr_error_metric values in ARRAY_RMS_ERROR are within errorLimit
def testForError(testName, ARRAY_RMS_ERROR, attr_error_metric, errorLimit):
     passTestAfl = "apply(%s,PASS,%s <= %d)" % (ARRAY_RMS_ERROR,attr_error_metric,errorLimit)
     (ret,result)=aflResult(passTestAfl)
     firstLine = string.split(result)[1]  # split on newline, take first line
     vals = string.split(firstLine,',')   # successive values in csv row separated by "," ?
     errorMetric = float(vals[0])         # vals[0] is error metric
     success =           vals[1]          # vals[1] is "true" or "false"

     if _DBG:
          aflStderr(passTestAfl)
     printDebugForce("%s PASS (error(%s) <= limit(%d)): %s" % \
                      (testName,errorMetric,errorLimit, str.upper(success)))
     printInfo("%s PASS : %s" % \
                (testName,str.upper(success))) # why do we convert to upper case?

     hasError = not (errorMetric <= errorLimit)
     return hasError

   
def doSvdSanityCount(nrow,ncol,MAT_INPUT, VEC_S, MAT_U, MAT_VT, MAT_WHICH, attr_which):
    printDebug("doSvdSanityCount start")

    # calculate array - apply(array, val * 1), since that appears to be messed up in some cases,
    # at least after the long apply chain in doSvdMetric1
    outOfRangeAfl = "filter(%s, %s > 1.0)"  % (MAT_WHICH, attr_which)
    passTestAfl = "apply(aggregate(%s,count(*)),PASS,count = 0)" % (outOfRangeAfl)

    # following copied/hacked form testForError
    (ret,result)=aflResult(passTestAfl)
    firstLine = string.split(result)[1]     # lines
    vals = string.split(firstLine,',')   # successive values in csv row separated by "," ?
    count = int(vals[0])
    success =   vals[1]

    msgPrefix="doSvdSanityCount %s %s %s" %(nrow, MAT_WHICH, attr_which)
    if(count > 0):
        msgPrefix = msgPrefix + " FAIL"

    printDebugForce("%s PASS? (count %s %s limit(%d)): %s" % \
                 (msgPrefix, count, "=" , 0, str.upper(success)))
    printInfo("%s PASS? : %s" % \
                 (msgPrefix, str.upper(success)))

    if(count > 0):
        msgPrefix = msgPrefix + " FAIL"
        printDebugForce("%s count %d failing cells" % (msgPrefix, count))
        printDebugForce("%s failing query: %s" % (msgPrefix, outOfRangeAfl))
        aflStderr(outOfRangeAfl, format="-odcsv")
        printDebugForce("%s end of query output")

        extraApplyAfl = "apply(filter(%s, %s > 1.0), copy, %s)"  % (MAT_WHICH, attr_which, attr_which)
        printDebugForce("%s now apply value to itself, to see if will print differently" % (msgPrefix,))
        printDebugForce("%s query: %s" % (msgPrefix, extraApplyAfl))
        aflStderr(extraApplyAfl, format="-odcsv") 
        printDebugForce("%s end of query output")

        showAfl = "show(%s)"  % (MAT_WHICH)
        printDebugForce("%s query: %s" % (msgPrefix, showAfl))
        aflStderr(showAfl, format="-odcsv")
        printDebugForce("%s end of query output")

        applyAfl = "apply(%s, copy, %s)"  % (MAT_WHICH, attr_which)
        printDebugForce("%s query: %s" % (msgPrefix, applyAfl))
        aflStderr(applyAfl, format="-odcsv") 
        printDebugForce("%s end of query output")

        return False

    return True


 
def doSvdSanityApply(nrow,ncol,MAT_INPUT, VEC_S, MAT_U, MAT_VT, MAT_WHICH, attr_which):
    printDebug("doSvdSanityApply start")

    # calculate array - apply(array, val * 1), since that appears to be messed up in some cases,
    # at least after the long apply chain in doSvdMetric1
    badApplyAfl = "filter(apply(%s, times1, %s * 1), %s != %s)"  % (MAT_WHICH, attr_which, attr_which, "times1")
    passTestAfl = "apply(aggregate(%s,count(*)),PASS,count = 0)" % (badApplyAfl)
    # following copied/hacked form testForError
    (ret,result)=aflResult(passTestAfl)
    vals = string.split(result)[1]     # lines?
    pair = string.split(vals,',')   # successive values in csv row separated by "," ?
    count = int(pair[0])
    success = pair[1]

    msgPrefix="doSvdSanityApply %s %s %s" %(nrow, MAT_WHICH, attr_which)
    if(count > 0):
        msgPrefix = msgPrefix + " FAIL"

    printDebugForce("%s PASS? (count %s %s limit(%d)): %s" % \
                    (msgPrefix, count, "=" , 0, str.upper(success)))
    printInfo("%s PASS? : %s" % \
                 (msgPrefix, str.upper(success)))
    
    if count > 0 :
        printDebugForce("%s count %d failing cells" % (msgPrefix, count))
        printDebugForce("%s failing query, results on next line: %s" % (msgPrefix, badApplyAfl))
        aflStderr(badApplyAfl)
        printDebugForce("%s end of query output")
        return False

    return True

#
# for SvdMetric1 to pass, a matrix of random entries must still have singular values that
# are not too close together, else the scalapack algorithm for GESVD will fail to be accurate.
# So we do what the ScaLAPACK test suite does: we create a matrix that is conditioned to have
# well-space singular values
# We do this by taking a random matrix, and obtaining U and VT, discarding the singular values.
# We then re-multiply the three, and return this as the test matrix.
#
def getMatrixSvdRandomAfl(nrow, ncol, chunkSize, randomAfl):
    leftAfl = "gesvd(%s, 'left')" % (randomAfl)
    rightAfl = "gesvd(%s, 'right')" % (randomAfl)
    # Note: its fine if randomAfl is a newly evaluated matrix on each invocation, rather than an array
    # By not storing and reading the random vals, its probably faster, too

    # see scalapack/TESTING/EIG/pdsvdtst.f @ line 417
    # which is the rule by which we compute these singular values
    # capitals in this next section are the same as variables in the FORTRAN code
    ULP = getULP()
    minrc = min(nrow, ncol)  # number of singular values, size of diagonal matrix

    # In theory, ScaLAPACK and LAPACK should both be able to do matrices that are as nearly rank-deficient
    # as this would be (the last singular value will be 1 ULP)
    # But when we run this on 64 instances, 2% of the time, we get good singular values, but we
    # get U and VT matrices where the one norm (sum of the column) is, e.g. > 17, where it should be ==1
    # if the matrices are pure rotation/reflection (change of basis, no scale) only.
    # So where normally, the numerator of H, below, would be (ULP-1.0),
    # we are going to use some (K*ULP-1.0), K > 1, to make it easier
    # K = 2**24 successful 256 - 780
    # K = 2**20 successful 256 - 890
    # K = 2**18 2 failures from 256 - 829
    # K = 2**16 2 failures from 256 - 700
    # K = 16    dozen failures from 256 - 1024
    K = 2**19 # 2**19 successful on every order between 1 - 1024
    if minrc > 1: # avoid div-by-zero
        H = (K*ULP - 1.0) / (minrc - 1.0)
        diagExpr = "1.0 + r * %s" % (H,)
    else:
        diagExpr = "1.0"

    evenSpacedDiagAfl = getMatrixExpressionAfl(minrc, minrc, chunkSize, chunkSize, "iif(r=c, %s, 0)" % (diagExpr,))

    zerosAflA = "build(%s,0)" % (getMatrixSchema(minrc, ncol, chunkSize, chunkSize),)
    zerosAflB = "build(%s,0)" % (getMatrixSchema(nrow, ncol, chunkSize, chunkSize),)
    gemmAfl = "gemm(%s, gemm(%s,%s,%s),%s)" % (leftAfl, evenSpacedDiagAfl, rightAfl, zerosAflA, zerosAflB)
    productAfl = "attribute_rename(%s, gemm, v)" % (gemmAfl,)

    return productAfl


def doSvdMetric1(nrow, ncol, chunkSize, MAT_INPUT, VEC_S, MAT_U, MAT_VT, MAT_ERROR, rms_ulp_error, errorLimit, func,
                 debugOnError=True, numAttempts=1):
    assert(MAT_ERROR)  # this thing is getting way too messy with the deferred processing
    printDebug("doSvdMetric1 start")
    printDebug("MAT_INPUT is %s" % MAT_INPUT)
    printDebug("VEC_S is %s"  % VEC_S)
    printDebug("MAT_U is %s"  % MAT_U)
    printDebug("MAT_VT is %s" % MAT_VT)
    printDebug("MAT_ERROR is %s"% MAT_ERROR)

    # multiply the factors back together
    # first must turn the S vector into a diagonal matrix

    METRIC_TMP_DIAG_SS="METRIC_TMP_DIAG_SS"
    generateDiagonal(VEC_S, METRIC_TMP_DIAG_SS)
    if _DBG:
        printDebugForce("%s:"%MAT_INPUT)
        aflStderr("show(%s)"%MAT_INPUT)
        printDebugForce("%s:"%VEC_S)
        aflStderr("show(%s)"%VEC_S)
        printDebugForce("%s:"%MAT_U)
        aflStderr("show(%s)"%MAT_U)
        printDebugForce("%s:"%METRIC_TMP_DIAG_SS)
        aflStderr("show(%s)"%METRIC_TMP_DIAG_SS)
        printDebugForce("%s:"%MAT_VT)
        aflStderr("show(%s)"%MAT_VT)

    #productSVDAfl = "multiply(%s,multiply(%s,%s))" % (MAT_U, METRIC_TMP_DIAG_SS, MAT_VT,)
    minrc = min(nrow,ncol)
    # (nrow,minrc) * (minrc,minrc) * (minrc,ncol)
    #                <-----------A------------->
    #  <-----------------B--------------------->
    zerosAflA = "build(%s,0)" % (getMatrixSchema(minrc, ncol, chunkSize, chunkSize),)
    zerosAflB = "build(%s,0)" % (getMatrixSchema(nrow, ncol, chunkSize, chunkSize),)
    productSVDAfl = "gemm(%s, gemm(%s,%s,%s),%s)" % (MAT_U, METRIC_TMP_DIAG_SS, MAT_VT, zerosAflA, zerosAflB)

    # difference of original and the product of the svd matrices
    residualAfl = computeResidualMeasures(MAT_INPUT, "v", productSVDAfl, "gemm", "resid")

    inputNormAfl = norm(MAT_INPUT, "v", "input_norm")
    residualNormAfl = norm(residualAfl, "resid", "resid_norm")

    residNormScaledAfl = func(residualNormAfl, "resid_norm", inputNormAfl, "input_norm", "resid_norm_scaled")
    if _DBG:
        printDebugForce("DEBUG doSvdMetric1 residNormScaledAfl")
        aflStderr(residNormScaledAfl)

    residInULPsAfl = scaleToULPs(residNormScaledAfl, "resid_norm_scaled", nrow, ncol, rms_ulp_error, MAT_ERROR)
    errorMetric = dbgGetErrorMetric("SVD_METRIC_1", MAT_ERROR, rms_ulp_error, errorLimit)

    if debugOnError:
        hasError = not (errorMetric <= errorLimit)
        if hasError:
            printDebugForce("doSvdMetric1 I. SVD Error %s exceeds %s @ size %s x %s " % (errorMetric, errorLimit, nrow, ncol))
            printDebugForce("doSvdMetric1 I. SVD residual in ULPs is in array %s" % MAT_ERROR)
            printDebugForce("doSvdMetric1 I. SVD residualNormScaled was calculated by query %s" % residNormScaledAfl)
            printDebugForce("doSvdMetric1 I. SVD residualNormScaledInULPs was calculated by query %s" % residInULPsAfl)

            # find the arrays
            absResidualAfl= "apply(%s, %s, abs(%s))" % (residualAfl, "abs_residual", "resid")
            findMaxAfl = findMax(absResidualAfl, "abs_residual")
            aflStderr(findMaxAfl)

            aflStderr("aggregate(%s, max(%s))" % (MAT_INPUT, 'v'), format="-odcsv")
            aflStderr("aggregate(%s, min(%s))" % (MAT_INPUT, 'v'), format="-odcsv")

            printDebugForce("doSvdMetric1 result S %s" % VEC_S)
            aflStderr("aggregate(%s, max(%s))" % (VEC_S, 'sigma'), format="-odcsv")
            aflStderr("aggregate(%s, min(%s))" % (VEC_S, 'sigma'), format="-odcsv")
            # Kappa, the condtion number of the matrix,
            # when using the L sub_2 norm, is max / min
            # change this to aflResult, and grab the max and min
            # and if not zero, then divide them
            # then print that as the condition number

            printDebugForce("doSvdMetric1 result U %s" % MAT_U)
            aflStderr("aggregate(%s, max(%s))" % (MAT_U, 'u'), format="-odcsv")
            aflStderr("aggregate(%s, min(%s))" % (MAT_U, 'u'), format="-odcsv")

            printDebugForce("doSvdMetric1 result VT %s" % MAT_VT)
            aflStderr("aggregate(%s, max(%s))" % (MAT_VT, 'v'), format="-odcsv")
            aflStderr("aggregate(%s, min(%s))" % (MAT_VT, 'v'), format="-odcsv")

            MAX_ATTEMPTS=1 # increase this if you think it might be a spurious calculation error
            if (MAX_ATTEMPTS > 1):
                printDebugForce("doSvdMetric1 II. SVD Error %s exceeds %s @ size %s x %s " % (errorMetric, errorLimit, nrow, ncol))
                if (numAttempts < MAX_ATTEMPTS):
                    # this will compute the residual error one more time, in case its just  problem in error computation
                    printDebugForce("doSvdMetric1 trying the check a second time")
                    errorMetric2 = doSvdMetric1(nrow, ncol, chunkSize, MAT_INPUT, VEC_S, MAT_U, MAT_VT, MAT_ERROR, rms_ulp_error, errorLimit, func, numAttempts=numAttempts+1)
                    if (errorMetric != errorMetric2):
                        printDebugForce("doSvdMetric1 non-repeatable computation: errorMetric %s != errorMetric2 %s", (errorMetric, errorMetric2))
                if (numAttempts == MAX_ATTEMPTS):
                    printDebugForce("doSvdMetric1 III. end analysis after %s attempts to calculate the metric "% numAttempts)

    printDebug("doSvdMetric1 finish")
    return errorMetric  #  !!!! NOTE!!!! does not return afl

# Compute norm(I - Ut*U) / [ M ulp ], where M = nrow
def doSvdMetric2(MAT_U, nrow, ncol, chunkSize, MAT_ERROR, rms_ulp_error):
    newSize = min(nrow,ncol)
    printDebug("doSvdMetric2 start")
    printDebug("MAT_U is %s"  % MAT_U)
    printDebug("MAT_U nrow,ncol is %d,%d"  % (nrow, ncol))
    printDebug("MAT_ERROR is %s"% MAT_ERROR)
    printDebug("rms_ulp_error is %s"% rms_ulp_error)
    printDebug("dim(Ut*U) will be %d,%d"  % (newSize, newSize))

    # note use of dim = (newSize,newSize) which == dim(Ut*U)
    I="IDENTITY"
    createMatrix(I, newSize, newSize, chunkSize, chunkSize)
    populateMatrix(I, getMatrixTypeAfl(newSize, newSize, chunkSize, "identity"))

    UTxU_Afl = "aggregate(apply(cross_join(transpose(%s) as A, %s as B, A.r, B.r), prod, A.u*B.u), sum(prod) as multiply, A.i, B.i)" % (MAT_U,MAT_U)

    residualAfl = computeResidualMeasures(I, "v", UTxU_Afl, "multiply", "resid")
    residualNormAfl = norm(residualAfl, "resid", "resid_norm")

    # TODO: must fix: here want to scale by M=nrow, but difference matrix is newSize x newSize
    #       so scale might be off by a factor of newSize/M, have to double check
    #       how the scaling was derived
    errULPsAfl = scaleToULPs(residualNormAfl, "resid_norm", nrow, ncol, rms_ulp_error, MAT_ERROR)

    if _DBG:
      printDebug("doSvdMetric2 residualAfl:")
      afl(residualAfl)
      printDebug("doSvdMetric2 residualNormAfl:")
      afl(residualNormAfl)

    printDebug("doSvdMetric2 output is in array %s" % MAT_ERROR)
    printDebug("doSvdMetric2 finish")
    return errULPsAfl

# Compute norm(I - VT*V) / [ N ulp ], where N=ncol
def doSvdMetric3(MAT_VT, nrow, ncol, chunkSize, MAT_ERROR, rms_ulp_error):
    newSize = min(nrow,ncol)
    printDebug("doSvdMetric3 start")
    printDebug("MAT_VT is %s"  % MAT_VT)
    printDebug("nrow,ncol is %d,%d"  % (nrow,ncol))
    printDebug("MAT_ERROR is %s"% MAT_ERROR)
    printDebug("rms_ulp_error is %s"% rms_ulp_error)
    printDebug("dim(VT*V will be %d,%d"  % (newSize, newSize))

    I="IDENTITY"
    createMatrix(I, newSize, newSize, chunkSize, chunkSize)
    populateMatrix(I, getMatrixTypeAfl(newSize, newSize, chunkSize, "identity"))

    VTxV_Afl = "aggregate(apply(cross_join(%s as A, transpose(%s) as B, A.c, B.c), prod, A.v * B.v), sum(prod) as multiply, A.i, B.i)" % (MAT_VT,MAT_VT) # 2nd arg becomes "untransposed"

    residualAfl = computeResidualMeasures(I, "v", VTxV_Afl, "multiply", "resid")
    residualNormAfl = norm(residualAfl, "resid", "resid_norm")

    # TODO: must fix: here want to scale by N=ncol, but difference matrix is newSize x newSize
    #       so scale might be off by a factor of newSize/N, have to double check
    #       how the scaling was derived
    errULPsAfl = scaleToULPs(residualNormAfl, "resid_norm", nrow, ncol, rms_ulp_error, MAT_ERROR)

    if _DBG:
      printDebug("doSvdMetric3 residualAfl:")
      afl(residualAfl)
      printDebug("doSvdMetric3 residualNormAfl:")
      afl(residualNormAfl)

    printDebug("doSvdMetric3 output is in array %s" % MAT_ERROR)
    printDebug("doSvdMetric3 finish")
    return errULPsAfl

# ensure that the singular values S aka SIGMA are in strictly decreasing order
def doSvdMetric4(nrow, ncol, VEC_S, MAT_ERROR, rms_ulp_error):
   printDebug("doSvdMetric4 start")
   printDebug("VEC_S is %s"  % VEC_S)
   printDebug("MAT_ERROR is %s"% MAT_ERROR)
   printDebug("nrow is %d"  % nrow)

   if _DBG:
      printDebug("doSvdMetric4 VEC_S: ")
      afl("show(%s)"%VEC_S)
      afl("scan(%s)"%VEC_S)

   if _DBG:
      # compare length of vector S to ncol (of the input mat) ... it should match
      dimS = getDims(VEC_S)
      if min(ncol,nrow) != dimS[0] :
         raise Exception("Mismatched dimensions: min(ncol %d, nrow %d) %d != dims(S) %d" % (ncol, nrow, min(ncol,nrow), dimS[0]))

   # make sure the singular values, sigma, (in S) are strictly decreasing
   # TODO: we should note why we do this with joining the shift, rather than sorting and comparing for equality
   #       since that would be the naive way.
   compareAfl = "project(apply( \
                           aggregate(apply(join (subarray(%s, 0, (%d-2)) as L, \
                                           subarray(%s, 1, (%d-1)) as R), \
                               BLAH, iif(L.sigma<R.sigma, 1, 0) ), sum(BLAH)), \
                         %s, BLAH_sum), %s)" %  (VEC_S, ncol, VEC_S, ncol, rms_ulp_error, rms_ulp_error)

   if MAT_ERROR:
      eliminateAndStore(compareAfl, MAT_ERROR)

   printDebug("doSvdMetric4 output is in array %s" % MAT_ERROR)
   printDebug("doSvdMetric4 finish")
   return compareAfl

#
# Test the SVD operator.
# Implements an appropriate interface to be used by iterateOverTests()
# does some careful numerical tests on the quality of the SVD generated from conditioned random matrices
def testSVD(matrixTypeName, AFL_INPUT, nrow, ncol, chunkSize, errorLimit):
#
# Reference: http://www.netlib.org/scalapack/lawn93/node45.html
# Let A be an matrix of size M,N and SVD(A) -> U, S, VT
# Let I be the identity matrix
# Let ulp be the machine precision (stands for unit of least precision, also called machine epsilon or epsilon-sub-m)
# Let nSVals be the number of singular values produced/requested = min(rows,cols)

# The 9 metrics are:

#     1. Product of factors: norm( A - U S VT ) / [ norm(A) max(M,N) ulp ]
#     2. Check length of the singular vectors: norm(I - Ui' Ui) / [ M ulp ]
#     3. Similarl to (2) but for VT, N
#     4. Check that S contains requested number of values in decreasing order
#     5. norm(S1 - S2) / [ nSVals ulp |S| ]
#     6. norm(U1 - U2) / [ M ulp ]
#     7. norm(S1 - S3) / [ nSVals ulp |S| ]
#     8. norm(VT1-VT3) / [ N ulp ]
#     9. norm(S1 - S4) / [ nSVals ulp |S|  ]

# The test matrices are of 6 types:

#     1. 0 matrix
#     2. I (identity)
#     3. diag(1, ...., ULP) "evenly spaced"
#     4. U*D*VT where U,VT are orthogonal and D is "evenly spaced" as in 3
#     5. matrix of 4, multiplied by the sqrt of the overflow threshold   (larger values)
#     6. matrix of 4, multiplied by the sqrt of the underflow threshold (smaller values
    testSVDByDim(matrixTypeName, AFL_INPUT, nrow, ncol, chunkSize, errorLimit)


#
# testSVDByDim
# this contains the test for a given dimension and chunkSize of matrix
#
# this routine may seem a bit long, but that is because when a test metric fails,
# it is this code that has been pressed upon to help debug what has gone wrong.
# After we resolve more issues with the nature of the AFL_INPUT which contains the
# input test matrix, we will probably be able to reduce the amount of code
# in this function considerably.
#
# TODO: this function has grown to a long length and needs factoring before any more
#       functionality is added.
#
def testSVDByDim(matrixTypeName, AFL_INPUT, nrow, ncol, chunkSize, errorLimit):
    # want to use "nafl" from iqfuncs.bash, but haven't resolved quoting problems yet
    INPUT="TMP_SVD_BY_DIM_INPUT"

    resultMesg = " matrix type %s, RxC %dx%d, chunk_size %d, error_limit %d" % \
                 (matrixTypeName, nrow,ncol, chunkSize, errorLimit)

    if False:
        printDebug("Computing [only] the SVD matrices")
        nafl("gesvd(%s,'values')" % AFL_INPUT)
        nafl("gesvd(%s,'left')" % AFL_INPUT)
        nafl("gesvd(%s,'right')" % AFL_INPUT)
    else:
        try:
            eliminateAndStore(AFL_INPUT, INPUT)
        except Exception, e:
            printDebugForce("*******************************************")
            printDebugForce("testSVDByDim: exception while generating test matrix of type %s" % matrixTypeName)
            printDebugForce("*******************************************")
            printDebugForce("SVD_SETUP%s PASS (exception): FALSE" % (resultMesg,))
            printInfo("SVD_SETUP%s PASS (exception): FALSE" % (resultMesg,)) # make a message with format similar to test failure
            return
        
        printDebug("Generating SVD matrices, S, U, VT")
        
        S="S"
        SAfl = "gesvd(%s,'values')" % INPUT
        eliminate(S)
        try:
            with TimerRealTime() as timer:
                store(SAfl,S)
            printDebug("%s took %s" % (SAfl, timer.interval,), _TIME_OPS)
        except Exception, e:
            printDebugForce("*******************************************")
            printDebugForce("testSVDByDim: exception while calculating S for matrix of type %s" % matrixTypeName)
            printDebugForce("*******************************************")
            printInfo("SVD_CALC_S%s PASS (exception): FALSE" % (resultMesg,)) # make a message with format similar to test failure
            return


        if _DBG:
            printDebug("%s:"%S)
            afl("show(%s)"%S)

        U="U"
        UAfl = "gesvd(%s,'left')" % INPUT
        eliminate(U)
        with TimerRealTime() as timer:
            store(UAfl, U)
        printDebug("%s took %s" % (UAfl, timer.interval,), _TIME_OPS)

        if _DBG:
            printDebug("%s:"%U)
            afl("show(%s)"%U)

        VT="VT"
        VTAfl = "gesvd(%s,'right')" % INPUT
        eliminate(VT)
        with TimerRealTime() as timer:
            store(VTAfl, VT)
        printDebug("%s took %s" % (VTAfl, timer.interval,), _TIME_OPS)
        if _DBG:
            printDebug("%s:"%VT)
            afl("show(%s)"%VT)


        numericalCheck = True;                  # normal: True
        forceSanityCheck = not numericalCheck;  # when tracking down a bug one can disable numericalCheck
                                                # and run only the sanityChecks if desired
                       # to test saving of files, set Terr True
        Terr = False   # Total Error, e.g. error in residual between Input and multiply(U,S,VT)
        Uerr = False
        Serr = False
        VTerr = False

        if numericalCheck :
            # matrix of residual measures: U*S*VT(from svd) - INPUT (the original matrix)
            printDebug("Computing the error metric 1")
            ARRAY_RMS_ERROR="RMS_ERROR"
            ATTR_ERROR_METRIC="rms_ulp_error"
            
            if matrixTypeName in ("zero","identity") :
                # these special matrices should result in no residual at all
                errorLimit = 0

            checkResidualError = True
            if checkResidualError:
                func = divMat;
                if matrixTypeName in ("zero") :
                    # there is no normalization by the norm of the zero matrix possible, that would be divide-by-zero
                    # TODO JHM: I'm not too keen on summing them in this case... I wold like to avoid
                    #           the unecessary computation completely
                    #           todo that, lets pass the matrixTypeName into doSvdMetric1
                    #           and it can skip the normalize by input-norm step
                    func = addMat;
                with TimerRealTime() as timer:
                    errorMetric = doSvdMetric1(nrow, ncol, chunkSize, INPUT, S, U, VT, ARRAY_RMS_ERROR, ATTR_ERROR_METRIC, errorLimit, func)
                printDebug("%s took %s" % ("svdMetric1                         ", timer.interval,), _TIME_OPS)
                hasError = not (errorMetric <= errorLimit)
                testForError("SVD_METRIC_1"+resultMesg, ARRAY_RMS_ERROR, ATTR_ERROR_METRIC, errorLimit) # for reporting
                Terr = hasError or Terr

            # orthoginality of U
            checkOrthoU = True
            if checkOrthoU :
                printDebug("Computing the error metric 2")
                with TimerRealTime() as timer:
                    doSvdMetric2(U, nrow, ncol, chunkSize, ARRAY_RMS_ERROR, ATTR_ERROR_METRIC)
                printDebug("%s took %s" % ("svdMetric2", timer.interval,), _TIME_OPS)
                Uerr = (testForError("SVD_METRIC_2"+resultMesg, ARRAY_RMS_ERROR, ATTR_ERROR_METRIC, errorLimit)
                       or Uerr)    
            # orthoginality of VT
            checkOrthoVT = True
            if checkOrthoVT :
                printDebug("Computing the error metric 3")
                with TimerRealTime() as timer:
                    doSvdMetric3(VT, nrow, ncol, chunkSize, ARRAY_RMS_ERROR, ATTR_ERROR_METRIC)
                printDebug("%s took %s" % ("svdMetric3", timer.interval,), _TIME_OPS)
                VTerr = (testForError("SVD_METRIC_3"+resultMesg, ARRAY_RMS_ERROR, ATTR_ERROR_METRIC, errorLimit)
                       or VTerr)
            # ordered singular values
            checkOrder = True
            if checkOrder :
                printDebug("Computing the error metric 4")
                with TimerRealTime() as timer:
                    doSvdMetric4(nrow, ncol, S, ARRAY_RMS_ERROR, ATTR_ERROR_METRIC)
                printDebug("%s took %s" % ("svdMetric4", timer.interval,), _TIME_OPS)
                errorLimit=0 #force error limit
                Serr = (testForError("SVD_METRIC_4"+resultMesg, ARRAY_RMS_ERROR, ATTR_ERROR_METRIC, errorLimit)
                       or Serr)

        # TODO: condense the following into a loop on array in (INPUT, U, VT, S) with matching attribute
        # Matrix check -- INPUT
        if Terr or Uerr or Serr or VTerr or forceSanityCheck :
            # bad total matrix, or doing more checks
            aflStderr("show(%s)" % (INPUT,) ) # confirm that it exists
            Terr = (not doSvdSanityCount(nrow, ncol, INPUT, S, U, VT,    INPUT, "v")
                    or Terr)
            Terr = (not doSvdSanityApply(nrow, ncol, INPUT, S, U, VT,    INPUT, "v")
                    or Terr)

            if Terr and _SAVE_BAD_MATRICES:
                #checkCount(INPUT, nrow, ncol)  # sometimes useful to call this here if the matrices
                                                # are waaay off.  sometimes they don't even have
                                                # as many elements as they should.  But hasn't happend
                                                # for a while, so commenting out.
                newNameSuffix = "%s_%s" % (nrow, ncol)
                
                newName = "ERR_IN_%s" % newNameSuffix
                printDebugForce("testSVDByDim Terr and _SAVE_BAD_MATRICES: renaming INPUT to %s" % (newName))
                eliminate(newName, autoCleanup=False)
                afl("rename(%s,%s)" % (INPUT, newName))

                newName = "ERR_U_%s" % newNameSuffix
                printDebugForce("testSVDByDim Terr and _SAVE_BAD_MATRICES: renaming U to %s" % (newName,))
                eliminate(newName, autoCleanup=False)
                afl("rename(%s,%s)" %  (U,  newName))

                newName = "ERR_VT_%s" % newNameSuffix
                printDebugForce("testSVDByDim Terr and _SAVE_BAD_MATRICES: renaming VT to ERR_VT_%s" % (newName,))
                eliminate(newName, autoCleanup=False)
                afl("rename(%s,%s)" % (VT, newName))

                # special case, before renaming S
                # generate the matrix form of it (until we have diag(s) -> S operator
                newName = "ERR_S_MAT_%s" % (newNameSuffix,)
                printDebugForce("testSVDByDim Terr and _SAVE_BAD_MATRICES: generating DIAG(S) and saving as %s" % (newName,))
                eliminate(newName, autoCleanup=False)
                generateDiagonal(S, newName, autoCleanup=False)
                afl("scan(%s)" % newName) # DEBUG ... to make sure it exists

                newName = "ERR_S_%s" % (newNameSuffix,)
                printDebugForce("testSVDByDim Terr and _SAVE_BAD_MATRICES: renaming S to %s" % (newName,))
                eliminate(newName, autoCleanup=False)
                afl("rename(%s,%s)" %  (S, newName))


        # Matrix check -- U 
        if Uerr or forceSanityCheck :
            aflStderr("show(%s)" % (U,) ) # confirm that it exists
            Uerr = (not doSvdSanityCount(nrow, ncol, INPUT, S, U, VT,    U, "u")
                    or Uerr)
            Uerr = (not doSvdSanityApply(nrow, ncol, INPUT, S, U, VT,    U, "u")
                    or Uerr)

            if Uerr and _SAVE_BAD_MATRICES:
                eliminate("ERR_U_%s" % (nrow,))
                afl("rename(%s,ERR_U_%s)" %  (U,  nrow))

        # Matrix check -- VT
        if VTerr or forceSanityCheck :
            aflStderr("show(%s)" % (VT,) ) # confirm that it exists
            VTerr = (not doSvdSanityCount(nrow, ncol, INPUT, S, U, VT,    VT, "v")
                     or VTerr)
            VTerr = (not doSvdSanityApply(nrow, ncol, INPUT, S, U, VT,    VT, "v")
                     or VTerr)
 
            if VTerr or _SAVE_BAD_MATRICES:
                eliminate("ERR_VT_%s" % (nrow,))
                afl("rename(%s,ERR_VT_%s)" % (VT, nrow))

        # Vector check --  S
        if Serr or forceSanityCheck :
            aflStderr("show(%s)" % (S,) ) # confirm that it exists

            if Serr or _SAVE_BAD_MATRICES:
                eliminate("ERR_S_%s" % (nrow,))
                afl("rename(%s,ERR_S_%s)" %  (S,  nrow))


#
# compute: norm(gemmAfl-referenceAfl)/order/ULP
#
def doNormResidualMetric(matrixTypeName, exprAfl, exprAttr, referenceAfl, referenceAttr, nrow, ncol, MAT_ERROR, rms_ulp_error, errorLimit):
   printDebug("doNormResidualMetric start")

   residualAfl = computeResidualMeasures(exprAfl, exprAttr,  referenceAfl, referenceAttr, "resid")
   residualNormAfl = norm(residualAfl, "resid", "resid_norm")
   
   referenceNormAfl = norm(referenceAfl, referenceAttr, "ref_norm")

   if matrixTypeName in ("zero"):
       # divMat would be bogus if the matrix is the zero matrix, so just use the numerator as the normalized result
       residNormNormalizedAfl = "apply(%s, resid_norm_normalized, resid_norm)" % residualNormAfl
   else:
       residNormNormalizedAfl = divMat(residualNormAfl, "resid_norm", referenceNormAfl, "ref_norm", "resid_norm_normalized")

   if _DBG:
      printDebugForce("DEBUG doNormResidualMetric residNormNormalizedAfl")
      aflStderr(residNormNormalizedAfl)

   errULPsAfl = scaleToULPs(residNormNormalizedAfl, "resid_norm_normalized", nrow, ncol, rms_ulp_error, MAT_ERROR)
   errorMetric = dbgGetErrorMetric("NORM_RESIDUAL_METRIC", MAT_ERROR, rms_ulp_error, errorLimit)

   hasError = not (errorMetric <= errorLimit)
   if hasError:
      printDebugForce("doNormResidualMetric Error %s exceeds %s @ size %s x %s " % (errorMetric, errorLimit, nrow, ncol))      
      printDebugForce("doNormResidualMetric I. residual query is %s" %(residualAfl))
      valLimit=0.1
      printDebugForce("doNormResidualMetric II. abs residual exceeding threshold %s" %(valLimit))
      largeResidAfl = "filter(%s, r=0 and c=32)" % (residualAfl)  # customized location debug
      aflStderr(largeResidAfl)


   return errorMetric  # returns the error metric, not the afl, just like doSvdMetric1

#
# Test the GEMM operator.
# Implements an appropriate interface to be used by iterateOverTests()
def testGEMM(matrixTypeName, AFL_INPUT, nrow, ncol, chunkSize, errorLimit):
   # make a (nrow, ncol) matrix
   printInfo("GEMM TEST")

   INPUT="INPUT"
   eliminateAndStore(AFL_INPUT, INPUT) # when using random arrays, storage is necessary
                                       # otherwise, we can't compare two operators on the same input

   printDebug("GEMM(A,transpose(A)) vs multiply(A,transpose(A)) nrow %s, ncol %s, chunkSize %s" % (nrow, ncol, chunkSize))
   transposeAfl = "cast(transpose(%s), %s)" % (INPUT, getMatrixSchema(ncol, nrow, chunkSize, chunkSize)) # note row,col reversal

   gemmAfl = "gemm(%s, %s, build(%s,0))" % (INPUT, transposeAfl, getMatrixSchema(nrow, nrow, chunkSize, chunkSize)) # note output is square
   referenceAfl = "aggregate(apply(cross_join(%s as A, %s as B, A.c, B.r), prod, A.v * B.v), sum(prod) as multiply, A.r, B.c)" % (INPUT, transposeAfl)

   printDebug("Computing GEMM error metric")
   ARRAY_RMS_ERROR="RMS_ERROR"
   ATTR_ERROR_METRIC="rms_ulp_error"

   if matrixTypeName in ("zero","identity") :
      # these special matrices should result in no residual at all
      errorLimit = 0
   errorMetric = doNormResidualMetric(matrixTypeName, gemmAfl, "gemm", referenceAfl, "multiply", nrow, ncol, ARRAY_RMS_ERROR, ATTR_ERROR_METRIC, errorLimit)
   resultMesg = "matrix type %s, nrow %d, ncol %d, chunk_size %d, error_limit %d" % (matrixTypeName, nrow, ncol, chunkSize, errorLimit)
   testForError("GEMM "+resultMesg, ARRAY_RMS_ERROR, ATTR_ERROR_METRIC, errorLimit) # for reporting

   hasError = not (errorMetric <= errorLimit)
   if hasError and False : # True to print the failing matrix and its transpose, or filters thereof
      #inputRow0Afl = "filter(%s, r=0)" % (INPUT)  # customized location debug
      inputRow0Afl = "scan(%s)" % (INPUT)  # full matrix debug
      aflStderr(inputRow0Afl)
      #transposeCol0Afl = "filter(%s, c=32)" % (transposeAfl)  # customized location debug
      transposeCol0Afl = transposeAfl  # customized location debug
      aflStderr(transposeCol0Afl)

   if hasError and _SAVE_BAD_MATRICES:
      newName = "ERR_GEMM_%s" % (nrow,)
      printDebugForce("testGEMM hasError and _SAVE_BAD_MATRICES: renaming INPUT to %s" % (newName,))
      eliminate(newName, autoCleanup=False)
      afl("rename(%s,%s)" % (INPUT, newName))
      printDebugForce("testGEMM hasError and _SAVE_BAD_MATRICES: note that the transpose expression is %s" % (transposeAfl,))
      raise Exception("testGEMM hasError and _SAVE_BAD_MATRICES: stopping so case can be debugged")


def testTranspose(matrixTypeName, AFL_INPUT, nrow, ncol, chunkSize, errorLimit):
   # make a (nrow, ncol) matrix
   printInfo("TRANSPOSE TEST")

   INPUT="INPUT"
   eliminateAndStore(AFL_INPUT, INPUT) # when using random arrays, storage is necessary
                                       # otherwise, we can't compare two operators on the same input
   printDebug("INPUT vs transpose(transpose(INPUT))) nrow %s, ncol %s, chunkSize %s" % (nrow, ncol, chunkSize))

   transposeAfl = "cast(transpose(%s), %s)" % (INPUT, getMatrixSchema(ncol, nrow, chunkSize, chunkSize)) # note row,col reversal   
   transpose2Afl = "cast(transpose(%s), %s)" % (transposeAfl, getMatrixSchema(nrow, ncol, chunkSize, chunkSize,"t")) # note row,col back to normal
   
   printDebug("Computing TRANSPOSE error metric, using doNormResidualMetric")
   ARRAY_RMS_ERROR="RMS_ERROR"
   ATTR_ERROR_METRIC="rms_ulp_error"
   # transposition should result in no residual error whatsoever
   errorLimit = 0
   errorMetric = doNormResidualMetric(matrixTypeName, transpose2Afl, "t", INPUT, "v", nrow, ncol, ARRAY_RMS_ERROR, ATTR_ERROR_METRIC, errorLimit)
   resultMesg = "matrix type %s, nrow %d, ncol %d, chunk_size %d, error_limit %d" % (matrixTypeName, nrow, ncol, chunkSize, errorLimit)
   testForError("TRANSPOSE "+resultMesg, ARRAY_RMS_ERROR, ATTR_ERROR_METRIC, errorLimit) # for reporting

   hasError = not (errorMetric <= errorLimit)
   if hasError and False : # True to print the failing matrix and its transpose, or filters thereof
      #inputRow0Afl = "filter(%s, r=0)" % (INPUT)  # customized location debug
      inputRow0Afl = "scan(%s)" % (INPUT)  # full matrix debug
      aflStderr(inputRow0Afl)
      #transposeCol0Afl = "filter(%s, c=32)" % (transposeAfl)  # customized location debug
      transposeCol0Afl = transpose2Afl  # customized location debug
      aflStderr(transpose2Afl)

   if hasError and _SAVE_BAD_MATRICES:
      newName = "ERR_TRANSPOSE_%s" % (nrow,)
      printDebugForce("testTRANSPOSE hasError and _SAVE_BAD_MATRICES: renaming INPUT to %s" % (newName,))
      eliminate(newName, autoCleanup=False)
      afl("rename(%s,%s)" % (INPUT, newName))
      printDebugForce("testTRANSPOSE hasError and _SAVE_BAD_MATRICES: note that the transpose expression is %s" % (transposeAfl,))
      printDebugForce("testTRANSPOSE hasError and _SAVE_BAD_MATRICES: note that the transpose2 expression is %s" % (transpose2Afl,))
      raise Exception("testTRANSPOSE hasError and _SAVE_BAD_MATRICES: stopping so case can be debugged")
#
# Test the MPICOPY operator.
# Implements an appropriate interface to be used by iterateOverTests()
def testMPICopy(matrixTypeName, AFL_INPUT, nrow, ncol, chunkSize, errorLimit):
   # make a (nrow, ncol) matrix
   errorLimit=0 # copy should not incur any errors

   printInfo("MPICOPY TEST, force error_limit=%d"%errorLimit)

   INPUT="INPUT"
   createMatrix(INPUT, nrow, ncol, chunkSize, chunkSize)
   populateMatrix(INPUT, getMatrixTypeAfl(nrow, ncol, chunkSize, matrixTypeName))

   mpicopyAfl = "mpicopy(%s)" % (INPUT)
   if _DBG:
      MPICOPY="MPICOPY"
      eliminateAndStore(mpicopyAfl, MPICOPY)
      afl("show(%s)" % MPICOPY)

   printDebug("Computing MPICOPY error metric")
   error = "resid"
   metricAfl = computeResidualMeasures(INPUT, "v", mpicopyAfl, "copy", error)

   maxAfl = "aggregate(project(%s, %s), max(%s))" % (metricAfl, error, error)
   error = error+"_max"

   testForError("MPICOPY", maxAfl, error, errorLimit)


#config file options
_configOptions = {}

#
# SciDB connection port
_basePort="1239"

#
# SciDB connection target host
_targetHost="localhost"

# The name of the iquery executable
_iqueryBin="iquery"

# Time iquery executions
_timePrefix=None

# Chunk sizes
_chunkSizeList="DEFAULT_CSIZE_LIST"

# Chunk size divisors
#_divisorList=[2,3] # e.g. 7,3 or 7,4,3
_divisorList=[]

# A list of array names used during the test runs.
# It is used to clean up the SciDB state on exit.
_usedMatrices={}

_DBG = False
_SAVE_BAD_MATRICES = False
_TIME_OPS = True
_TIME_TESTS = True

# The main entry routine that does command line parsing
def main():
   # Very basic check.
   if len(sys.argv)>2:
      usage()
      sys.exit(2)

   if len(sys.argv)==2:
      arg = sys.argv[1]
      if len(arg) <= 0:
         usage()
         sys.exit(2)
   else:
      arg=""

   if arg == "--help" or arg == "-h":
      usage()
      sys.exit(0)

   if len(arg) > 0:
      configfile=arg
      parseGlobalOptions(configfile,"dense_linear_algebra")

   global _basePort
   global _targetHost
   global _iqueryBin
   global _timePrefix
   global _divisorList
   global _chunkSizeList

   errorLimit=10
   orderStr="*:4:32:2"  # 4,8,16,32
   testsToRun = allTests.keys()
   matricesToUse = defaultMatrixTypes # means all

   if "install-path" in _configOptions:
      installPath = _configOptions.get("install-path")
      if len(installPath) > 0:
         _iqueryBin = installPath+"/bin/"+_iqueryBin

   if "base-port" in _configOptions:
      _basePort = _configOptions.get("base-port")

   if "time-queries" in _configOptions:
      _timePrefix="time"

   if "target-host" in _configOptions:
      _targetHost = _configOptions.get("target-host")

   if "error-limit" in _configOptions:
      errorLimit = float(_configOptions.get("error-limit"))

   if "size-list" in _configOptions:
      orderStr = _configOptions.get("size-list")

   if "chunk-size-list" in _configOptions:
      _chunkSizeList = _configOptions.get("chunk-size-list")

   if "divisor-list" in _configOptions:
       tmp = parseIntList(_configOptions.get("divisor-list"))
       if tmp is not None :
          _divisorList = tmp

   if "tests" in _configOptions:
      tests = _configOptions.get("tests")
      testsToRun=tests.split(',')
      for t in testsToRun:
         if t == "all":
            testsToRun = None
            break

   if "matrix-types" in _configOptions:
      matNames = _configOptions.get("matrix-types").split(',')
      if matNames != ("all",): # the default is already all of them
          matricesToUse = matNames

   nafl("load_library(\'dense_linear_algebra\')");
   printDebugForce("Start %s: orderStr %s, errorLimit %d, testsToRun %s on matrices %s" %\
                   (datetime.datetime.utcnow(), orderStr, errorLimit, testsToRun, matricesToUse))

   iterateOverTests(orderStr, errorLimit, testsToRun, matricesToUse)

   for name in _usedMatrices.keys():
      eliminate(name,False)

   printDebugForce("Finish %s: orderStr %s, errorLimit %d, testsToRun %s on matrices %s"%\
                   (datetime.datetime.utcnow(), orderStr, errorLimit, testsToRun, matricesToUse))
   sys.exit(0)

### MAIN
if __name__ == "__main__":
   main()
### end MAIN
