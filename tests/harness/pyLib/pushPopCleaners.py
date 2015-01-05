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

import sys
import subprocess
import os
import pickle
import re
import random
import time
import string
import shutil
from scidbIqueryLib import scidbIquery as Iquery

#-----------------------------------------------------------------------------
# Constant commands to iquery.
#.............................................................................
# Command to get a list of all arrays currently in the database:
listSciDBArraysCmd = "list('arrays');"

# Command to removed a specific array from the database:
removeSciDBArrayCmd = 'remove(<array>);'

# Simple debug command for checking subprocess spawning:
testCommand = ['echo', 'Test']

IGNORE_PATTERNS = [ os.sep + 'mpi' ]

class DebugStreamWriter(object):
    def __init__(self,debug=False,stream=None):
        self._debugMsg = lambda x: None
        self._dbgStream = stream
        if (debug):
            self._debugMsg = lambda x: sys.stderr.write(x+'\n')
            if (not stream is None):
                if (callable(getattr(stream,'write'))):
                    self._debugMsg = lambda x: self._dbgStream.write(x+'\n')
    def dbgPrint(self,msg):
        self._debugMsg(msg)

class DataDirCleaner(object):
    def __init__(self,dataDir,debug=False,stream=None):
        self._dataDir = dataDir
        self._initDataFiles = set([])
        self._pushed = False
        self._dbgWriter = DebugStreamWriter(debug,stream)
                    
    def push(self):
        if (os.path.isdir(os.path.abspath(self._dataDir))):
            self._dataDir = os.path.abspath(self._dataDir)
            self._initDataFiles = set([os.path.join(self._dataDir,x) for x in os.listdir(self._dataDir)])
            self._pushed = True
    def pop(self):
        if (self._pushed):
            dataFiles = set([os.path.join(self._dataDir,x) for x in os.listdir(self._dataDir)])
            delta = dataFiles - (dataFiles & self._initDataFiles)
            #-----------------------------------------------------------------
            # Take out the folders that fall into the "do-not-touch" category:
            # some operators create 'mpi*' folders in the data folder.  We 
            # cannot delete them easily (so, we do not touch them).
            delta = set([x for x in delta if not any([p in x for p in IGNORE_PATTERNS])])
            #-----------------------------------------------------------------
            for d in delta:
                self._dbgWriter.dbgPrint('Removing: {0}'.format(d))
                if (os.path.isdir(d)):
                    shutil.rmtree(d)
                    continue
                if (os.path.isfile(d)):
                    os.remove(d)
                    continue
            self._pushed = False


class ArrayCleaner(object):
    def __init__(self,iqueryPath='iquery',debug=False,stream=None):
        self._pushedArrayNames = set([])
        self._iquery = Iquery(iqueryPath)
        self._pushed = False
        self._dbgWriter = DebugStreamWriter(debug,stream)
    #-----------------------------------------------------------------------------
    # getArrayNames: extract the names of arrays currently stored in scidb from
    #                the "raw" data returned by the list-array query.  The names
    #                are returned as a set back to the caller.
    def _getArrayNames(self, iqueryData):
        # Run the regular expression check to grab the names of the arrays with 
        # some extraneous characters.  L will contain regular expression match
        # objects.
        lines = iqueryData.split('\n')
        arrayNames = [line.split(',')[0] for line in lines[1:]]
        arrayNames = [name.replace('\'','') for name in arrayNames if len(name) > 0]
        return set(arrayNames)
    #-----------------------------------------------------------------------------
    # getSciDBArrays: get a set of arrays currently stored in the scidb database
    #                 (as reported by a call to iquery).
    def getSciDBArrays(self):
        # Run iquery to get the list of arrays.
        _,stdoutData,stderrData = self._iquery.runQuery(listSciDBArraysCmd,opts=['-o','csv','-aq'])
        
        # Get the names from the "raw" data and return them:
        return self._getArrayNames(stdoutData)
    #-----------------------------------------------------------------------------
    # removeSciDBArrays: remove all of the arrays specified by the user; the
    #                    function repeatedly calls iquery to remove a single array
    #                    for every array in user-specified set.
    def removeSciDBArrays(self, arraySet):
        if (len(arraySet) > 0):
            self._dbgWriter.dbgPrint('Will remove arrays...')
            self._dbgWriter.dbgPrint(''.join([x + ' ' for x in arraySet]))

        for A in arraySet:
            # Substitute the name of the array in the command list (quick and
            # dirty way: no need to remember which parameter needs the 
            # replacement).
            removeArrayCmd = removeSciDBArrayCmd
            removeArrayCmd = removeArrayCmd.replace('<array>',A)
            _,stdoutData,stderrData = self._iquery.runQuery(removeArrayCmd)

    def push(self):
        # Get the arrays out of the database and store them in temp file
        self._pushedArrayNames = self.getSciDBArrays()
        self._pushed = True
        
    def pop(self):
        if (self._pushed):
            currentArrayNames = self.getSciDBArrays()
            self.removeSciDBArrays(
                currentArrayNames - (self._pushedArrayNames & currentArrayNames)
                )
            self._pushed = False
#-----------------------------------------------------------------------------
# getTempFileName: get a name for a temporary file based on a seed for the 
#                  random number generator.  The seed provides pseudo-random
#                  mode of operation: if the function is called with the same
#                  seed several times, the exact same name will be generated
#                  every time.
def getTempFileName(seed, prefix='scidb_arr_',fext='.txt'):
    # Initialize random number generator.
    random.seed(seed)
    
    # Compute the temporary name and return it:
    tempFile = ''.join(random.sample(string.letters,10)) + fext
    tempFile = prefix + tempFile
    tempPath = os.path.join('/','tmp',tempFile)
    
    print tempPath
    return tempPath
#-----------------------------------------------------------------------------
# main: entry point of the program; the program requires 2 command line
#       arguments:
#       1) seed - some number or a string that will be used to initialize the
#                 random number generator
#       2) mode - indicator of what the program should do; 3 values are
#                 allowed - setup, cleanup, and cleanstart
#
#   The program operates in 3 modes.  If the program is called with
# "cleanstart" mode, then it removes all of the arrays currently stored in
# scidb database and exits.  The idea behind this is that each new test
# harness run should start with a "clean slate"; otherwise tests may start
# failing on the next invocation because their "create array..." statements
# run into problems (arrays still in database).
#   Next mode is setup: the program reads the arrays currently stored in the
# database and stores their names in a file with a pseudo-random name based
# on the seed value passed into the program.
#   Finally, in cleanup mode the program again gets the names of the arrays
# currently stored in scidb database and compares them with the names in the
# file that was recorded before (setup mode).  "Delta" of names is then what
# was left behind after executing some test.  This delta of names is removed
# from the database, and the pseudo-random file is deleted from the system.
#
# The proposed operation of this script is as follows:
# 1) call the script once at the start of testharness run with any random 
#    seed (ignored in this case) to clean out the database of all arrays
# 2) next, call the script with a random seed and setup mode at the
#    beginning of a single test execution (array names will be stored)
# 3) finally, call the script again with the same random seed and cleanup
#    mode to remove any "leftover" arrays the test did not cleanup for 
#    some reason.
#
# This scheme of opertion will not work, of course, in a multi-threaded
# harness: if several tests want to create and remove arrays with exactly 
# the same names.  Some other cleanup method will have to be designed for 
# that execution of tests.
# 
def cleanArrays(seed,mode):   
    cleaner = pushPopCleaner()
    tempPath = getTempFileName(seed)
    if (mode == 'setup'):
        # Get the arrays out of the database and store them in temp file
        currentArrayNames = cleaner.getSciDBArrays()
        with open(tempPath,'w') as fd:
            pickle.dump(currentArrayNames,fd)
    elif (mode == 'cleanup'):
        # Get the current array names from the database:
        currentArrayNames = cleaner.getSciDBArrays()       
        
        # Retrieve the array names stored in the temp file during setup mode:
        origArrayNames = set([])
        if (os.path.isfile(tempPath)):
            with open(tempPath,'r') as fd:
                origArrayNames = pickle.load(fd)
            # Remove the temp file since it is no longer needed:
            os.remove(tempPath)
        
        # Delete the difference left by the test (if any):
        cleaner.removeSciDBArrays(
            currentArrayNames - (origArrayNames & currentArrayNames)
            )
    elif (mode == 'cleanstart'):
        # Clean start mode: just remove all of the arrays currently in the
        # database
        cleaner.removeSciDBArrays(cleaner.getSciDBArrays())
    else:
        # Unknown mode: do not do anything.
        pass
#-----------------------------------------------------------------------------
# Entry point of hte script: call main.
#if __name__ == '__main__':
#    main()
