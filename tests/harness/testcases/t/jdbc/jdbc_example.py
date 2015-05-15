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
import subprocess
import time
import sys
import os

def runSubProcess(
    cmd, # Command to run (list of string options).
    si=None, # Standard in.
    so=subprocess.PIPE, # Standard out.
    se=subprocess.PIPE, # Standard error.
    useShell=False # Flag to use shell option when starting process.
    ):

    localCmd = list(cmd) # Copy list to make sure it is not referenced
                         # elsewhere.
    if (useShell): # If command is for shell, flatten it into a single
        localCmd = ' '.join(localCmd) # string.

    proc = subprocess.Popen( # Run the command.
        localCmd,
        stdin=si,
        stdout=so,
        stderr=se,
        shell=useShell
        )
    return proc
def main():
    print 'SCIDB_INSTALL_PATH',os.environ['SCIDB_INSTALL_PATH']

    iquery_host = 'localhost'
    iquery_port = '1239'
    if (os.environ.has_key('IQUERY_HOST')):
        iquery_host = os.environ['IQUERY_HOST']
    if (os.environ.has_key('IQUERY_PORT')):
        iquery_port = os.environ['IQUERY_PORT']

    cmd = [
        'java',
        '-classpath',
        '${SCIDB_INSTALL_PATH}/jdbc/example.jar:${SCIDB_INSTALL_PATH}/jdbc/scidb4j.jar:/usr/share/java/protobuf.jar:/usr/share/java/protobuf-java.jar',
        'org.scidb.JDBCExample',
        iquery_host,
        iquery_port
        ]
    proc = runSubProcess(cmd,useShell=True)
    exitCode = proc.poll()
    while (exitCode is None):
        time.sleep(0.1)
        exitCode = proc.poll()

    sOut = proc.stdout.read().strip()
    sErr = proc.stderr.read().strip()

    sOut = sOut.split('\n')

    if (exitCode != 0):
        print 'Bad exit code!'
        print sErr
        sys.exit(1)

    expectedData = [
        '0 0 a',
        '0 1 b',
        '0 2 c',
        '1 0 d',
        '1 1 e',
        '1 2 f',
        '2 0 123',
        '2 1 456',
        '2 2 789'
    ]

    compData = sOut[8:]
    if (set(expectedData) != set(compData)):
        print 'Error: data mismatch!'
        print 'Expected:'
        print '\n'.join(expectedData)
        print 'Received:'
        print '\n'.join(compData)
        sys.exit(1)

    print 'PASS'

if __name__ == '__main__':
    main()
