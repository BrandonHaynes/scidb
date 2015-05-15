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
        ':'.join(('${SCIDB_INSTALL_PATH}/jdbc/scidb4j.jar',
                  '${SCIDB_INSTALL_PATH}/jdbc/jdbctest.jar',
                  '/usr/share/java/protobuf.jar',
                  '/usr/share/java/protobuf-java.jar',
                  '/usr/share/java/junit.jar')),
        'org.scidb.JDBCTest',
        iquery_host,
        iquery_port
        ]

    proc = subprocess.Popen( # Run the command.
        ' '.join(cmd),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        shell=True
        )

    exitCode = proc.poll()
    while (exitCode is None):
        time.sleep(0.1)
        exitCode = proc.poll()

    sOut = proc.stdout.read().strip()
    sErr = proc.stderr.read().strip()

    if (exitCode != 0):
        print 'Bad exit code!'
        print sErr
        sys.exit(1)

    if (('FAILURES!!!' in sOut) or ('FAILURES!!!' in sErr)):
        print sOut
        print sErr
        print 'FAIL'
        sys.exit(1)
    print 'PASS'

if __name__ == '__main__':
    main()
