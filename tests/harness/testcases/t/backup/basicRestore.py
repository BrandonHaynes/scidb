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
import CommandRunner
import BackupHelper
import BasicRestoreTests
import unittest
import os
import pwd

def main():
    q = [
        'iquery',
        '-c',
        os.environ['IQUERY_HOST'],
        '-p',
        os.environ['IQUERY_PORT'],
        '-ocsv',
        '-q',
        'SELECT * FROM sort(list(\'instances\'),instance_id)'
        ]
    cr = CommandRunner.CommandRunner()
    exits,outputs = cr.waitForProcesses(
        [cr.runSubProcess(q)],
        True
        )

    if (any(exits)):
        sys.stderr.write('Error: could not get scidb instance data!\n')
        errors = outputs[0][1]
        print errors
        sys.exit(1)
    lines = [line.strip() for line in outputs[0][0].split('\n')]
    lines = [line.replace('\'','') for line in lines if len(line) > 0]
    lines = lines[1:]
    hostList = sorted(
        [line.split(',') for line in lines],
        key=lambda line_tokens: int(line_tokens[2])
        )
    hosts = reduce( # Scidb hosts (machines)
        lambda host_list,host_sublist: host_list + host_sublist if host_sublist[0] not in host_list else host_list,
        [[host_tokens[0]] for host_tokens in hostList]
        )

    nInst = len(lines) / len(hosts)

    user = pwd.getpwuid(os.getuid())[0]

    cq = BackupHelper.CREATE_400x400_ARRAYS

    bkpFolder = r'/tmp/bkpTest'

    arrays = [a + str(i+1) for a in ['A','B','C'] for i in range(3)]

    bh = BackupHelper.BkpTestHelper(
        user,
        hosts,
        nInst,
        bkpFolder,
        arrays
        )
    bh.setCreateQuery(cq)
    bh.createArrays(arrays,[5,6,7,8],True)

    suite = BasicRestoreTests.getNonParallelTestSuite(user,hosts,nInst,bh,cq)

    runner = unittest.TextTestRunner(verbosity=2)
    ret = map(runner.run,[suite])

    bh.removeArrays(arrays,True)

    if (
        any([len(x.errors) > 0 for x in ret]) or
        any([len(x.failures) > 0 for x in ret]) or
        any([x.testsRun <= 0 for x in ret])
            ):
        sys.stderr.write('FAIL\n')
        sys.exit(1)
    
if __name__ == '__main__':
    main()
