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
#-------------------------------------------------------------------------------
# Imports:
import sys
import os
import re
import types
import subprocess
import functools
import time
import unittest
from CommandRunner import CommandRunner
import BackupHelper

BACKUP=os.path.join(os.environ['SCIDB_INSTALL_PATH'],'bin','scidb_backup.py')

allTests = [
    'test_tmp_text_parallel',
    'test_tmp_text_allVersions',
    'test_tmp_text_zip',
    'test_tmp_text_parallel_allVersions',
    'test_tmp_text_parallel_zip',
    'test_tmp_text_allVersions_zip',
    'test_tmp_text_parallel_allVersions_zip',
    'test_tmp_binary_parallel',
    'test_tmp_binary_allVersions',
    'test_tmp_binary_zip',
    'test_tmp_binary_parallel_allVersions',
    'test_tmp_binary_parallel_zip',
    'test_tmp_binary_allVersions_zip',
    'test_tmp_binary_parallel_allVersions_zip',
    'test_tmp_opaque_parallel',
    'test_tmp_opaque_allVersions',
    'test_tmp_opaque_zip',
    'test_tmp_opaque_parallel_allVersions',
    'test_tmp_opaque_parallel_zip',
    'test_tmp_opaque_allVersions_zip',
    'test_tmp_opaque_parallel_allVersions_zip',
    'test_tmp_text',
    'test_tmp_binary',
    'test_tmp_opaque',
    'test_tmp_neg_binary'
    ]
parallelTests = [
    'test_tmp_text_parallel',
    'test_tmp_text_parallel_allVersions',
    'test_tmp_text_parallel_zip',
    'test_tmp_text_parallel_allVersions_zip',
    'test_tmp_binary_parallel',
    'test_tmp_binary_parallel_allVersions',
    'test_tmp_binary_parallel_zip',
    'test_tmp_binary_parallel_allVersions_zip',
    'test_tmp_opaque_parallel',
    'test_tmp_opaque_parallel_allVersions',
    'test_tmp_opaque_parallel_zip',
    'test_tmp_opaque_parallel_allVersions_zip'
    ]


def testWatchdog(testFunc):
    @functools.wraps(testFunc)
    def wrapper(obj):
        try:
            testFunc(obj)
        except AssertionError as e:
            print e
            obj.fail('Test failed!')
            raise e
    return wrapper

class BackupTest(unittest.TestCase):
    _cmdRunner = CommandRunner()

    def __init__(self,*args,**kwargs):
        super(BackupTest,self).__init__(*args,**kwargs)
        self._tempArrays = []
        self._arrays = []

    def setBackupHelper(self,bh):
        self.bkpHelper = bh

    def setArrays(self,AL,temp=False):
        if (temp):
            self._tempArrays = list(AL)
        else:
            self._arrays = list(AL)

    def setUp(self):        
        self.bkpHelper.removeBackup(BACKUP)
        self.bkpHelper.removeArrays(self._arrays + self._tempArrays)
        self.bkpHelper.reCreateArrays(self._arrays,versions=4)
        self.bkpHelper.reCreateArrays(self._tempArrays,temp=True)
    def tearDown(self):
        self.bkpHelper.removeBackup(BACKUP)
        self.bkpHelper.removeArrays(self._arrays + self._tempArrays)

    def commonTestBody(
        self,
        toRestore,
        saveCmd,
        restoreCmd,
        versions=False
        ):
        sys.stderr.write('\n' + ' '.join(restoreCmd) + '\n')
        exits,outs=self._cmdRunner.waitForProcesses(
            [self._cmdRunner.runSubProcess(saveCmd,useShell=True)],
            True
            )
            
        self.bkpHelper.removeArrays(self._arrays + self._tempArrays)
        exits,outs = self._cmdRunner.waitForProcesses(
            [self._cmdRunner.runSubProcess(restoreCmd,useShell=True)],
            True
            )
        restored = self.bkpHelper.getDBArrays(versions)
        
        self.assertTrue(
            set(toRestore) == set(restored),
            'Restored arrays do not match initial conditions!'
            )
        
        self.assertTrue(
            self.bkpHelper.checkArrayData(toRestore),
            'Restored array data does not match initial conditions!'
            )
    #-------------------------------------------------------------------------
    @testWatchdog
    def test_tmp_text_parallel(self):
        toRestore = self.bkpHelper.getAllArrayNames() # All arrays should be restored.
        toRestore = [ # Keep only non-temp arrays (temps are skipped).
            a_name for a_name in toRestore if not any([a_name in t_name for t_name in self._tempArrays])
            ]
        saveCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--save',
            'text',
            '--parallel',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]
        restoreCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--restore',
            'text',
            '--parallel',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]

        self.commonTestBody(toRestore,saveCmd,restoreCmd)
        
    @testWatchdog
    def test_tmp_text_allVersions(self):
        toRestore = self.bkpHelper.getAllArrayNames(4) # All versions of arrays should be restored.
        toRestore = [ # Keep only non-temp arrays (temps are skipped).
            a_name for a_name in toRestore if not any([a_name in t_name for t_name in self._tempArrays])
            ]
        print 'toRestore',toRestore
        saveCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--save',
            'text',
            '--allVersions',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]
        restoreCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--restore',
            'text',
            '--allVersions',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]

        self.commonTestBody(toRestore,saveCmd,restoreCmd,versions=True)

    @testWatchdog
    def test_tmp_text_zip(self):
        toRestore = self.bkpHelper.getAllArrayNames() # All arrays should be restored.
        toRestore = [ # Keep only non-temp arrays (temps are skipped).
            a_name for a_name in toRestore if not any([a_name in t_name for t_name in self._tempArrays])
            ]
        saveCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--save',
            'text',
            '-z',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]
        restoreCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--restore',
            'text',
            '-z',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]

        self.commonTestBody(toRestore,saveCmd,restoreCmd)

    @testWatchdog
    def test_tmp_text_parallel_allVersions(self):
        toRestore = self.bkpHelper.getAllArrayNames(4) # All versions of arrays should be restored.
        toRestore = [ # Keep only non-temp arrays (temps are skipped).
            a_name for a_name in toRestore if not any([a_name in t_name for t_name in self._tempArrays])
            ]
        saveCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--save',
            'text',
            '--parallel',
            '--allVersions',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]
        restoreCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--restore',
            'text',
            '--parallel',
            '--allVersions',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]

        self.commonTestBody(toRestore,saveCmd,restoreCmd,versions=True)

    @testWatchdog
    def test_tmp_text_parallel_zip(self):
        toRestore = self.bkpHelper.getAllArrayNames() # All arrays should be restored.
        toRestore = [ # Keep only non-temp arrays (temps are skipped).
            a_name for a_name in toRestore if not any([a_name in t_name for t_name in self._tempArrays])
            ]
        saveCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--save',
            'text',
            '--parallel',
            '-z',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]
        restoreCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--restore',
            'text',
            '--parallel',
            '-z',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]

        self.commonTestBody(toRestore,saveCmd,restoreCmd)

    @testWatchdog
    def test_tmp_text_allVersions_zip(self):
        toRestore = self.bkpHelper.getAllArrayNames(4) # All versions of arrays should be restored.
        toRestore = [ # Keep only non-temp arrays (temps are skipped).
            a_name for a_name in toRestore if not any([a_name in t_name for t_name in self._tempArrays])
            ]
        saveCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--save',
            'text',
            '--allVersions',
            '-z',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]
        restoreCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--restore',
            'text',
            '--allVersions',
            '-z',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]

        self.commonTestBody(toRestore,saveCmd,restoreCmd,versions=True)

    @testWatchdog
    def test_tmp_text_parallel_allVersions_zip(self):
        toRestore = self.bkpHelper.getAllArrayNames(4) # All versions of arrays should be restored.
        toRestore = [ # Keep only non-temp arrays (temps are skipped).
            a_name for a_name in toRestore if not any([a_name in t_name for t_name in self._tempArrays])
            ]
        saveCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--save',
            'text',
            '--parallel',
            '--allVersions',
            '-z',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]
        restoreCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--restore',
            'text',
            '--parallel',
            '--allVersions',
            '-z',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]

        self.commonTestBody(toRestore,saveCmd,restoreCmd,versions=True)

    @testWatchdog
    def test_tmp_binary_parallel(self):
        toRestore = self.bkpHelper.getAllArrayNames() # All arrays should be restored.
        toRestore = [ # Keep only non-temp arrays (temps are skipped).
            a_name for a_name in toRestore if not any([a_name in t_name for t_name in self._tempArrays])
            ]
        saveCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--save',
            'binary',
            '--parallel',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]
        restoreCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--restore',
            'binary',
            '--parallel',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]

        self.commonTestBody(toRestore,saveCmd,restoreCmd)

    @testWatchdog
    def test_tmp_binary_allVersions(self):
        toRestore = self.bkpHelper.getAllArrayNames(4) # All versions of arrays should be restored.
        toRestore = [ # Keep only non-temp arrays (temps are skipped).
            a_name for a_name in toRestore if not any([a_name in t_name for t_name in self._tempArrays])
            ]
        saveCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--save','binary',
            '--allVersions',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]
        restoreCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--restore',
            'binary',
            '--allVersions',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]

        self.commonTestBody(toRestore,saveCmd,restoreCmd,versions=True)

    @testWatchdog
    def test_tmp_binary_zip(self):
        toRestore = self.bkpHelper.getAllArrayNames() # All arrays should be restored.
        toRestore = [ # Keep only non-temp arrays (temps are skipped).
            a_name for a_name in toRestore if not any([a_name in t_name for t_name in self._tempArrays])
            ]
        saveCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--save',
            'binary',
            '-z',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]
        restoreCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--restore',
            'binary',
            '-z',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]

        self.commonTestBody(toRestore,saveCmd,restoreCmd)

    @testWatchdog
    def test_tmp_binary_parallel_allVersions(self):
        toRestore = self.bkpHelper.getAllArrayNames(4) # All versions of arrays should be restored.
        toRestore = [ # Keep only non-temp arrays (temps are skipped).
            a_name for a_name in toRestore if not any([a_name in t_name for t_name in self._tempArrays])
            ]
        saveCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--save',
            'binary',
            '--parallel',
            '--allVersions',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]
        restoreCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--restore',
            'binary',
            '--parallel',
            '--allVersions',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]

        self.commonTestBody(toRestore,saveCmd,restoreCmd,versions=True)

    @testWatchdog
    def test_tmp_binary_parallel_zip(self):
        toRestore = self.bkpHelper.getAllArrayNames() # All arrays should be restored.
        toRestore = [ # Keep only non-temp arrays (temps are skipped).
            a_name for a_name in toRestore if not any([a_name in t_name for t_name in self._tempArrays])
            ]
        saveCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--save',
            'binary',
            '--parallel',
            '-z',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]
        restoreCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--restore',
            'binary',
            '--parallel',
            '-z',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]

        self.commonTestBody(toRestore,saveCmd,restoreCmd)

    @testWatchdog
    def test_tmp_binary_allVersions_zip(self):
        toRestore = self.bkpHelper.getAllArrayNames(4) # All versions of arrays should be restored.
        toRestore = [ # Keep only non-temp arrays (temps are skipped).
            a_name for a_name in toRestore if not any([a_name in t_name for t_name in self._tempArrays])
            ]
        saveCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--save',
            'binary',
            '--allVersions',
            '-z',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]
        restoreCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--restore',
            'binary',
            '--allVersions',
            '-z',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]

        self.commonTestBody(toRestore,saveCmd,restoreCmd,versions=True)

    @testWatchdog
    def test_tmp_binary_parallel_allVersions_zip(self):
        toRestore = self.bkpHelper.getAllArrayNames(4) # All versions of arrays should be restored.
        toRestore = [ # Keep only non-temp arrays (temps are skipped).
            a_name for a_name in toRestore if not any([a_name in t_name for t_name in self._tempArrays])
            ]
        saveCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--save',
            'binary',
            '--parallel',
            '--allVersions',
            '-z',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]
        restoreCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--restore',
            'binary',
            '--parallel',
            '--allVersions',
            '-z',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]

        self.commonTestBody(toRestore,saveCmd,restoreCmd,versions=True)

    @testWatchdog
    def test_tmp_opaque_parallel(self):
        toRestore = self.bkpHelper.getAllArrayNames() # All arrays should be restored.
        toRestore = [ # Keep only non-temp arrays (temps are skipped).
            a_name for a_name in toRestore if not any([a_name in t_name for t_name in self._tempArrays])
            ]
        saveCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--save',
            'opaque',
            '--parallel',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]
        restoreCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--restore',
            'opaque',
            '--parallel',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]

        self.commonTestBody(toRestore,saveCmd,restoreCmd)

    @testWatchdog
    def test_tmp_opaque_allVersions(self):
        toRestore = self.bkpHelper.getAllArrayNames(4) # All versions of arrays should be restored.
        toRestore = [ # Keep only non-temp arrays (temps are skipped).
            a_name for a_name in toRestore if not any([a_name in t_name for t_name in self._tempArrays])
            ]
        saveCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--save','opaque',
            '--allVersions',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]
        restoreCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--restore',
            'opaque',
            '--allVersions',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]

        self.commonTestBody(toRestore,saveCmd,restoreCmd,versions=True)

    @testWatchdog
    def test_tmp_opaque_zip(self):
        toRestore = self.bkpHelper.getAllArrayNames() # All arrays should be restored.
        toRestore = [ # Keep only non-temp arrays (temps are skipped).
            a_name for a_name in toRestore if not any([a_name in t_name for t_name in self._tempArrays])
            ]
        saveCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--save',
            'opaque',
            '-z',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]
        restoreCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--restore',
            'opaque',
            '-z',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]

        self.commonTestBody(toRestore,saveCmd,restoreCmd)

    @testWatchdog
    def test_tmp_opaque_parallel_allVersions(self):
        toRestore = self.bkpHelper.getAllArrayNames(4) # All versions of arrays should be restored.
        toRestore = [ # Keep only non-temp arrays (temps are skipped).
            a_name for a_name in toRestore if not any([a_name in t_name for t_name in self._tempArrays])
            ]
        saveCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--save',
            'opaque',
            '--parallel',
            '--allVersions',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]
        restoreCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--restore',
            'opaque',
            '--parallel',
            '--allVersions',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]

        self.commonTestBody(toRestore,saveCmd,restoreCmd,versions=True)

    @testWatchdog
    def test_tmp_opaque_parallel_zip(self):
        toRestore = self.bkpHelper.getAllArrayNames() # All arrays should be restored
        toRestore = [ # Keep only non-temp arrays (temps are skipped).
            a_name for a_name in toRestore if not any([a_name in t_name for t_name in self._tempArrays])
            ]
        saveCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--save',
            'opaque',
            '--parallel',
            '-z',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]
        restoreCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--restore',
            'opaque',
            '--parallel',
            '-z',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]

        self.commonTestBody(toRestore,saveCmd,restoreCmd)

    @testWatchdog
    def test_tmp_opaque_allVersions_zip(self):
        toRestore = self.bkpHelper.getAllArrayNames(4) # All versions of arrays should be restored
        toRestore = [ # Keep only non-temp arrays (temps are skipped).
            a_name for a_name in toRestore if not any([a_name in t_name for t_name in self._tempArrays])
            ]
        saveCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--save',
            'opaque',
            '--allVersions',
            '-z',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]
        restoreCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--restore',
            'opaque',
            '--allVersions',
            '-z',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]

        self.commonTestBody(toRestore,saveCmd,restoreCmd,versions=True)

    @testWatchdog
    def test_tmp_opaque_parallel_allVersions_zip(self):
        toRestore = self.bkpHelper.getAllArrayNames(4) # All versions of arrays should be restored
        toRestore = [ # Keep only non-temp arrays (temps are skipped).
            a_name for a_name in toRestore if not any([a_name in t_name for t_name in self._tempArrays])
            ]
        saveCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--save',
            'opaque',
            '--parallel',
            '--allVersions',
            '-z',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]
        restoreCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--restore',
            'opaque',
            '--parallel',
            '--allVersions',
            '-z',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]

        self.commonTestBody(toRestore,saveCmd,restoreCmd,versions=True)

    @testWatchdog
    def test_tmp_text(self):
        toRestore = self.bkpHelper.getAllArrayNames() # All arrays should be restored.
        toRestore = [ # Keep only non-temp arrays (temps are skipped).
            a_name for a_name in toRestore if not any([a_name in t_name for t_name in self._tempArrays])
            ]
        saveCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--save',
            'text',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]
        restoreCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--restore',
            'text',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]

        self.commonTestBody(toRestore,saveCmd,restoreCmd)

    @testWatchdog
    def test_tmp_binary(self):
        toRestore = self.bkpHelper.getAllArrayNames() # All arrays should be restored.
        toRestore = [ # Keep only non-temp arrays (temps are skipped).
            a_name for a_name in toRestore if not any([a_name in t_name for t_name in self._tempArrays])
            ]
        saveCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--save',
            'binary',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]
        restoreCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--restore',
            'binary',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]

        self.commonTestBody(toRestore,saveCmd,restoreCmd)

    @testWatchdog
    def test_tmp_opaque(self):
        toRestore = self.bkpHelper.getAllArrayNames() # All arrays should be restored.
        toRestore = [ # Keep only non-temp arrays (temps are skipped).
            a_name for a_name in toRestore if not any([a_name in t_name for t_name in self._tempArrays])
            ]
        saveCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--save',
            'opaque',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]
        restoreCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--restore',
            'opaque',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]

        self.commonTestBody(toRestore,saveCmd,restoreCmd)

    # "Litmus" test to make sure the testing infrastructure is working
    # as advertized: the test intentionally overwrites the restored
    # arrays and makes sure that they do NOT macth the original arrays.
    @testWatchdog
    def test_tmp_neg_binary(self):
        toRestore = self.bkpHelper.getAllArrayNames() # All arrays should be restored.
        toRestore = [ # Keep only non-temp arrays (temps are skipped).
            a_name for a_name in toRestore if not any([a_name in t_name for t_name in self._tempArrays])
            ]
        saveCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--save',
            'binary',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]
        restoreCmd = [
            BACKUP,
            '--host',
            '$IQUERY_HOST',
            '--port',
            '$IQUERY_PORT',
            '--restore',
            'binary',
            self.bkpHelper.getBackupFolder(),
            '-f',
            '"[A|B|C][^_]+$"'
            ]

        sys.stderr.write('\n' + ' '.join(restoreCmd) + '\n')
        exits,outs=self._cmdRunner.waitForProcesses(
            [self._cmdRunner.runSubProcess(saveCmd,useShell=True)],
            True
            )
        self.bkpHelper.removeArrays(self._tempArrays + self._arrays)
        exits,outs = self._cmdRunner.waitForProcesses(
            [self._cmdRunner.runSubProcess(restoreCmd,useShell=True)],
            True
            )
        restored = self.bkpHelper.getDBArrays(versions=False)
        print exits,outs
        print 'restored',restored

        self.assertTrue(
            set(toRestore) == set(restored),
            'Restored arrays do not match initial conditions!'
            )

        # Mess up the data in the restored arrays.
        iqueryCmd = ['iquery','-c','$IQUERY_HOST','-p','$IQUERY_PORT','-naq']
        for a_pair in [('B1','A3'),('C2','B1'),('A3','C2')]:
            cmd = iqueryCmd + ['\"store(' + a_pair[0] + ',' + a_pair[1] + ')\"']
            exits,outs = self._cmdRunner.waitForProcesses(
                [self._cmdRunner.runSubProcess(cmd,useShell=True)],
                True
                )

        # Check that the arrays are NOT the same.
        self.assertTrue(
            not self.bkpHelper.checkArrayData(toRestore),
            'Restored array data should not match initial conditions!'
            )

def getTestSuite(tests,user,hosts,ninst,bh,arrays,tempArrays,q=None):

    suite = unittest.TestSuite()
    testList = map(BackupTest,tests)
    map(lambda x: x.setBackupHelper(bh),testList)
    map(lambda x: x.setArrays(arrays,temp=False),testList)
    map(lambda x: x.setArrays(tempArrays,temp=True),testList)
    if (q is not None):
        map(lambda x: x.bkpHelper.setCreateQuery(q),testList)
    
    suite.addTests(testList)
    return suite

def getFullTestSuite(user,hosts,ninst,bh,arrays,tempArrays,q=None):
    return getTestSuite(allTests,user,hosts,ninst,bh,arrays,tempArrays,q)

def getParallelTestSuite(user,hosts,ninst,bh,arrays,tempArrays,q=None):
    tests = [t for t in allTests if 'parallel' in t]
    return getTestSuite(tests,user,hosts,ninst,bh,arrays,tempArrays,q)

def getNonParallelTestSuite(user,hosts,ninst,bh,arrays,tempArrays,q=None):
    tests = [t for t in allTests if 'parallel' not in t]
    return getTestSuite(tests,user,hosts,ninst,bh,arrays,tempArrays,q)
