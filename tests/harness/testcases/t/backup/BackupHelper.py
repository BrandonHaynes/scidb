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
import argparse
import sys
import os
import re
import types
import subprocess
import time
import itertools
import functools
import unittest

from CommandRunner import CommandRunner

CREATE_400x400_ARRAYS = [
    'iquery',
    '-c',
    os.environ['IQUERY_HOST'],
    '-p',
    os.environ['IQUERY_PORT'],
    '-naq',
    'store(build(<a:double NULL>[i=0:399,101,0,j=0:399,101,0],random()%10),<name>)'
    ]

CREATE_200x200_ARRAYS = [
    'iquery',
    '-c',
    os.environ['IQUERY_HOST'],
    '-p',
    os.environ['IQUERY_PORT'],
    '-naq',
    'store(build(<a:double NULL>[i=0:199,53,0,j=0:199,53,0],random()%10),<name>)'
    ]

##############################################################################
class BkpTestHelper:
    def __init__(
        self,
        scidbUser,
        hosts,
        nInst,
        bkpFolder,
        allArrays
        ):
        self._scidbUser = scidbUser
        self._hosts = hosts
        self._nInst = nInst
        self._createQuery = CREATE_200x200_ARRAYS
        self._cmdRunner = CommandRunner()
        self.__bkpFolder = bkpFolder
        self.__allArrays = allArrays

    def setCreateQuery(self,q):
        self._createQuery = q

    def getAllArrayNames(self,versions=0):
        R = list(self.__allArrays)
        if (versions > 0):
            R = [a + '@' + str(i+1) for a in R for i in range(versions)]
        return R

    def createArrays(self,arrays,iRange=[5],backup=False,temp=False):
        # For regular arrays - different store and backup queries
        storeQueries = [
            [x.replace('<name>',n).replace('<i>',str(i)) for x in self._createQuery] for n in arrays for i in iRange
            ]
        
        bkpStoreQueries = [
            ['iquery','-naq','store(' + n + ',' + n + '_bkp)'] for n in arrays for i in iRange
            ]
            
        createQueries = itertools.cycle([[]])

        if (temp): 
            # Create queries are strictly for temp arrays.
            schema = re.compile('\<[^\]]+\]').search(self._createQuery[-1])
            createQueries = [
                self._createQuery[:-1] + ['create temp array ' + n + ' ' + schema.group()] for n in arrays
                ]

        for qz in zip(storeQueries,bkpStoreQueries,createQueries):
            if (len(qz[2]) > 0):
                R0 = self._cmdRunner.waitForProcesses(
                    self._cmdRunner.runSubProcesses(
                        [qz[2]]
                        ),
                    outputs=False
                    )
                
            R = self._cmdRunner.waitForProcesses(
                self._cmdRunner.runSubProcesses(
                    [qz[0]]
                    ),
                outputs=False
                )

            if (backup):
                self._cmdRunner.waitForProcesses(
                    self._cmdRunner.runSubProcesses(
                        [qz[1]]
                        ),
                    outputs=False
                    )
    def reCreateArrays(self,arrays,versions=0,temp=False):
        bkpNames = [[name + '_bkp@' + str(v+1) for v in range(versions)] for name in arrays]
        
        if (temp):
            bkpNames = [[name + '_bkp'] for name in arrays]
            # Create queries are strictly for temp arrays.
            schema = re.compile('\<[^\]]+\]').search(self._createQuery[-1])
        
        for pair in zip(arrays,bkpNames):
            name = pair[0]
            bkps = pair[1]
            for bkp in bkps:
                if (temp):
                    createQ = ['iquery','-aq','create temp array ' + name + ' ' + schema.group()]
                    R0 = self._cmdRunner.waitForProcesses(
                        self._cmdRunner.runSubProcesses([createQ]),outputs=True
                        )                
                
                storeQ = ['iquery','-naq','store(' + bkp + ',' + name + ')']

                R = self._cmdRunner.waitForProcesses(
                    self._cmdRunner.runSubProcesses([storeQ]),outputs=True
                    )                

    def removeArrays(self,arrays,backups=False):
        template = [
            'iquery',
            '-naq',
            'remove(<name>)'
            ]
        removeQueries = [[x.replace('<name>',n) for x in template] for n in arrays]
        bkpRemoveQueries = [[x.replace('<name>',n + '_bkp') for x in template] for n in arrays]

        self._cmdRunner.waitForProcesses(
            self._cmdRunner.runSubProcesses(
                removeQueries
                ),
            outputs=False
            )
        if (backups):
            self._cmdRunner.waitForProcesses(
                self._cmdRunner.runSubProcesses(
                    bkpRemoveQueries
                    ),
                outputs=False
                )

    def getDBArrays(self,versions=False):
        v = ''
        if (versions):
            v = ',true'
        q = ['iquery','-o','csv','-aq','list(\'arrays\'' + v +  ')']
        exits,outs = self._cmdRunner.waitForProcesses(
            self._cmdRunner.runSubProcesses(
                [q],
                ),
            True
            )
        M = [re.compile('\'[^\']+\'').match(x) for x in outs[0][0].split('\n')[1:]]
        A = [m.group().replace('\'','') for m in M if not (m is None)]
        # Filter out backups:
        A = [a for a in A if '_bkp' not in a]
        if (versions):
            A = [a for a in A if '@' in a]
        return A

    def removeBackup(self,bkpUtil):
        rmCmd = [bkpUtil,'-d',self.__bkpFolder]
        proc = self._cmdRunner.runSubProcess(rmCmd,si=subprocess.PIPE)
        proc.stdin.write('y' + '\n')
        proc.stdin.close()
        proc.stdin = None
        self._cmdRunner.waitForProcesses([proc])
    
    def getBackupFolder(self):
        return self.__bkpFolder

    def strToNumList(self,data):
        L = [re.compile('[0-9\.\+\-Ee]+').search(x) for x in data.split('\n')]
        L = [m.group() for m in L if not m is None]
        return L

    def checkArrayData(self,arrays):
        compQ = [
            'iquery',
            '-c',
            os.environ['IQUERY_HOST'],
            '-p',
            os.environ['IQUERY_PORT'],
            '-ocsv',
            '-aq',
            'aggregate(filter(apply(join(T1,T2),diff,T1.a - T2.a), diff<>0.0),count(*))'
            ]
        copyQ = [
            'iquery',
            '-c',
            os.environ['IQUERY_HOST'],
            '-p',
            os.environ['IQUERY_PORT'],
            '-ocsv',
            '-naq'
            ]

        removeQ = [
            'iquery',
            '-c',
            os.environ['IQUERY_HOST'],
            '-p',
            os.environ['IQUERY_PORT'],
            '-ocsv',
            '-naq',
            'remove(<name>)'
            ]            

        copyQueries = [copyQ + ['store(' + n + ',T1)'] for n in arrays]
        copyBkpQueries = [copyQ + ['store(' + n + '_bkp,T2)'] for n in arrays]

        if (len(arrays) > 0):
            if ('@' in arrays[0]):
                nVer = [x.split('@') for x in arrays]
                copyBkpQueries = [copyQ + ['store(' + z[0] + '_bkp@' + z[1] + ',T2)'] for z in nVer]                

        counts = []
        for i in range(len(arrays)):

            cp = copyQueries[i]
            cpBkp = copyBkpQueries[i]
            
            R=self._cmdRunner.waitForProcesses(
                self._cmdRunner.runSubProcesses(
                    [cp]
                    ),
                outputs=True
                )
            R = self._cmdRunner.waitForProcesses(
                self._cmdRunner.runSubProcesses(
                    [cpBkp]
                    ),
                outputs=True
                )
            R = self._cmdRunner.waitForProcesses(
                self._cmdRunner.runSubProcesses(
                    [compQ]
                    ),
                outputs=True
                )

            text = R[1][0][0]
            lines = text.split('\n')
            counts.append(int(lines[1]) == 0)
            rqs = [
                ['iquery','-c',os.environ['IQUERY_HOST'],'-p',os.environ['IQUERY_PORT'],'-naq','remove(T1)'],
                ['iquery','-c',os.environ['IQUERY_HOST'],'-p',os.environ['IQUERY_PORT'],'-naq','remove(T2)'],
                ]
            self._cmdRunner.waitForProcesses(
                self._cmdRunner.runSubProcesses(
                    rqs
                    ),
                outputs=False
                )
        return all(counts)
