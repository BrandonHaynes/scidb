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

##############################################################################
class CommandRunner:

    def runSubProcess(self,cmd,si=None,so=subprocess.PIPE,se=subprocess.PIPE,useShell=False):
        localCmd = list(cmd) # Copy list to make sure it is not referenced 
                             # elsewhere.
        exe = None
        if (useShell): # If command is for shell, flatten it into a single
            exe = '/bin/bash'
            localCmd = [' '.join(localCmd)]

        proc = subprocess.Popen(
            localCmd,
            stdin=si,
            stdout=so,
            stderr=se,
            shell=useShell,
            executable=exe
            )
        return proc
        
    def runSshCommand(self,user,host,cmd,si=subprocess.PIPE,so=subprocess.PIPE,se=subprocess.PIPE):       
        sshCmd = ['ssh', user + '@' + host]
        proc = self.runSubProcess(sshCmd,si,so,se)
        proc.stdin.write(' '.join(cmd) + '\n')
        proc.stdin.close()
        proc.stdin = None
        return proc
        
    def waitForProcesses(self,procs,outputs=False):
        outs = []
        outs = map(lambda p: p.communicate(),procs)
        exits = map(lambda p: p.returncode,procs)
        return exits,outs
        
    def runSubProcesses(self,cmds,si=None,so=subprocess.PIPE,se=subprocess.PIPE,useShell=False):
        return map(
            lambda cmd: self.runSubProcess(cmd,si,so,se,useShell),cmds
            )
            
    def runSshCommands(self,user,hosts,cmds,si=subprocess.PIPE,so=subprocess.PIPE,se=subprocess.PIPE):
        return map(
            lambda i: self.runSshCommand(user,hosts[i],cmds[i],si,so,se),range(len(hosts))
            )

