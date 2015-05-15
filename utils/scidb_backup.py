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
import pwd
import re
import types
import subprocess
import time
import itertools
import functools
import signal
from distutils.util import strtobool

#-------------------------------------------------------------------------------
class ProgramArgs:
    #-------------------------------------------------------------------------
    def __init__(self):
        descMsg = 'Backup and restore utility for SciDB array data.  ' \
            'The utility saves/restores scidb arrays into folder(s) specified by the user (positional dir argument).  ' \
            'It must be run on the coordinator host in either save or restore mode (--save or --restore options respectively).  ' \
            'Each array will be saved into its own file under the specified folder(s). The utility treats positional dir ' \
            'argument as a "base" folder name: if --parallel option is not used, then all arrays are saved into the base folder on the coordinator. ' \
            'If --parallel option is specified, then the utility saves all arrays into multiple folders (one per each scidb ' \
            'instance).  These parallel folders are saved on their respective cluster hosts: host0 with instances 0 and 1 will ' \
            'have folders base.0 and base.1 while host1 with instances 2 and 3 will have folders base.2 and base.3. Conversely, when user ' \
            'restores the previously saved arrays, the restore operation must be run with the same options as the original save (e.g. ' \
            'if --parallel option was used to save, then it has to be specified during the restore).  By default, the utility saves all arrays. ' \
            'However, if user decides to save only a few arrays, that can be accomplished by filtering the array names via --filter option.  ' \
            'IMPORTANT: iquery executable must be in the PATH.'
            
        self._parser = argparse.ArgumentParser(
            description=descMsg,
            epilog='To run the program, either save or restore option is required.'
            )
        self._parser.add_argument(
            'dir',
            type=types.StringType,
            help='backup/restore directory (absolute path)'
            )
        self._parser.add_argument(
            '-s',
            '--save', 
            metavar='SAVE', 
            type=types.StringType,
            nargs=1,
            choices=['binary','text','opaque'],
            help='format for saving array data in files (allowed values are binary, text, or opaque)'
            )
        self._parser.add_argument(
            '-r',
            '--restore', 
            metavar='RESTORE', 
            type=types.StringType,
            nargs=1,
            choices=['binary','text','opaque'],
            help='format for restoring array data from previously saved files (allowed values are binary, text, or opaque)'
            )
        self._parser.add_argument(
            '-d',
            '--delete', 
            default=False,
            action='store_true',
            help='delete all saved array data files and folder(s) on all cluster hosts'
            )
        self._parser.add_argument(
            '--port', 
            default='1239',
            type=types.StringType,
            help='network port used by iquery'
            )
        self._parser.add_argument(
            '--host', 
            default='localhost',
            type=types.StringType,
            help='network host used by iquery'
            )
        self._parser.add_argument(
            '-a',
            '--allVersions', 
            default=False,
            action='store_true',
            help='saves/restores all versions of arrays (potentially a lot of data)'
            )
        self._parser.add_argument(
            '-f',
            '--filter',
            metavar='PATTERN', 
            type=types.StringType, 
            nargs=1,
            help='Python regex pattern to match against array names (escape and/or quote as necessary to avoid shell expansion)'
            )
        self._parser.add_argument(
            '-p',
            '--parallel', 
            default=False,
            action='store_true',
            help='backup/restore array data in parallel mode (to all scidb instances simultaneously)'
            )
        self._parser.add_argument(
            '-z',
            '--zip', 
            default=False,
            action='store_true',
            help='compress data files with gzip when saving/restoring array data'
            )
        self._parser.add_argument(
            '--force', 
            default=False,
            action='store_true',
            help='force silent removal of arrays before restoring them'
            )

        # Put all of the parser arguments and values into a dictionary for 
        # easier retrieval.
        self._args = self._parser.parse_args()
        self._argsDict = {}
        self._argsDict['dir'] = os.path.abspath(self._args.dir)
        self._argsDict['allVersions'] = self._args.allVersions
        self._argsDict['parallel'] = self._args.parallel
        self._argsDict['save'] = self._args.save
        self._argsDict['restore'] = self._args.restore
        self._argsDict['delete'] = self._args.delete
        self._argsDict['filter'] = self._args.filter
        self._argsDict['zip'] = self._args.zip
        self._argsDict['force'] = self._args.force
        self._argsDict['host'] = self._args.host
        self._argsDict['port'] = self._args.port

        if (types.ListType == type(self._argsDict['save'])):
            self._argsDict['save'] = self._argsDict['save'][0]
            
        if (types.ListType == type(self._argsDict['restore'])):
            self._argsDict['restore'] = self._argsDict['restore'][0]
            
        if (types.ListType == type(self._argsDict['filter'])):
            self._argsDict['filter'] = self._argsDict['filter'][0]
            
        if (not (self._argsDict['filter'] is None)):
            self._argsDict['filter'] = self._argsDict['filter'].replace('\"','')
            self._argsDict['filter'] = self._argsDict['filter'].replace('\'','')

    #-------------------------------------------------------------------------
    # get: small function for argument value retrieval
    def get(self,arg):
        if (arg in self._argsDict.keys()):
            return self._argsDict[arg]
        return None
    #-------------------------------------------------------------------------
    # getAllArgs: small function for getting the entire dictionary of 
    #             command line arguments.
    def getAllArgs(self):
        D = {}
        D.update(self._argsDict.iteritems())
        return D
    def print_help(self):
        self._parser.print_help()
##############################################################################
def yesNoQuestion(q):
    while True:
        try:
            return strtobool(raw_input(q).lower())
        except ValueError:
            sys.stdout.write('Please type \'y\' or \'n\'.\n')

##############################################################################
# abnormalExitWatchdog:
#    This function "wraps" or decorates the waitForProcesses method in 
#    CommandRunner class.  The function traps SystemExit and KeyboardInterrupt
#    signals in order to attempt some cleanup.  The function terminates 
#    processes that were being waited on.  This is a partial cleanup since 
#    there could be processes that were started on remote hosts which this 
#    program has little control over.
def abnormalExitWatchdog(procWaitFunc):
    @functools.wraps(procWaitFunc)
    def wrapper(obj,proc,raiseOnError=False):
        abnormalExit = False
        exceptMsg = ''
        try:
            exitCode,output = procWaitFunc(obj,proc,raiseOnError)
        except (KeyboardInterrupt, SystemExit):
            exceptMsg = 'Abnormal termination - exiting...'
            abnormalExit = True
        except Exception, e:
            exceptMsg = 'Bad exception - exiting...\n'
            exceptionMsg += e.message
            abnormalExit = True
        finally:
            pass

        thisCommand = obj.pop_pid_from_table(proc.pid)

        if (abnormalExit):
            obj.kill_all_started_pids()
            
            cmd = thisCommand[0]
            msg_list = [exceptMsg]
            msg_list.append('Exception(s) encountered while running command:')
            raise Exception('\n'.join(msg_list))
        if (exitCode != 0):
            msg_list = ['Abnormal return encountered while running command:']
            msg_list.append(thisCommand[0])
            errs = ''
            if (len(output) > 0):
                errs = output[1]
            if (raiseOnError):
                obj.kill_all_started_pids()
                raise Exception('\n'.join(msg_list))
        return (exitCode,output)
    return wrapper
##############################################################################
class CommandRunner:
    def __init__(self):
        self.pids = {}

    def get_command_from_pid(self,pid):
        cmd = ''
        if (str(pid) in self.pids.keys()):
            cmd = self.pids[str(pid)][0]
        return cmd

    def pop_pid_from_table(self,pid):
        cmdInfo = ['',None]
        if (str(pid) in self.pids.keys()):
            cmdInfo = self.pids.pop(str(pid)) 

        return cmdInfo

    def kill_all_started_pids(self):
        for pid in self.pids:
            msg_list = ['Terminating process:']
            msg_list.append('pid: ' + pid + ', command line: ' + self.pids[pid][0])
            sys.stderr.write('\n'.join(msg_list) + '\n')
            proc = self.pids[pid][1]
            try:
                proc.kill()
            except:
                sys.stderr.write('Could not kill process!')
    #-------------------------------------------------------------------------
    # runSubProcess: execute one command and return the subprocess object; the
    #                command is a list of strings representing executables and
    #                their options.  By default, the function attaches pipes
    #                to standard out and standard error streams and disables
    #                standard in.
    def _wrapForBash(self,cmd):
        localCmd = ' '.join(cmd)
        localCmd = localCmd.replace('\'','\\\'')
        localCmd = '/bin/bash -c $\'' + localCmd + '\''
        return localCmd
        
    def runSubProcess(
        self,
        cmd, # Command to run (list of string options).
        si=None, # Standard in.
        so=subprocess.PIPE, # Standard out.
        se=subprocess.PIPE, # Standard error.
        useShell=False # Flag to use shell option when starting process.
        ):
        
        localCmd = list(cmd) # Copy list to make sure it is not referenced 
                             # elsewhere.

        stringLocalCmd = ' '.join(localCmd) 
        exe = None
        if (useShell): # If command is for shell, flatten it into a single
            exe = '/bin/bash'
            localCmd = [stringLocalCmd]
        
        try:
            proc = subprocess.Popen( # Run the command.
                localCmd,
                stdin=si,
                stdout=so,
                stderr=se,
                shell=useShell,
                executable=exe
                )
        except Exception, e:
            msg_list = ['Error encountered while running command:']
            msg_list.append(stringLocalCmd)

            self.kill_all_started_pids()

            raise Exception('\n'.join(msg_list))
        self.pids[str(proc.pid)] = [stringLocalCmd,proc]
        return proc
    #-------------------------------------------------------------------------
    # runSShCommand: execute one command via ssh; the function relies on the
    #                fact that the specified user has the ability to run ssh
    #                commands without entering password.  The function starts
    #                the ssh subprocess with all three standard streams 
    #                attached (stdin, stdout, and stderr).  Once ssh starts,
    #                the function writes the entire user-specified command
    #                to the stdin of the ssh subprocess and closes it.  The
    #                return value is the subprocess object of the ssh.
    def runSShCommand(
        self,
        user, # Username to use with ssh.
        host, # Machine name/ip to ssh into.
        cmd, # Command to run over ssh.
        si=subprocess.PIPE, # Standard in.
        so=subprocess.PIPE, # Standard error.
        se=subprocess.PIPE # Standard error.
        ):       
        sshCmd = ['ssh', user + '@' + host] # Actual ssh command to run.
        
        try:
            # Start the ssh command with out any actual useful things to
            # execute.
            proc = self.runSubProcess(sshCmd,si,so,se)

            localCmd = self._wrapForBash(cmd)

            # Write the actual command to the ssh standard in.
            proc.stdin.write(localCmd + '\n')
        except:
            sys.stderr.write(localCmd + '\n')
            if (proc is not None):
                proc.kill()
            raise Exception('Error running ssh command!')
        finally:
            if (proc is not None):
                proc.stdin.close() # Closing ssh standard in possibly
            proc.stdin = None      # prevents it from hanging.
        return proc
    @abnormalExitWatchdog # Decorator to watch for abnormal exit conditions.
    def waitForProcess(self,proc,raiseOnError=False):
        outs = proc.communicate()
        exitCode = proc.returncode
        return (exitCode,outs)
    #-------------------------------------------------------------------------
    # waitForProcesses: waits until all of the specified processes have
    #     have exited; optionally, the function returns the exit codes and
    #     (stdout,stderr) text tuples from the processes.  The function is
    #     decorated with the watchdog function.
    #
    def waitForProcesses(self,procs,raiseOnError=False):
        exits_and_outs = map(lambda proc: self.waitForProcess(proc,raiseOnError),procs)
        exits = [ret_tuple[0] for ret_tuple in exits_and_outs]
        outs = [ret_tuple[1] for ret_tuple in exits_and_outs]
        return exits,outs
    #-------------------------------------------------------------------------
    # runSubProcesses: calls runSubProcess in quick succession and returns a
    #                  list of subprocess objects.
    def runSubProcesses(
        self,
        cmds, # Command to run (list of string options).
        si=None, # Standard in.
        so=subprocess.PIPE, # Standard out.
        se=subprocess.PIPE, # Standard error.
        useShell=False # Flag for subprocess to use shell or not.
        ):
        return map(
            lambda cmd: self.runSubProcess(cmd,si,so,se,useShell),cmds
            )
    #-------------------------------------------------------------------------
    # runSshCommands: calls runSShCommand in quick succession on the specified
    #                 commands and returns the list of subprocess objects.
    def runSshCommands(
        self,
        user, # Username to use with ssh.
        hosts, # Machine names/ips to ssh into.
        cmds, # Commands to run over ssh (one element per each machine).
        si=subprocess.PIPE, # Standard in.
        so=subprocess.PIPE, # Standard out.
        se=subprocess.PIPE # Standard error.
        ):
        return map(
            lambda i: self.runSShCommand(user,hosts[i],cmds[i],si,so,se),range(len(hosts))
            )
##############################################################################
# ScidbCommander: central class for dealing with scidb and related operations.
#
class ScidbCommander:
    #-------------------------------------------------------------------------
    # __init__: collects some initial info; the assumption here is that the 
    #           currently logged in user will do all of the ssh and scidb
    #           queries.
    def __init__(self, progArgs):
        self._iqueryHost = progArgs.get('host')
        self._iqueryPort = progArgs.get('port')
        self._user = pwd.getpwuid(os.getuid())[0] # Collect some info: get username.
        self._cmdRunner = CommandRunner()
        # Run iquery and stash the cluster instance data for later use.
        self._hosts,self._instanceData = self._getHostInstanceData()
        self._coordinator = self._hosts[0] # Coordinator is always first.

        self._list_arrays_query_command = [
            'iquery',
            '-c',
            self._iqueryHost,
            '-p',
            self._iqueryPort,
            '-ocsv',
            '-aq',
            'filter(list(\'arrays\'),temporary=false)'
            ]

        self._list_all_versions_arrays_query_command = \
            self._list_arrays_query_command[:-1]

        self._list_all_versions_arrays_query_command.append(
            'sort(filter(list(\'arrays\',true),temporary=false),aid)'
            )
    #-------------------------------------------------------------------------
    # _getHostInstanceData: gets the information about all scidb instances 
    #     currently running.  Basically, the function internalizes the scidb
    #     query list('instances') and keeps the information for later use by
    #     others.
    #     Instance data list is grouped by hosts and has the following format:
    #     [
    #       [ # Host 0 instances
    #         [port0,id0,start time,data folder0],
    #         [port1,id1,start time,data folder1],
    #         ...
    #         [portX,idX,start time,data folderX],
    #       ],
    #       [ #Host 1 instances
    #         [portX+1,idX,start time,data folderX+1],
    #         ...
    #       ],
    #       [ # Host N instances 
    #         ...
    #         [portN,idN,start time,data folderN]
    #       ]
    #     ]
    #     The fields are more or less self-explanatory: scidb instance network
    #     port, instance id, time when scidb started, and data folder for the
    #     instance.
    #     Hosts are returned in a separate list.
    def _getHostInstanceData(self):
        cmd = [
            'iquery',
            '-c',
            self._iqueryHost,
            '-p',
            self._iqueryPort,
            '-ocsv',
            '-aq',
            "list('instances')"
            ] # Instances query.
        
        proc = self._cmdRunner.runSubProcess(cmd)
        exits,outs = self._cmdRunner.waitForProcesses([proc],True)
        
        # Once the process has finished, get the outputs and split them into
        # internals lists.
        text = outs[0][0].strip().replace('\'','')
        err_text = outs[0][1].strip().replace('\'','')
        lines = text.split('\n')

        try:
            hostList = sorted(
                [line.split(',') for line in lines[1:]],
                key=lambda x: int(x[2])
                )
        except:
            raise Exception('Error: unable to parse instance data from output!')

        hosts = reduce( # Make a list of host names and preserve the order.
            lambda hList,hostData: hList + hostData if hostData[0] not in hList else hList,
            [[h[0]] for h in hostList]
            )
        instanceData = [[] for i in range(len(hosts))]
        map(
            lambda x: instanceData[hosts.index(x[0])].append(x[1:]),
            hostList
            )
        return hosts,instanceData
    #-------------------------------------------------------------------------
    # _getExistingScidbArrays: returns a list of arrays currently in the 
    #    database.  
    def _getExistingScidbArrays(self):

        exits,outs = self._cmdRunner.waitForProcesses(
            [self._cmdRunner.runSubProcess(self._list_arrays_query_command)],
            True
            )

        if (any(exits)):
            raise Exception('Error: could not run array list query!')

        text = outs[0][0].strip() # Get the raw text
        lines = text.split('\n')
        arrayNames = [line.split(',')[0].replace('\'','') for line in lines[1:]]
        return arrayNames
    #-------------------------------------------------------------------------
    # _removeArrays: delete specified arrays from scidb database.
    def _removeArrays(self,arrays):
        iqueryCmd = [
            'iquery',
            '-c',
            self._iqueryHost,
            '-p',
            self._iqueryPort,
            '-ocsv',
            '-naq'
            ]
        removeCmds = [iqueryCmd + ['remove(' + a + ')'] for a in arrays]
        
        exits = []
        outs = []
        for cmd in removeCmds:
            proc = self._cmdRunner.runSubProcess(cmd)
            pExit,pOut = self._cmdRunner.waitForProcesses([proc],raiseOnError=False)
            stdErrText = pOut[0][1]
            stdOutText = pOut[0][0]
            procExitCode = pExit[0]
            if (procExitCode != 0):
                if ('SCIDB_LE_ARRAY_DOESNT_EXIST' not in stdErrText):
                    msgList = []
                    msgList.append('Error: command \"' + ' '.join(cmd) + '\" failed!\n')
                    msgList.extend([stdOutText,stdErrText])
                    raise Exception(''.join(msgList))
        
    #-------------------------------------------------------------------------
    # _prepInstanceCommands: replicates the command over all instances 
    #     of scidb and groups the results by host (returned command list is 
    #     grouped by hosts).  For instance, if user has a scidb cluster with
    #     2 hosts and 2 scidb instances on each host, then this function will
    #     return a list of commands like this:
    #     [
    #       [cmd0,cmd1],
    #       [cmd2,cmd3]
    #     ]
    #     User supplies cmd in this case which can contain replaceable tokens
    #     like "<i>".  The function substitutes the replacment values based
    #     on what the rep_gen parameter function generates: the replacements
    #     generator function receives the instance list as the parameter and
    #     produces a list of replacement pairs - for instance,
    #     [('<i>',x[1])].  In this case, the function will replace any
    #     occurrence of string '<i>' with instance id.  Replacement 
    #     generator can return as many pairs as needed.
    #     
    #     Trim option removes the shell-separator strings (i.e. &, &&, ||,
    #     etc.) from the last command in the server's list.
    #     
    #     Combine option collapses all individual command lists into commands-
    #     per-host lists:
    #     [
    #       [cmd0-options,cmd1-options],
    #       [cmd2-options,cmd3-options]
    #     ]
    #     The consumers can run the entire list of collapsed command options on
    #     each host.
    def _prepInstanceCommands(
        self,
        template,
        rep_gen,
        trim=True,
        combine=True,
        end=None
        ):
        # The following lines replicate the command per each instance in the
        # cluster: N commands for N instances grouped into lists for each host.
        # The lines also perform token replacements on each individual option
        # string within the command (see above).  So, for each host's instances,
        # for each instance, for each option in template - replace tokens and 
        # place the option into a new command list.  The result is the list of
        # command lists grouped by host (into lists, of course).
        cmds = [
            [[ reduce(lambda a,kv: a.replace(*kv),rep_gen(inst),c) for c in template] for inst in dataL] for dataL in self._instanceData
            ]

        # Optionally remove last shell separator from each host command.
        if (trim):
            edge = -1
            if (cmds[0][-1][-1][-2:] in ['&&','||']):
                edge = -2
            cmds = [
                hCmds[:-1] + [hCmds[-1][:-1] + [hCmds[-1][-1][:edge]]] for hCmds in cmds
                ]
        # Optionally, append special en-suffix to last command list in each
        # host's list.
        if (end is not None):
            cmds = [
                hCmds[:-1] + [hCmds[-1] + [end]] for hCmds in cmds
                ]
            
        # Otionally combine commands for each host into a single list of 
        # options.
        if (combine):    
            cmds = [
                [c for c in itertools.chain(*hCmds)]for hCmds in cmds
                ]
        return cmds

    #-------------------------------------------------------------------------
    # _replaceTokensInCommand: replace placeholder string tokens found in 
    #    specified command's options.  The function replaces every occurrence
    #    of tokens in every option of the command list with the corresponding
    #    replacement values.
    def _replaceTokensInCommand(
        self,
        tokens, # String tokens possibly found in command options.
        replacements, # Replacements for the tokens.
        cmd # Command option list.
        ):

        # Reducer is a short function that takes a string and a token/value
        # pair.  It then returns the string with the token replaced by the 
        # value.  The * operator converts the token/value pair argument into
        # two arguments to the string replace method.
        reducer = lambda option,tokenAndVal: option.replace(*tokenAndVal)
        
        # Apply the reducer onto every option in the command and return the
        # result with the appropriate replacements made back to the caller.
        return [
            reduce(reducer,zip(tokens,replacements),option) \
            for option in cmd
            ]
    def _getCommandIterator(
        self,
        repTokens,
        repData,
        cmd,
        precompute=False
        ):
        iterator = []
        if (precompute):
            tempIterator =  [
                self._replaceTokensInCommand(zipStruct[0],zipStruct[1],zipStruct[2]) \
                    for zipStruct in zip(itertools.cycle([repTokens]),repData,itertools.cycle([cmd]))
                ]
            iterator = (cmd for cmd in tempIterator)
        else:
            iterator = (
                self._replaceTokensInCommand(zipStruct[0],zipStruct[1],zipStruct[2]) \
                    for zipStruct in zip(itertools.cycle([repTokens]),repData,itertools.cycle([cmd]))
                )
        return iterator

    #-------------------------------------------------------------------------
    # cleanUpLinks: removes the links from instance data folders to backup
    #               folders.
    def cleanUpLinks(self,args):
        baseFolder = args.get('dir')
        _,baseFolderName = os.path.split(baseFolder)
        if (baseFolder[-1] == os.sep):
            baseFolder = baseFolder[:-1]
        
        rmCmd = ['rm', '-f', '<dd>' + os.sep + baseFolderName + ';']
        
        rmCmds = self._prepInstanceCommands(
            rmCmd,
            lambda x: [('<i>',x[1]),('<dd>',x[-1])]
            )
        self._cmdRunner.waitForProcesses(
            self._cmdRunner.runSshCommands(
                self._user,
                self._hosts,
                rmCmds
                ),
            False # Outputs not checked.
            )
    #-------------------------------------------------------------------------
    # removeBackup: delete backup folders and all their contents on all scidb
    #     hosts.
    def removeBackup(self,args):
        baseFolder = args.get('dir')

        if (baseFolder[-1] == os.sep):
            baseFolder = baseFolder[:-1]
            
        rmCmd = ['rm', '-rf', baseFolder + '*']
        rmCmds = [list(rmCmd) for i in range(len(self._hosts))]
        question = 'Delete backup folder(s) ' + baseFolder + '*' + ' on all hosts? [y/n]:'
        userAnswer = yesNoQuestion(question)
        if (userAnswer):
            self._cmdRunner.waitForProcesses(
                self._cmdRunner.runSshCommands(
                    self._user,
                    self._hosts,
                    rmCmds
                    ),
                False # Outputs not checked.
                )
            print 'Backup folder(s) deleted.'
    #-------------------------------------------------------------------------
    def verifyBackup(self,args,arrayNames):
        baseFolder = args.get('dir')
        _,baseFolderName = os.path.split(baseFolder)
        if (baseFolder[-1] == os.sep):
            baseFolder = baseFolder[:-1]
        
        if (args.get('parallel')):
            cmd = ['ls', baseFolder + '.<i>' + ' &&']
            cmds = self._prepInstanceCommands(list(cmd),lambda x: [('<i>',x[1])])
            hosts = self._hosts
            instData = self._instanceData
            countCheck = lambda z: z[0].count(a) == z[1]
        else:
            cmds = [['ls', baseFolder]]
            hosts = [self._coordinator]
            instData = self._instanceData[0]
            countCheck = lambda z: z[0].count(a) == 1
        
        R = self._cmdRunner.waitForProcesses(
            self._cmdRunner.runSshCommands(
                self._user,
                hosts,
                cmds
                ),
            True
            )
        text = [t[0].strip().split('\n') for t in R[1]]
        # Now, we can test if all arrays are found in correct quantity.
        nInst = [ len(instD) for instD in instData]
        arrayChecks = [[ countCheck(z) for a in arrayNames] for z in zip(text,nInst)]
        totalChecks = [all(ac) for ac in arrayChecks]
        if (not all(totalChecks)):
            # Backup is corrupted: pinpoint the host and exit.            
            badBkpHosts = [z[0] for z in zip(hosts,totalChecks) if not z[1]]
            for badHost in badBkpHosts:
                print 'Backup folders are missing files on host',badHost,'.'
            raise Exception('Backup folders are corrupted!  Exiting...')
        print 'Ok.'
    #-------------------------------------------------------------------------
    # setupDataFolders: prepares backup data folders for the save operations.
    #     In case of parallel mode, multiple data folders have to be set up
    #     (one for each instance) and linked (from instance data folder to its
    #     corresponding backup folder).
    #
    def setupDataFolders(self,args):
        baseFolder = args.get('dir')
        _,baseFolderName = os.path.split(baseFolder)
        if (baseFolder[-1] == os.sep):
            baseFolder = baseFolder[:-1]
        folderList = [baseFolder]
        dataPaths = []
        if (args.get('parallel')):
            # For save operation, both the folders and the links must be
            # created.
            setupDirsTemplate = [ 
                'mkdir',
                '-p',
                baseFolder + '.<i>;',
                'ln',
                '-snf',
                baseFolder + '.<i>',
                '<dd>' + os.sep + baseFolderName + ';'
                ]
            if (not (args.get('restore') is None)):
                # In case of restore operation, only the links must be created.
                setupDirsTemplate = [
                    'ln',
                    '-snf',
                    baseFolder + '.<i>',
                    '<dd>' + os.sep + baseFolderName + ';'
                    ]

                # Check to make sure backup folder(s) are present.
                checkCmd = ['ls',baseFolder + '.<i>' + ';']
                checkCmds = self._prepInstanceCommands(
                    checkCmd,
                    lambda x: [('<i>',x[1])]
                    )
                exits,outs = self._cmdRunner.waitForProcesses(
                    self._cmdRunner.runSshCommands(self._user,self._hosts,checkCmds),
                    True
                    )
                if (any([x != 0 for x in exits])):
                    raise Exception('Error: not all backup folders found (bad exit codes); verify that backup folders exist on on cluster hosts!')
                lsErrMsg = 'ls: cannot access'
                errorOuts = [e[1] for e in outs]
                if (any([ lsErrMsg in x for x in errorOuts ])):
                    raise Exception('Error: not all backup folders found (error messages); verify that backup folders exist on on cluster hosts!')
            # Set up the folder and/or link making commands.
            cmds = self._prepInstanceCommands(
                setupDirsTemplate,
                lambda x: [('<i>',x[1]),('<dd>',x[-1])]
                )
            self._cmdRunner.waitForProcesses(
                self._cmdRunner.runSshCommands(
                    self._user,
                    self._hosts,
                    cmds
                    ),
                False
                )
        else: # This is a non-parallel operation: only one folder needed.
            if (not (os.path.isdir(baseFolder))):
                os.makedirs(baseFolder)
    #-------------------------------------------------------------------------
    # _getArrayListingFromScidb: runs the specified array listing query and 
    #     and returns the array info list.
    def _getArrayListingFromScidb(
        self,
        listArraysCmd
        ):
        proc = self._cmdRunner.runSubProcess(listArraysCmd)
        exits,outs = self._cmdRunner.waitForProcesses([proc],True)
        text = outs[0][0].strip()
        arrayInfo = text.split('\n')

        return arrayInfo[1:]
    #-------------------------------------------------------------------------
    # _getArrayListingFromBackup: gets the array listing info from the backed
    #     backed up .manifest file.
    def _getArrayListingFromBackup(
        self,
        manifest
        ):
        with open(manifest,'r') as fd:
            contents = fd.read()
        contents = contents.split('\n')
        return contents

    #-------------------------------------------------------------------------
    # _formatArrayInfo: puts the raw array listing information into a more 
    #     useful list structure for later use.
    #     The listing info for arrays have the following structure:
    #     [
    #       [name1,id1,schema1,1dschema,binFmt],
    #       [name2,id2,schema2,1dschema,binFmt],
    #       ...
    #       [nameN,idN,schemaN,1dschema,binFmt]
    #     ]
    #     The last two entries in each listing get added only during the
    #     binary format operation since binary format requires redimension.
    #
    def _formatArrayInfo(
        self,
        rawArrayInfo, # Raw array listing from scidb query (in list form).
        allVersions, # Flag for all versions mode.
        nameFilter=None, # RegExp to filter array names..
        binFmt=False, # Flag for binary format.
        binFmtQTemplate=None # Binary format input array template.
        ):

        arrayInfo = list(rawArrayInfo)
        # Remove array names from schema.
        arrayInfo = [
            re.compile('([^,]+),([^,]+),[^\<]*(\<[^\]]+\])').match(x) for x in arrayInfo
            ]
        arrayInfo = [
            [x.group(1),x.group(2),x.group(3)] for x in arrayInfo if not (x is None)
            ]
        arrayInfo = [[y.replace('\'','') for y in x] for x in arrayInfo]

        # Run the array names through the filter.
        if (not (nameFilter is None)):
            matches = [re.compile(nameFilter).match(x[0]) for x in arrayInfo]
            arrayInfo = [arrayInfo[i] for i in range(len(matches)) if not (matches[i] is None)]

        # In case of all versions, we only keep version-ed names.
        if (allVersions):
            matches = [re.compile('.*\@[0-9]+').match(x[0]) for x in arrayInfo]
            arrayInfo = [
                arrayInfo[i] for i in range(len(matches)) if not (matches[i] is None)
                ]

        if (binFmt):
            # In case of binary format, we add extra fields to the listings.
            
            # Prepare the command to query the database for additional info:
            # binary format operations require redimensioning.
            cmdTemplate = binFmtQTemplate
            nameTokens = ['<name>','<id>','<schema>']
            cmds = [[reduce(lambda x,y: x.replace(*y),zip(nameTokens,ai),c) for c in cmdTemplate] for ai in arrayInfo]
            exits,outs = self._cmdRunner.waitForProcesses(
                self._cmdRunner.runSubProcesses(cmds),
                True
                )
            # Brush up the raw query output.
            textOuts = [o[0].split('\n')[1] for o in outs]
            textOuts = [ 
                z[0].replace(z[1].group(),'') for z in zip(textOuts,[re.compile('^[^\<]*').search(t) for t in textOuts]) if not (z[1] is None)
                ]
            textOuts = [x.replace('\'','') for x in textOuts]
            
            binFmts = [reduce(lambda x,y: x.replace(y,''),re.findall('[^\<]*\<',z),z) for z in textOuts]
            binFmts = [reduce(lambda x,y: x.replace(y,''),re.findall('\>[^\>]*',z),z) for z in binFmts]
            binFmts = [reduce(lambda x,y: x.replace(y,''),re.findall('[^:,]+?:',z),z) for z in binFmts]
            binFmts = [reduce(lambda x,y: x.replace(y,''),re.findall('\s*DEFAULT[^,]*',z),z) for z in binFmts]
            binFmts = ['(' + z + ')' for z in binFmts]
            
            # Attach extra fields to the array info listings.
            arrayInfo = [x[0] + [x[1],x[2]] for x in zip(arrayInfo,textOuts,binFmts)]

        return arrayInfo
    #-------------------------------------------------------------------------
    # _saveManifest: saves array info listings into .manifest file for later
    #     retrieval by the restore operation.
    def _saveManifest(self,manifest,fmtArrayInfo):
        # Only save the name,id, and original array schema since the rest of
        # the info can be retrieved later (for binary format).
        saveInfo = [','.join(x[:3]) for x in fmtArrayInfo]
        text = '\n'.join(saveInfo)
        fd = open(manifest,'w')
        fd.write(text)
        fd.close()
    #-------------------------------------------------------------------------
    # _saveOptsFile: record options used during the save operation to prevent
    #     acidental option mismatches during the restore operations.
    def _saveOptsFile(self,optsFile,args):
        argsDict = args.getAllArgs()
        text = []
        if (not (args.get('save') is None)):
            text.append('--save')
            text.append(args.get('save'))
        if (args.get('parallel')):
            text.append('--parallel')
        if (args.get('allVersions')):
            text.append('--allVersions')
        if (not (args.get('filter') is None)):
            text.append('--filter')
            text.append(args.get('filter'))
        if (args.get('zip')):
            text.append('-z')
        text.append(args.get('dir'))
        
        fd = open(optsFile,'w')
        fd.write(' '.join(text))
        fd.close()
    #-------------------------------------------------------------------------
    # _runSave: saves all arrays (one-by-one) in either parallel or 
    #     non-parallel mode without zip option.
    def _runSave(
        self,
        fmtArrayInfo,  # Info for arrays from list operator with extra stuff.
        inputArrayExp, # Either array name or unpack(<name>,__row) for binary.
        saveFmts, # Format to save arrays in (different for formats).
        baseFolder, # Backup folder path.
        parallel=False # Parallel mode flag.
        ):
       
        # Extract plain array names from array information.
        arrayNames = [x[0] for x in fmtArrayInfo]
        
        # Set up a list of formats for the save operator; the format could be 
        # a simple string like 'auto' or a more complicated string like 
        # '(int64,double)' for binary save.
        fmts = [saveFmts[i] for i in range(len(fmtArrayInfo))]
        
        idNum = '0' # Zero means "save to coordinator", while "-1" means 
                    # "save to all instances" (parallel mode).
        saveFolder = baseFolder # By default, absolute save folder is assumed.
        
        if (parallel):
            idNum = '-1' # Parallel mode: save to all instances
            # Similarly, in parallel mode save folder value for the save
            # operator is a path relative to scidb data folder (the data paths
            # for all instances have been outfitted with the appropriate sym.
            # links ).
            _,baseFolderName = os.path.split(baseFolder)
            saveFolder = baseFolderName
        
        # This is the main save query command template: it still contains 
        # unreplaced tokens like <name> (will be replaced by array name).
        q = 'save(' + inputArrayExp + ',' + \
            '\'' + os.path.join(saveFolder,'<name>') + '\',' + \
            idNum + \
            ', <fmt>)'
        # These are the save commands: one for each array.  The list is
        # "reduced": each string in the command is run through the replacement
        # function to insert real array names and folder paths where
        # necessary.
        # A side note: the commands are organized into a generator (not a
        # list).  This is done to control the size of the list since it 
        # can get rather large.  During the array save loop, each command is
        # evaluated/"materialized" by the generator.
        cmds = (
            ['iquery','-c',self._iqueryHost,'-p',self._iqueryPort,'-naq', reduce(lambda x,y: x.replace(*y),[('<name>',z[0]),('<fmt>',z[1])],q)] \
            for z in zip(arrayNames,fmts)
        )
        
        savedArrays = 0
        for i in range(len(arrayNames)): # Save all of the arrays.
            cmd = cmds.next()
            print 'Archiving array ' + arrayNames[i]
            
            # Run save command, wait for its completion, and get its exit
            # codes and output.
            exits,outs = self._cmdRunner.waitForProcesses(
                [self._cmdRunner.runSubProcess(cmd)],
                True
                )

            text = outs[0][0].strip() # Print iquery output for the user.
            print text
            savedArrays += 1
        print 'Saved arrays:',savedArrays
    #-------------------------------------------------------------------------
    # _runZipSave: save arrays with gzip in non-parallel mode.
    #
    def _runZipSave(
        self,
        fmtArrayInfo,  # Info for arrays from list operator with extra stuff.
        inputArrayExp, # Either array name or unpack(<name>,__row) for binary.
        saveFmts, # Format to save arrays in (different for formats).
        baseFolder # Backup folder path.
        ):
        
        # Extract plain array names from array information.
        arrayNames = [x[0] for x in fmtArrayInfo]
        
        # Set up a list of formats for the save operator; the format could be 
        # a simple string like 'auto' or a more complicated string like 
        # '(int64,double)' for binary save.
        fmts = [saveFmts[i] for i in range(len(fmtArrayInfo))]
        
        # This is the main save query command template: it still contains 
        # unreplaced tokens like <name> (will be replaced by array name).
        q = 'save(' + inputArrayExp + ',' + \
            '\'' + os.path.join(baseFolder,'<name>') + '\',' + \
            '0' + \
            ', <fmt>)'

        # Path to the named pipe where the save operator will be storing data.
        pipeName = os.path.join(baseFolder,'<name>')
        
        # Set up pipe making shell commands here for each array.
        makePipeCmd = ['rm', '-f',pipeName, ';','mkfifo','--mode=666',pipeName]
        
        # Set up the gzip commands here for each array.  The command lists
        # have several shell commands in them: gzip the pipe contents and
        # move/rename the named pipe to the array name (final file name).
        gzipCmd = [
            'gzip',
            '-c',
            '<',
            os.path.join(baseFolder,'<name>'),
            '>',
            os.path.join(baseFolder,'<name>.gz'),
            '&&',
            'mv',
            os.path.join(baseFolder,'<name>.gz'),
            os.path.join(baseFolder,'<name>')
            ]
        
        # These are the save commands: one for each array.  They are 
        # "reduced": each string in the command is run through the replacement
        # function to insert real array names and folder paths where
        # necessary.
        # Note that all three command lists below are set up as generators (not
        # precomputed lists).  This is done to control the space used up by
        # data since these command lists tend to get rather large.

        repTokens = ['<name>','<fmt>']
        repData = zip(arrayNames,fmts)
        fullQuery = ['iquery','-c',self._iqueryHost,'-p',self._iqueryPort,'-ocsv','-naq', q]
        
        cmds = self._getCommandIterator(repTokens,repData,fullQuery,True)
        makePipeCmds = self._getCommandIterator(repTokens,repData,makePipeCmd,True)
        gzipCmds = self._getCommandIterator(repTokens,repData,gzipCmd,True)

        savedArrays = 0
        for i in range(len(arrayNames)):
            arrayName = arrayNames[i]
            makePipeCmd = makePipeCmds.next()
            saveArrayCmd = cmds.next()
            gzipCmd = gzipCmds.next()
            
            print 'Archiving array ' + arrayName
            # Make the named pipe and wait for the command to complete (no
            # need to read the output).
            self._cmdRunner.waitForProcesses(
                [self._cmdRunner.runSubProcess(makePipeCmd,useShell=True)],
                False
                )
            # Start the iquery save command into the named pipe and do not
            # wait for completion.
            mainProc = self._cmdRunner.runSubProcess(saveArrayCmd)
            
            # Start gzip/move shell command and wait for its completion: when
            # this command is done, save operator is done, gzip is done, and
            # the file has been renamed with the proper name (array name).
            self._cmdRunner.waitForProcesses(
                [self._cmdRunner.runSubProcess(gzipCmd,useShell=True)],
                False
                )
            # Get the output from the iquery save command.
            exits,outs = self._cmdRunner.waitForProcesses([mainProc],True)

            text = outs[0][0].strip() # Print iquery output for user.
            print text
            savedArrays += 1

        print 'Saved arrays:',savedArrays
    #-------------------------------------------------------------------------
    # _runParallelZipSave: save all arrays in parallel mode and with gzip.
    #
    def _runParallelZipSave(
        self,
        fmtArrayInfo,  # Info for arrays from list operator with extra stuff.
        inputArrayExp, # Either array name or unpack(<name>,__row) for binary.
        saveFmts, # Format to save arrays in (different for formats).
        baseFolder # Backup folder path.
        ):
        
        # Extract plain array names from array information.
        arrayNames = [x[0] for x in fmtArrayInfo]
        
        # Set up a list of formats for the save operator; the format could be 
        # a simple string like 'auto' or a more complicated string like 
        # '(int64,double)' for binary save.
        fmts = [saveFmts[i] for i in range(len(fmtArrayInfo))]
        
        _,baseFolderName = os.path.split(baseFolder);
        
        # This is the main save query command template: it still contains 
        # unreplaced tokens like <name> (will be replaced by array name).
        q = 'save(' + inputArrayExp + ',' + \
            '\'' + os.path.join(baseFolderName,'<name>') + '\',' + \
            '-1' + \
            ', <fmt>)'
            
        # These are the save commands: one for each array.  The list is
        # "reduced": each string in the command is run through the replacement
        # function to insert real array names and folder paths where
        # necessary.

        cmds = (
            ['iquery','-c',self._iqueryHost,'-p',self._iqueryPort,'-naq', reduce(lambda x,y: x.replace(*y),[('<name>',z[0]),('<fmt>',z[1])],q)] \
            for z in zip(arrayNames,fmts)
        )
        # Path to the named pipe where the save operator will be storing data.
        pipeName = os.path.join(baseFolder + '.<i>','<name>')
        
        # Set up pipe making shell commands here for each array.
        makePipeCmd = ['rm', '-f',pipeName, ';','mkfifo','--mode=666',pipeName + ';']

        # Set up the gzip commands here for each array.  The command lists
        # have several shell commands in them: gzip the pipe contents and
        # move/rename the named pipe to the array name (final file name).
        gzipCmd = [
            'gzip',
            '-c',
            '<',
            pipeName,
            '>',
            pipeName + '.gz',
            '&',
            'export',
            'GZ_EXIT_CODES=\"$GZ_EXIT_CODES $!\";'
            ]
        # Final command template: move/rename .gz file to its final
        # (array) name.
        moveCmd = [
            'mv',
            pipeName + '.gz',
            pipeName + ';'
            ]
        reducers = map(lambda a: lambda x: [('<i>',x[1]),('<name>',a)],arrayNames)

        allMakePipeCmds = (self._prepInstanceCommands(list(makePipeCmd),r) for r in reducers)
        allGzipCmds = (self._prepInstanceCommands(list(gzipCmd),r,trim=False,end='wait $GZ_EXIT_CODES') for r in reducers)
        allMoveCmds = (self._prepInstanceCommands(list(moveCmd),r) for r in reducers)
        
        savedArrays = 0
        for i in range(len(arrayNames)):
            makePipeCmdList = allMakePipeCmds.next()
            saveArrayCmd = cmds.next()
            gzipCmdList = allGzipCmds.next()
            moveCmdList = allMoveCmds.next()
            
            print 'Archiving array ' + arrayNames[i]
            # Make the named pipe and wait for the command to complete (no
            # need to read the output).
            self._cmdRunner.waitForProcesses(
                self._cmdRunner.runSshCommands(
                    self._user,
                    self._hosts,
                    makePipeCmdList
                    ),
                False
                )
            # Start the iquery save command into the named pipes and do not
            # wait for completion.
            mainProc = self._cmdRunner.runSubProcess(saveArrayCmd)
            
            # Start gzip/move shell command and wait for its completion: when
            # this command is done, save operator is done, gzip is done, and
            # the file has been renamed with the proper name (array name).
            self._cmdRunner.waitForProcesses(
                self._cmdRunner.runSshCommands(
                    self._user,
                    self._hosts,
                    gzipCmdList
                    ),
                False
                )
            # Get the output from the iquery save command.
            exits,outs = self._cmdRunner.waitForProcesses([mainProc],True)

            # Finally, move all of the .gz files into the array files.
            self._cmdRunner.waitForProcesses(
                self._cmdRunner.runSshCommands(
                    self._user,
                    self._hosts,
                    moveCmdList
                    ),
                False
                )

            text = outs[0][0].strip() # Print iquery output for user.
            print text

            savedArrays += 1
        print 'Saved arrays:',savedArrays
    #-------------------------------------------------------------------------
    # _runRestore: restore all arrays in parallel or in non-parallel modes.
    #
    def _runRestore(
        self,
        fmtArrayInfo,  # Formatted array info listing.
        restoreQuery, # Query with the store operator. 
        restoreFmts, # Format to restore arrays in (different for formats).
        baseFolder, # Base backup folder path.
        parallel=False # Parallel mode flag.
        ):
        
        # Extract plain array names from array information.
        arrayNames = [x[0] for x in fmtArrayInfo]
        
        # If names have version numbers, strip them out and keep unversioned
        # array names separately.
        noVerArrayNames = list(arrayNames)
        m = map(re.compile('\@[0-9]+').search,arrayNames)

        if (all(m)):
            noVerArrayNames = [z[0].replace(z[1].group(),'') for z in zip(noVerArrayNames,m)]
        
        idNum = '0' # Zero means "save to coordinator", while "-1" means 
                    # "save to all instances" (parallel mode).
        restoreFolder = baseFolder # By default, absolute restore folder is
                                   # assumed.
        
        if (parallel):
            idNum = '-1' # Parallel mode: save to all instances
            # Similarly, in parallel mode save folder value for the save
            # operator is a path relative to scidb data folder (the data paths
            # for all instances have been outfitted with the appropriate sym.
            # links ).
            _,baseFolderName = os.path.split(baseFolder) # Use relative path.
            restoreFolder = baseFolderName 
        
        # This is the main save query command template: it still contains 
        # unreplaced tokens like <name> (will be replaced by array name).
        q = restoreQuery
        
        # These are replacement tokens and values that will be used below to
        # transform command templates into actual commands.
        repTokens = ['<fname>','<id>','<schema>','<fmt>','<opt>']
        repData = [fmtArrayInfo[i] + [restoreFmts[i],idNum] for i in range(len(fmtArrayInfo))]

        if (len(fmtArrayInfo[0]) > 3): # Binary format requires extra information.
            repTokens = ['<fname>','<id>','<schema>','<bin_schema>','<fmt>','<opt>']
            repData = [fmtArrayInfo[i] + [idNum] for i in range(len(fmtArrayInfo))]

        # Array name might be different from the non-versioned name.  During
        # restore operation array info is coming from the manifest file.  If
        # user saved all versions of arrays, filenames will have @X suffixes,
        # but we need unversion-ed names of those same array for restoring
        # them.
        repTokens.append('<name>')
        repData = [z[1] + [z[0]] for z in zip(noVerArrayNames,repData)]

        # These are the actual store commands.  Note that these are kept in a
        # "generator" form (not a precomputed list).  This is done to keep
        # memory usage low since these lists can get very large: generators
        # compute only one value at a time.
        cmds = (
            ['iquery','-c',self._iqueryHost,'-p',self._iqueryPort,'-naq', reduce(lambda x,y: x.replace(*y),zip(repTokens,repData[i]),q)] \
            for i in range(len(fmtArrayInfo))
            )
        
        restoredArrays = 0

        for i in range(len(arrayNames)):
            arrayName = arrayNames[i]
            cmd = cmds.next()
            noVerName = noVerArrayNames[i]

            print 'Restoring array ' + arrayName
            # Run restore command, wait for its completion, and get its exit
            # codes and output.
            exits,outs = self._cmdRunner.waitForProcesses(
                [self._cmdRunner.runSubProcess(cmd)],
                True
                )

            text = outs[0][0].strip() # Print iquery output for the user.
            print text
            restoredArrays += 1
        print 'Restored arrays:',restoredArrays
    #-------------------------------------------------------------------------
    # _runZipRestore: restore all arrays with gzip option.
    #
    def _runZipRestore(
        self,
        fmtArrayInfo,  # Formatted array info listings.
        restoreQuery, # Store operator query template.
        restoreFmts, # Format to restore arrays in (different for formats).
        baseFolder # Backup folder path.
        ):
        
        # Extract plain array names from array information.
        arrayNames = [x[0] for x in fmtArrayInfo]
        
        # If names have version numbers, strip them out and keep unversioned
        # array names separately.
        noVerArrayNames = list(arrayNames)
        m = map(re.compile('\@[0-9]+').search,arrayNames)
        if (all([not(x is None) for x in m])):
            noVerArrayNames = [z[0].replace(z[1].group(),'') for z in zip(noVerArrayNames,m)]
        
        idNum = '0' # Zero means "save to coordinator", while "-1" means 
                    # "save to all instances" (parallel mode).
        
        # This is the main save query command template: it still contains 
        # unreplaced tokens like <name> (will be replaced by array name).
        q = restoreQuery
        
        # These are replacement tokens and values that will be used below to
        # transform command templates into actual commands.
        repTokens = ['<fname>','<id>','<schema>','<fmt>','<opt>']
        repData = [fmtArrayInfo[i] + [restoreFmts[i],idNum] for i in range(len(fmtArrayInfo))]
        
        if (len(fmtArrayInfo[0]) > 3): # Binary restore requires extra info.
            repTokens = ['<fname>','<id>','<schema>','<bin_schema>','<fmt>','<opt>']
            repData = [fmtArrayInfo[i] + [idNum] for i in range(len(fmtArrayInfo))]

        pipeName = os.path.join(baseFolder,'<fname>.p')
        pipeCmd = ['rm','-f',pipeName,'>','/dev/null','2>&1',';','mkfifo','--mode=666',pipeName]

        gzipCmd = ['gzip','-d','-c','<',os.path.join(baseFolder,'<fname>'),'>',pipeName]
        cleanupCmd = ['rm','-f',pipeName]

        # Start preparing the store query.
        q = q.replace('<in_path>', pipeName)
        
        # Array name might be different from the non-versioned name.  During
        # restore operation array info is coming from the manifest file.  If
        # user saved all versions of arrays, filenames will have @X suffixes,
        # but we need unversion-ed names of those same array for restoring
        # them.
        repTokens.append('<name>')
        repData = [z[1] + [z[0]] for z in zip(noVerArrayNames,repData)]

        # These are the actual commands for scidb hosts.  They are put
        # together by sending command templates through a series of 
        # string replacements.  Note that these are kept in "generator" form
        # in order to keep memory usage in check as these lists can get rather
        # large.

        fullQuery =  ['iquery','-c',self._iqueryHost,'-p',self._iqueryPort,'-naq', q]

        cmds = self._getCommandIterator(repTokens,repData,fullQuery,True)
        pipeCmds = self._getCommandIterator(repTokens,repData,pipeCmd,True)
        gzipCmds = self._getCommandIterator(repTokens,repData,gzipCmd,True)
        cleanupCmds = self._getCommandIterator(repTokens,repData,cleanupCmd,True)

        restoredArrays = 0
        
        for i in range(len(arrayNames)):
        
            arrayName = arrayNames[i] 
            cmd = cmds.next() 
            pipeCmd = pipeCmds.next()
            gzipCmd = gzipCmds.next()
            cleanupCmd = cleanupCmds.next()
            noVerName = noVerArrayNames[i]
            
            print 'Restoring array ' + arrayName
            
            self._cmdRunner.waitForProcesses(
                [self._cmdRunner.runSubProcess(pipeCmd,useShell=True)],
                False
                )
            self._cmdRunner.runSubProcess(gzipCmd,useShell=True)
            exits,outs = self._cmdRunner.waitForProcesses(
                [self._cmdRunner.runSubProcess(cmd)],
                True
                )
            self._cmdRunner.waitForProcesses(
                [self._cmdRunner.runSubProcess(cleanupCmd,useShell=True)],
                False
                )            
            text = outs[0][0].strip() # Print iquery output for user.
            print text
            restoredArrays += 1
        print 'Restored arrays:',restoredArrays
    #-----------------------------------------------------------------------------
    # _runParallelZipRestore: restore all arrays in parallel mode and with gzip
    #     option.
    def _runParallelZipRestore(
        self,
        fmtArrayInfo,  # Info for arrays from list operator with extra stuff.
        restoreQuery, # Query template for the store operator.
        restoreFmts, # Format to restore arrays in (different for formats).
        baseFolder # Backup folder path.
        ):
        
        # Extract plain array names from array information.
        arrayNames = [x[0] for x in fmtArrayInfo]
        
        # If names have version numbers, strip them out and keep unversioned
        # array names separately.
        noVerArrayNames = list(arrayNames)
        m = map(re.compile('\@[0-9]+').search,arrayNames)
        if (all([not(x is None) for x in m])):
            noVerArrayNames = [z[0].replace(z[1].group(),'') for z in zip(noVerArrayNames,m)]
        
        idNum = '-1' # Zero means "save to coordinator", while "-1" means 
                    # "save to all instances" (parallel mode).
        
        _,baseFolderName = os.path.split(baseFolder)
        # This is the main save query command template: it still contains 
        # unreplaced tokens like <name> (will be replaced by array name).
        q = restoreQuery
        
        # These are replacement tokens and values that will be used below to
        # transform command templates into actual commands.
        repTokens = ['<fname>','<id>','<schema>','<fmt>','<opt>']
        repData = [fmtArrayInfo[i] + [restoreFmts[i],idNum] for i in range(len(fmtArrayInfo))]
        
        if (len(fmtArrayInfo[0]) > 3): # Binary restore requires extra info.
            repTokens = ['<fname>','<id>','<schema>','<bin_schema>','<fmt>','<opt>']
            repData = [fmtArrayInfo[i] + [idNum] for i in range(len(fmtArrayInfo))]

        # Array name might be different from the non-versioned name.  During
        # restore operation array info is coming from the manifest file.  If
        # user saved all versions of arrays, filenames will have @X suffixes,
        # but we need unversion-ed names of those same array for restoring
        # them.
        repTokens.append('<name>')
        repData = [z[1] + [z[0]] for z in zip(noVerArrayNames,repData)] 
       
        # Set up all command templates.
        pipeName = os.path.join(baseFolder + '.<i>','<fname>.p')
        pipeCmd = ['rm','-f',pipeName,'>','/dev/null','2>&1',';','mkfifo','--mode=666',pipeName + '&&']

        gzipCmd = [
            'gzip',
            '-d',
            '-c',
            '<',
            os.path.join(baseFolder + '.<i>','<fname>'),
            '>',
            pipeName + '&',
            'export GZ_EXIT_CODES=\"$GZ_EXIT_CODES $!\";'
            ]
        cleanupCmd = ['rm','-f',pipeName + ';']

        # Start preparing the store query.
        q = q.replace('<in_path>',os.path.join(baseFolderName,'<fname>.p'))

        # These are the actual commands for scidb hosts.  They are put
        # together by sending command templates through a series of 
        # string replacements.  Note that these are kept in "generator" form
        # in order to keep memory usage in check as these lists can get rather
        # large.
        cmds = (
            ['iquery','-c',self._iqueryHost,'-p',self._iqueryPort,'-naq', reduce(lambda x,y: x.replace(*y),zip(x[0],x[1]),q)] \
            for x in zip(itertools.cycle([repTokens]),repData)
        )
        # Reducers are small functions that, when given input (instance info
        # list), produce a list of replacer tuples - [('<i>',x[1]),...].
        # These are used to scrub the commands and substitute temporary tokens
        # for actual data - <i> will be replaced by the scidb instance id in a
        # command.  See contruction of repTokens and repData above.
        # A side note: reducers cannot be a generator expression because it
        # is being traversed multiple times (generators get exhausted after a
        # single traverse).
        reducers = [lambda x: zip(repTokens,repData[i]) + [('<i>',x[1])] for i in range(len(arrayNames))]

        # Put together pipe-creation commands generator.
        allPipeCmds = (self._prepInstanceCommands(pipeCmd,x) for x in reducers)

        # Put together gzip commands generator.
        allGzipCmds = (self._prepInstanceCommands(gzipCmd,x,trim=False,end='wait $GZ_EXIT_CODES') for x in reducers)

        # Put together cleanup commands generator.
        allCleanupCmds = (self._prepInstanceCommands(cleanupCmd,x) for x in reducers)
        
        arraysRestored = 0

        for i in range(len(arrayNames)):

            arrayName = arrayNames[i]
            cmd=cmds.next() 
            pipeCmds = allPipeCmds.next()
            gzipCmds = allGzipCmds.next()
            cleanupCmds = allCleanupCmds.next()
            noVerName = noVerArrayNames[i]

            print 'Restoring array ' + arrayName

            self._cmdRunner.waitForProcesses(
                self._cmdRunner.runSshCommands(self._user,self._hosts,pipeCmds),
                False
                )
            self._cmdRunner.runSshCommands(self._user,self._hosts,gzipCmds)
            exits,outs = self._cmdRunner.waitForProcesses(
                [self._cmdRunner.runSubProcess(cmd)],
                True
                )
            self._cmdRunner.waitForProcesses(
                self._cmdRunner.runSshCommands(self._user,self._hosts,cleanupCmds),
                False
                )            
            text = outs[0][0].strip() # Print iquery output for user.
            print text
            arraysRestored += 1
        print 'Restored arrays:',arraysRestored
    #-----------------------------------------------------------------------------
    # restoreArrays: determines what kind of restore function to call based on 
    #     specified user arguments.
    def restoreArrays(self,args,manifestPath):
    
        # Grab the initial specified backup folder.
        baseFolder = args.get('dir')
        _,baseFolderName = os.path.split(baseFolder)
        if (baseFolder[-1] == os.sep):
            baseFolder = baseFolder[:-1]
                
        # Obtain array list here.
        arrayInfo = self._getArrayListingFromBackup(manifestPath)

        binFmt = False # Flag for binary format.
        binQTemplate = None # Template to find out binary (1D) array schema.

        # For non-binary backups, this is the restore query.
        restoreQuery = 'store(input(<schema>,\'<in_path>\',<opt>,<fmt>),<name>)'
        if (args.get('restore') == 'binary'):
            binFmt = True
            # For binary backups, this is the restore query with redimension.
            restoreQuery = 'store(redimension(input(<bin_schema>,\'<in_path>\',<opt>,\'<fmt>\'),<schema>),<name>)'
            # This is the query to determine 1d schema for the saved arrays.
            binQTemplate = [
                'iquery',
                '-c',
                self._iqueryHost,
                '-p',
                self._iqueryPort,
                '-ocsv',
                '-aq',
                'show(\'unpack(input(<schema>,\\\'/dev/null\\\'),__row)\',\'afl\')'
                ]
        
        fmtArrayInfo = self._formatArrayInfo(
            arrayInfo,
            args.get('allVersions'),
            args.get('filter'),
            binFmt,
            binQTemplate
            )
        
        print 'Verifying backup...'
        self.verifyBackup(args,[x[0] for x in fmtArrayInfo])

        # Get the simple restore formats (in case of non-binary restore).
        restoreFmts = ['\'' + args.get('restore') + '\'' for i in range(len(fmtArrayInfo))]
        
        if (args.get('save') == 'binary'):
            # In case of the binary restore, formats are not simple, and must
            # be gathered from the formatted array listing info.
            restoreFmts = [f[4] for f in fmtArrayInfo]
            
        # Prior to restoring an array, it has to be deleted from the database.
        # Usually, the list of arrays to remove is the same as the list of 
        # arrays to create (when --allVersions is specified, arrays to remove
        # have no versioning suffixes).
        arraysToRemove = set([fmtInfo[0].split('@')[0] for fmtInfo in fmtArrayInfo])

        # If user chose the --force option, then the arrays will be silently
        # removed before restoration.  Otherwise we check below if any of
        # the arrays are still in the database.
        if (not args.get('force')):
            # Check that the database does not already have the arrays we are
            # about to remove.
            existingArrays = self._getExistingScidbArrays()

            # Just in case array names are version-ed, remove the versions for 
            # set comparison.
            intersection = set(existingArrays) & arraysToRemove

            if (len(intersection) > 0):
                msgList = ['The following arrays still exist:']
                msgList.extend([a for a in intersection])
                msgList.append('Please remove them manually and re-run the restore operation!')
                raise Exception('\n'.join(msgList))
        else:
            self._removeArrays(arraysToRemove) # Remove all 

        if (args.get('parallel')):
            if (args.get('zip')):
                self._runParallelZipRestore(
                    fmtArrayInfo,
                    restoreQuery,
                    restoreFmts,
                    baseFolder
                    )
            else:
                # This is a local parallel restore.
                restoreQuery = restoreQuery.replace('<in_path>',os.path.join(baseFolderName,'<fname>'))
                self._runRestore(fmtArrayInfo,restoreQuery,restoreFmts,baseFolder,True)
        else:
            if (args.get('zip')):
                self._runZipRestore(fmtArrayInfo,restoreQuery,restoreFmts,baseFolder)
            else:
                restoreQuery = restoreQuery.replace('<in_path>',os.path.join(baseFolder,'<fname>'))
                self._runRestore(fmtArrayInfo,restoreQuery,restoreFmts,baseFolder)
    #-------------------------------------------------------------------------
    # saveArrays: determine which save method to call based on user-specified 
    #     options        
    def saveArrays(self,args,manifestPath,optsFilePath):

        # Get the user-specified value for the backup folder.
        baseFolder = args.get('dir')
        _,baseFolderName = os.path.split(baseFolder)
        if (baseFolder[-1] == os.sep):
            baseFolder = baseFolder[:-1]
            
        # Query for listing arrays in scidb database.
        listArraysCmd = self._list_arrays_query_command

        if (args.get('allVersions')):
            # If --allVersions option is set, then we need to grab all
            # versions of arrays.  Scidb sorts the array list based on id so
            # that when we reload arrays back, the versions of each array are
            # loaded in correct order.

            listArraysCmd = self._list_all_versions_arrays_query_command

        # Obtain array list here (filter out temp arrays).
        arrayInfo = self._getArrayListingFromScidb(listArraysCmd)
        
        binQTemplate = None # Query for determining 1d schema (binary format).
        binFmt = False # Binary format flag.
        
        inputArrayExp = '<name>' # For non-binary format, it is array name.
        
        if (args.get('save') == 'binary'):
            binFmt = True
            binQTemplate = [ # Query to get array 1d schema (for binary).
                'iquery',
                '-c',
                self._iqueryHost,
                '-p',
                self._iqueryPort,
                '-ocsv',
                '-aq',
                'show(\'unpack(input(<name>,\\\'/dev/null\\\'),__row)\',\'afl\')'
                ]
            inputArrayExp = 'unpack(<name>,__row)' # For binary, it is an expression.
        
        fmtArrayInfo = self._formatArrayInfo(
            arrayInfo,
            args.get('allVersions'),
            args.get('filter'),
            binFmt,
            binQTemplate
            )

        # For non-binary saves, formats are simple string specifiers.
        saveFmts = [args.get('save') for i in range(len(fmtArrayInfo))]
        
        if (args.get('save') == 'binary'):
            # For binary operations, formats are actual data types (already
            # in formatted array listings).
            saveFmts = [f[4] for f in fmtArrayInfo]
        
        # Even though users specify "text", the actual parameter to the save
        # operator is named "auto".
        saveFmts = ['\'' + x.replace('text','auto') + '\'' for x in saveFmts]
        
        # Save array listings in manifest and user selected options in
        # save_opts file.
        self._saveManifest(manifestPath,fmtArrayInfo)
        self._saveOptsFile(optsFilePath,args)
        
        if (args.get('parallel')):
            if (args.get('zip')):
                self._runParallelZipSave(
                    fmtArrayInfo,
                    inputArrayExp,
                    saveFmts,
                    baseFolder
                    )
            else:
                # This is a local parallel save.
                self._runSave(fmtArrayInfo,inputArrayExp,saveFmts,baseFolder,True)
        else:
            if (args.get('zip')):
                self._runZipSave(fmtArrayInfo,inputArrayExp,saveFmts,baseFolder)
            else:
                self._runSave(fmtArrayInfo,inputArrayExp,saveFmts,baseFolder)

        print 'Verifying backup...'
        self.verifyBackup(args,[x[0] for x in fmtArrayInfo])

#-----------------------------------------------------------------------------
# checkProgramArgs: perform checks on specified arguments beyond what the 
#     options parser has already done.
def checkProgramArgs(args):
    actions = [args.get('save'),args.get('restore')]
    if (args.get('delete') and actions.count(None) <= 1):
        print 'Error: delete action cannot be combined with other actions!'
        sys.exit(1)
        
    if (args.get('delete')): # Done checking: this is a simple delete job.
        return
    
    if (actions.count(None) > 1):
        print 'Error: no action specified (--save FMT, --restore FMT)!'
        sys.exit(1)
    if (actions.count(None) <= 0):
        print 'Error: both save and restore actions specified; please choose only one!'
        sys.exit(1)
    
    if (args.get('save') is None): # This is a restore.
        # Check manifest and saved options files.
        bkpDir = args.get('dir')
        if (bkpDir[-1] == os.sep):
            bkpDir = bkpDir[:-1]
        if (program_args.get('parallel')):
            inst0bkpDir = bkpDir + '.0'
            if (not os.path.isdir(inst0bkpDir )):
                print 'Error: coordinator backup directory ' + inst0bkpDir + ' is missing!'
                sys.exit(1)
        manifestPath = os.path.join(bkpDir,'.manifest')
        optsFilePath = os.path.join(bkpDir,'.save_opts')
        if (program_args.get('parallel')):
            manifestPath = os.path.join(inst0bkpDir,'.manifest')
            optsFilePath = os.path.join(inst0bkpDir,'.save_opts')
        if (not (os.path.isfile(manifestPath))):
            print 'Error: backup is corrupted; manifest file  ' + manifestPath + ' is missing!'
            sys.exit(1)
        if (not (os.path.isfile(optsFilePath))):
            print 'Error: backup is corrupted; saved options file  ' + optsFilePath + ' is missing!'
            sys.exit(1)
#-----------------------------------------------------------------------------
# Script main entry point.
#
if __name__ == '__main__':
    # For proper subprocess management:
    signal.signal(signal.SIGPIPE, signal.SIG_DFL)
    
    # Collect program arguments first:
    program_args = ProgramArgs()
    
    checkProgramArgs(program_args)

    sh = ScidbCommander(program_args)
    
    sh.setupDataFolders(program_args)
    
    baseFolder = program_args.get('dir')
    if (baseFolder[-1] == os.sep):
        baseFolder = baseFolder[:-1]
    manifestPath = os.path.join(baseFolder,'.manifest')
    optsFilePath = os.path.join(baseFolder,'.save_opts')
    if (program_args.get('parallel')):
        manifestPath = os.path.join(baseFolder + '.0','.manifest')
        optsFilePath = os.path.join(baseFolder + '.0','.save_opts')
        
    if (program_args.get('delete')):
        sh.removeBackup(program_args)
        sys.exit(0)
    
    if (not (program_args.get('save') is None)):
        sh.saveArrays(
            program_args,
            manifestPath,
            optsFilePath
            )
    else:
        sh.restoreArrays(
            program_args,
            manifestPath
            )
    # Remove links to backup folders from instance data folders.
    sh.cleanUpLinks(program_args) 
    
