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
import signal
import subprocess
import sys
import string
import time
import traceback
import copy
import argparse
import re

_DBG = os.environ.get("SCIDB_DBG",False)

# not order preserving
# should be O(n log n) or O(n) depending on
# whether set uses hashing or trees
def noDupes(seq):
    # simple implementation:
    # there are faster ways, but significant amounts of
    # dictionary code is involved.
    return list(set(seq))

# bad style to use from, removes the namespace colission-avoidance mechanism
import ConfigParser

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

# borrowed from http://code.activestate.com/recipes/541096/
def confirm(prompt=None, resp=False):
    """prompts for yes or no response from the user. Returns True for yes and
    False for no.
    """
    if prompt is None:
        prompt = 'Confirm'

    if resp:
        prompt = '%s [%s]|%s: ' % (prompt, 'y', 'n')
    else:
        prompt = '%s [%s]|%s: ' % (prompt, 'n', 'y')

    while True:
        ans = raw_input(prompt)
        if not ans:
            return resp

        if ans not in ['y', 'Y', 'n', 'N']:
            print 'please enter y or n.'
            continue

        if ans == 'y' or ans == 'Y':
            return True

        if ans == 'n' or ans == 'N':
            return False
    return False

def parse_test_timing_file(timing_file):
    """ Read the file with test timing info and convert the data
        into a list.  Timing file is obtained from the harness test
        log.  Resulting list returned from the function contains
        tuples - [test_name,time].
        
        @param timing_file path to the file with test time
        @return list of tuples each containing a test name and its
                running time
    """
    contents = '' # Pull in the full content of the file
    with open(timing_file,'r') as fd:
        contents = fd.read()

    # Split the file content into individual lines.
    test_lines = [line.strip() for line in contents.split('\n')]
    
    # Remove blank lines
    while ('' in test_lines):
        test_lines.remove('')
        
    # Pick out the timing information and record it in a list.
    tests = []
    for test_line in test_lines:
        m = re.compile('(\[end\]\s+t\.)(.+)').search(test_line)
        if (m is None):
            continue
        
        test_info = m.group(2).split(' ')
        test_info = [test_info[0],float(test_info[2])]
        tests.append(test_info)
        
    # Sort the list by test time in acending order.
    tests = sorted(tests,key=lambda x: x[1])
    
    # Return the result.
    return tests
    
def find_long_tests(test_timing_list, cutoff):
    """ Find tests whose running time is greater than
        some cutoff value.
        @param test_timing_list list with test names and their 
               running times
        @param cutoff floating point number for running time cutoff
        
        @return list of test names whose running time is greater
                than the specified cutoff.
    """
    for i in xrange(len(test_timing_list)):
        if (test_timing_list[i][1] <= cutoff):
            continue
        break
    long_test_info = test_timing_list[i:]
    long_tests = [test_info[0] for test_info in long_test_info]
    
    return long_tests
    
def make_new_skip_file(old_skip_file, new_skip_file, skip_tests):
    """ Append the specified list of test names to the existing disable
        test file and save it with a new file name.
        
        @param old_skip_file path to the disabled tests file
        @param new_skip_file path to the new disabled tests file
        @param skip_tests list of tests to skip
    """
    # Read the contents of the old file.
    contents = ''
    with open(old_skip_file,'r') as fd:
        contents = fd.read()

    # Append the tests to skip and write the whole new contents into
    # the new file.
    with open(new_skip_file,'w') as fd:
        contents = fd.write(contents + '\n' + '\n'.join(skip_tests))        

# Parse a config file
def parseOptions(filename, section_name):
   config = ConfigParser.RawConfigParser()
   config.read(filename)
   options={}
   try:
       for (key, value) in config.items(section_name):
           options[str(key)] = value
   except Exception, e:
      printError("config file parser error in file: %s, reason: %s" % (filename, e))
      sys.exit(1)
   return options

# from http://www.chiark.greenend.org.uk/ucgi/~cjwatson/blosxom/2009-07-02
def subprocess_setup():
    # Python installs a SIGPIPE handler by default. This is usually not what
    # non-Python subprocesses expect.
    signal.signal(signal.SIGPIPE, signal.SIG_DFL)

#
# Execute OS command
# This is a wrapper method for subprocess.Popen()
# If waitFlag=True and raiseOnBadExitCode=True and the exit code of the child process != 0,
# an exception will be raised.
def executeIt(cmdList,
              env=None,
              cwd=None,
              useShell=False,
              cmd=None,
              stdinFile=None,
              stdoutFile=None,
              stderrFile=None,
              waitFlag=True,
              collectOutput=False,
              raiseOnBadExitCode=True):
    ret = 0
    out = ''
    err = ''

    if not cwd:
        cwd = os.getcwd()

    my_env = copy.copy(os.environ)

    if env:
        for (key,value) in env.items():
            my_env[key] = value

    if useShell:
       cmdList=[" ".join(cmdList)]

    try:
       stdIn = None
       if stdinFile:
          stdIn=open(stdinFile,"r")

       stdOut = None
       if stdoutFile:
          # print "local - about to open stdoutFile log file:", stdoutFile
          stdOut=open(stdoutFile,"w")
       elif not waitFlag:
          stdOut=open("/dev/null","w")

       stdErr = None
       if stderrFile:
          #print "local - about to open stderrFile log file:", stderrFile
          stdErr=open(stderrFile,"w")
       elif not waitFlag:
          stdErr=open("/dev/null","w")

       if collectOutput:
           if not waitFlag:
               raise Exception("Inconsistent arguments: waitFlag=%s and collectOutput=%s" % (str(waitFlag), str(collectOutput)))
           if not stdErr:
               stdErr = subprocess.PIPE
           if not stdOut:
               stdOut = subprocess.PIPE

       printDebug("Executing: "+str(cmdList))

       p = subprocess.Popen(cmdList,
                            preexec_fn=subprocess_setup,
                            env=my_env, cwd=cwd,
                            stdin=stdIn, stderr=stdErr, stdout=stdOut,
                            shell=useShell, executable=cmd)
       if collectOutput:
           out, err = p.communicate() # collect stdout,stderr, wait

       if waitFlag:
          p.wait()
          ret = p.returncode
          if ret != 0 and raiseOnBadExitCode:
             raise Exception("Abnormal return code: %s on command %s" % (ret, cmdList))
    finally:
       if stdIn:
          stdIn.close()
       if stdOut and not isinstance(stdOut,int):
          stdOut.close()
       if stdErr and not isinstance(stdErr,int):
          stdErr.close()

    return (ret,out,err)

def getOS (scidbEnv):
    bin_path=os.path.join(scidbEnv.source_path,"deployment/common/os_detect.sh")
    curr_dir=os.getcwd()

    cmdList=[bin_path]
    ret,out,err = executeIt(cmdList,
                            useShell=True,
                            collectOutput=True,
                            cwd=scidbEnv.build_path)
    os.chdir(curr_dir)
    printDebug(out)
    return out

def rm_rf(path,force=False,throw=True):
    if not force and not confirm("WARNING: about to delete *all* contents of "+path, True):
        if throw:
            raise Exception("Cannot continue without removing the contents of "+path)
        else:
            return

    cmdList=["/bin/rm", "-rf", path]
    ret = executeIt(cmdList,
                    useShell=True,
                    stdoutFile="/dev/null",
                    stderrFile="/dev/null")
    return ret

def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as e:
        if e.errno != errno.EEXIST or not os.path.isdir(path):
            raise

def runSetup(force, sourcePath,buildPath,installPath,defs={},name='scidb'):
    oldCmakeCache = os.path.join(sourcePath,"CMakeCache.txt")
    if os.access(oldCmakeCache, os.R_OK):
        printInfo("WARNING: Deleting old CMakeCache file:"+oldCmakeCache)
        os.remove(oldCmakeCache)

    oldCmakeCache = os.path.join(buildPath,"CMakeCache.txt")
    if os.access(oldCmakeCache, os.R_OK):
        printInfo("WARNING: Deleting old CMakeCache file:"+oldCmakeCache)
        os.remove(oldCmakeCache)

    build_type=os.environ.get("SCIDB_BUILD_TYPE","Debug")
    rm_rf(os.path.join(buildPath,"*"),force, throw=False)
    mkdir_p(buildPath)
    curr_dir=os.getcwd()
    cmdList=["cmake",
             "-DCMAKE_BUILD_TYPE=%s"%build_type,
             "-DCMAKE_INSTALL_PREFIX=%s"%installPath]

    # add -D's
    for (key, value) in defs.items():
        cmdList.append("-D%s=%s"%(key,str(value)))
    cmdList.append(sourcePath)

    ret = executeIt(cmdList,
                    cwd=buildPath)
    #..............................................................
    # Record the source path inside the file <name>_installpath.txt
    # (saved on the root of the build flder.
    with open(os.path.join(buildPath, name + '_srcpath.txt'),'w') as fd:
        fd.write(sourcePath)
    #..............................................................
    os.chdir(curr_dir)

def setup(scidbEnv):
    runSetup(scidbEnv.args.force, scidbEnv.source_path, scidbEnv.build_path, scidbEnv.install_path)

def getPluginBuildPath(buildPath,name):
    return os.path.join(buildPath,"external_plugins",name)

def pluginSetup(scidbEnv):
    defs={}
    defs["SCIDB_SOURCE_DIR"]=scidbEnv.source_path
    defs["SCIDB_BUILD_DIR"]=scidbEnv.build_path
    pluginBuildPath=getPluginBuildPath(scidbEnv.build_path, scidbEnv.args.name)
    runSetup(
        scidbEnv.args.force,
        scidbEnv.args.path,
        pluginBuildPath,
        scidbEnv.install_path,
        defs,
        name=scidbEnv.args.name
        )

def runMake(scidbEnv, makeDir):
    jobs=os.environ.get("SCIDB_MAKE_JOBS","1")
    if scidbEnv.args.jobs:
       jobs = scidbEnv.args.jobs

    curr_dir=os.getcwd()

    cmdList=["/usr/bin/make", "-j%s"%jobs]
    if scidbEnv.args.target:
        cmdList.append(scidbEnv.args.target)

    ret = executeIt(cmdList,
                    cwd=makeDir)
    os.chdir(curr_dir)

def make(scidbEnv):
    runMake(scidbEnv, scidbEnv.build_path)

def pluginMake(scidbEnv):
    pluginBuildPath=getPluginBuildPath(scidbEnv.build_path, scidbEnv.args.name)
    runMake(scidbEnv, pluginBuildPath)

def make_packages (scidbEnv):
    bin_path=os.path.join(scidbEnv.source_path,"deployment/deploy.sh")
    curr_dir=os.getcwd()

    rm_rf(os.path.join(scidbEnv.args.package_path,"*"), scidbEnv.args.force)

    extra_env={}
    extra_env["SCIDB_BUILD_PATH"]=scidbEnv.build_path
    extra_env["SCIDB_SOURCE_PATH"]=scidbEnv.source_path
    cmdList=[bin_path,
             "build_fast",
             scidbEnv.args.package_path]
    ret = executeIt(cmdList,
                    env=extra_env,
                    cwd=scidbEnv.build_path)
    os.chdir(curr_dir)

def pluginMakePackages(scidbEnv):
    pluginBuildPath=getPluginBuildPath(scidbEnv.build_path, scidbEnv.args.name)
    pluginPath=getRecordedVar(pluginBuildPath,"CMAKE_HOME_DIRECTORY")
    buildType=getRecordedVar(pluginBuildPath,"CMAKE_BUILD_TYPE")
    packagePath=scidbEnv.args.package_path
    scidbBinPath=scidbEnv.args.scidb_bin_path
    if not packagePath:
        packagePath=os.path.join(pluginBuildPath, "packages")

    rm_rf(os.path.join(packagePath,"*"), scidbEnv.args.force)

    binPath=os.path.join(pluginPath,"deployment/deploy.sh")
    curr_dir=os.getcwd()

    # Usage: plugin-trunk/deployment/deploy.sh build <Debug|RelWithDebInfo> <packages_path> <scidb_packages_path> <scidb_bin_path>

    cmdList=[binPath,
             "build", buildType,
             packagePath, scidbEnv.source_path, scidbBinPath ]
    ret = executeIt(cmdList,
                    cwd=pluginPath)
    os.chdir(curr_dir)

def confirmRecordedInstallPath (scidbEnv):
    installPath = getRecordedVar (scidbEnv.build_path,"CMAKE_INSTALL_PREFIX")
    if installPath != scidbEnv.install_path:
        raise Exception("Inconsistent install path: recorded by setup=%s vs default/environment=%s" %
                        (installPath, scidbEnv.install_path))

def getRecordedVar (buildPath, varName):
    cmakeConfFile=os.path.join(buildPath,"CMakeCache.txt")

    curr_dir=os.getcwd()
    cmdList=["grep", varName, cmakeConfFile]
    ret,out,err = executeIt(cmdList, collectOutput=True)
    os.chdir(curr_dir)

    printDebug(out+" (raw var line)")
    value = out.split('=')[1].strip()
    printDebug(value+" (parsed var)")
    return value

def getScidbVersion (scidbEnv):
    bin_path=os.path.join(scidbEnv.source_path,"deployment/deploy.sh")
    curr_dir=os.getcwd()

    cmdList=[bin_path,
             "usage", "|", "grep", "\"SciDB version:\""]
    ret,out,err = executeIt(cmdList,
                            useShell=True,
                            collectOutput=True,
                            cwd=scidbEnv.build_path)
    os.chdir(curr_dir)
    printDebug(out+" (raw)")
    version = out.split(':')[1]
    printDebug(version+" (parsed)")
    return version.strip()

def version(scidbEnv):
    print getScidbVersion(scidbEnv)

def make_src_package(scidbEnv):
    jobs=os.environ.get("SCIDB_MAKE_JOBS","1")
    if scidbEnv.args.jobs:
        jobs = scidbEnv.args.jobs

    mkdir_p(scidbEnv.args.package_path)

    curr_dir=os.getcwd()

    cmdList=["/usr/bin/make", "-j%s"%jobs , "src_package"]
    ret = executeIt(cmdList,
                    cwd=scidbEnv.build_path)
    os.chdir(curr_dir)

    tar = os.path.join(scidbEnv.build_path,"scidb-*.tgz")
    cmdList=[ "mv", tar, scidbEnv.args.package_path ]
    ret = executeIt(cmdList,
                    useShell=True)
    os.chdir(curr_dir)

def cleanup(scidbEnv):
    curr_dir=os.getcwd()

    configFile = getConfigFile(scidbEnv)
    if os.access(configFile, os.R_OK):
        stop(scidbEnv)
        db_name=os.environ.get("SCIDB_NAME","mydb")
        dataPath = getDataPath(configFile, db_name)
        rm_rf(dataPath+"/*", scidbEnv.args.force)

    rm_rf(scidbEnv.install_path+"/*", scidbEnv.args.force)
    rm_rf(scidbEnv.build_path+"/*", scidbEnv.args.force)
    os.chdir(curr_dir)

def generateConfigFile(scidbEnv, db_name, host, port, data_path, instance_num, no_watchdog, configFile):
    if os.access(configFile, os.R_OK):
        os.remove(configFile)
    fd = open(configFile,"w")
    print >>fd, "[%s]"            %(db_name)
    print >>fd, "server-0=%s,%d"  %(host,instance_num-1)
    print >>fd, "db_user=%s"      %(db_name)
    print >>fd, "db_passwd=%s"    %(db_name)
    print >>fd, "install_root=%s" %(scidbEnv.install_path)
    print >>fd, "pluginsdir=%s"   %(os.path.join(scidbEnv.install_path,"lib/scidb/plugins"))
    print >>fd, "logconf=%s"      %(os.path.join(scidbEnv.install_path,"share/scidb/log1.properties"))
    print >>fd, "base-path=%s"    %(data_path)
    print >>fd, "base-port=%d"    %(port)
    print >>fd, "interface=eth0"
    if no_watchdog:
        print >>fd, "no-watchdog=true"
    if instance_num > 1:
        print >>fd, "redundancy=1"
    fd.close()

def getConfigFile(scidbEnv):
    return os.path.join(scidbEnv.install_path,"etc","config.ini")

def getDataPath(configFile, dbName):
    configOpts=parseOptions(configFile, dbName)
    return str(configOpts["base-path"])

def getCoordHost(configFile, dbName):
    configOpts=parseOptions(configFile, dbName)
    return str(configOpts["server-0"]).split(",")[0]

def getCoordPort(configFile, dbName):
    configOpts=parseOptions(configFile, dbName)
    return str(configOpts["base-port"])

def getDBUser(configFile, dbName):
    configOpts=parseOptions(configFile, dbName)
    return str(configOpts["db_user"])

def getDBPasswd(configFile, dbName):
    configOpts=parseOptions(configFile, dbName)
    return str(configOpts["db_passwd"])

def removeAlternatives(scidbEnv):
    # remove the local alternatives information
    # effectively removing all previously installed alternatives
    altdir=os.path.join(scidbEnv.install_path,"alternatives*")
    rm_rf(altdir, scidbEnv.args.force, throw=False)

def install(scidbEnv):
    configFile = getConfigFile(scidbEnv)

    if os.access(configFile, os.R_OK):
        stop(scidbEnv)

    db_name=os.environ.get("SCIDB_NAME","mydb")
    pg_user=os.environ.get("SCIDB_PG_USER","postgres")

    #
    # This section is for update-alternatives
    # It MUST be kept in sync with the scidb.spec file
    #
    if scidbEnv.args.light:
        # remove all links to the "alternative" libraries
        # thus, allow for the scidb libraries to be loaded again
        printInfo("Removing linker metadata for alternative plugins/libraries, please confirm");
        removeAlternatives(scidbEnv)

    curr_dir=os.getcwd()
    os.chdir(scidbEnv.build_path)

    if not scidbEnv.args.light:
        rm_rf(scidbEnv.install_path+"/*", scidbEnv.args.force)

    mkdir_p(scidbEnv.install_path)

    cmdList=["/usr/bin/make", "install"]
    ret = executeIt(cmdList)

    os.chdir(curr_dir)

    if scidbEnv.args.light:
        return

    # Generate config.ini or allow for a custom one

    data_path=""
    if scidbEnv.args.config:
        cmdList=[ "cp", scidbEnv.args.config, configFile]
        ret = executeIt(cmdList)
        data_path = getDataPath(configFile, db_name)
    else:
        data_path=os.path.join(scidbEnv.stage_path, "DB-"+db_name)
        data_path=os.environ.get("SCIDB_DATA_PATH", data_path)
        instance_num=int(os.environ.get("SCIDB_INSTANCE_NUM","4"))
        port=int(os.environ.get("SCIDB_PORT","1239"))
        host=os.environ.get("SCIDB_HOST","127.0.0.1")
        no_watchdog=os.environ.get("SCIDB_NO_WATCHDOG","false")
        no_watchdog=(no_watchdog in ['true', 'True', 'on', 'On'])
        generateConfigFile(scidbEnv, db_name, host, port, data_path, instance_num, no_watchdog, configFile)

    # Create log4j config files

    log4jFileSrc = os.path.join(scidbEnv.build_path,"bin/log1.properties")
    log4jFileTgt = os.path.join(scidbEnv.install_path,"share/scidb/log1.properties")

    cmdList=[ "cp", log4jFileSrc, log4jFileTgt]
    ret = executeIt(cmdList)

    version = getScidbVersion(scidbEnv)
    platform = getOS(scidbEnv).strip()
    printDebug("platform="+platform)
    if platform.startswith("CentOS 6"):
        # boost dependencies should be installed here

        boostLibs  = os.path.join("/opt/scidb",version,"3rdparty","boost","lib","libboost*.so.*")
        libPathTgt = os.path.join(scidbEnv.install_path,"lib")

        # Move boost libs into the install location

        cmdList=[ "cp", boostLibs, libPathTgt]
        ret = executeIt(cmdList,useShell=True)

    # Create PG user/role

    cmdList=[ "sudo","-u", pg_user, os.path.join(scidbEnv.install_path,"bin/scidb.py"),
              "init_syscat", db_name,
              os.path.join(scidbEnv.install_path,"etc/config.ini")]
    ret = executeIt(cmdList)

    # Initialize SciDB

    cmdList=[ os.path.join(scidbEnv.install_path,"bin/scidb.py"),
              "initall-force", db_name, os.path.join(scidbEnv.install_path,"etc/config.ini")]
    ret = executeIt(cmdList)

    # Setup test links

    os.chdir(curr_dir)

def pluginInstall(scidbEnv):
    pluginBuildPath=getPluginBuildPath(scidbEnv.build_path, scidbEnv.args.name)

    if not os.access(pluginBuildPath, os.R_OK):
        raise Exception("Invalid plugin %s build directory %s" % (scidbEnv.args.name, pluginBuildPath))

    pluginInstallPath = os.path.join(scidbEnv.install_path,"lib","scidb","plugins")
    if not os.access(pluginInstallPath, os.R_OK):
        raise Exception("Invalid plugin install directory %s" % (pluginInstallPath))

    curr_dir=os.getcwd()

    cmdList=["/usr/bin/make", "install"]
    ret = executeIt(cmdList, cwd=pluginBuildPath)

    os.chdir(curr_dir)

def start(scidbEnv):
    db_name=os.environ.get("SCIDB_NAME","mydb")
    cmdList=[ os.path.join(scidbEnv.install_path,"bin/scidb.py"),
              "startall", db_name, os.path.join(scidbEnv.install_path,"etc/config.ini")]
    ret = executeIt(cmdList)

def stop(scidbEnv):
    db_name=os.environ.get("SCIDB_NAME","mydb")
    cmdList=[ os.path.join(scidbEnv.install_path,"bin/scidb.py"),
              "stopall", db_name, os.path.join(scidbEnv.install_path,"etc/config.ini")]
    ret = executeIt(cmdList)

def initall(scidbEnv):
    db_name=os.environ.get("SCIDB_NAME","mydb")
    cmdList=[ os.path.join(scidbEnv.install_path,"bin/scidb.py"),
              "initall", db_name, os.path.join(scidbEnv.install_path,"etc/config.ini")]
    print "initializing cluster '" + db_name + "' from " + os.path.join(scidbEnv.install_path,"etc/config.ini")
    ret = executeIt(cmdList)


def getScidbPidsCmd(dbName=None):
    cmd = "ps --no-headers -e -o pid,cmd | awk \'{print $1 \" \" $2}\' | grep SciDB-000"
    if dbName:
        cmd = cmd + " | grep \'%s\'"%(dbName)
    cmd = cmd + " | awk \'{print $1}\'"
    return cmd

def forceStop(scidbEnv):
    db_name=os.environ.get("SCIDB_NAME","mydb")

    cmdList = [getScidbPidsCmd(db_name) + ' | xargs kill -9']
    executeIt(cmdList,
              useShell=True,
              cwd=scidbEnv.build_path,
              stdoutFile="/dev/null",
              stderrFile="/dev/null")

def runTests(scidbEnv, testsPath, srcTestsPath, commands=[]):
    curr_dir=os.getcwd()

    configFile = getConfigFile(scidbEnv)
    db_name   = os.environ.get("SCIDB_NAME","mydb")
    dataPath  = getDataPath(configFile, db_name)
    coordHost = getCoordHost(configFile, db_name)
    coordPort = getCoordPort(configFile, db_name)
    dbUser    = getDBUser(configFile, db_name)
    dbPasswd  = getDBPasswd(configFile, db_name)

    version = getScidbVersion(scidbEnv)

    libPath = os.path.join(scidbEnv.install_path,"lib")
    binPath = os.path.join(scidbEnv.install_path,"bin")
    testEnv = os.path.join(scidbEnv.build_path,"tests","harness","scidbtestharness_env.sh")
    testBin = os.path.join(scidbEnv.install_path,"bin","scidbtestharness")

    #...........................................................................
    # Add paths to PYTHONPATH.
    pythonPath = ':'.join(
        [
            os.path.join(scidbEnv.install_path,'lib'),
            os.path.join(scidbEnv.build_path,'bin')
        ]
        )
    if ('PYTHONPATH' in os.environ.keys()):
        pythonPath = '${PYTHONPATH}:' + os.path.join(scidbEnv.install_path,'lib')

    cmdList=["export", "SCIDB_NAME=%s"%db_name,";",
             "export", "SCIDB_HOST=%s"%coordHost,";",
             "export", "SCIDB_PORT=%s"%coordPort,";",
             "export", "SCIDB_BUILD_PATH=%s"%scidbEnv.build_path,";",
             "export", "SCIDB_INSTALL_PATH=%s"%scidbEnv.install_path,";",
             "export", "SCIDB_SOURCE_PATH=%s"%scidbEnv.source_path,";",
             "export", "SCIDB_DATA_PATH=%s"%dataPath, ";",
             "export", "SCIDB_DB_USER=%s"%dbUser, ";",
             "export", "SCIDB_DB_PASSWD=%s"%dbPasswd, ";",
             "export", "PYTHONPATH=%s"%pythonPath, ";",
             ".", testEnv, ";"]

    for cmd in commands:
       cmdList.extend([cmd,";"])
    #.........................................................................
    # Determine test root directory:
    testRootDir = srcTestsPath
    scratchDir = os.path.join(testsPath,'testcases')
    skipTests = os.path.join(testsPath,'testcases','disable.tests')
    #.........................................................................
    # Wipe out *.expected files from the scratch folder to ensure
    # the harness pulls everything from the source tree.
    #.........................................................................
    cmdList.extend(['find',scratchDir,'-name','"*.expected"','-print0',
        '|',
        'xargs','--null','rm','-f',';'
        ])
    #...........................................................
    if (scidbEnv.args.cutoff > 0.0): # User specified a running time cutoff.
        # Take the default timing file.
        timing_file = os.path.join(testsPath,'testcases','test_time.txt')

        # If user specified a custom timing file, take its path instead.
        if (scidbEnv.args.timing_file != ''):
            timing_file = scidbEnv.args.timing_file

        # Get the list of tests' timing info and pare it down based
        # on the specified cutoff value.
        all_tests = parse_test_timing_file(timing_file)
        long_tests = find_long_tests(all_tests,scidbEnv.args.cutoff)

        # Emit the new disabled tests file for the harness.
        base_path,skip_ext = os.path.splitext(skipTests)
        new_skip_tests = base_path + '_timed' + skip_ext
        make_new_skip_file(skipTests,new_skip_tests,long_tests)

        skipTests = new_skip_tests

    cmdList.extend(["PATH=%s:${PATH}"%(binPath),
                    testBin,
                    "--port=${IQUERY_PORT}",
                    "--connect=${IQUERY_HOST}",
                    "--scratch-dir=" + scratchDir,
                    "--log-dir=$SCIDB_BUILD_PATH/tests/harness/testcases/log",
                    "--skip-tests=" + skipTests,
                    "--root-dir=" + testRootDir])

    if scidbEnv.args.all:
        pass  # nothing to add
    elif  scidbEnv.args.test_id:
        cmdList.append("--test-id="+ scidbEnv.args.test_id)
    elif  scidbEnv.args.suite_id:
        cmdList.append("--suite-id="+ scidbEnv.args.suite_id)
    else:
        raise Exception("Cannot figure out which tests to run")

    if scidbEnv.args.record:
        cmdList.append("--record")

    cmdList.extend([ "|", "tee", "run.tests.log" ])

    ret = executeIt(cmdList,
                    useShell=True,
                    cwd=testsPath)
    os.chdir(curr_dir)


def tests(scidbEnv):
    srcTestsPath = os.path.join(scidbEnv.source_path,'tests','harness','testcases')
    testsPath=os.path.join(scidbEnv.build_path,"tests","harness")
    runTests(scidbEnv, testsPath, srcTestsPath)

def pluginTests(scidbEnv):
    pluginBuildPath=getPluginBuildPath(scidbEnv.build_path, scidbEnv.args.name)

    if not os.access(pluginBuildPath, os.R_OK):
        raise Exception("Invalid plugin %s build directory %s" % (scidbEnv.args.name, pluginBuildPath))

    pluginInstallPath = os.path.join(scidbEnv.install_path,"lib","scidb","plugins")
    if not os.access(pluginInstallPath, os.R_OK):
        raise Exception("Invalid plugin install directory %s" % (pluginInstallPath))

    plugin_tests=os.environ.get("SCIDB_PLUGIN_TESTS","test")

    pluginTestsPath = os.path.join(pluginBuildPath, plugin_tests)

    sourceTxtFile = os.path.join(
            pluginBuildPath,scidbEnv.args.name + '_srcpath.txt'
            )
    with open(sourceTxtFile,'r') as fd:
        contents = fd.read()

    pluginSourceTestsPath = os.path.join(contents.strip(),'test','testcases')

    pluginTestEnv = os.path.join(pluginBuildPath,'test','scidbtestharness_env.sh')

    commands=[]
    if os.access(pluginTestEnv, os.R_OK):
        commands.append(". "+pluginTestEnv)

    runTests(
            scidbEnv,
            pluginTestsPath,
            pluginSourceTestsPath,
            commands
            )

# Environment setup (variables with '_' are affected by environment)

#XXX TODO: support optional CMAKE -D

class SciDBEnv:
    def __init__(self, bin_path, source_path, stage_path, build_path, install_path, args):
        self.bin_path = bin_path
        self.source_path = source_path
        self.stage_path = stage_path
        self.build_path = build_path
        self.install_path = install_path
        self.args = args

def getScidbEnv(args):
    bin_path=os.path.abspath(os.path.dirname(sys.argv[0]))
    source_path=bin_path
    stage_path=os.path.join(source_path, "stage")
    build_path=os.path.join(stage_path, "build")
    install_path=os.path.join(stage_path, "install")

    printDebug("Source path: "+source_path)

    build_path=os.environ.get("SCIDB_BUILD_PATH", build_path)
    printDebug("Build path: "+build_path)

    install_path=os.environ.get("SCIDB_INSTALL_PATH",install_path)
    printDebug("Install path: "+install_path)

    return SciDBEnv(bin_path, source_path, stage_path, build_path, install_path, args)

# The main entry routine that does command line parsing
def main():
    scidbEnv=getScidbEnv(None)
    parser = argparse.ArgumentParser()
    parser.add_argument('-v','--verbose', action='store_true', help="display verbose output")
    subparsers = parser.add_subparsers(dest='subparser_name',
                                       title="Environment variables affecting all subcommands:"+
                                       "\nSCIDB_BUILD_PATH - build products location, default = %s"%(os.path.join(scidbEnv.bin_path,"stage","build"))+
                                       "\nSCIDB_INSTALL_PATH - SciDB installation directory, default = %s\n\nSubcommands"%(os.path.join(scidbEnv.bin_path,"stage","install")),
                                       description="Use -h/--help with a particular subcommand from the list below to learn its usage")


    pluginBuildPathStr = "$SCIDB_BUILD_PATH/external_plugins/<name>"

    subParser = subparsers.add_parser('setup', usage=
    """%(prog)s [-h | options]\n
Creates a new build directory for an out-of-tree build and runs cmake there.
Environment variables:
SCIDB_BUILD_TYPE - [RelWithDebInfo | Debug | Release], default = Debug""")
    subParser.add_argument('-f','--force', action='store_true', help=
                           "automatically confirm any old state/directory cleanup")
    subParser.set_defaults(func=setup)


    subParser = subparsers.add_parser('plugin_setup', usage=
                                      "%(prog)s [-h | options]\n"+
"\nCreates the %s directory for an out-of-tree plugin build and runs cmake there."%(pluginBuildPathStr)+
"\nThe plugin in the directory specified by --path must conform to the following rules:"
"\n1. It is based on cmake."+
"\n2. The plugin build directory %s must be configurable by \'cmake -DCMAKE_INSTALL_PREFIX=<scidb_installation_dir>  -DSCIDB_SOURCE_DIR=<scidb_source_dir> ...\'"%(pluginBuildPathStr)+
"\n3. Running \'make\' in that directory must build all the deliverables."+
"\n4. Running \'make install\' in that directory must install all the deliverables into the scidb installation."+
"\n5. Running \'./deployment/deploy.sh build $SCIDB_BUILD_TYPE <package_path> <scidb_bin_path> %s\' in the directory specified by --path must generate installable plugin packages. See \'plugin_make_packages --help\'"%(scidbEnv.bin_path)+
"\n6. \'scidbtestharness --rootdir=PATH/test/tescases --scratchDir=$SCIDB_BUILD_DIR/tests/harness/testcases ...\' must be runnable in %s."%(pluginBuildPathStr+"/$SCIDB_PLUGIN_TESTS")+
"""\nEnvironment variables:
SCIDB_BUILD_TYPE - [RelWithDebInfo | Debug | Release], default = Debug""")
    subParser.add_argument('-n', '--name',  required=True,
                           help= "plugin name")
    subParser.add_argument('-p','--path', required=True,
                           help= "directory path for plugin src")
    subParser.add_argument('-f','--force', action='store_true', help=
                           "automatically confirm any old state/directory cleanup")
    subParser.set_defaults(func=pluginSetup)


    subParser = subparsers.add_parser('make', usage=
    """%(prog)s [-h | options]\n
Builds the sources
Environment variables:
SCIDB_MAKE_JOBS - number of make jobs to spawn (the -j parameter of make)""")
    subParser.add_argument('target', nargs='?', default=None, help=
                           "make target, default is no target")
    subParser.add_argument('-j','--jobs', type=int, help=
                           "number of make jobs to spawn (the -j parameter of make)")
    subParser.set_defaults(func=make)


    subParser = subparsers.add_parser('plugin_make', usage=
    """%(prog)s [-h | options]\n
Builds the plugin sources
Environment variables:
SCIDB_MAKE_JOBS - number of make jobs to spawn (the -j parameter of make)""")
    subParser.add_argument('-n', '--name',  required=True,
                           help= "plugin name")
    subParser.add_argument('target', nargs='?', default=None, help=
                           "make target, default is no target")
    subParser.add_argument('-j','--jobs', type=int, help=
                           "number of make jobs to spawn (the -j parameter of make)")
    subParser.set_defaults(func=pluginMake)


    subParser = subparsers.add_parser('make_packages', usage=
    """%(prog)s [-h | options]\n
Builds deployable SciDB packages""")
    subParser.add_argument('package_path', default=os.path.join(scidbEnv.build_path,"packages"),
                           nargs='?', help=
                           "full directory path for newly generated_packages, default is $SCIDB_BUILD_PATH/packages")
    subParser.add_argument('-f','--force', action='store_true', help=
                           "automatically confirm any old state/directory cleanup")
    subParser.set_defaults(func=make_packages)


    subParser = subparsers.add_parser('plugin_make_packages', usage=
    """%(prog)s [-h | options]\n
Builds deployable plugin packages by invoking \'deploy.sh build $SCIDB_BUILD_TYPE ...\' in the plugin source directory See \'plugin_setup --help\'.
WARNING:
Currently, the plugin packages are allowed to be generated from scratch (not from the results of \'plugin_make\') on every invocation.
""")
    subParser.add_argument('-n', '--name',  required=True,
                           help= "plugin name")
    subParser.add_argument('package_path',
                           nargs='?', help=
                           "full directory path for newly generated_packages, default is $SCIDB_BUILD_PATH/external_plugins/<name>/packages")
    subParser.add_argument('scidb_bin_path', default=os.path.join(scidbEnv.build_path,"bin"),
                           nargs='?', help=
                           "full path for scidb bin directory, default is $SCIDB_BUILD_PATH/bin")
    subParser.add_argument('-f','--force', action='store_true', help=
                           "automatically confirm any old state/directory cleanup")
    subParser.set_defaults(func=pluginMakePackages)


    subParser = subparsers.add_parser('make_src_package', usage=
    """%(prog)s [-h | options]\n
Builds SciDB source tar file
Environment variables:
SCIDB_MAKE_JOBS - number of make jobs to spawn (the -j parameter of make)""")
    subParser.add_argument('package_path', default=os.path.join(scidbEnv.build_path,"packages"),
                           nargs='?', help=
                           "directory path for newly generated tar file, default is $SCIDB_BUILD_PATH/packages")
    subParser.add_argument('-j','--jobs', type=int, help=
                           "number of make jobs to spawn (the -j parameter of make)")
    subParser.set_defaults(func=make_src_package)


    subParser = subparsers.add_parser('plugin_install', usage=
    "%(prog)s [-h | options]\n"+
"\nInstalls the plugin by depositing the contents of %s into %s"%(pluginBuildPathStr+"<name>/plugins",
                                                                "$SCIDB_INSTALL_PATH/lib/scidbplugins"))
    subParser.add_argument('-n', '--name', required=True,
                           help= "plugin name")
    subParser.set_defaults(func=pluginInstall)


    subParser = subparsers.add_parser('install', usage=
    """%(prog)s [-h | options]\n
Re-create SciDB Postgres user. Install and initialize SciDB.
Environment variables:
SCIDB_NAME      - the name of the SciDB database to be installed, default = mydb
SCIDB_HOST      - coordinator host DNS/IP, default = 127.0.0.1
SCIDB_PORT      - coordinator TCP port, default = 1239
SCIDB_PG_USER   - OS user under which the Postgres DB is running, default = postgres
SCIDB_DATA_PATH - the common directory path prefix used to create SciDB instance directories (aka base-path).
                  It is overidden by the command arguments, default is $SCIDB_BUILD_PATH/DB-$SCIDB_NAME
SCIDB_INSTANCE_NUM - the number of SciDB instances to initialize.
                  It is overidden by the command arguments, default is 4.
SCIDB_NO_WATCHDOG - do not start a watch-dog process, default is false
""")
    subParser.add_argument('config', default=None, nargs='?', help=
                           "config.ini file to use with scidb.py, default is generated")

    group = subParser.add_mutually_exclusive_group()
    group.add_argument('-f','--force', action='store_true', help=
                       "automatically confirm any old state/directory cleanup")
    group.add_argument('-l','--light', action='store_true', help=
                       "just install new binaries, no changes to configuration are made")
    subParser.set_defaults(func=install)

    subParser = subparsers.add_parser('start', usage=
    """%(prog)s [-h]\n
Start SciDB (previously installed by \'install\')
Environment variables:
SCIDB_NAME - the name of the SciDB database to start, default = mydb""")
    subParser.set_defaults(func=start)

    subParser = subparsers.add_parser('stop', usage=
    """%(prog)s [-h]\n
Stop SciDB (previously installed by \'install\')
Environment variables:
SCIDB_NAME - the name of the SciDB database to stop, default = mydb""")
    subParser.set_defaults(func=stop)

    subParser = subparsers.add_parser('initall', usage=
    """%(prog)s [-h]\n
Initialize SciDB (previously installed by \'install\')
Environment variables:
SCIDB_NAME - the name of the SciDB database to stop, default = mydb""")
    subParser.set_defaults(func=initall)

    subParser = subparsers.add_parser('forceStop', usage=
    """%(prog)s [-h]\n
Stop SciDB instances with \'kill -9\'
Environment variables:
SCIDB_NAME - the name of the SciDB database to stop, default = mydb""")
    subParser.set_defaults(func=forceStop)

    subParser = subparsers.add_parser('tests', usage=
    """%(prog)s [-h | options]\n
Run scidbtestharness for a given set of tests
The results are stored in $SCIDB_BUILD_PATH/tests/harness/run.tests.log
Environment variables:
SCIDB_NAME - the name of the SciDB database to be tested, default = mydb"""+
"\n%s/tests/harness/scidbtestharness_env.sh is source'd to create the environment for scidbtestharness"%(scidbEnv.build_path))
    group = subParser.add_mutually_exclusive_group()
    group.add_argument('--all', action='store_true', help="run all scidbtestharness tests")
    group.add_argument('--test-id', help="run a specific scidbtestharness test")
    group.add_argument('--suite-id', default='checkin', help="run a specific scidbtestharness test suite, default is \'checkin\'")
    subParser.add_argument('--record', action='store_true', help="record the expected output")
    subParser.add_argument('-c', '--cutoff', default=-1.0, type=float, help="threshold for the running time of the test: tests whose running time is greater will be skipped.")
    subParser.add_argument('--timing-file', default='', help="path to the file containing test names and their running times (obtained from the harness tests log).")
    subParser.set_defaults(func=tests)


    subParser = subparsers.add_parser('plugin_tests', usage=
    """%(prog)s [-h | options]\n
Run scidbtestharness for a given set of tests of a given plugin."""+
" The results are stored in %s/$SCIDB_PLUGIN_TESTS/run.tests.log"%(pluginBuildPathStr)+
"""\nEnvironment variables:
SCIDB_NAME - the name of the SciDB database on which to test the plugin, default = mydb+
SCIDB_PLUGIN_TESTS - the subdirectory wrt the plugin build path where the the scidbtestharness rootdir is located, default = test"""+
"\nSCIDB_BUILD_PATH and SCIDB_DATA_PATH are exported into the environment"+
"\n%s/tests/harness/scidbtestharness_env.sh is source'd to create the environment for scidbtestharness."%("$SCIDB_BUILD_PATH")+
"\n%s/$SCIDB_PLUGIN_TESTS/scidbtestharness_env.sh is source'd to create the plugin specific environment for scidbtestharness"%(pluginBuildPathStr))

    subParser.add_argument('-n', '--name',  required=True,
                               help= "plugin name")
    group = subParser.add_mutually_exclusive_group()
    group.add_argument('--all', action='store_true', help="run all scidbtestharness tests")
    group.add_argument('--test-id', help="run a specific scidbtestharness test")
    group.add_argument('--suite-id', default='checkin', help="run a specific scidbtestharness test suite, default is \'checkin\'")
    subParser.add_argument('--record', action='store_true', help="record the expected output")
    subParser.add_argument('-c', '--cutoff', default=-1.0, type=float, help="threshold for the running time of the test: tests whose running time is greater will be skipped.")
    subParser.add_argument('--timing-file', default='', help="path to the file containing test names and their running times (obtained from the harness tests log).")
    subParser.set_defaults(func=pluginTests)


    subParser = subparsers.add_parser('cleanup', usage=
    """%(prog)s [-h | options]\n
Remove build, install, SciDB data directory trees.
It will execute stop() if config.ini is present in the install directory.""")
    subParser.add_argument('-f','--force', action='store_true',
                           help="automatically confirm any old state/directory cleanup")
    subParser.set_defaults(func=cleanup)

    subParser = subparsers.add_parser('version', usage=
    """%(prog)s\n
 Print SciDB version (in short form)""")
    subParser.set_defaults(func=version)

    args = parser.parse_args()
    scidbEnv.args=args

    global _DBG
    if args.verbose:
       _DBG=args.verbose

    printDebug("cmd="+args.subparser_name)

    curr_dir=os.getcwd()
    try:
        if args.subparser_name != "setup" and args.subparser_name != "cleanup" and args.subparser_name != "forceStop" :
            confirmRecordedInstallPath(scidbEnv)
        args.func(scidbEnv)
    except Exception, e:
        printError("Command %s failed: %s\n"% (args.subparser_name,e) +
                   "Make sure commands setup,make,install,start "+
                   "are performed (in that order) before stop,stopForce,tests" )
        if _DBG:
            traceback.print_exc()
        sys.stderr.flush()
        os.chdir(curr_dir)
        sys.exit(1)
    os.chdir(curr_dir)
    sys.exit(0)

### MAIN
if __name__ == "__main__":
   main()
### end MAIN
