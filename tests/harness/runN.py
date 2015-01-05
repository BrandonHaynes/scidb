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
#
# Load the data and run all the test scripts
#

from glob import glob
import subprocess
import sys
import time
import os
import string
import re
import argparse

# Just for sanity. You could probably run more if you wanted to
MAX_SUPPORTED_INSTANCES = 64

MODE_INIT_START = 0
MODE_START = 1
MODE_CREATE_LOAD = 2
MODE_INIT_START_TEST = 3
MODE_INIT = 4

mode = MODE_INIT_START_TEST

# Number of instances
numInstances = 4
# Name of this scidb instance.
instanceName = "mydb"
# Tests to run
# Running all checkin tests
# Port number (currently used for iquery)
portNumber = "1239"

args = sys.argv
harnessArgs = []
runNpyArgs = args[1:]
if '--harness-args' in args:
  harnessArgs = args[args.index('--harness-args')+1:]
  runNpyArgs = args[1:args.index('--harness-args')]

parser = argparse.ArgumentParser(
  formatter_class=argparse.RawDescriptionHelpFormatter,
  epilog='harness arguments:\n  --harness-args [argument [argument ...]]\n')
parser.add_argument('numInstances', metavar='instances number', type=int, nargs='?', help='number of instances')
parser.add_argument('instanceName', metavar='instance name', type=str, nargs='?', help='instance name')
groupMode = parser.add_mutually_exclusive_group()
groupMode.add_argument('--istart', dest='mode', action='store_const', const=MODE_INIT_START, help='initialize and start cluster')
groupMode.add_argument('--start', dest='mode', action='store_const', const=MODE_START, help='start cluster')
groupMode.add_argument('--create-load', dest='mode', action='store_const', const=MODE_CREATE_LOAD, help='launch createload.test')
groupMode.add_argument('--init', dest='mode', action='store_const', const=MODE_INIT, help='initialize cluster')
parser.add_argument('--port', dest='portNumber', type=int, nargs=1, help='SciDB port')
parser.add_argument('--force', dest='forceRun', action='store_true', help='Force running tests on any SciDB build')

parser.set_defaults(
  mode=mode,
  numInstances=numInstances,
  instanceName=instanceName,
  portNumber=portNumber)
args = vars(parser.parse_args(runNpyArgs))

numInstances = args['numInstances']
instanceName = args['instanceName']
mode = args['mode']
port = args['portNumber']
forceRun = args['forceRun']

if ( numInstances < 1 or numInstances > MAX_SUPPORTED_INSTANCES ):
  print "Invalid <numInstances>:", numInstances, "must be between 1 and", MAX_SUPPORTED_INSTANCES
  sys.exit(1)

if numInstances > 1:
  redundancyArgs = ["--redundancy", "1"]
else:
  redundancyArgs = []

numInstances = numInstances - 1
basepath = os.path.realpath(os.path.dirname(sys.argv[0])) 
binpath = basepath + "/../../bin"
meta_file = binpath + "/data/meta.sql"
plugins_dir = binpath + "/plugins"
storage_dir = os.environ.get('SCIDB_STORAGE', binpath)
pidfile = "/tmp/runN.pids"
test_suite = os.environ.get('TEST_SUITE', "checkin")

storage_file = storage_dir + '/storage.scidb'
storage_files = []
for i in range (0, numInstances):
  storage_files.append(storage_dir + "/instance" + str(i+1) + "/storage.scidb")

instancePaths = []
for i in range (0, numInstances):
  instancePaths.append(binpath + "/instance" + str(i+1))

print "DB instance name:", instanceName, "with root +", numInstances, "instances."

# Connection string, for now derive it from the instanceName. 
connstr="host=localhost port=5432 dbname=" + instanceName +" user=" + instanceName + " password=" + instanceName

# Cleanup and initialize
def cleanup(path, storage_path): 
  print "Cleaning up old logs and storage files."
  subprocess.Popen(["rm", "-f", "scidb.log"], cwd=path).wait()
  subprocess.Popen(["rm", "-f", "init.log"], cwd=path).wait()
  subprocess.Popen(["rm", "-f", storage_path + "/storage.header", storage_path + "/storage.cfg", storage_path + "/storage.scidb", storage_path + "/storage.log_1",
            storage_path + "/storage.log_2", storage_path + "/storage.data1"], cwd=path).wait()  

def master_init():
  cleanup(binpath, storage_dir)
  subprocess.Popen(["mkdir", storage_dir]).wait()
  print "Initializing catalog..."
  subprocess.Popen(["sudo", "-u", "postgres", "./init-db.sh", instanceName, instanceName, instanceName, meta_file], 
                   cwd=binpath, stdout=open("init-stdout.log","w"), stderr=open("init-stderr.log","w")).wait()
  subprocess.Popen([binpath + "/scidb",
                    "-p", str(portNumber),
                    "--storage", storage_file, "--plugins", plugins_dir,
                    "-rc", connstr, "--initialize"], cwd=binpath).wait()

def instances_init():
  for i in range (0, numInstances):
    print "Initializing instance ", i + 1, "..."
    subprocess.Popen(["rm", "-rf", instancePaths[i]]).wait()
    subprocess.Popen(["mkdir", instancePaths[i]]).wait()
    if storage_dir != binpath:
        instance_storage = storage_dir + "/instance" + str(i+1)
        subprocess.Popen(["rm", "-rf", instance_storage]).wait()
        subprocess.Popen(["mkdir", instance_storage]).wait()
    subprocess.Popen([binpath + "/scidb",
                      "-p", str(int(portNumber)+i+1),
                      "--storage", storage_files[i], "--plugins", plugins_dir,
                      "-rc", connstr], cwd=instancePaths[i]).wait()

# Start the single instance server. 
def getArgs(path,options,storage,port,argsOut):
  valgrindArgList = ["valgrind", "--num-callers=50", "--track-origins=yes",
#                     "--leak-check=full",
#                     "--leak-resolution=high",
#                     "--show-reachable=yes",
                     "-v", "--log-file=" + path + "/valgrind.log"]
  argList = [binpath + "/scidb", "-p", port,
             "-m", "64",
             "--storage", storage,
             "-l", binpath + "/log1.properties",
             "--merge-sort-buffer", "64",
             "--plugins", plugins_dir,
             "-" + options + "c", connstr]
  argList.extend(redundancyArgs)

  rle_arg = []
  if os.environ.get('USE_RLE', '0') == '1':
    rle_arg = ["--rle-chunk-format", "1"]

  argList.extend(rle_arg)

  prefetch_queue_size_arg = []
  prefetch_queue_size = os.environ.get('PREFETCH_QUEUE_SIZE', None)
  if not (prefetch_queue_size is None):
    prefetch_queue_size_arg = ["--prefetch-queue-size=" + prefetch_queue_size ]
  
  argList.extend(prefetch_queue_size_arg)

  tile_size_arg = []
  tile_size = os.environ.get('TILE_SIZE', None)
  if not (tile_size is None):
    tile_size_arg = ["--tile-size=" + tile_size ]

  argList.extend(tile_size_arg)

  if os.environ.get('USE_VALGRIND', '0') == '1':
    argList.extend(["--no-watchdog", "True"])
    valgrindArgList.extend(argList)
    argList = valgrindArgList
    if dlaRunner != '0':
      print "Using valgrind is not supported for DLA jobs"
      sys.exit(1)
  argsOut.extend(argList)

def start(path,argList,my_env=os.environ):
  if os.environ.get('USE_VALGRIND', '0') == '1':
    print "Starting SciDB server under valgrind..."
  else:
    print "Starting SciDB server..."
  p = subprocess.Popen(argList, cwd=path, env=my_env,
                       stdout=open(path + "/scidb-stdout.log","w"),
                       stderr=open(path + "/scidb-stderr.log","w"))
  return p

def shutdown(p):
  p.terminate()

def get_children(pid):
  ps = subprocess.Popen(["ps", "-o", "pid", "--no-headers", "--ppid", str(pid)], stdout=subprocess.PIPE, cwd=".")
  ps.wait()
  out, _ = ps.communicate()
  result = filter(len, map(string.strip, out.split(os.linesep)))
  return result

def check_pidfile():
  RETRY_COUNT = int(os.environ.get('SCIDB_KILL_TIMEOUT', 5))
  if (os.path.exists(pidfile)):
    def get_prog_name(pid):
      result = None
      # proc file name
      pfn = "/proc/%s/stat" % pid
      if os.path.exists(pfn):
        # proc file
        with open(pfn, 'r') as pf:
          result = string.split(pf.readline())[1]
          pf.close()
      return result

    def is_actual_watchdog(pid):
      prog_name = get_prog_name(pid)
      if prog_name is None:
        return False
      else:
        return prog_name == "(scidb)" or (dlaRunner != '0' and prog_name== "(%s)" % dlaRunner)

    watchdog_list = filter(is_actual_watchdog, map(string.strip, open(pidfile, 'r')))

    def get_children_list(watchdog_list):
      child_list = []
      for watchdog_pid, item_list in zip(watchdog_list, map(get_children, watchdog_list)):
        print 'Watchdog pid: %s  children: [%s]' % (watchdog_pid, ', '.join(item_list))
        for item in item_list:
          child_list.append(item)
      return child_list

    child_list = get_children_list(watchdog_list)

    def kill(pid_list, signal):
      killer_list = []
      for pid in pid_list:
        print "NOTE: killing old scidb instance at PID %s by %s signal" % (pid, signal)
        killer_list.append(subprocess.Popen(["kill", signal, pid], cwd="."))

      time.sleep(1)

      for killer in killer_list:
        killer.wait()

    def wait(pid_list, condition, message):
      retry = RETRY_COUNT
      pid_list = filter(condition, pid_list)
      while retry > 0 and len(pid_list):
        print "%s, pid list is [%s]" % (message, ",".join(pid_list))
        time.sleep(1)
        pid_list = filter(condition, pid_list)
        retry -= 1
      return pid_list

    def kill_and_wait(pid_list, signal, condition, message):
      kill(pid_list, signal)
      return wait(pid_list, condition, message)

    watchdog_list = kill_and_wait(watchdog_list,
                                  "-SIGTERM",
                                  is_actual_watchdog,
                                  "Waiting while SciDB processes are completing")
    if len(watchdog_list):
      watchdog_list = kill_and_wait(watchdog_list,
                                    "-SIGKILL",
                                    is_actual_watchdog,
                                    "Waiting while SciDB processes are completing")
      if len(watchdog_list):
        sys.stderr.write('Some SciDB processes still runing: [%s]\n' % (','.join(watchdog_list)))
        exit(-1)

    def is_actual_pid(pid):
      ps = subprocess.Popen(["ps", "ax", "-o", "pid", "--no-headers"],
                            stdout=subprocess.PIPE,
                            cwd=".")
      ps.wait()
      out, _ = ps.communicate()
      pid_list = filter(len, map(string.strip, out.split(os.linesep)))
      return pid in pid_list

    child_list = wait(child_list, is_actual_pid, "Waiting while SciDB child processes are completing")

    child_list = kill_and_wait(child_list,
                                  "-SIGTERM",
                                  is_actual_pid,
                                  "Waiting while SciDB child processes are completing")
    if len(child_list):
      child_list = kill_and_wait(child_list,
                                    "-SIGKILL",
                                    is_actual_pid,
                                    "Waiting while SciDB child processes are completing")
      if len(child_list):
          sys.stderr.write('Some SciDB child processes still runing: [%s]\n' % (','.join(child_list)))
          exit(-1)

def savepids(master_pid, instancePids):
  subprocess.Popen(["rm", "-f", pidfile], cwd=".").wait()
  f = open(pidfile, 'w')
  f.write(str(master_pid.pid))
  print "Master pid: %s children: [%s]" % (master_pid.pid, ', '.join(get_children(master_pid.pid)))
  f.write("\n")
  for popen in instancePids:
    f.write(str(popen.pid))
    print "instance pid: %s children: [%s]" % (popen.pid, ', '.join(get_children(popen.pid)))
    f.write("\n")
  f.close()

def run_unit_tests():
  ut_path = basepath + "/../unit"
  res = subprocess.Popen([ut_path + "/unit_tests", "-c", connstr], cwd=".", stderr=open("/dev/null","w"), stdout=open("/dev/null","w")).wait()
  if (res != 0):
    bad_results.append ( "UNIT_TESTS" )

def scidb_buildtype():
  ps = subprocess.Popen([binpath + "/scidb", "--version"], stdout=subprocess.PIPE)
  ps.wait()
  out, _ = ps.communicate()
  if ps.returncode != 0:
    print "Error when running 'scidb --version'"
    sys.exit(1)
  
  m = re.search("Build Type: (.*)", out)
  if not m:
    print "Can't parse output of 'scidb --version'"
    sys.exit(1)
  return m.group(1)

if __name__ == "__main__":
  os.chdir(basepath)

  if scidb_buildtype() != "Debug" and not forceRun:
    print "Please use Debug version of SciDB for testing! To force running use key --force"
    exit()

  dlaRunner = os.environ.get('RUN_DLA', '0')
  check_pidfile()

  if mode in (MODE_INIT_START, MODE_INIT_START_TEST, MODE_CREATE_LOAD, MODE_INIT): 
    master_init()
    instances_init()

  if mode == MODE_INIT:
    sys.exit(0)

  my_env=os.environ
  if "LD_LIBRARY_PATH" in my_env:
    my_env["LD_LIBRARY_PATH"] = binpath +":"+ my_env["LD_LIBRARY_PATH"]
  else:
    my_env["LD_LIBRARY_PATH"] = binpath
  my_env["LD_LIBRARY_PATH"] = plugins_dir +":"+ my_env["LD_LIBRARY_PATH"]
  instancePids =[]
  master_pid=-1
  if dlaRunner == '0':
    args=[]
    getArgs(binpath, "k", storage_file, portNumber, args)
    master_pid = start(binpath, args, my_env)

    for i in range (0, numInstances):
      args=[]
      print "debug: instancePaths[i] is " + instancePaths[i]
      getArgs(instancePaths[i], "", storage_files[i], str(int(portNumber)+i+1), args)
      worker_pid = start(instancePaths[i], args, my_env)
      instancePids.append(worker_pid)
      if os.environ.get('USE_VALGRIND', '0') == '1':
        time.sleep(10*(numInstances+1))
  else:
    args=[dlaRunner, "-np", str(1), "-wd", str(binpath), "--output-filename", "./scidb-stderr.log"]
    getArgs(binpath, "k", storage_file, portNumber, args)

    for i in range (0, numInstances):
      args.extend([":", "-np", str(1), "-wd", str(instancePaths[i]), "--output-filename", "./scidb-stderr.log"])
      getArgs(instancePaths[i], "", storage_files[i], str(int(portNumber)+i+1), args)
    master_pid = start(binpath, args, my_env)
  time.sleep(3)
  savepids(master_pid, instancePids)

  # wait for instance to come on-line
  while True:
    p = subprocess.Popen([binpath + "/iquery", "-p", str(portNumber), "-naq", "list()", portNumber],
                         stderr=open("/dev/null","w"), stdout=open("/dev/null","w")).wait()
    if p == 0 :
      break;
    time.sleep(1)

  if ( mode != MODE_INIT_START_TEST ):
    if mode == MODE_CREATE_LOAD:
	os.environ["PATH"] = binpath + ":" + os.environ["PATH"]
	os.environ["IQUERY_HOST"] = 'localhost'
	os.environ["IQUERY_PORT"] = portNumber
	subprocess.Popen([binpath + "/scidbtestharness", "--plugins",
                          plugins_dir, "--root-dir", "createload",
                          "--test-name", "createload.test",
                          "--debug", "5", "--port", portNumber] + harnessArgs,
                         cwd=".", env=os.environ).wait()
    sys.exit(0)
        
  # Add environment variables
  os.environ["PATH"] = binpath + ":" + os.environ["PATH"]
  os.environ["IQUERY_HOST"] = 'localhost'
  os.environ["IQUERY_PORT"] = portNumber
  subprocess.Popen([binpath + "/scidbtestharness", "--plugins", plugins_dir,
                    "--root-dir", "testcases", "--suite-id", test_suite,
                    "--debug", "5", "--port", portNumber] + harnessArgs,
                   cwd=".", env=os.environ).wait()
  sys.exit(0)
