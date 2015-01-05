#!/usr/bin/env python
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

# This script is for running multiple instances of scidbtestharness simultaneously
#
# Present features:
# * no need to manually start/stop the daemon
# * start a single TEST instance at a time by passing parameters
# * use config file for specifying multiple instances at one go
# * kill/stop/pause/resume individual/all tests
# * check status of running tests
# * option to specify looping of TESTs (duration, or no. of times, or infinite)
# * option to specify whether to save output of failed tests
# * default options can be specified in the DEFAULT section in the config file
# * status displays 'probably hung' if a test is taking too long. The threshold duration is configurable using the variable 'secondsOld'
# * check SciDB server liveliness and kill TEST suite if the server or one of its instances is offline.
#

import subprocess
import ConfigParser
import datetime
import signal
import pickle
import pwd
import sys
import os
import time
import shlex

if sys.version_info < (2,4):
        print "Please use Python 2.4 or greater"
        sys.exit(1)

def rpartition(mystr,mysep):		# custom function for str.rpartition()
	mylist = mystr.split(mysep)
	lside = ''
	for i in range(len(mylist)-1):
	  if mylist[i] != '':
		lside = lside + mysep + mylist[i]
	mside = mysep
	rside = mylist[-1]
	return([lside,mside,rside])

# import daemon.py script from the utils folder
daemonpath = rpartition(os.getcwd(),os.sep)[0]+os.sep+'utils'
if daemonpath not in sys.path:
	sys.path.append(daemonpath)

script_path = os.path.dirname(sys.argv[0])
script_path = os.path.dirname(script_path) # Up one level.
etcpath = script_path + "/etc"
if etcpath not in sys.path:
        sys.path.append(etcpath)


try:
	from daemon import Daemon
except ImportError:
	print "Could not find script daemon.py!"
	sys.exit(1)


LOG_LEVEL = 1				# default log level for verbosity (1-5)

config_file = etcpath + '/mu_config.ini'  # default config.ini file to be used

process_list = []			# list of process handles of running instances
dict_harness = {}			# dictionary to store info of TESTs
stat_file = "/tmp/MU_stat.dat"          # file to store info about TESTS
HARNESS_CMD = "scidbtestharness"	# path for harness command
D_TIMER = 5                           	# sleep timer duration in seconds
st_file_path = "/tmp/"			# parent path for creation of log files for TESTs
D_pidfile = "/tmp/mu_d.pid"		# file to store pid of daemon process
ltime = time.asctime().replace(' ','_')
D_stdout = st_file_path+"mu_test_output_"+ltime+"_.log"		# file to log stdout of daemon process
D_stderr = st_file_path+"mu_test_errors_"+ltime+"_.log"		# file to log stderr of daemon process
D_stop_count = 6
D_autostart_timer = 2			# timer for completing auto-start housekeeping
secondsOld = 60*2			# threshold to check if a TEST is hung/stuck/too slow


# Structure of stat_dict: (used to store info of individual TESTs)
#
# tname : [0-pid, 1-user, 2-rootdir, 3-suites, 4-port, 5-stime, 6-duration, 7-loop, 8-status, 9-saveresults, 10-conffilename, 11-logpath]


if not os.path.exists(st_file_path):
	print "Path: ",st_file_path," does not exist. Please specify valid 'st_file_path' in the ",sys.argv[0]," script."
	sys.exit(1)


def run_cmd(cmd):
        ret = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, close_fds=True)
        return ret.stdout.read()


class Mu_Daemon(Daemon):

	def log_it(self,msg,level):
	  if LOG_LEVEL >= level:
	    print msg
	    sys.stdout.flush()


	def istart(self):		# Start the deamon service if not started
	  self.log_it("in istart()",5)
	  try:
	    pf = file(self.pidfile,'r')
	    pid = int(pf.read().strip())
	    pf.close()
	  except IOError:
	    pid = None

	  if (not pid) or (not os.path.exists("/proc/" + str(pid))):				# start the daemon if not already running
	    print "starting MU daemon"
	    if os.path.exists(stat_file):
	      self.log_it("removing stat file in istart",5)
	      os.remove(stat_file)
	    self.daemonize()
	    self.run()


	# check if SciDB server is up and running
	def server_status(self,port):
	  self.log_it('in server_status()',5)
	  try:
	    op = subprocess.run_cmd('iquery -p ' + port + ' -aq "list(\'libraries\')"')
	    self.log_it('Checking if system library is loaded',3)
	    if op.find('libsystem.so') == -1:
	      self.log_it('Trying to load system library',3)
	      subprocess.run_cmd('iquery -p ' + port + ' -aq "load_library(\'system\')"')
	      time.sleep(10)

	    try:
	      self.log_it('checking server liveliness',3)
	      op = subprocess.run_cmd('iquery -p ' + port + ' -aq "list(\'instances\')"')

	      # SciDB returns 'error' for a few seconds until offline state of instances is detected.
	      if ((op.find('error') >= 0) or (op.find('offline') >= 0)):
		self.log_it('Atleast one of the SciDB instances is offline.',1)
		return 'offline'
	    except:
	      self.log_it('Atleast one of the SciDB instances is offline.',1)
	  except:
	    self.log_it('Cannot load \'system\' library!',1)
	    return 'no-system-lib'
	  return 'online'

	# write to stat_file
	def write_to_stat_file(self):
	  self.log_it("in write_to_stat_file()",5)
	  fp = open(stat_file,'w')
	  pickle.dump(dict_harness,fp)
	  fp.close()


        # submit TEST to stat_file
	# pid is set to -1 to indicate that TEST is yet to be started
        def submit_test(self,currD):
	  self.log_it("in submit_test()",5)
          global dict_harness
          self.read_stat_file()
          if not dict_harness.has_key(currD['tname']):
            dict_harness[currD['tname']] = [-1,currD['user'],currD['rootdir'],currD['suites'],currD['port'],currD['stime'],currD['duration'],currD['loop'],'starting',currD['saveresults'],currD['conffilename'],currD['logpath']]
            self.update_stats()
	    print "added TEST %s to list" % currD['tname']
          else:
            print "Cannot use TEST name " + currD['tname'] + " again"


	def check_options(self,D): # Return list of options for which value was not found
	  options_list = ['user', 'rootdir', 'suites', 'port', 'duration', 'loop', 'saveresults', 'logpath']
	  incomplete_list = []
	  for op in options_list:
	    if op not in D.keys():
	      incomplete_list.append(op)
	  self.log_it(["total options = ",len(incomplete_list)],5)
	  return incomplete_list


	# parse .ini file
	def parse_config(self,cfile):
	  self.log_it("in parse_config()",5)
	  config = ConfigParser.RawConfigParser()
	  if not os.path.exists(cfile):
		print "file %s does not exists." % cfile
		sys.stdout.flush()
		sys.exit(1)
	  self.log_it("reading config file: %s" % cfile,1)
	  config.read(cfile)
	  tnames = config.sections()
	  d = {}
	  self.log_it("total sections= %d" % len(tnames),3)
	  for section_name in tnames:
		try:
		  d.clear()
		  for (key,value) in config.items(section_name):
			d[key] = value
		  if len(d) < 8:
			print "Options not found for TEST ",section_name,": ",self.check_options(d)
			continue
		  if not os.path.exists(os.path.abspath(d['rootdir'])):
		    print "Invalid rootdir for",section_name,":",d['rootdir']
		    continue
		  d['tname'] = section_name
		  d['stime'] = datetime.datetime.now()
		  d['conffilename'] = cfile
		  self.submit_test(d)
		except:
		  sys.stderr.write(sys.exc_info())
		  sys.stderr.flush()
		  sys.exit(1)


	# get process state
	def get_pstate(self,pid):
	  self.log_it("in get_pstate()",5)
	  pstate = '-'
	  if os.path.exists("/proc/" + str(pid)):
	    pfile = open("/proc/" + str(pid) + "/stat","r")
	    pstate = pfile.readline().split()[2]
	    pfile.close()
	  self.log_it(["pstate = ",pstate],5)
	  return pstate


	# check if the processes are still running and update the list accordingly
	def check_processes(self):
	  self.log_it("in check_processes()",5)
	  global dict_harness
	  dlist = []
	  for k,v in dict_harness.iteritems():
		self.log_it("for TEST %s" % k,3)
		if v[0] == -1:
		  self.log_it("pid=-1",3)
		  continue
		if v[8] in ['killing', 'pausing', 'resuming']:
		  continue
		if not os.path.exists("/proc/" + str(v[0])):
		  self.log_it("execution completed",3)
		  dlist.append(k)
		else:
		  pstate = self.get_pstate(str(v[0]))
		  if pstate == 'Z':	 # check if process turned zombie
		    self.log_it("Zombie process detected (pid=%d)" % v[0],3)
		    dlist.append(k)
	  for e in dlist:
		dict_harness[e][8] = 'done'    # update status as 'done'


	# update the process information in the stat_file
	def update_stats(self):
	  self.log_it("in update_stats()",5)
	  self.check_processes()
	  self.log_it("writing stat file (%d entries)" % len(dict_harness),5)
	  self.log_it(time.asctime(),5)
	  self.write_to_stat_file()


	# read process information from the stat_file
	def read_stat_file(self):
	  self.log_it("in read_stat_file()",5)
	  global dict_harness
	  sys.stdout.flush()
	  if os.path.exists(stat_file):
		fp = open(stat_file,'r')
		dict_harness = pickle.load(fp)
		fp.close()
		self.log_it("read stat file (%d entries)" % len(dict_harness),5)
		self.log_it(time.asctime(),5)
		self.check_processes()
	  else:
		self.log_it("stat file %s does not exist" % stat_file,3)

	# start a new TEST
	def runHarness(self,currD):
	  self.log_it("in runHarness()",5)
	  global dict_harness
	  global process_list
	  self.read_stat_file()
	  try:
	      iname = 'TEST'
	      if currD['conffilename'] != '-':
		iname = rpartition(os.path.splitext(os.path.abspath(currD['conffilename']))[0],'/')[2]

	      log_path = os.path.abspath(currD['logpath'])+'/'
	      if not os.path.exists(log_path):
		print "logpath '"+log_path+"' does not exists!"
		print "using default logpath '"+st_file_path+"'."
		log_path = st_file_path
	      self.log_it(log_path+currD['user']+'_'+currD['tname']+"_"+iname+"_output_"+currD['stime'].ctime().replace(' ','_')+"_.log",5)
	      self.log_it("opening file to write",5)

	      fp = open(log_path+currD['user']+'_'+currD['tname']+"_"+iname+"_output_"+currD['stime'].ctime().replace(' ','_')+"_.log",'a')
	      fp.write(os.linesep+"=====================================================")
	      fp.write(os.linesep+"TEST "+currD['tname']+" started at: ")
	      fp.write(time.asctime())
	      fp.write(os.linesep+"====================================================="+os.linesep)
	      fp.close()
	      save_res = ''
	      _suites = ''

	      if currD['saveresults'] != 'no':
		save_res = ' --save-failures'
	      if currD['suites'] != 'all':
		_suites = ' --suite-id=' + currD['suites']

	      log_prop_path = 'log4j.properties'
	      if not os.path.exists(os.path.abspath(log_prop_path)):
	        log_prop_path = etcpath + '/log4j.properties'

	      self.log_it("going to create subprocess",5)
	      cmd_args = HARNESS_CMD+' --root-dir='+currD['rootdir']+_suites+' --port='+currD['port']+save_res+' --log-properties-file='+log_prop_path
	      sp = subprocess.Popen(shlex.split(cmd_args),stdout=open(log_path+currD['user']+'_'+currD['tname']+"_"+iname+"_output_"+currD['stime'].ctime().replace(' ','_')+"_.log",'a'),stderr=subprocess.STDOUT)

	      dict_harness[currD['tname']][0] = sp.pid
	      dict_harness[currD['tname']][8] = "running"
	      process_list.append([currD['tname'],sp])
	      self.write_to_stat_file()
	      self.log_it([currD['tname'],dict_harness[currD['tname']]," :started"],1)
	  except Exception, inst:
	      sys.stderr.write("Exception occurred while starting new TEST: %s" % currD['tname'])
	      self.log_it("Exception occurred while starting new TEST: %s" % currD['tname'],1)
	      self.log_it(type(inst),5)
	      self.log_it(inst,5)
	  self.update_stats()


	# STOP a running TEST
	def stopHarness(self,tname):
	  self.log_it("in stopHarness()",5)
	  self.read_stat_file()
	  if dict_harness.has_key(tname):
	    dict_harness[tname][7] = 'no'   # update loop to be 'no'
	    self.update_stats()
	    print "looping stopped for TEST: %s" % tname
	  else:
	    print "No Test found with name: " + tname

	# stop all tests
	def stopAll(self):
	  self.log_it("in stopAll()",5)
	  self.read_stat_file()
	  for t in dict_harness:
	    dict_harness[t][7] = 'no'
	  self.update_stats()


	# Kill/Pause/Resume specific test
	def controlTest(self,tname,tcmd):
	  self.log_it("in controlTest()",5)
	  global dict_harness
	  self.read_stat_file()
	  tok = ''
	  if tcmd == 'kill':
	    tok = 'killing'
	  elif tcmd == 'pause':
            tok = 'pausing'
	  elif tcmd == 'resume':
	    tok = 'resuming'
  
	  for k,v in dict_harness.items():
	    if tname == k:
	      try:
		pstate = self.get_pstate(str(v[0]))
		if (tcmd=='kill' and pstate!='-') or (tcmd=='pause' and pstate!='T') or (tcmd=='resume' and (pstate=='T' or pstate=='-')):
		  print "TEST %s marked for %s" % (tname,tok)
		  dict_harness[k][8] = tok
		  self.write_to_stat_file()
		else:
		  self.log_it("invalidated as pstate=%s and tcmd=%s" % (pstate,tcmd),3)
		  self.log_it("Please check status of TEST %s with pid=%d" % (tname,v[0]),3)
		  self.log_it("Cannot issue %s command" % tcmd,3)
	      except:
		print "Error %s TEST %s with pid %d" % (tok,tname,v[0])
	  sys.stdout.flush()


	# Kill/Pause/Resume all TESTs
	def controlAllTests(self,tcmd):
	  self.log_it("in controlAllTests()",5)
	  global dict_harness
          tok = ''
          if tcmd == 'kill':
            tok = 'killing'
          elif tcmd == 'pause':
            tok = 'pausing'
          elif tcmd == 'resume':
            tok = 'resuming'

	  self.read_stat_file()
	  test_list = []
	  tnames = []
	  for k,v in dict_harness.items():
	    test_list.append(v[0])
	    tnames.append(k)
	  self.log_it("total = %d" % len(test_list),5)
	  self.log_it(test_list,5)
	  p_len = len(test_list)
	  for i in range(len(test_list)):
	    try:
	      pstate = self.get_pstate(str(test_list[i]))
	      self.log_it("validating command for process",5)
	      if (tcmd=='kill' and pstate!='-') or (tcmd=='pause' and pstate!='T') or (tcmd=='resume' and (pstate=='T' or pstate=='-')):
		self.log_it("validated",5)
		self.log_it("pid=%d, cmd=%s" % (test_list[i],tcmd),5)
		dict_harness[tnames[i]][8] = tok
		self.write_to_stat_file()
	      else:
		self.log_it("invalidated as pstate=%s and cmd=%s" % (pstate,tcmd),3)
		self.log_it("Please check status of TEST %s with pid=%d" % (tnames[i],test_list[i]),3)
                self.log_it("Cannot issue %s command" % tcmd,3)
		p_len -= 1
	    except Exception, inst:
	      print inst
	      p_len -= 1
	      self.log_it("Error %s process with pid %d" % (tok,test_list[i]),1)
	  print "(%d / %d) TESTs marked for %s" % (p_len,len(test_list),tok)


	# check if the TEST is hung/stuck/too slow
	def test_hung(self,tname,v):
	  self.log_it("in test_hung()",5)
	  pid = v[0]
	  user = v[1]
	  stime = v[5]
	  conffilename = v[10]
	  logpath = v[11]+'/'
	  if pid == -1:
	    self.log_it("pid=-1",3)
	    return False
	  if not os.path.exists(os.path.abspath(logpath)):
	    self.log_it("logpath reinitialized",5)
	    logpath = st_file_path
	  if conffilename == '-':
	    conffilename = 'TEST'
	  else:
	    conffilename = rpartition(os.path.splitext(os.path.abspath(conffilename))[0],'/')[2]

	  self.log_it(["conffilename=",conffilename],5)
	  log_file = logpath+user+'_'+tname+"_"+conffilename+"_output_"+stime.ctime().replace(' ','_')+"_.log"
	  self.log_it(["logfile=",log_file],3)
	  file_mtime = datetime.datetime.fromtimestamp(os.path.getmtime(log_file))
	  self.log_it(["file_mtime=",file_mtime],5)
	  self.log_it(["secondsOld=",secondsOld],5)
	  self.log_it(["ctime=",datetime.datetime.now()],5)
	  self.log_it(datetime.datetime.now() - file_mtime,5)
	  self.log_it(datetime.timedelta(seconds=secondsOld),5)
	  if datetime.datetime.now() - file_mtime >= datetime.timedelta(seconds=secondsOld):
	    self.log_it("file mtime greater",3)
	    return True
	  self.log_it("file mtime smaller",5)
	  return False


	# show TESTs status
	def sys_state(self):
	  self.log_it("in sys_state()",5)
	  self.read_stat_file()
	  cstatus = ''
	  if len(dict_harness) == 0:
		print "No tests in progress"
	  else:
		print "TEST      user      duration   elapsed   loop   status"
		for k,v in dict_harness.iteritems():
		  tdiff = (datetime.datetime.now()-v[5]).seconds / 60
		  cstatus = v[8]
		  self.log_it("cstatus="+cstatus,5)
		  self.log_it(self.test_hung(k,v),5)
		  self.log_it(cstatus not in ['killed','paused'],5)
		  if self.test_hung(k,v) and (cstatus not in ['killed','paused']):
		    cstatus = 'probably hung'  # update status
		  print "%-9s %-9s %-3dmin     %-3dmin %7s   %s" % (k,v[1],int(v[6]),tdiff,v[7],cstatus)
	  self.update_stats()

	# get dictionary structure from list
	def get_d(self,k,v):
	  self.log_it("in get_d()",5)
	  D = {}
	  D['tname'] = k
	  D['pid'] = v[0]
          D['user'] = v[1]
          D['rootdir'] = v[2]
          D['suites'] = v[3]
          D['port'] = v[4]
          D['stime'] = v[5]
          D['duration'] = v[6]
          D['loop'] = v[7]
          D['saveresults'] = v[9]
	  D['conffilename'] = v[10]
	  D['logpath'] = v[11]
	  return D


	# check if TEST is already in the process_list[]
	def in_plist(self,k):
	  for p in process_list:
	    if p[0] == k:
	      return True
	  return False


	# main looping/iteration code
	def control_iterations(self):
	  self.log_it("in control_iterations()",5)
	  global dict_harness
	  self.log_it("going to read stat file in control_itr",1)
	  print time.asctime()
	  sys.stdout.flush()
	  self.read_stat_file()
	  if not (len(dict_harness) == 0):
	    self.log_it("dict_harness has %d entries" % len(dict_harness),3)
	    for k,v in dict_harness.items():
		  self.log_it("curr TEST name: %s" % k,1)
		  self.log_it(v,3)
		  if self.server_status(v[4]) == 'offline':
		    print "killing TEST from list as server is down.."
		    v[8]='killing'
		  if v[8] == 'killing':
		    if self.get_pstate(v[0]) != '-': 
		      self.log_it('sending kill signal to TEST '+k,3)
		      os.kill(v[0],signal.SIGKILL)
		    dict_harness.pop(k)
		    self.write_to_stat_file()
		    continue
		  elif v[8] == 'pausing':
		    if self.get_pstate(v[0]) == 'S':
		      self.log_it('sending pause signal to TEST '+k,3)
		      os.kill(v[0],signal.SIGSTOP)
		    else:
		      self.log_it('pausing completed TEST '+k,3)
		      dict_harness[k][0] = -1
		    dict_harness[k][8] = 'paused'
		    self.write_to_stat_file()
		    continue
		  elif v[8] == 'resuming':
		    if self.get_pstate(v[0]) == 'T':
		      self.log_it('sending resume signal to TEST '+k,3) 
		      os.kill(v[0],signal.SIGCONT)
		      dict_harness[k][8] = 'resumed'
		      self.write_to_stat_file()
		    elif not self.in_plist(k):
		      self.log_it('resuming completed TEST '+k,3)
		      self.runHarness(self.get_d(k,v))
		    else:				# should not reach here
		      self.log_it('ERROR: Invalid process state: command=resume, state='+self.get_pstate(v[0]),1)
		    continue


		  if v[0] == -1 and v[8] != 'paused':             	# new TEST to be started
		    self.log_it("submitting test",1)
		    self.runHarness(self.get_d(k,v))
 		  elif v[8] == 'done':       	# if iteration completed
		    self.log_it("iteration completed",1)
		    dict_harness.pop(k)
		    self.write_to_stat_file()

		    if v[7].isdigit():		# loop x times
		      self.log_it("loop %s times" % v[7],1)
		      if int(v[7]) <= 0:
			continue
		      self.log_it("looping...",1)
		      v[7] = str(int(v[7]) - 1)
		      self.submit_test(self.get_d(k,v))
		    elif v[7] == 'yes':         # check loop variable
		      self.log_it(v[6],2)
		      if int(v[6]) <= 0:	# loop indefinitely
			self.log_it("loop indefinitely",1)
			self.submit_test(self.get_d(k,v))
			continue
		      self.log_it("check loop duration",2)
		      ctime = datetime.datetime.now()
		      tdiff = ctime - v[5]   	# substract stime
		      self.log_it(v[5],3)
		      self.log_it(ctime,3)
		      self.log_it(int(v[6])*60,3)
		      self.log_it(tdiff.seconds,3)
		      if not (int(v[6])*60 <= tdiff.seconds):   # if duration not yet reached, then loop
			self.log_it("re-loop",1)
			self.submit_test(self.get_d(k,v))


	# daemon process code
	def run(self):
	  self.log_it("in run()",5)
	  global process_list
	  zero_run_count = 0
	  while True:
	    self.control_iterations()
	    process_list[:] = [p for p in process_list if p[1].poll() is None]
	    print "total processes : %d" % len(process_list)
	    sys.stdout.flush()
	    if len(process_list) == 0:
	      zero_run_count += 1
	    else:
	      zero_run_count = 0
	    if zero_run_count == D_stop_count:
	      if os.path.exists(self.pidfile):
	        os.remove(self.pidfile)
	      if os.path.exists(stat_file):
		self.log_it("removing stat file in self-stop",5)
		sys.stdout.flush()
		os.remove(stat_file)
	      sys.exit(0)
	    time.sleep(D_TIMER)


def usage():
  print ""
  print "Usage:"
#  print "%s start_d" % sys.argv[0]
#  print "%s stop_d" % sys.argv[0]
#  print "%s restart_d" % sys.argv[0]
  print "%s STATUS" % sys.argv[0]
  print "%s CONFIG [<config-file>]" % sys.argv[0]
  print "%s TEST <test-name> <user-name> <rootdir> <suites> <port> <duration> <loop> <saveresults> <logpath>" % sys.argv[0]
  print "%s STOP/KILL/PAUSE/RESUME <test-name>" % sys.argv[0]
  print "%s STOPALL/KILLALL/PAUSEALL/RESUMEALL" % sys.argv[0]


if __name__ == "__main__":

  mu_d = Mu_Daemon(D_pidfile,stdout=D_stdout,stderr=D_stderr)

  if len(sys.argv) < 2:
    usage()
  elif sys.argv[1] == 'TEST' and len(sys.argv) == 11:
    subprocess.Popen([sys.argv[0],'istart_d'])
    time.sleep(D_autostart_timer)    # sleep necessary for proper auto-start
    curD = {}
    curD['tname'] = sys.argv[2]
    curD['user'] = sys.argv[3]
    curD['rootdir'] = sys.argv[4]
    curD['suites'] = sys.argv[5]
    curD['port'] = sys.argv[6]
    curD['stime'] = datetime.datetime.now()
    curD['duration'] = sys.argv[7]
    curD['loop'] = sys.argv[8]
    curD['saveresults'] = sys.argv[9]
    curD['conffilename'] = '-'
    curD['logpath'] = sys.argv[10]
    if not os.path.exists(os.path.abspath(curD['rootdir'])):
      print "Invalid rootdir specified: ",curD['rootdir']
    else:
      mu_d.submit_test(curD)
  elif sys.argv[1] == 'STOP' and len(sys.argv) == 3:
    mu_d.stopHarness(sys.argv[2])
    print "Note - The stopped TEST will run to completion. This may take a while."
  elif sys.argv[1] == 'STOPALL':
    mu_d.stopAll()
    print "Note - currently executing suites will run to completion. This may take a while."
  elif sys.argv[1] == 'STATUS':
    mu_d.sys_state()
  elif sys.argv[1] == 'CONFIG':
    subprocess.Popen([sys.argv[0],'istart_d'])
    time.sleep(D_autostart_timer)     # sleep necessary for proper auto-start
    if len(sys.argv) == 2:
      mu_d.parse_config(config_file)
    else:
      mu_d.parse_config(sys.argv[2])
  elif sys.argv[1] == 'istart_d':
    mu_d.istart()
#  elif sys.argv[1] == 'start_d':
#	  print "starting %s daemon" % sys.argv[0]
#	  mu_d.start()
#  elif sys.argv[1] == 'stop_d':
#	  print "stopping %s daemon" % sys.argv[0]
#	  mu_d.stop()
#  elif sys.argv[1] == 'restart_d':
#	  print "restarting %s daemon" % sys.argv[0]
#	  mu_d.restart()
  elif sys.argv[1] == 'KILL' and len(sys.argv) == 3:
    mu_d.controlTest(sys.argv[2],'kill')
  elif sys.argv[1] == 'KILLALL':
    mu_d.controlAllTests('kill')
  elif sys.argv[1] == 'PAUSE' and len(sys.argv) == 3:
    mu_d.controlTest(sys.argv[2],'pause')
  elif sys.argv[1] == 'PAUSEALL':
    mu_d.controlAllTests('pause')
  elif sys.argv[1] == 'RESUME' and len(sys.argv) == 3:
    mu_d.controlTest(sys.argv[2],'resume')
  elif sys.argv[1] == 'RESUMEALL':
    mu_d.controlAllTests('resume')
  else:
    usage()

