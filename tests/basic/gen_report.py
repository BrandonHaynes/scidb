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
# Check test results and update report

from glob import glob
import subprocess
import sys
import time
import os
import string

# list of directories to be processed for test report
test_dirs = ["load", "other", "other/anonymous_schema", "negative", "expression", "update", "newaql", "newaql/select_into", "newaql_neg", "aggregate"]

report_path = "../harness/testcases/Report.xml"

pass_count = 0
fail_count = 0

def check_result(dname):
  global pass_count
  global fail_count
  aql_names = glob(dname + "/*.aql")
  aql_names.sort()
  afl_names = glob(dname + "/*.afl")
  afl_names.sort()
  names = aql_names + afl_names
  for fname in names:
    try:
      if os.path.exists(fname+".diff"):
        os.remove(fname+".diff")
      res = 1;
      try:
        open(fname + ".tmp")
        if not os.path.exists(fname+".res") and not os.path.exists(fname+".err"):
           update_report(fname,'FAIL','positive','Expected result file Not found.')
           continue
        if os.path.exists(fname+".res"):
           res = subprocess.Popen(["diff", "-w", fname + ".res", fname + ".tmp"]).wait()
        else:
           res = 1
      except:
        if os.path.exists(fname+".err") and os.path.getsize(fname+".err"):
           raise Exception()
      if (res != 0):
        fail_count = fail_count + 1
        dfile=open(fname+".diff","w")
        freason = ''
        tcase = 'positive'
        if os.path.exists(fname+".tmp") and os.path.exists(fname+".res"):
           res = subprocess.Popen(["diff", "-w", fname + ".res", fname + ".tmp"],stdout=dfile).wait()
           freason = 'Expected output and Actual output differ.'
        elif os.path.exists(fname+".stderr") and not os.path.getsize(fname+".stderr"):
           res = subprocess.Popen(["diff", "-w", fname + ".err", fname + ".tmp"],stdout=dfile).wait()
           freason = 'Negative testcase passed.'
           tcase = 'negative'
        else:
           res = subprocess.Popen(["diff", "-w", fname + ".res", fname + ".stderr"],stdout=dfile).wait()
           freason = 'Query execution failed.'
        dfile.close()
        update_report(fname,'FAIL',tcase,freason)
      else:
	pass_count = pass_count + 1
        update_report(fname,'PASS','positive','')
    except:
      expected_errfile = fname + ".err"
      test_errfile = fname + ".stderr"

      if not os.path.exists(test_errfile):
        test_errfile = "/dev/null"

      #Ignore the "Error query id:" line when comparing files
      res = subprocess.Popen(["diff", "-I", "Error query id:", "-w", expected_errfile, test_errfile]).wait()
      if (res != 0 or not os.path.exists(expected_errfile)):
	fail_count = fail_count + 1
        dfile=open(fname+".diff","w")
        res = subprocess.Popen(["diff", "-I", "Error query id:", "-w", expected_errfile, test_errfile],stdout=dfile).wait()
        dfile.close()
        update_report(fname,'FAIL','negative','Check .diff file')
      else:
	pass_count = pass_count + 1
        update_report(fname,'PASS','negative','')


def check_create():
	global fail_count
	stderr_names = glob("create/*.stderr")
	stderr_names.sort()
	for fname in stderr_names:
		if os.path.getsize(fname):
			update_report(fname,'FAIL','create','Array creation failed.')


def create_report():
        try:
		os.rename(report_path,report_path+'_old.xml')
		s_file=open(report_path+'_old.xml','r')
                rep_file=open(report_path, 'w')
		for line in s_file:
			if line.lstrip().startswith("</TestResults>"):
				rep_file.close()
				check_create()
				for t in test_dirs:
	                		print "processing "+t
                			check_result(t)
				rep_file=open(report_path, 'a')
			elif line.lstrip().startswith("<TotalTestCases>"):
				p_count=int(line.split('>')[1].split('<')[0])
				line='<TotalTestCases>%d</TotalTestCases>\n' % (p_count+pass_count+fail_count)
			elif line.lstrip().startswith("<TotalTestsPassed>"):
				p_count=int(line.split('>')[1].split('<')[0])
				line='<TotalTestsPassed>%d</TotalTestsPassed>\n' % (p_count+pass_count)
                        elif line.lstrip().startswith("<TotalTestsFailed>"):
                                p_count=int(line.split('>')[1].split('<')[0])
                                line='<TotalTestsFailed>%d</TotalTestsFailed>\n' % (p_count+fail_count)
 			rep_file.write(line)
		s_file.close()
                rep_file.close()
        except Exception, inst:
                print "     Exception Type: %s" % type(inst)     # the exception instance


def update_report(cfname,cres,tcase,fail_reason):
        diff_file = ''
        if os.path.exists(cfname+".diff") and os.path.getsize(cfname+".diff"):
                diff_file = cfname + '.diff'

        if tcase == 'positive':
                exp_file = cfname + '.res'
                act_file = cfname + '.tmp'
	elif tcase == 'create':
		cfname = cfname.replace('.stderr','')
		exp_file = ''
		act_file = cfname + '.stderr'
        else:
                exp_file = cfname + '.err'
                act_file = cfname + '.stderr'
        if fail_reason == 'Negative testcase passed.':
                act_file = cfname + '.tmp'

        if not (os.path.exists(exp_file) and os.path.getsize(exp_file)):
                   exp_file = ''
        if not (os.path.exists(act_file) and os.path.getsize(act_file)):
                   act_file = ''

        try:
                rep_file=open(report_path, 'a')
                rep_file.write('\n<IndividualTestResult>\
                \n<TestID>'+cfname+'</TestID>\
                \n<TestDescription></TestDescription>\
                \n<TestStartTime></TestStartTime>\
                \n<TestEndTime></TestEndTime>\
                \n<TestTotalExeTime>1</TestTotalExeTime>\
                \n<TestcaseFile>'+cfname+'</TestcaseFile>\
                \n<TestcaseExpectedResultFile>'+exp_file+'</TestcaseExpectedResultFile>\
                \n<TestcaseActualResultFile>'+act_file+'</TestcaseActualResultFile>\
                \n<TestcaseTimerFile></TestcaseTimerFile>\
                \n<TestcaseDiffFile>'+diff_file+'</TestcaseDiffFile>\
                \n<TestcaseResult>'+cres+'</TestcaseResult>\
                \n<TestcaseFailureReason>'+fail_reason+'</TestcaseFailureReason>\
                \n<TestcaseLogFile></TestcaseLogFile>\
                \n</IndividualTestResult>\n')
                rep_file.close()
        except Exception, inst:
                print "     Exception Type: %s" % type(inst)     # the exception instance


if __name__ == "__main__":
	print "Updating test report..."
	create_report()
	print "Report has been updated."

