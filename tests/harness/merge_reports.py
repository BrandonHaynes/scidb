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
report_1 = "checkin/Report_1.xml"
report_2 = "testcases/Report_2.xml"
merged_report = "testcases/Merged_report.xml"

pass_count = 0
fail_count = 0
total = 0

def init_report():
	try:
		s_file = open(report_1,'r')
                d_file = open(merged_report,'w')
                line = s_file.readline()
                while not line.lstrip().startswith("<TestResults>"):
			d_file.write(line)
			line = s_file.readline()
		d_file.write(line)
		s_file.close()
		d_file.close()
	except Exception, inst:
                print "     Exception Type: %s" % type(inst)     # the exception instance


def read_results(fname):
	global pass_count
	global fail_count
	global total
	try:
		s_file = open(fname,'r')
		d_file = open(merged_report,'a')
		line = s_file.readline()
		while not line.lstrip().startswith("<TestResults>"):
			line = s_file.readline()
		line = s_file.readline()
		while not line.lstrip().startswith("</TestResults>"):
			d_file.write(line)
			line = s_file.readline()
		while not line.lstrip().startswith("<TotalTestCases>"):
			line = s_file.readline()
		total = total + int(line.split('>')[1].split('<')[0])
		line = s_file.readline()
		pass_count = pass_count + int(line.split('>')[1].split('<')[0])
                line = s_file.readline()
		fail_count = fail_count + int(line.split('>')[1].split('<')[0])
		s_file.close()
		d_file.close()
	except Exception, inst:
                print "     Exception Type: %s" % type(inst)     # the exception instance


def complete_report():
	try:
                d_file = open(merged_report,'a')
		d_file.write('\n</TestResults>\
			\n<FinalStats>\
			\n<TotalTestCases>%d</TotalTestCases>\
			\n<TotalTestsPassed>%d</TotalTestsPassed>\
			\n<TotalTestsFailed>%d</TotalTestsFailed>\
			\n<TotalTestsSkipped>0</TotalTestsSkipped>\
			\n<TotalSuitesSkipped>0</TotalSuitesSkipped>\
			\n</FinalStats>\
			\n</SciDBTestReport>\
			\n</boost_serialization>\n' % (total,pass_count,fail_count))
		d_file.close()
	except Exception, inst:
                print "     Exception Type: %s" % type(inst)     # the exception instance


if __name__ == "__main__":
        print "Merging reports..."
	init_report()
	read_results(report_1)
	read_results(report_2)
	complete_report()
        print "Reports have been merged."
