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
# Generate testcase files for test harness from outputs of dense_matrix_tests.py and sparse_matrix_tests.py

import os
import string

# path to store the generated .test files
tests_path = "new_tests"

# list of files to be processed for generating testcases for test harness
input_files = ["dense_matrix_out.txt", "sparse_matrix_out.txt"]
debug_file = "d.txt"
failed_tests = []
dicts = {}

def create_test():
	global test_id
	global dicts
	commands = []
	global failed_tests
	prev_test_id = ""
	try:
	   for i_file in input_files:
		print "file: " + i_file
		file_handle = open(i_file,'r')
		for line in file_handle:
			logg("line : " + line)
			if line.lstrip().startswith("FAIL: Test"):
				failed_tests.append(prev_test_id)
			if line.lstrip().startswith("iquery -q"):
				commands.append('aql,'+line.split('\'')[1])
				logg("line is aql")
			elif line.lstrip().startswith("iquery -a"):
				commands.append('afl,'+line.split('\'')[1])
				logg("line is afl")
			elif line.lstrip().startswith("Error code:"):
				logg("line is error")
				e_code = line.partition('(')[0].replace('Error code:','').strip()
				commands[len(commands)-1] = "--error --code=" + e_code + "," + commands[len(commands)-1]
			elif line.lstrip().startswith("Test") or line.lstrip().startswith("FAIL: Test"):
				logg("line is test")
				#my_key = line.split('(')[0].replace('Test ','').replace('FAIL: ','')
				my_key = line.split('(')[1].split(',')[1].strip()
				
				# my_key is the name of the aggregate function
				# dicts[] is used to store the count of my_key
				
				if not dicts.has_key(my_key):
					dicts[my_key] = 0
				dicts[my_key] += 1
				test_id = "%s_%d.test" % (my_key, dicts[my_key])
				prev_test_id = test_id
				generate_file(test_id, commands)
				commands = []
		file_handle.close()
	   print "\nfailed_test_ids: (total=%d)" % len(failed_tests)
	   for j in failed_tests:
		print j
	except Exception, inst:
		logg("exception_raised_in_create_test")
                print "     Exception Type (create_test): %s" % type(inst)     # the exception instance	


def generate_file(test_id, commands):
	try:
		logg("in generate_file")
		logg("test_id = " + test_id)
		o_file = open(tests_path+'/'+test_id,"w")
		o_file.write("\n--setup\n--start-query-logging\n--start-igdata\n")
		for i in range(len(commands)):
			stmt = commands[i]
			logg("stmt : " + stmt)
			if i == len(commands)-1:
				o_file.write("--stop-igdata\n\n--test\n")
			if stmt.partition(',')[0] == 'afl':
				o_file.write(stmt.partition(',')[2] + "\n")
			elif stmt.partition(',')[0] == 'aql':
				o_file.write("--aql " + stmt.partition(',')[2] + "\n")
			elif stmt.partition(',')[0].startswith("--error"):
				e_code = stmt.split(',')[0]
				q_type = stmt.split(',')[1]
				query = stmt.partition(',')[2].partition(',')[2]
				if q_type == 'afl':
					o_file.write(e_code + " \"" + query + "\"\n")
				elif q_type == 'aql':
					o_file.write(e_code + " --aql=\"" + query + "\"\n")
				else:
					print "invalid q_type " + q_type
			else:
				print "invalid query type " + stmt.partition(',')[0]
		o_file.write("\n--cleanup\n")
		o_file.write("remove(T)\nremove(E)\nremove(R)\n--stop-query-logging")
		o_file.close()
	except Exception, inst:
		logg("exception_raised_in_generate_file")
                print "     Exception Type (generate_file): %s" % type(inst)     # the exception instance	


def logg(st):
	d_file = open(debug_file,"a")
	d_file.write("\n")
	d_file.write(st)
	d_file.close()


if __name__ == "__main__":
	print "Processing..."
	d_file = open(debug_file,"w")
	d_file.close()
	create_test()
	print "\nStatistics:"
	count = 0
	for n in dicts:
		count += dicts[n]
		print n,dicts[n]
	print "----------"
	print "total =",count
	print ""
