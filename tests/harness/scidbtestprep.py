#!/usr/bin/python

# Initialize, start and stop scidb. 
# Supports single instance and cluster configurations. 
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

import sys
import time
import datetime
import os
import string
import errno
import socket
from ConfigParser import *
from ConfigParser import RawConfigParser

d = {}
cmd = ''
srcpath = ''
linkdst = ''
db = ''
configfile = ''
baseDataPath = None

def usage():
  progname = sys.argv[0]
  print ""
  print "\t Usage: %s <cmd> <srcpath> <linkdst> <db> <conffile> [ [--clear-files=<older than n days>] [--root-dir=<path for root directory>] ]" % progname
  print "\t Setup and cleanup for P4 tests."
  print "\t Supported commands:" 
  print "\t linkdata"
  sys.exit(2)

def remove_old_files(daysOld,dir_to_search):
  old_files_count = 0
  for dirpath, dirnames, filenames in os.walk(dir_to_search):
    for file in filenames:
      curpath = os.path.join(dirpath, file)
      f_name,f_ext = os.path.splitext(curpath)
      if not (f_ext == ".log" or f_ext == ".diff" or f_ext == ".out"):
        continue
      file_mtime = datetime.datetime.fromtimestamp(os.path.getmtime(curpath))
      if datetime.datetime.now() - file_mtime > datetime.timedelta(days=int(daysOld)):
        os.remove(curpath)
        old_files_count += 1
  print "Removed %d result files which were older than %s days." % (old_files_count,daysOld)

def check_clear_files():
  cmd_found = False
  dir_to_search = os.path.curdir
  root_dir = ''
  no_of_days_old = 99
  for option in sys.argv:
    if option.startswith("--clear-files="):
      cmd_found = True
      no_of_days_old = option.split("=")[1]
    elif option.startswith("--root-dir="):
      root_dir = option.split("=")[1]
  if cmd_found == True:
    if root_dir != '':
      dir_to_search = os.path.abspath(root_dir)
    if not os.path.isdir(dir_to_search):
      print "root directory specified is invalid"
    else:
      remove_old_files(no_of_days_old,dir_to_search)
  elif len(sys.argv) != 6:
    usage()

# Parse a ini file
def parse_global_options(filename):
    config = RawConfigParser()
    config.read(filename)
    section_name = db

    # First process the "global" section. 
    try:
      #print "Parsing %s section." % (section_name)
      for (key, value) in config.items(section_name):
        d[key] = value
            
    except ParsingError:
        print "Error"
        sys.exit(1)
    d['db_name'] = db
    print d

def linkData(dstpath, srcpath):
  try:
    os.remove(dstpath)
  except OSError, detail: 
    if detail.errno != errno.ENOENT:
        print "Cannot remove symlink %s OSError: " % dstpath, detail
        sys.exit(detail.errno)
  os.symlink(srcpath, dstpath)

if __name__ == "__main__":

  if len(sys.argv) != 6:
    check_clear_files()

  if len(sys.argv) < 6:
    usage()

# Defaults
# Set the rootpath to the parent directory of the bin directory
  cmd = sys.argv[1]
  srcpath = sys.argv[2]
  linkdst = sys.argv[3]
  db = sys.argv[4]
  configfile = sys.argv[5]

  parse_global_options(configfile)
  baseDataPath = d.get('base-path')

  if (cmd == "linkdata"):
    linkData(baseDataPath + "/000/" + linkdst, srcpath)

  sys.exit(0)
