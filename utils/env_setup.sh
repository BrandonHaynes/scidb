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
#  This script illustrates how we might set the range of 
# environment variables needed by scidb.py to initialize, 
# start and stop SciDB compute instance instances. The idea is 
# that SciDB (currently) uses a Postgres DBMS instance to 
# manage all of the meta-data. And these details are needed
# to connecto to that server. 
#
#  To set up variables that are relevant to your instance, 
# replace each of the env variables exported below w/ 
# appropriate values, and 'source' this file. 
#
# Pick which one you want - set or unset 
# 
# SCIDB_DB_HOST - Name or IP address of the machine on which 
#                 the meta-data service runs. 
# 
# SCIDB_DB_NAME - The name of the Postgres database within the 
#                 Postgres instance running on SCIDB_DB_HOST.
# 
# SCIDB_DB_USER - The name of a user with appropriate privileges
#                 over the SCIDB_DB_NAME database on the 
#                 SCIDB_DB_HOST server. 
#
# SCIDB_DB_PASSWD - Password for SCIDB_DB_USER. 
# SCIDB_DATA_DIR  - DATA_DIR for the SciDB engine. 
#                
# Yes. This is a "password in the clear". My bad. We'll 
# fix this when it becomes a priority.
#
# unset SCIDB_DB_HOST
# unset SCIDB_DB_NAME
# unset SCIDB_DB_USER
# unset SCIDB_DB_PASSWD
#
export SCIDB_DB_HOST='localhost'
export SCIDB_DB_NAME='scidb_test'
export SCIDB_DB_USER='scidb_user'
export SCIDB_DB_PASSWD='scidb_user'
export SCIDB_DATA_DIR=`pwd`
