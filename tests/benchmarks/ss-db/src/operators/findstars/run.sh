#!/bin/sh
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
make &&cp ../../../../../../bin/plugins/libfind_stars.so /opt/scidb/11.10/lib/scidb/plugins 
scidb -t 1 -x 1 -q 1 -i localhost -p 1239 --merge-sort-buffer 512 --cache 256 --chunk-cluster-size 1048576 -k -l /opt/scidb/current/share/scidb/log4cxx.properties --plugins /opt/scidb/current/lib/scidb/plugins -s /mnt/data/scidb/dbs4/000/0/storage.cfg -c 'host=localhost port=5432 dbname=test11 user=drkwolf password=test'&
iquery -aq "findstars(subarray(dense3x3,0,0,1,2) ,a,400)"
