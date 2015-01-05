#!/bin/bash
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
#INFO:
#This is a utility scripts that should be modified as needed
#Each node must have the same path ex:/data/normal and binaries (ssdbgen,tiledata,chunkDistributer.sh)
serverId=0
for i in 10.140.5.79 10.64.73.162 10.198.125.59 10.198.71.191 10.191.206.57 10.111.55.229 10.110.245.80 10.206.37.236
do
  echo "$serverId $i"
  pdsh -w $i "cp /opt/scidb/12.3/bin/ssdbgen /opt/scidb/12.3/bin/tileData /opt/scidb/12.3/bin/chunkDistributer.sh /data/normal"
done 
