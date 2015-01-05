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
#~/bin/bash
#

#FILE="~/work/data/expo/microExpo.csv"
FILE="nanoExpo.csv"

scidbLoadCsv.sh -a ID,AffyID,Response -d 0,1       -m '1:4,1:4' -c 4,4 int32,int32,double tmpA < $FILE
scidbLoadCsv.sh -a ID,AffyID,Response -D ID,AffyID -m '1:4,1:4' -c 4,4 int32,int32,double tmpB < $FILE

set -x
iquery -aq "show(tmpA)"
iquery -aq "aggregate(tmpA,count(*))"
#iquery -aq "scan(tmpA)"

iquery -aq "show(tmpB)"
iquery -aq "aggregate(tmpB,count(*))"
#iquery -aq "scan(tmpB)"

iquery -aq "scan(tmpA)" > /tmp/tmpA.txt
iquery -aq "scan(tmpB)" > /tmp/tmpB.txt
set +x

diff /tmp/tmpA.txt /tmp/tmpB.txt
RESULT=$?
echo RESULT is $RESULT

if false ; then
    iquery    -aq "remove(tmpJ)"
    iquery    -aq "remove(tmpD)"
    iquery    -aq "remove(tmpR)"

    iquery -n -aq "store(join(tmpA, tmpB), tmpJ)"
    iquery    -aq "show(tmpJ)"
    iquery -n -aq "store(apply(joined, d, Response + Response_2), tmpD)"
    iquery    -aq "show(tmpD)"
    iquery    -aq "store(project(tmpD, d), tmpR)"
    iquery    -aq "show(tmpR)"
    iquery    -aq "aggregate(tmpR,count(*))"
    iquery    -aq "aggregate(tmpR,sum(d))"
fi

# failure cases that should diagnose the user:

echo "**** the following cases should all result in failures with diagnostics ****"
set -x
# -c : chunksize list, make sure nums aren't followed by letters
scidbLoadCsv.sh -a ID,AffyID,Response -d 0 -m '1:4' -c 4a int32,int32,double tmpC < $FILE

# -c : chunksize list, make sure their number matches  num(-a) - num(-d):
scidbLoadCsv.sh -a ID,AffyID,Response -d 0 -m '1:4' -c 4,4 int32,int32,double tmpC < $FILE

# number of -d/D's must be less than -a/h's
scidbLoadCsv.sh -a ID,AffyID,Response -d 0,1,2 -m '1:4,1:4,1:4' -c 4,4,4 int32,int32,double tmpC < $FILE

# must be at least one -d
scidbLoadCsv.sh -a ID,AffyID,Response int32,int32,double tmpC < $FILE

# -d / -D : dimensions check that numbers not followed by letters
scidbLoadCsv.sh -a ID,AffyID,Response -d 0a -m '1:4' -c 4 int32,int32,double tmpC < $FILE

# -f : only one character allowed!
scidbLoadCsv.sh -f xyz -a ID,AffyID,Response -d 0 -m '1:4' -c 4 int32,int32,double tmpC < $FILE

# -p must only be numbers
scidbLoadCsv.sh -p 1239a -a ID,AffyID,Response -d 0 -m '1a:4' -c 4 int32,int32,double tmpC < $FILE

# -m must be integer with colon
scidbLoadCsv.sh -p 1239 -a ID,AffyID,Response -d 0 -m '1x4' -c 4 int32,int32,double tmpC < $FILE

# -m must have the same number as -d/D
scidbLoadCsv.sh -p 1239 -a ID,AffyID,Response -d 0 -m '1:4,1:4' -c 4 int32,int32,double tmpC < $FILE

# -F can only be 1 character long
scidbLoadCsv.sh -p 1239 -a ID,AffyID,Response -F 'ab' -d 0 -m '1:4' -c 4 int32,int32,double tmpC < $FILE
scidbLoadCsv.sh -p 1239 -a ID,AffyID,Response -F '' -d 0 -m '1:4' -c 4 int32,int32,double tmpC < $FILE
set +x
set +x

