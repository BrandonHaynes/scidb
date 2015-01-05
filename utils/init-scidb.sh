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
# Script for preparing SciDB catalog database
#
if [[ 3 != $# ]]; then
    echo "Usage: $0 owner database password"
    exit;
fi

PD_DIR=$(dirname $(readlink -f $0))
owner=$1
database=$2
password=$3

#sudo -u postgres ./init-db.sh $owner $database $password

rm -f ./storage.*
rm -f s1/storage.*
rm -f s2/storage.*

./scidb  -c "host=localhost port=5432 dbname=$database user=$owner password=$password" --register
./scidb  -d -k -l log1.properties -c "host=localhost port=5432 dbname=$database user=$owner password=$password"

