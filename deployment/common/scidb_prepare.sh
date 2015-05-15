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
set -eu
SCIDB_VER="${1}"
ORIG=~/.bashrc
NEW=~/.bashrc.new

if ! grep SCIDB_VER ${ORIG} ; then
    cp -p ${ORIG} ${NEW}
    # Ubunutu aparently has a 'return' command
    grep -v return ${ORIG} > ${NEW} || true
    echo "export SCIDB_VER=${SCIDB_VER}" >> ${NEW}
    echo "export PATH=/opt/scidb/\$SCIDB_VER/bin:/opt/scidb/\$SCIDB_VER/share/scidb:\$PATH" >> ${NEW}
    echo "export IQUERY_PORT=1239" >> ${NEW}
    echo "export IQUERY_HOST=localhost" >> ${NEW}
    mv ${NEW} ${ORIG}
    echo ". ${ORIG}" >> ~/.bash_profile
fi;
