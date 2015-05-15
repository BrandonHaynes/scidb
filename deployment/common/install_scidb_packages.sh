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
# Requires root privs
if [ $# -lt 1 ]; then
   echo "Usage: install_scidb_packages.sh [<package_file_name>]+"
   exit 1
fi

PACKAGE_FILE_NAME_LIST=$@

function ubuntu1204
{
echo "Install SciDB packages: ${PACKAGE_FILE_NAME_LIST}"
dpkg -i ${PACKAGE_FILE_NAME_LIST}
}

function ubuntu1404
{
echo "Install SciDB packages: ${PACKAGE_FILE_NAME_LIST}"
dpkg -i ${PACKAGE_FILE_NAME_LIST}
}

function centos6
{
echo "Install SciDB packages: ${PACKAGE_FILE_NAME_LIST}"
rpm -i ${PACKAGE_FILE_NAME_LIST}
}


OS=`./os_detect.sh`

if [ "${OS}" = "CentOS 6" ]; then
    centos6
fi

if [ "${OS}" = "RedHat 6" ]; then
    centos6
fi

if [ "${OS}" = "Ubuntu 12.04" ]; then
    ubuntu1204
fi

if [ "${OS}" = "Ubuntu 14.04" ]; then
    ubuntu1404
fi

rm -rf ${PACKAGE_FILE_NAME_LIST}
