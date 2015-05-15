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

function centos6()
{
(cat <<EOF
[scidb3rdparty]
name=SciDB 3rdparty repository
baseurl=https://downloads.paradigm4.com/centos6.3/3rdparty
gpgkey=https://downloads.paradigm4.com/key
gpgcheck=1
enabled=0
EOF
) | tee scidb3rdparty.repo
REPO_FILE=/etc/yum.repos.d/scidb3rdparty.repo
mv scidb3rdparty.repo ${REPO_FILE}
yum clean all
}

function ubuntu1204()
{
wget -O- https://downloads.paradigm4.com/key | apt-key add -
echo "deb https://downloads.paradigm4.com/ ubuntu12.04/3rdparty/" | tee scidb3rdparty.list
REPO_FILE=/etc/apt/sources.list.d/scidb3rdparty.list
mv scidb3rdparty.list ${REPO_FILE}
apt-get update
}

function ubuntu1404()
{
wget -O- https://downloads.paradigm4.com/key | apt-key add -
echo "deb https://downloads.paradigm4.com/ ubuntu14.04/3rdparty/" | tee scidb3rdparty.list
REPO_FILE=/etc/apt/sources.list.d/scidb3rdparty.list
mv scidb3rdparty.list ${REPO_FILE}
apt-get update
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
