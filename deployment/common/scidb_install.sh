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

function centos6 ()
{
    /sbin/chkconfig iptables off
    /sbin/service iptables stop
    yum install --enablerepo=scidb3rdparty -y $(ls *.rpm) || exit 1
}

function ubuntu ()
{
    function dependencies ()
    {
	(for package in $(ls *.deb); do
	    dpkg --info $package | grep Depends | sed -e "s/Depends://g" | sed -e "s/,/\n/g" | awk '{print $1}' | grep -v scidb;
	    dpkg --info $package | grep Depends | sed -e "s/Depends://g" | sed -e "s/,/\n/g" | awk '{print $1}' | grep scidb | grep libcsv;
	    dpkg --info $package | grep Depends | sed -e "s/Depends://g" | sed -e "s/,/\n/g" | awk '{print $1}' | grep scidb | grep mpich2;
	    dpkg --info $package | grep Depends | sed -e "s/Depends://g" | sed -e "s/,/\n/g" | awk '{print $1}' | grep scidb | grep libboost;
	done;) | sort -u
    }
    apt-get update
    apt-get install --no-install-suggests --no-install-recommends -y $(dependencies) || exit 1
    dpkg -R -i . || exit 1

# remark: possible to use a method more like that of the centos6() function above? i dislike the current approach because
# it hard codes knowledge of specific 3rd party deps (mpich2 and libboost). Perhaps set up a Local Repository and then use
# apt-get install as usual? Also, investigate use of dpkg-scanpackages, which may prove helpful. jab 8/2013. See #3506
}

OS=`./os_detect.sh`

if [ "${OS}" = "CentOS 6" ]; then
    centos6
fi

if [ "${OS}" = "RedHat 6" ]; then
    centos6
fi

if [ "${OS}" = "Ubuntu 12.04" ]; then
    ubuntu
fi

if [ "${OS}" = "Ubuntu 14.04" ]; then
    ubuntu
fi
