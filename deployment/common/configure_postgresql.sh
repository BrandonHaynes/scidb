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

username=${1}
password="${2}"
network=${3}

if !(echo "${network}" | grep / 1>/dev/null); then
   echo "Invalid network format in ${network}"
   echo "Usage: configure_postgresql.sh network_ip (where network_ip=W.X.Y.Z/N) "
   exit 1;
fi

function postgresql_sudoers ()
{
    POSTGRESQL_SUDOERS=/etc/sudoers.d/postgresql
    echo "Defaults:${username} !requiretty" > ${POSTGRESQL_SUDOERS}
    echo "${username} ALL =(postgres) NOPASSWD: ALL" >> ${POSTGRESQL_SUDOERS}
    chmod 0440 ${POSTGRESQL_SUDOERS}
}

function centos6()
{
    yum install -y postgresql postgresql-server postgresql-contrib expect
    /sbin/chkconfig postgresql on
    service postgresql initdb || true
    restart="service postgresql restart"
    status="service postgresql status"
}

function u1204()
{
    apt-get update
    apt-get install -y python-paramiko python-crypto postgresql-8.4 postgresql-contrib-8.4 expect
    restart="/etc/init.d/postgresql restart"
    status="/etc/init.d/postgresql status"
}

function u1404()
{
    apt-get update
    apt-get install -y python-paramiko python-crypto postgresql-9.3 postgresql-contrib-9.3 expect
    restart="/etc/init.d/postgresql restart"
    status="/etc/init.d/postgresql status"
}

OS=`./os_detect.sh`
case ${OS} in
    "CentOS 6")
	centos6
	;;
    "RedHat 6")
	centos6
	;;
    "Ubuntu 12.04")
	u1204
	;;
    "Ubuntu 14.04")
	u1404
	;;
    *)
	echo "Not supported OS";
	exit 1
esac;

postgresql_sudoers
./configure_postgresql.py "${OS}" "${username}" "${password}" "${network}" || echo "WARNING: failed to configure postgres !"
${restart}
${status}
