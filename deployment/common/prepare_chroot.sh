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
OS=`./os_detect.sh`

function chroot_sudoers_mock ()
{
CHROOT_SUDOERS=/etc/sudoers.d/chroot_builder
echo "Defaults:${username} !requiretty" > ${CHROOT_SUDOERS}
echo "Cmnd_Alias RMSCIDB_PACKAGING = /bin/rm -rf /tmp/scidb_packaging.*" >> ${CHROOT_SUDOERS}
echo "${username} ALL = NOPASSWD:/usr/sbin/mock, NOPASSWD:/bin/which, NOPASSWD:RMSCIDB_PACKAGING" >> ${CHROOT_SUDOERS}
chmod a-wx,o-r,ug+r ${CHROOT_SUDOERS}
}

function centos6 ()
{
yum install --enablerepo=scidb3rdparty -y gcc make rpm-build mock python-argparse
chroot_sudoers_mock
}

function chroot_sudoers_pbuilder ()
{
CHROOT_SUDOERS=/etc/sudoers.d/chroot_builder
echo "Defaults:${username} !requiretty" > ${CHROOT_SUDOERS}
echo "Cmnd_Alias RMSCIDB_PACKAGING = /bin/rm -rf /tmp/scidb_packaging.*" >> ${CHROOT_SUDOERS}
echo "${username} ALL = NOPASSWD:/usr/sbin/pbuilder, NOPASSWD:/bin/which, NOPASSWD:RMSCIDB_PACKAGING" >> ${CHROOT_SUDOERS}
chmod a-wx,o-r,ug+r ${CHROOT_SUDOERS}
}
#
# Need to add apt-get install apt-transport-https
# before the othermirror (https://downloads.paradigm4.com)
# is loaded.
#
# This creates a pbuilder --create hook that loads the https transport into apt-get.
# It will be found by pbuilder because the flag "--hookdir /var/cache/pbuilder/hook.d"
# has been added to the class UbuntuChroot(): init function in utils/chroot_build.py
#
function pbuilder_apt-transport-https ()
{
    mkdir -p /var/cache/pbuilder/hook.d/
    echo "#!/bin/sh" > /var/cache/pbuilder/hook.d/G01https
    echo "apt-get install -y apt-transport-https" >> /var/cache/pbuilder/hook.d/G01https
    echo "apt-get install -y ca-certificates" >> /var/cache/pbuilder/hook.d/G01https
    chmod 555 /var/cache/pbuilder/hook.d/G01https
}

function ubuntu1204 ()
{
apt-get update
apt-get install -y build-essential dpkg-dev pbuilder debhelper m4 cdbs quilt apt-transport-https
chroot_sudoers_pbuilder
pbuilder_apt-transport-https
}

function ubuntu1404 ()
{
apt-get update
apt-get install -y build-essential dpkg-dev pbuilder debhelper m4 cdbs quilt apt-transport-https
chroot_sudoers_pbuilder
pbuilder_apt-transport-https
}

function redhat63 ()
{
echo "We do not support build SciDB under RedHat 6. Please use CentOS 6 instead"
exit 1
}

if [ "${OS}" = "CentOS 6" ]; then
    centos6
fi

if [ "${OS}" = "RedHat 6" ]; then
    redhat63
fi

if [ "${OS}" = "Ubuntu 12.04" ]; then
    ubuntu1204
fi

if [ "${OS}" = "Ubuntu 14.04" ]; then
    ubuntu1404
fi
