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

set -u

SCIDB_VER="${1}"

function ubuntu1204 ()
{
echo "Prepare SciDB coordinator on Ubuntu 12.04"

INSTALL="apt-get install -y"

# Install R
# ...but need most recent (3.01) so add Cran to repo list
echo 'deb http://watson.nci.nih.gov/cran_mirror/bin/linux/ubuntu precise/' >> /etc/apt/sources.list
apt-key adv --keyserver keyserver.ubuntu.com --recv-keys E084DAB9
apt-get update
# ...now install R
${INSTALL} r-base

# Install SciDB R package
# ...SciDB-R needs curl
${INSTALL} libcurl4-gnutls-dev
# ...now install SciDB R package
R --slave -e "install.packages('scidb',contriburl='http://cran.r-project.org/src/contrib')"

echo "DONE"
}

function ubuntu1404 ()
{
echo "Prepare SciDB coordinator on Ubuntu 14.04"

INSTALL="apt-get install -y"

# Install R
# ...but need most recent (3.01) so add Cran to repo list
echo 'deb http://watson.nci.nih.gov/cran_mirror/bin/linux/ubuntu trusty/' >> /etc/apt/sources.list
apt-key adv --keyserver keyserver.ubuntu.com --recv-keys E084DAB9
apt-get update
# ...now install R
${INSTALL} r-base

# Install SciDB R package
# ...SciDB-R needs curl
${INSTALL} libcurl4-gnutls-dev
# ...now install SciDB R package
R --slave -e "install.packages('scidb',contriburl='http://cran.r-project.org/src/contrib')"

echo "DONE"
}

function centos_6 ()
{
echo "Prepare SciDB coordinator on ${OS}"

INSTALL="yum install -y"

# Install R
# ...setup epel repo (Cran's R package is in there)
rpm -U http://dl.fedoraproject.org/pub/epel/6/x86_64/epel-release-6-8.noarch.rpm || true
# ...now install R
${INSTALL} R

# Install SciDB R package
# ...and install make needed by cran.r
${INSTALL} make
# ...and install curl-devel for Rcurl
${INSTALL} libcurl-devel
R --slave -e "install.packages('scidb',contriburl='http://cran.r-project.org/src/contrib')"

echo "DONE"
}

function rh_6 ()
{
echo "Prepare SciDB coordinator on ${OS}"

INSTALL="yum install -y"
# Install R
#   For redhat need texinfo-text and libjpeg
${INSTALL} /public/software/texinfo-tex-4.13a-8.el6.x86_64.rpm
${INSTALL} /public/software/libjpeg-turbo-1.2.1-1.el6.x86_64.rpm

# Latest R :-( RedHat no longer has blas-devel, lapack-devel, nor libicu-devel in its repository
#   wget'ed them from CentOS
${INSTALL} /public/software/blas-devel-3.2.1-4.el6.x86_64.rpm
${INSTALL} /public/software/lapack-devel-3.2.1-4.el6.x86_64.rpm
${INSTALL} /public/software/libicu-devel-4.2.1-9.1.el6_2.x86_64.rpm

# ...setup epel repo (Cran's R package is in there)
rpm -U http://dl.fedoraproject.org/pub/epel/6/x86_64/epel-release-6-8.noarch.rpm || true
# ...now install R
${INSTALL} R

# Install SciDB R package
# ...and install make needed by cran.r
${INSTALL} make
# ...and install curl-devel for Rcurl
${INSTALL} libcurl-devel
R --slave -e "install.packages('scidb',contriburl='http://cran.r-project.org/src/contrib')"

echo "DONE"
}

OS=`./os_detect.sh`

if [ "${OS}" = "CentOS 6" ]; then
    centos_6
fi

if [ "${OS}" = "RedHat 6" ]; then
    rh_6
fi

if [ "${OS}" = "Ubuntu 12.04" ]; then
    ubuntu1204
fi

if [ "${OS}" = "Ubuntu 14.04" ]; then
    ubuntu1404
fi
