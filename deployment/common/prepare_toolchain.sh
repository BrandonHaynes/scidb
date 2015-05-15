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

SCIDB_VER="${1}"

function ubuntu1204 ()
{
echo "Prepare Ubuntu 12.04 for building SciDB"
apt-get update

# Build dependencies:
apt-get install -y build-essential cmake libpqxx-3.1 libpqxx3-dev libprotobuf-dev protobuf-compiler doxygen flex bison liblog4cxx10 liblog4cxx10-dev libcppunit-dev libbz2-dev zlib1g-dev subversion libreadline6-dev libreadline6 python-paramiko python-crypto xsltproc gfortran libscalapack-mpi1 liblapack-dev libopenmpi-dev swig2.0 expect debhelper sudo ant ant-contrib ant-optional libprotobuf-java openjdk-6-jdk junit git libpam-dev

# Boost package build requires:
apt-get install -y python3

# Scidb 3rd party packages
apt-get install -y scidb-${SCIDB_VER}-libboost1.54-all-dev scidb-${SCIDB_VER}-libmpich2-dev scidb-${SCIDB_VER}-mpich2 scidb-${SCIDB_VER}-libcsv

# Reduce rebuild time:
apt-get install -y ccache

# Documentation: 
apt-get install -y fop docbook-xsl

# Testing:
apt-get install -y postgresql-8.4 postgresql-contrib-8.4

# ScaLAPACK tests:
apt-get install -y time

echo "DONE"
}

function ubuntu1404 ()
{
echo "Prepare Ubuntu 14.04 for building SciDB"
apt-get update

# Build dependencies:
apt-get install -y build-essential cmake libpqxx-3.1 libpqxx3-dev libprotobuf-dev protobuf-compiler doxygen flex bison liblog4cxx10 liblog4cxx10-dev libcppunit-dev libbz2-dev zlib1g-dev subversion libreadline6-dev libreadline6 python-paramiko python-crypto xsltproc gfortran libscalapack-mpi1 liblapack-dev libopenmpi-dev swig2.0 expect debhelper sudo ant ant-contrib ant-optional libprotobuf-java openjdk-6-jdk junit git libpam-dev

# Boost package build requires:
apt-get install -y python3

# Scidb 3rd party packages
apt-get install -y scidb-${SCIDB_VER}-libboost1.54-all-dev scidb-${SCIDB_VER}-libmpich2-dev scidb-${SCIDB_VER}-mpich2 scidb-${SCIDB_VER}-libcsv

# Reduce rebuild time:
apt-get install -y ccache

# Documentation: 
apt-get install -y fop docbook-xsl

# Testing:
apt-get install -y postgresql-9.3 postgresql-contrib-9.3

# ScaLAPACK tests:
apt-get install -y time

echo "DONE"
}

function centos6 ()
{
echo "Prepare CentOS 6 for building SciDB"

# ...setup epel repo (libcsv is in there)
rpm -U http://dl.fedoraproject.org/pub/epel/6/x86_64/epel-release-6-8.noarch.rpm || true

INSTALL="yum install --enablerepo=scidb3rdparty -y"
# Build dependencies:
${INSTALL} gcc gcc-c++ gcc-gfortran subversion doxygen flex flex-devel bison zlib-devel bzip2-devel readline-devel rpm-build python-paramiko postgresql-devel cppunit-devel python-devel cmake make scidb-${SCIDB_VER}-libboost-devel swig2 protobuf-devel log4cxx-devel libpqxx-devel expect scidb-${SCIDB_VER}-mpich2-devel scidb-${SCIDB_VER}-mpich2 lapack-devel blas-devel sudo java-1.6.0-openjdk-devel ant ant-contrib ant-nodeps ant-jdepend protobuf-compiler protobuf-java junit git pam-devel libcsv libcsv-devel

# Reduce build time
${INSTALL} ccache

# Documentation
${INSTALL} fop libxslt docbook-style-xsl

# Testing:
${INSTALL} postgresql postgresql-server postgresql-contrib python-argparse

# ScaLAPACK tests:
${INSTALL} time

echo "DONE"
}

function redhat63 ()
{
echo "We do not support build SciDB under RedHat 6. Please use CentOS 6 instead"
exit 1
}

OS=`./os_detect.sh`

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
