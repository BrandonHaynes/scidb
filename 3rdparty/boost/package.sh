#!/bin/bash
#
# This script will create our own package of boost.
#
################################################################
# Necessary packages to configure, build, install, package
#
OS=`../../deployment/common/os_detect.sh`

if [ "${OS}" = "CentOS 6" ]; then
    sudo yum install --enablerepo=scidb3rdparty -y checkinstall
    sudo rpm -Uvh http://download.fedoraproject.org/pub/epel/6/i386/epel-release-6-8.noarch.rpm
    sudo yum install dpkg-devel
fi
if [ "${OS}" = "Ubuntu 12.04" ]; then
    sudo apt-get install -y checkinstall
fi
################################################################
# SciDB version
#
export SCIDB_VERSION_MAJOR=`awk -F . '{print $1}' $(dirname "$0")/../../version`
export SCIDB_VERSION_MINOR=`awk -F . '{print $2}' $(dirname "$0")/../../version`
export SCIDB_VERSION_PATCH=`awk -F . '{print $3}' $(dirname "$0")/../../version`
################################################################
# Local variables
#
PWDIR=`pwd`
INSTALLDIR="/opt/scidb/${SCIDB_VERSION_MAJOR}.${SCIDB_VERSION_MINOR}/3rdparty"
#
SOURCE_URL="http://sourceforge.net/projects/boost/files/boost/1.54.0/boost_1_54_0.tar.gz"
SOURCE_FILE="boost_1_54_0.tar.gz"
TMPDIR="`mktemp -d --tmpdir=/tmp ${USER}_boost_XXXXXX`"
################################################################
# Start in TMPDIR
#
cd "${TMPDIR}"
echo ""
pwd
echo ""
#
# Get the source
#
#wget "${SOURCE_URL}"
cp ${PWDIR}/${SOURCE_FILE} .
#
# Unroll the source
#
tar -zxf "${SOURCE_FILE}"
#
# Configure it
#
cd boost_1_54_0
./bootstrap.sh --prefix="${INSTALLDIR}"
./b2
#
# Prepare for checkinstall
#
cat > description-pak <<EOF
The Boost C++ headers and shared development libraries.
EOF
if [ "${OS}" = "CentOS 6" ]; then
    PKGTYPE="debian"
    PKGARCH="x86_64"
fi
if [ "${OS}" = "Ubuntu 12.04" ]; then
    PKGTYPE="rpm"
    PKGARCH="amd64"
fi
# --pkgarch="${PKGARCH}"
sudo checkinstall --type="${PKGTYPE}" --fstrans --pkgname="scidb-${SCIDB_VERSION_MAJOR}.${SCIDB_VERSION_MINOR}-libboost-devel" --pkgversion=1.54.0 --pkgrelease=0 --pkgsource="http://www.boost.org" --pkglicense="Boost Software License" --pkggroup="Development/Libraries" --maintainer="paradigm4.com" --nodoc ./b2 install --prefix="${INSTALLDIR}"
