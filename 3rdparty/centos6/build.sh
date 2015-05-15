#!/bin/bash

[ -f /usr/sbin/mock ] && HAS_MOCK=1 || HAS_MOCK=0

if [ ! $HAS_MOCK -eq 1 ]; then
    echo You do not have the mock utility.
    echo You probably need to run \"deploy.sh prepare_chroot\".
    exit 1
fi

function usage()
{
    echo "Usage: $0 <result dir>"
    exit 1
}

function die()
{
    echo $1
    exit 1
}

[ ! "$#" -eq 1 ] && usage

export SCIDB_VERSION_MAJOR=`awk -F . '{print $1}' $(dirname "$0")/../../version`
export SCIDB_VERSION_MINOR=`awk -F . '{print $2}' $(dirname "$0")/../../version`
export SCIDB_VERSION_PATCH=`awk -F . '{print $3}' $(dirname "$0")/../../version`

build_dir="`mktemp -d /tmp/scidb_packaging.XXXXX`"
chroot_result_dir="$1"
source_dir=~/scidb_3rdparty_sources
SCIDB_VER=$SCIDB_VERSION_MAJOR.$SCIDB_VERSION_MINOR

pushd "$(dirname "$0")"
script_dir="`pwd`"
popd

baseurl="http://downloads.paradigm4.com/centos6.3/3rdparty_sources"

# original URLs stored in ${base_url}/original.txt
sources="
mpich2-1.2.1.tar.gz
boost_1_54_0.tar.bz2
libpqxx-3.1.tar.gz
cmake-2.8.10.tar.gz
swig-2.0.8.tar.gz
pigz-2.2.5.tar.gz
mock-1.1.24.tar.xz
apache-log4cxx-0.10.0.tar.gz
protobuf-2.4.1.tar.bz2
argparse-1.2.1.tar.gz
apache-maven-3.0.4-bin.tar.gz
ccache-3.1.6.tar.xz
cppunit-1.12.1.tar.gz
"

echo Preparing dirs
mkdir -p "${build_dir}"/{BUILD,BUILDROOT,RPMS,SOURCES,SPECS,SRPMS} "${source_dir}" "${chroot_result_dir}" || die
echo build: ${build_dir}, source: ${source_dir}, chroot: ${chroot_result_dir}

echo Downloading sources to "${source_dir}"
pushd "${source_dir}"
    for filename in $sources; do
        [ ! -f $filename ] && wget "$baseurl/${filename}" -O tmp && mv tmp $filename
    done
popd

echo Copying sources to "${build_dir}/SOURCES"
cp "${source_dir}"/*  "${script_dir}"/centos-6-x86_64.cfg "${script_dir}"/patches/*.patch "${script_dir}"/patches/*.in "${build_dir}/SOURCES"

echo Copying specs to "${build_dir}/SOURCES"
for spec_file_name in `(cd ${script_dir}; ls *.spec)`; do
    cat "${script_dir}"/${spec_file_name} | sed -e "s/SCIDB_VERSION_MAJOR/${SCIDB_VERSION_MAJOR}/" | sed -e "s/SCIDB_VERSION_MINOR/${SCIDB_VERSION_MINOR}/" | sed -e "s/SCIDB_VERSION_PATCH/${SCIDB_VERSION_PATCH}/" > "${build_dir}"/SPECS/${spec_file_name}
done;

echo Building source packages
pushd "${build_dir}"/SPECS
    for f in *.spec; do
        echo Building $f source package
        rpmbuild -D"_topdir ${build_dir}" -bs $f || die "Can't build $f"
    done
popd

echo Building dependencies in chroot
pushd "${build_dir}"/SRPMS
    for f in *.src.rpm; do
        echo Building binary package from $f
        python ${script_dir}/../../utils/chroot_build.py -b -d centos-6-x86_64 -s $f -r "${chroot_result_dir}" || die Can not build $f
    done
popd

echo Removing build dir "${build_dir}"
sudo rm -rf "${build_dir}"

echo Done. Take result from "${chroot_result_dir}"
