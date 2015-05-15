#!/bin/bash
#
set -eu
################################################################
# script_dir
#   set script_dir to the directory this script is in
pushd "$(dirname "$0")" > /dev/null
    script_dir="`pwd`"
popd > /dev/null
################################################################
# Global settings
if [ ! -f ${script_dir}/../../../version ]; then
    echo "Version file not found at ${script_dir}/../../../version" 1>&2
    echo "Something is wrong. This script is in the wrong place or your workspace is invalid" 1>&2
    exit 1
fi
export SCIDB_VERSION_MAJOR=`awk -F . '{print $1}' ${script_dir}/../../../version`
export SCIDB_VERSION_MINOR=`awk -F . '{print $2}' ${script_dir}/../../../version`
export SCIDB_VERSION_PATCH=`awk -F . '{print $3}' ${script_dir}/../../../version`
export SCIDB_VER=$SCIDB_VERSION_MAJOR.$SCIDB_VERSION_MINOR

baseurl="https://downloads.paradigm4.com/ubuntu12.04/3rdparty_sources"

upstream_tarball="libcsv_3.0.3.orig.tar.gz"
upstream_dir="libcsv-3.0.3"
result_files="
scidb-${SCIDB_VER}-libcsv_3.0.3-1_amd64.deb
"
################################################################
# Functions
function usage()
{
    echo "Usage: $0 <result_dir>"
    exit 1
}

[ ! "$#" -eq 1 ] && usage

if [ ! -d "$1" ]; then
    mkdir -p "$1"
fi
if [ ! -d "$1" ]; then
    echo "Unable to create result dir $1" 1>&2
    exit 1
fi

chroot_result_dir="$(readlink -f $1)"
build_dir="${chroot_result_dir}/chroot_scidb_packaging"

function cleanup()
{
    echo "Removing build dir: ${build_dir}"
    cd ${script_dir}
    rm -rf "${build_dir}"
    sudo rm -rf "${build_dir}"
}

function die()
{
    cleanup
    echo $1 1>&2
    exit 1
}

cleanup

echo "Preparing build dirs: ${build_dir}"
mkdir -p "${build_dir}"      || die "Can not mkdir ${build_dir}"
#----------------------------------------------------------------------#
#  Download libcsv source files                                        #
#----------------------------------------------------------------------#
echo "Downloading source to: ${build_dir}"
cd "${build_dir}"
wget "${baseurl}/${upstream_tarball}" || die "Unable to wget ${baseurl}/${upstream_tarball}"
#----------------------------------------------------------------------#
#  Untaring libcsv source files                                        #
#----------------------------------------------------------------------#
echo "Untaring upstream_tarball to: ${build_dir}"
tar -xf ${upstream_tarball} || die "Unable to untar upstream tarball ${upstream_tarball}"
#----------------------------------------------------------------------#
#  Copying debian directory to build_dir editing SCIDB_VERSION strings #
#----------------------------------------------------------------------#
cd ${upstream_dir}
echo "Copying debian directory to ${build_dir}/${upstream_dir}"
cp -r "${script_dir}"/debian . || die "Unable to copy debian directory"
sed -i "s/SCIDB_VERSION_MAJOR/${SCIDB_VERSION_MAJOR}/g" debian/control
sed -i "s/SCIDB_VERSION_MINOR/${SCIDB_VERSION_MINOR}/g" debian/control
sed -i "s/SCIDB_VERSION_PATCH/${SCIDB_VERSION_PATCH}/g" debian/control
sed -i "s/SCIDB_VERSION_MAJOR/${SCIDB_VERSION_MAJOR}/g" debian/rules
sed -i "s/SCIDB_VERSION_MINOR/${SCIDB_VERSION_MINOR}/g" debian/rules
sed -i "s/SCIDB_VERSION_PATCH/${SCIDB_VERSION_PATCH}/g" debian/rules
#----------------------------------------------------------------------#
#  Building the package                                                #
#----------------------------------------------------------------------#
echo Build package
debuild --no-lintian -us -uc || die "Unable to debuild"
#----------------------------------------------------------------------#
#  Copy results
#----------------------------------------------------------------------#
cd ../
for file in ${result_files}
do
    cp ${file} "${chroot_result_dir}"
    if [ ! -f "${chroot_result_dir}"/$(basename ${file}) ]; then
	echo "Unable to copy result file ${file} into ${chroot_result_dir}" 1>&2
	echo
	echo "build_dir ${build_dir} has been left for you to find result files" 1>&2
	exit 1
    fi
done
##----------------------------------------------------------------------#
##  Cleanup                                                             #
##----------------------------------------------------------------------#
cleanup
echo "Done. Take result from ${chroot_result_dir}"
