#!/bin/bash
#
set -eu
################################################################
# script_dir
#   set script_dir to the directory this script is in
pushd "$(dirname "$0")" > /dev/null
    script_dir="`pwd`"
popd > /dev/null
# Ubuntu codename for this Ubuntu release
codename="`lsb_release -c -s`"
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

dsc_source="boost1.54_1.54.0-2.1.dsc"
dir_source="boost1.54-1.54.0"
dsc_result="boost1.54_1.54.0-2.1.dsc"
sources="
boost1.54_1.54.0-2.1.dsc
boost1.54_1.54.0.orig.tar.bz2
boost1.54_1.54.0-2.1.debian.tar.gz
"
################################################################
function usage()
{
    echo "Usage: $0 <result dir>"
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
chroot_tmp_dir="${chroot_result_dir}/chroot_tmp"

function cleanup()
{
    echo "Removing build dirs: ${build_dir} ${chroot_tmp_dir}"
    cd ${script_dir}
    rm -rf "${build_dir}" "${chroot_tmp_dir}"
    sudo rm -rf "${build_dir}"
}

function die()
{
    cleanup
    echo $1 1>&2
    exit 1
}

cleanup

echo "Preparing build dirs: ${build_dir} ${chroot_tmp_dir}"
mkdir -p "${build_dir}"      || die "Can not mkdir ${build_dir}"
mkdir -p "${chroot_tmp_dir}" || die "Can not mkdir ${chroot_tmp_dir}"
#----------------------------------------------------------------------#
#  Download source files                                               #
#----------------------------------------------------------------------#
echo "Downloading sources to: ${build_dir}"
pushd "${build_dir}" > /dev/null
   for filename in $sources; do
      wget "${baseurl}/${filename}" || die "Unable to wget ${baseurl}/${filename}"
   done
popd > /dev/null
#----------------------------------------------------------------------#
#  Copy patch files into build dir                                     #
#----------------------------------------------------------------------#
for patch in $(ls "${script_dir}"/patches/*.patch); do
    patch_file="${build_dir}"/$(basename ${patch})
    cat ${patch} | sed -e "s/SCIDB_VERSION_MAJOR/${SCIDB_VERSION_MAJOR}/g" | sed -e "s/SCIDB_VERSION_MINOR/${SCIDB_VERSION_MINOR}/g" | sed -e "s/SCIDB_VERSION_PATCH/${SCIDB_VERSION_PATCH}/g" > "${patch_file}"
    if [ ! -f "${patch_file}" ]; then
	die "Can not produce patch file ${patch_file}"
    fi
done
#----------------------------------------------------------------------#
# Rebuild source package adding new patches                            #
#----------------------------------------------------------------------#
echo "Building source package"
pushd ${build_dir} > /dev/null
    # Extract source
    dpkg-source -x ${dsc_source}  || die "Can not dpkg-source"
    # Add patches to it
    for patch_file in *.patch; do
	echo ${patch_file} >> ${dir_source}/debian/patches/series
	cp ${patch_file} ${dir_source}/debian/patches || die "Can not copy patch file ${patch_file} into patches directory"
    done
    pushd ${dir_source} > /dev/null
        dpkg-buildpackage -rfakeroot -S -us -uc -i.* || die "Can not dpkg-buildpackage"
    popd > /dev/null
popd > /dev/null
#----------------------------------------------------------------------#
# Build binary packages                                                #
#----------------------------------------------------------------------#
echo "Building dependencies in chroot"
pushd ${build_dir} > /dev/null
    python ${script_dir}/../../../utils/chroot_build.py -b -d ubuntu-${codename}-amd64 -s ${dsc_result} -r "${chroot_result_dir}" -t "${chroot_tmp_dir}"|| die "Can not build ${dsc_result}"
popd > /dev/null
#----------------------------------------------------------------------#
# Repackaging libboost packages into scidb-*-libboost packages         #
#----------------------------------------------------------------------#
pushd ${chroot_result_dir} > /dev/null
    cp ${script_dir}/scidb-repackage-boost . || die "Can not copy scidb-repackage-boost into ${chroot_result_dir}"
    cp ${script_dir}/scidb-repackage . || die "Can not copy scidb-repackage into ${chroot_result_dir}"
    for f in *.deb; do
	./scidb-repackage-boost $f $SCIDB_VER || die "Unable to process scidb-repackage-boost $f"
    done
popd > /dev/null
##----------------------------------------------------------------------#
##  Cleanup                                                             #
##----------------------------------------------------------------------#
cleanup
echo "Done. Take result from ${chroot_result_dir}"
