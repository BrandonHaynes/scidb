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

dsc_source="mpich2_1.2.1.1-4.dsc"
dir_source="mpich2-1.2.1.1"
dsc_result="scidb-${SCIDB_VER}-mpich2_1.2.1.1-4.dsc"
sources="
mpich2_1.2.1.1-4.diff.gz
mpich2_1.2.1.1-4.dsc
mpich2_1.2.1.1.orig.tar.gz
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
# Rebuild source package applying new patches                          #
#----------------------------------------------------------------------#
echo "Building source package"
pushd ${build_dir} > /dev/null
    # Extract source
    dpkg-source -x ${dsc_source} || die "Can not dpkg-source"
    # Patch it
    pushd ${dir_source} > /dev/null
        for patch in $(ls ../*.patch); do
            patch -p1 < ${patch} || die "Patch ${patch} not applied"
	done
	# Repackage it
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
##----------------------------------------------------------------------#
##  Cleanup                                                             #
##----------------------------------------------------------------------#
cleanup
echo "Done. Take result from ${chroot_result_dir}"
