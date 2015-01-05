#!/bin/bash

set -eu

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

baseurl="https://downloads.paradigm4.com/ubuntu12.04/3rdparty_sources"

echo Preparing dirs
mkdir -p "${build_dir}" "${source_dir}" "${chroot_result_dir}" || die
echo build: ${build_dir}, source: ${source_dir}, chroot: ${chroot_result_dir}

#----------------------------------------------------------------------#
#                    START BOOST PROCESSING                            #
#                                                                      #
#  1. Download Boost source files                                      #
#                                                                      #
#----------------------------------------------------------------------#
echo Downloading Boost sources to "${source_dir}"
pushd "${source_dir}"
    wget  -r -l1 --no-parent -nd  -A "boost*" ${baseurl}/
popd

 echo Copying sources to "${build_dir}"
 cp "${source_dir}"/* "${build_dir}"

#----------------------------------------------------------------------#
#                                                                      #
#  2. Boost: Unpack, pack, repack                                      #
#                                                                      #
#----------------------------------------------------------------------#
pushd "${build_dir}"

   for f in *.dsc;
   do dpkg-source -x $f
    done
    cd boost1.54-1.54.0
    patch -p1 -i ${script_dir}/patches/boost-container_leak.patch
    patch -p1 -i ${script_dir}/patches/boost-empty_macro.patch
    # I think some or all of these parameters are necessary.
    debuild -d -us -uc -sa -i -I -b
    echo Packages were built in ${build_dir}
    cd ${build_dir}

    # Now, we need to repackage them
    cp ${script_dir}/scidb-repackage-boost .
    cp ${script_dir}/scidb-repackage .

    for f in *.deb;
       do ./scidb-repackage-boost $f $SCIDB_VER;
    done
echo Doing the unpacking, packing, repacking for boost...
popd
#----------------------------------------------------------------------#
#                                                                      #
#  3. Move the updated packages to the output folder.                  #
#                                                                      #
#----------------------------------------------------------------------#
echo Moving  ${build_dir}/scidb-*.deb ${chroot_result_dir}
mv ${build_dir}/scidb-*.deb ${chroot_result_dir}
echo Removing ${build_dir}/*
rm -rf ${build_dir}/*
echo Removing  ${source_dir}/*
rm -rf ${source_dir}/*
#----------------------------------------------------------------------#
#                    START MPICH PROCESSING                            #
#                                                                      #
#  4. Download Mpich source files                                      #
#                                                                      #
#----------------------------------------------------------------------#
dsc_list_source="mpich2_1.2.1.1-4.dsc"
dir_list="mpich2-1.2.1.1"
dsc_list_result="scidb-${SCIDB_VERSION_MAJOR}.${SCIDB_VERSION_MINOR}-mpich2_1.2.1.1-4.dsc"
sources="
mpich2_1.2.1.1-4.diff.gz
mpich2_1.2.1.1-4.dsc
mpich2_1.2.1.1.orig.tar.gz
"
echo Downloading sources to "${source_dir}"
pushd "${source_dir}"
   for filename in $sources; do
      wget "${baseurl}/${filename}"
   done
popd

echo Copying sources to "${build_dir}"
cp "${source_dir}"/* "${build_dir}"
##----------------------------------------------------------------------#
##                                                                      #
##  5. Mpich: the old way (not using debuild)                           #
##                                                                      #
##----------------------------------------------------------------------#

for patch in $(ls "${script_dir}"/patches/*.patch); do
echo Patching ${patch}
    cat ${patch} | sed -e "s/SCIDB_VERSION_MAJOR/${SCIDB_VERSION_MAJOR}/g" | sed -e "s/SCIDB_VERSION_MINOR/${SCIDB_VERSION_MINOR}/g" | sed -e "s/SCIDB_VERSION_PATCH/${SCIDB_VERSION_PATCH}/g" > "${build_dir}"/$(basename ${patch})
done;

# build source packages
echo "Build source packages"
pushd ${build_dir}
   for dscfile in ${dsc_list_source}; do
       dpkg-source -x ${dscfile}
   done

   for dirname in ${dir_list}; do
       pushd ${dirname}
       for patch in $(ls ../${dirname}*.patch); do
           patch -p1 < ${patch}
       done
       dpkg-buildpackage -rfakeroot -S -us -uc
       popd
   done
popd

#build binary packages
echo "Build dependencies in chroot"
pushd ${build_dir}

for dscfile in ${dsc_list_result}; do
    python ${script_dir}/../../utils/chroot_build.py -b -d ubuntu-precise-amd64 -s ${dscfile} -r "${chroot_result_dir}" || die Can not build ${dscfile}
    done;

popd
##----------------------------------------------------------------------#
##                                                                      #
##  7. Cleanup                                                          #
##                                                                      #
##----------------------------------------------------------------------#
echo Removing ${build_dir}
rm -Rf ${build_dir}
echo Done. Take result from "${chroot_result_dir}"
