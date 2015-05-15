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

set -e

function usage()
{
cat <<EOF
Usage: 
    $0 <rpm|deb> insource <result dir> [chroot distro]
    $0 <rpm|deb> <local|chroot> <Debug|RelWithDebInfo> <result dir> [chroot distro]
EOF
    exit 1
}

[ "$#" -lt 3 ] && usage

type=$1
target=$2

case $type in
    deb|rpm);;
    *) usage;;
esac

case $target in
    "insource")
	result_dir=$3
	distro=$4
	;;
    "chroot"|"local")
	[ "$#" -lt 4 ] && usage
	build_type=$3
	result_dir=$4
	distro=${5-""}
	;;
    *) 
	usage
	;;
esac

jobs=$[`getconf _NPROCESSORS_ONLN`+1]


if [ "$target" == "chroot" ]; then
    [ "$#" -lt 4 ] && echo Looks like you forgot chroot distro! Try: centos-6-x86_64 or ubuntu-precise-amd64 or ubuntu-trusty-amd64 && usage
fi

scidb_src_dir=$(readlink -f $(dirname $0)/..)

if [ $target != "insource" ]; then
    build_dir="`pwd`/scidb_packaging"
    build_src_dir="${build_dir}/scidb-sources"
else
    build_dir="`pwd`"
fi

function cleanup()
{
  if [ $target != "insource" ]; then
       echo Removing ${build_dir}
       rm -rf "${build_dir}"
       sudo rm -rf "${build_dir}"
  fi
}

function die()
{
  cleanup
  echo $*
  exit 1
}

cleanup

pushd ${scidb_src_dir}
echo Extracting version
VERSION_MAJOR=`awk -F . '{print $1}' version`
VERSION_MINOR=`awk -F . '{print $2}' version`
VERSION_PATCH=`awk -F . '{print $3}' version`

if [ -d .git ]; then
    echo "Extracting revision from git."
    REVISION=$(git svn find-rev master)
elif [ -d .svn ]; then
    echo "Extracting revision from svn."
    REVISION=$(svn info|grep Revision|awk '{print $2}'|perl -p -e 's/\n//')
elif [ -f revision ]; then
    echo "Extracting revision from file."
    REVISION=$(cat revision)
else
    die "Can not extract source control revision."
fi
popd

echo "Version: $VERSION_MAJOR.$VERSION_MINOR.$VERSION_PATCH"
echo "Revision: $REVISION"

if [ -n "${SCIDB_INSTALL_PREFIX}" ]; then
    export SCIDB_INSTALL_PREFIX
    echo "SciDB installation: ${SCIDB_INSTALL_PREFIX}"
fi

M4="m4 -DVERSION_MAJOR=${VERSION_MAJOR} -DVERSION_MINOR=${VERSION_MINOR} -DVERSION_PATCH=${VERSION_PATCH} -DBUILD=${REVISION}"

if [ $target != "insource" ]; then
    M4="${M4} -DPACKAGE_BUILD_TYPE=${build_type}" || die ${M4} failed
fi

echo Preparing result dir
mkdir -p "${result_dir}" || die Can not create "${result_dir}"
result_dir=`readlink -f "${result_dir}"`

if [ $target != "insource" ]; then
    echo Preparing building dir ${build_dir}
    mkdir -p "${build_dir}" "${build_src_dir}" || die mkdir failed

    pushd ${scidb_src_dir}
    if [ -d .git ]; then
        echo Extracting sources from git
          git archive HEAD | tar -xC "${build_src_dir}"  || die git archive
          git diff HEAD > "${build_src_dir}"/local.patch || die git diff
        pushd "${build_src_dir}"
            (git apply local.patch && rm local.patch) > /dev/null 2>&1
        popd
    elif [ -d .svn ]; then
        echo Extracting sources from svn
        svn export --quiet --force . "${build_src_dir}" || die svn export
	if [ -f revision ]; then
	    cp revision "${build_src_dir}"
	fi
    elif [ -f revision ]; then
	mkdir -p ${build_src_dir} || die mkdir ${build_src_dir}
	cp -a . ${build_src_dir}  || die copy
    else
	die "Can not extract source control revision."
    fi
    popd

fi

if [ "$type" == "deb" ]; then

    debian_dir=$(readlink -f ${scidb_src_dir}/debian)
    [ ! -d ${debian_dir} ] && die Can not find ${debian_dir}

    function deb_prepare_sources ()
    {
        dirSrc="${1}"
        dirTgt="${2}"
        echo Preparing sources from ${dirSrc} to ${dirTgt}
	codename=`echo ${distro}|awk -F- '{print $2}'`
	$M4 ${dirSrc}/"control.${codename}.in" > ${dirTgt}/control || die $M4 failed
	for filename in changelog rules; do
	    $M4 ${dirSrc}/${filename}.in > ${dirTgt}/${filename} || die $M4 failed
	done
        $M4 ${dirSrc}/postinst_in > ${dirTgt}/scidb-${VERSION_MAJOR}.${VERSION_MINOR}-plugins.postinst || die $M4 failed
        $M4 ${dirSrc}/postrm_in > ${dirTgt}/scidb-${VERSION_MAJOR}.${VERSION_MINOR}-plugins.postrm || die $M4 failed
    }
    DSC_FILE_NAME="scidb-${VERSION_MAJOR}.${VERSION_MINOR}_${VERSION_PATCH}-$REVISION.dsc"

    if [ $target != "insource" ]; then
	    deb_prepare_sources ${debian_dir} "${build_src_dir}/debian"

        pushd "${build_src_dir}"
            echo Building source packages in ${build_src_dir}
            dpkg-buildpackage -rfakeroot -S -uc -us || die dpkg-buildpackage failed
        popd

        if [ "$target" == "local" ]; then
            echo Building binary packages locally
            pushd "${build_dir}"
                dpkg-source -x ${DSC_FILE_NAME} scidb-build || die dpkg-source failed
            popd
            pushd "${build_dir}"/scidb-build
                dpkg-buildpackage -rfakeroot -uc -us -j${jobs} || die dpkg-buildpackage failed
            popd
            pushd "${build_dir}"
                echo Moving result from `pwd` to ${result_dir}
                mv *.deb *.dsc *.changes *.tar.gz "${result_dir}" || die mv failed
            popd
        elif [ "$target" == "chroot" ]; then
            echo Building binary packages in chroot
            python ${scidb_src_dir}/utils/chroot_build.py -b -d "${distro}" -r "${result_dir}" -t "${build_dir}" -s "${build_dir}"/${DSC_FILE_NAME} -j${jobs} || die chroot_build.py failed
        fi
    else
        echo Cleaning old packages
        rm -f ${result_dir}/*.deb
        rm -f ${result_dir}/*.changes

        # dpkg-buildpackage wants to have ./debian in the build tree
        build_debian_dir=$(readlink -f ${build_dir}/debian)
        if [ "${build_debian_dir}" != "${debian_dir}" ]; then
           rm -rf ${build_debian_dir}
           cp -r ${debian_dir} ${build_debian_dir} || die cp failed
        fi

        deb_prepare_sources ${build_debian_dir} ${build_debian_dir} 

        echo Building binary packages locally
        pushd ${build_dir}
           BUILD_DIR="${build_dir}" INSOURCE=1 dpkg-buildpackage -rfakeroot -uc -us -b -j${jobs} || die dpkg-buildpackage failed
        popd

        # Apparently, dpkg-buildpackage has to generate .deb files in ../ (go figure ...)
        pushd ${build_dir}/..
           echo Moving result from ${build_dir}/.. to ${result_dir}
           mv *.deb *.changes "${result_dir}" || die mv failed
        popd
    fi
elif [ "$type" == "rpm" ]; then

    scidb_spec=${scidb_src_dir}/scidb.spec.in

    [ ! -f ${scidb_spec} ] && die Can not find ${scidb_spec} file

    function rpm_prepare_sources ()
    {
       dirSrc="${1}"
       dirTgt="${2}"
       echo Preparing sources from ${dirSrc} to ${dirTgt}

       $M4 ${dirSrc}/scidb.spec.in > "${dirTgt}"/scidb.spec || die $M4 failed
    }

    if [ $target != "insource" ]; then
        echo Preparing rpmbuild dirs
        mkdir -p "${build_dir}"/{BUILD,BUILDROOT,RPMS,SOURCES,SPECS,SRPMS} || die mkdir failed

	rpm_prepare_sources ${scidb_src_dir} "${build_dir}/SPECS"

        pushd "${build_src_dir}"
            tar czf ${build_dir}/SOURCES/scidb.tar.gz * || die tar failed
        popd

        echo Building SRPM
        pushd "${build_dir}"/SPECS/
            rpmbuild -D"_topdir ${build_dir}" -bs ./scidb.spec || die rpmbuild failed
        popd

	SCIDB_SRC_RPM=scidb-${VERSION_MAJOR}.${VERSION_MINOR}-${VERSION_PATCH}-$REVISION.src.rpm

        if [ "$target" == "local" ]; then
            echo Building RPM locally
            pushd ${build_dir}/SRPMS
                rpmbuild -D"_topdir ${build_dir}" --rebuild ${SCIDB_SRC_RPM} || die rpmbuild failed
            popd
            echo Moving result from "${build_dir}"/SRPMS and "${build_dir}"/RPMS and to ${result_dir}
            mv "${build_dir}"/SRPMS/*.rpm "${build_dir}"/RPMS/*/*.rpm "${result_dir}" || die mv failed
        elif [ "$target" == "chroot" ]; then
            echo Building RPM in chroot
            python ${scidb_src_dir}/utils/chroot_build.py -b -d "${distro}" -r "${result_dir}" -t "${build_dir}" -s "${build_dir}"/SRPMS/${SCIDB_SRC_RPM} || die chroot_build.py failed
        fi
    else
        echo Cleaning old files from ${build_dir}
        rm -rf ${build_dir}/rpmbuild

	rpm_prepare_sources ${scidb_src_dir} ${build_dir}

        echo Building binary packages insource
        rpmbuild --with insource -D"_topdir ${build_dir}/rpmbuild" -D"_builddir ${build_dir}" -bb ${build_dir}/scidb.spec || die rpmbuild failed

        echo Moving result from ${build_dir} to ${result_dir}
        mv ${build_dir}/rpmbuild/RPMS/*/*.rpm "${result_dir}" || die mv failed

        echo Cleaning files from ${build_dir}
        rm -rf ${build_dir}/rpmbuild

    fi
fi

cleanup

echo Done. Take result packages in ${result_dir}
