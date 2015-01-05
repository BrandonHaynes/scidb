#!/bin/bash
set -eu
################################################################
# Global settings
baseurl="https://downloads.paradigm4.com/ubuntu12.04/3rdparty_sources"
upstream_tarball="libcsv_3.0.3.orig.tar.gz"
upstream_dir="libcsv-3.0.3"
result_files="libcsv_3.0.3-1_amd64.deb libcsv_3.0.3-1.dsc libcsv_3.0.3-1.debian.tar.gz"
################################################################
# Functions
function usage()
{
    echo "Usage: $0 <result_dir>"
    exit 1
}
function die()
{
    echo $1
    exit 1
}
################################################################
# Argument processing
[ ! "$#" -eq 1 ] && usage
result_dir="$1"
################################################################
# Temporary files and directories
#   Note that any temp file or directory will be removed on exit
trap on_exit EXIT
function on_exit()
{
    rm -rf ${WORKING_DIR}
}
WORKING_DIR=`mktemp -d "/tmp/${USER}_libcsv_XXXX"`
################################################################
# Local settings
pushd "$(dirname "$0")"
    script_dir="`pwd`"
popd
################################################################
# Prepare
echo Preparing dirs
mkdir -p "${result_dir}" || die
echo build: ${WORKING_DIR}, result: ${result_dir}

echo Downloading source to "${WORKING_DIR}"
cd "${WORKING_DIR}"
wget "${baseurl}/${upstream_tarball}"

echo Untar upstream_tarball
tar -xf ${upstream_tarball}

cd ${upstream_dir}
echo Copy debian directory to "${WORKING_DIR}/${upstream_dir}"
cp -r "${script_dir}"/debian .

echo Build package
debuild -us -uc
################################################################
# Cleanup
cd ../
for file in ${result_files}
do
    cp ${file} "${result_dir}"
done
echo Copied results files "${result_files}" into "${result_dir}".
