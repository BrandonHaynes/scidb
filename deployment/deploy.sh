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

set -eu

function print_usage()
{
cat <<EOF

USAGE
  deploy.sh usage - print this usage
  deploy.sh help  - print verbose help

Configuring remote access:
  deploy.sh access  <os_user> <os_user_passwd> <ssh_public_key> <host ...>

Preparing remote machines:
  deploy.sh prepare_toolchain   <host ...>
  deploy.sh prepare_coordinator <host ...>
  deploy.sh setup_ccache        <scidb_os_user> <host ...>
  deploy.sh prepare_chroot      <scidb_os_user> <host ...>
  deploy.sh prepare_postgresql  <postgresql_os_username>
                                <postgresql_os_password>
                                <network/mask>
                                <scidb-coordinator-host>
Building packages:
  deploy.sh build       {Debug|RelWithDebInfo|Release} <packages_path>
  deploy.sh build_fast  <packages_path>
  deploy.sh build_deps  <packages_path>

SciDB control on remote machines:
  deploy.sh scidb_install    {<packages_path>|<ScidbVersion>} <coordinator-host> [host ...]
  deploy.sh scidb_remove     {<packages_path>|<ScidbVersion>} <coordinator-host> [host ...]
  deploy.sh scidb_prepare    <scidb_os_user> <scidb_os_passwd> <db_user> <db_passwd>
                             <database> <base_path>
                             <instance_count> <no_watchdog> <redundancy>
                             <coordinator-dns-host/IP> [host ...]
  deploy.sh scidb_start      <scidb_os_user> <database> <coordinator-host>
  deploy.sh scidb_stop       <scidb_os_user> <database> <coordinator-host>

EOF
}

function print_usage_exit ()
{
print_usage
exit ${1}
}

function print_example ()
{
cat <<EOF

EXAMPLE
Using deploy.sh to set up a development/test environment on local machine (127.0.0.1).

1) Password-less ssh access from localhost to localhost:

sudo su
if [ ! -f ~/.ssh/id_rsa.pub ] ; then  ssh-keygen ; fi #(all defaults or consult ssh manual)
exit
./deploy.sh access root "" 127.0.0.1

2) Install the packages required for building SciDB from sources:

./deploy.sh prepare_toolchain 127.0.0.1

3) Build SciDB packages in the current environment:

cd my_scidb_svn_trunk
mkdir /tmp/my_packages_path
cmake .
make
./deploy.sh build_fast /tmp/my_packages_path

4) Install & configure PostgreSQL:

./deploy.sh prepare_postgresql postgres my_postgres_password 192.168.0.0/24 127.0.0.1

5) Install SciDB packages to a cluster

./deploy.sh scidb_install /tmp/my_packages_path 127.0.0.1

6) Install SciDB release to a cluster

./deploy.sh scidb_install 13.6 coordinator-host host1 host2

7) Configure SciDB cluster on localhost with 4 instances redundancy=1
   and data directory root at ~/scidb-data

./deploy.sh scidb_prepare my_username "" mydb mydb mydb ~/scidb-data 4 1 default 127.0.0.1

8) Start SciDB:

./deploy.sh scidb_start my_username mydb 127.0.0.1

EOF
}

function print_help ()
{
print_usage
cat <<EOF

DESCRIPTION

  deploy.sh can be used to bootstrap a cluster of machines/hosts for building/running SciDB.
  It assumes that its user has the root privileges on all the hosts in the cluster.
  It also requires password-less ssh from the local host to the cluster as root (see access).

  WARNING: the 'expect' tool and the bash shell are required for running deploy.sh
  Ubuntu: apt-get install -y expect
  CentOS/RedHat: yum install -y expect

Commands:
  access               Provide password-less ssh access to each <host ...> for <scidb_os_user> with <ssh_public_key>.
                       do not supply <os_user_passwd> (first '') on the command line, which exposes it via ps(1)
                       and leaves a copy in your shell history file even after logout. The option is for backwards compatibility
                       only.
                       Giving '' for <ssh_public_key> uses ~/.ssh/id_rsa.pub key.

  prepare_toolchain    Install the package dependencies required for building SciDB from sources.
                       The operation is performed on all specified <host ...> as root.

  prepare_coordinator  Install the package dependencies required for testing SciDB on the coordinator node.
                       The operation is performed on all specified <host ...> as root.

  setup_ccache         Configure ccache. This operation is not required for any other deploy.sh operations.
                       The operation is performed on all specified <host ...> as the specified <scidb_os_user>.

  prepare_chroot       Install the package dependencies and tools required to prepare a 'chroot' environment for building SciDB packages.
                       The operation is performed on all specified <host ...> as the specified <scidb_os_user>.

  prepare_postgresql   Install & configure PostgreSQL on <scidb-coordinator-host>.
                       <postgresql_os_username> - OS user for PostgreSQL (commonly used name is 'postgres')
                       <postgresql_os_password> - password for PostgreSQL user
                       <network/mask> - subnet identifier in the CIDR (W.X.Y.Z/N) notation

  build                Build SciDB packages on the local host in a clean (chroot) environment.
                       The build type is either 'Debug','RelWithDebInfo' or 'Release'.
                       Deposit built packages to <packages_path>.

  build_fast           Build SciDB packages on the local host in the user's current environment.
                       Deposit built packages to <packages_path>.
                       This command must be invoked in a build-able SciDB tree
                       (i.e. the tree populated by 'cmake' where 'make' would build SciDB sources)

  build_deps           Build packages for SciDB (3rd party) dependencies on the local host. Deposit built packages to <packages_path>.
                       This command is useful only for populating a package repository (e.g. downloads.paradig4.com)

  scidb_install        Install SciDB packages in <packages_path> on <coordinator-host> and <host ...>.
                       or
                       Install SciDB release <ScidbVersion> on <coordinator-host> and <host ...>.
                       The required repositories for the SciDB packages are expected to be already registered on all hosts.
                       The first host is the cluster coordinator, and some packages are installed only on the coordinator.

  scidb_remove         Remove SciDB packages listed in <packages_path> from <coordinator-host> and <host ...>

  scidb_prepare        Prepare the cluster for running SciDB as <scidb_os_user>. <scidb_os_passwd> should be "" and be supplied on stdin.
                       Supplying passwords on the command line in clear text is a well-known security risk because they can be viewed by
                       other users of the system. The option is only for backwards compatibility.
                       The first host, <coordinator-host>, is the cluster coordinator, and some steps are performed only on that host.
                       The host names must not include 'localhost', use 127.0.0.1 instead.
                       Among other steps, this command generates a config.ini file describing a SciDB database as follows:
                       <database> - SciDB database name
                       <db_user> - PostgreSQL user/role to associated with the SciDB database
                       <db_passwd>  - PostgreSQL user password
                       <base_path> - directory root for SciDB instance data directories
                       <instance_count> - number of instances per host
                       <no_watchdog> - do not start watchdog process (default: 'false')
                       <redundancy> - the number of data replicas (distributed among the instances)
                       Use 'default' for either <redundancy> to keep SciDB defaults.
                       Consult a detailed description of config.ini in the user guide or elsewhere.
                       It will also setup a password-less ssh from <coordinator-host>
                       to *all* hosts using <scidb_os_user> and <scidb_os_passwd>
                       and update <scidb_os_user>'s default PATH in ~<scidb_os_user>/.bashrc

  scidb_start          Start SciDB cluster  <database> as <scidb_os_user> using <coordinator-host>
  scidb_stop           Start SciDB cluster  <database> as <scidb_os_user> using <coordinator-host>

EOF
print_example
}

# detect directory where we run
source_path=${SCIDB_SOURCE_PATH:=$(readlink -f $(dirname $0)/../)}
bin_path=${source_path}/deployment/common
build_path=${SCIDB_BUILD_PATH:=$(pwd)}
echo "Source path: ${source_path}"
echo "Script common path: ${bin_path}"
echo "Build path: ${build_path}"

# If we are in the source tree there is a file ../version with the version number
if [ -f "${source_path}/version" ]; then
    SCIDB_VER=${SCIDB_VERSION:=`awk -F . '{print $1"."$2}' ${source_path}/version`}
else
# If we are in a /opt/scidb/<VER>/deployment tree then ../ is the version number
    SCIDB_VER=`basename ${source_path}`
fi
echo "SciDB version: ${SCIDB_VER}"

SCP="scp -r -q -o StrictHostKeyChecking=no"
SSH="ssh -o StrictHostKeyChecking=no"

# get password for username from stdin
# assign the value to variable password
# if no password given, exit
function get_password()
{
    local username="${1}"

    read -s -p "Enter ${username}'s password (only once):" password
    if [ "${password}" == "" ]; then
       echo "No password given"
       exit 1
    fi
}

# run command on remote host
# if password specified, it would used on password prompt
function remote_no_password ()
{
local username=${1}
local password="${2}"
local hostname=${3}
shift 3
expect <<EOF
log_user 1
set timeout -1
spawn $@
expect {
  "${username}@${hostname}'s password:" { send "${password}\r"; exp_continue }
  eof                                   { }
}
catch wait result
exit [lindex \$result 3]
EOF
if [ $? -ne 0 ]; then
echo "Remote command failed!"
exit 1
fi
}

# Run command on remote host (with some prepared scripts/files)
# 1) copy ./deployment/common to remote host to /tmp/deployment
# 2) (If) specified files would be copied to remote host to /tmp/${username}/deployment
# 3) execute ${4} command on remote host
# 4) remove /tmp/${username}/deployment from remote host
function remote ()
{
local username=${1}
local password="${2}"
local hostname=${3}
local files=${5-""}
remote_no_password "${username}" "${password}" "${hostname}" "${SSH} ${username}@${hostname}  \"rm -rf /tmp/${username}/deployment && mkdir -p /tmp/${username}\""
remote_no_password "${username}" "${password}" "${hostname}" "${SCP} ${bin_path} ${username}@${hostname}:/tmp/${username}/deployment"
if [ -n "${files}" ]; then
    remote_no_password "${username}" "${password}" "${hostname}" "${SCP} ${files} ${username}@${hostname}:/tmp/${username}/deployment"
fi;
remote_no_password "${username}" "${password}" "${hostname}" "${SSH} ${username}@${hostname} \"cd /tmp/${username}/deployment && ${4}\""
remote_no_password "${username}" "${password}" "${hostname}" "${SSH} ${username}@${hostname}  \"rm -rf /tmp/${username}/deployment\""
}

# Provide password-less access to remote host
function provide_password_less_ssh_access ()
{
    local username=${1}
    local password="${2}"
    local key=${3}
    local hostname=${4}
    echo "Provide access by ~/.ssh/id_rsa.pub to ${username}@${hostname}"
    remote "${username}" "${password}" "${hostname}" "./user_access.sh \\\"${username}\\\" \\\"${key}\\\""
}

# create revision file
function revision ()
{
    pushd ${1}
    if [ -d .git ]; then
	echo "Extracting revision from git."
	git svn find-rev master > revision
    elif [ -d .svn ]; then
	echo "Extracting revision from svn."
	svn info|grep Revision|awk '{print $2}'|perl -p -e 's/\n//' > revision
    fi
    popd
}

# Copy source code to remote host to result
function push_source ()
{
    local username=${1}
    local hostname=${2}
    local source_path="${3}"
    local source_name=`basename ${source_path}`
    local remote_path="${4}"
    local remote_name=`basename ${remote_path}`
    echo "Archive the ${source_path} to ${source_path}.tar.gz"
    rm -f ${source_path}.tar.gz
    revision ${source_path}
    (cd ${source_path}/.. && tar -czpf ${source_path}.tar.gz --exclude-vcs ${source_name})
    echo "Remove ${username}@${hostname}:${remote_path}"
    remote_no_password "${username}" "" "${hostname}" "${SSH} ${username}@${hostname} \"rm -rf ${remote_path} && rm -rf ${remote_path}.tar.gz\""
    echo "Copy ${source_path} to ${username}@${hostname}:${remote_path}"
    remote_no_password "${username}" "" "${hostname}" "${SCP} ${source_path}.tar.gz ${username}@${hostname}:${remote_path}.tar.gz"
    echo "Unpack ${remote_path}.tar.gz to ${remote_path}"
    remote_no_password "${username}" "" "${hostname}" "${SSH} ${username}@${hostname} \"cd `dirname ${remote_path}` && tar xf ${remote_name}.tar.gz \""
    if [ "${source_name}" != "${remote_name}" ]; then
        remote_no_password "${username}" "" "${hostname}" "${SSH} ${username}@${hostname} \"cd `dirname ${remote_path}` && mv ${source_name} ${remote_name}\""
    fi;
}

# Configure script for work with rpm/yum
function configure_rpm ()
{
    # build target
    target=centos-6-x86_64
    # package kind
    kind=rpm
    # get package name from filename
    function package_info ()
    {
	rpm -qip ${1} | grep Name | awk '{print $3}'
    }
    # command for remove packages
    remove="yum remove -y"
}

# Configure script for work with deb/apt-get
function configure_deb_1204 ()
{
    # build target
    target=ubuntu-precise-amd64
    # package kind
    kind=deb
    # get package name from filename
    function package_info ()
    {
	dpkg -I ${1} | grep Package | awk '{print $2}'
    }
    # command for remove packages
    remove="apt-get remove -y"
}
function configure_deb_1404 ()
{
    # build target
    target=ubuntu-trusty-amd64
    # package kind
    kind=deb
    # get package name from filename
    function package_info ()
    {
	dpkg -I ${1} | grep Package | awk '{print $2}'
    }
    # command for remove packages
    remove="apt-get remove -y"
}

# Detect hostname OS and configure package manager for with it
# You can restrict work with Red Hat (if you want build packages, for example)
function configure_package_manager ()
{
    local hostname=${1}
    local with_redhat=${2}
    # Get file for detect OS
    FILE=/etc/issue
    if [ "${hostname}" != "localhost" -a "${hostname}" != "127.0.0.1" ]; then
	# grab remote /etc/issue to local file
	remote_no_password root "" "${hostname}" "${SCP} root@${hostname}:/etc/issue ./issue"
	FILE=./issue
    fi;
    # Detech OS
    local OS=`${bin_path}/os_detect.sh ${FILE}`
    if [ "${hostname}" != "localhost" -a "${hostname}" != "127.0.0.1" ]; then
	rm -f ./issue
    fi;
    # Match OS
    case "${OS}" in
	"CentOS 6")
	    configure_rpm
	    ;;
	"RedHat 6")
	    if [ ${with_redhat} == 1 ]; then
		configure_rpm
	    else
		echo "We do not support build SciDB under RedHat 6. Please use CentOS 6 instead"
		exit 1
	    fi;
	    ;;
	"Ubuntu 12.04")
	    configure_deb_1204
	    ;;
	"Ubuntu 14.04")
	    configure_deb_1404
	    ;;
	*)
	    echo "Not supported OS"
	    exit 1;
	    ;;
    esac
}

# Pull/Push packages from/to remote host
function push_and_pull_packages ()
{
    local username=${2}
    local hostname=${3}
    local push=${5}
    configure_package_manager ${hostname} 1
    local path_local=`readlink -f ${1}`
    local path_remote="${4}"
    local scp_args_remote="${username}@${hostname}:${path_remote}/*"
    if [ $push == 1 ]; then
	remote_no_password "${username}" "" "${hostname}" "rm -rf ${path_remote}"
	remote_no_password "${username}" "" "${hostname}" "mkdir -p ${path_remote}"
	remote_no_password "${username}" "" "${hostname}" "${SCP} ${path_local} ${scp_args_remote}"
    else
	rm -rf ${path_local}
	mkdir -p ${path_local}
	remote_no_password "${username}" "" "${hostname}" "${SCP} ${scp_args_remote} ${path_local}"
    fi;
}

# Build packages ("chroot" or "insource")
function build_scidb_packages ()
{
    configure_package_manager "127.0.0.1" 0
    local packages_path=`readlink -f ${1}`
    local way="${2}"
    rm -rf ${packages_path}
    revision ${source_path}
    (cd ${build_path}; ${source_path}/utils/make_packages.sh ${kind} ${way} ${packages_path} ${target})
}

# Setup ccache on remote host
function setup_ccache ()
{
    local username="${1}"
    local password="${2}"
    local hostname=${3}
    remote "${username}" "${password}" ${hostname} "./setup_ccache.sh"
}

# Register 3rdparty SciDB repository on remote host
function register_3rdparty_scidb_repository ()
{
    local hostname=${1}
    echo "Register SciDB 3rdparty repository on ${hostname}"
    remote root "" ${hostname} "./register_3rdparty_scidb_repository.sh"
}

# Register released SciDB repository on remote host
function register_scidb_repository ()
{
    local release=${1}
    local hostname=${2}
    echo "Register SciDB repository ${release} on ${hostname}"
    remote root "" ${hostname} "./register_scidb_repository.sh ${release}"
}

# Stop virtual bridge on remote host
function stop_virtual_bridge_zero ()
{
    local hostname=${1}
    remote root "" ${hostname} "./stop_virbr0.sh"
}

# Install & configure PostgreSQL
function install_and_configure_postgresql ()
{
    local username=${1}
    local password="${2}"
    local network=${3}
    local hostname=${4}
    remote root "" ${hostname} "./configure_postgresql.sh ${username} \\\"${password}\\\" ${network}"
}

# Prepare machine for developer (for build Packages)
function prepare_toolchain ()
{
    local hostname=${1}
    echo "Prepare toolchain @${hostname}"
    register_3rdparty_scidb_repository "${hostname}"
    remote root "" ${hostname} "./prepare_toolchain.sh ${SCIDB_VER}"
    stop_virtual_bridge_zero "${hostname}"
}


# Prepare machine for coordinator
function prepare_coordinator ()
{
    local hostname=${1}
    echo "Prepare coordinator @${hostname}"
    remote root "" ${hostname} "./prepare_coordinator.sh ${SCIDB_VER}"
}

# Prepare chroot on remote machine for build packages

function prepare_chroot ()
{
    local username="${1}"
    local password="${2}"
    local hostname=${3}
    echo "Prepare for build SciDB packages in chroot on ${hostname}"
    register_3rdparty_scidb_repository "${hostname}"
    remote root "" ${hostname} "./prepare_chroot.sh ${username}"
    remote "${username}" "${password}" ${hostname} "./chroot_build.sh" "${source_path}/utils/chroot_build.py"
}

# Get package names from filenames
function package_names()
{
    local filename
    for filename in $@; do
	package_info ${filename}
    done;
}

# Remove SciDB from remote host
function scidb_remove()
{
    local hostname=${2}
    configure_package_manager ${hostname} 1
    local packages_path=`readlink -f ${1}`
    local packages=`ls ${packages_path}/*.${kind} | xargs`
    remote root "" "${hostname}" "${remove} `package_names ${packages} | xargs`"
}

# Remove SciDB Release from remote host
function scidb_remove_release()
{
    local release=${1}
    local hostname=${2}
    local with_coordinator=${3}

    remote root "" "${hostname}" "./scidb_remove_release.sh ${release} ${with_coordinator}"
}

# Install SciDB to remote host from a package directory
function scidb_install()
{
    local hostname=${2}
    local with_coordinator=${3}
    register_3rdparty_scidb_repository "${hostname}"
    configure_package_manager ${hostname} 1
    local packages_path=`readlink -f ${1}`
    local packages
    if [ "1" == "${with_coordinator}" ]; then
	packages="$(ls ${packages_path}/*.${kind} | xargs)"
    else
	packages="$(ls ${packages_path}/*.${kind} | grep -v coord | xargs)"
    fi;
    remote root "" "${hostname}" "./scidb_install.sh" "${packages}"
}

# Install SciDB to remote host from a release on
function scidb_install_release()
{
    local release=${1}
    local hostname=${2}
    local with_coordinator=${3}
    register_scidb_repository "${release}" "${hostname}"
    remote root "" "${hostname}" "./scidb_install_release.sh ${release} ${with_coordinator}"
}

# Generate SciDB config
function scidb_config ()
{
local username="${1}"
local password="${2}"
local database="${3}"
local base_path="${4}"
local instance_count="${5}"
local no_watchdog="${6}"
local redundancy="${7}"
local coordinator="${8}"
shift 8
echo "[${database}]"
local coordinator_instance_count=${instance_count}
let coordinator_instance_count--
echo "server-0=${coordinator},${coordinator_instance_count}"
node_number=1
local hostname
for hostname in $@; do
    echo "server-${node_number}=${hostname},${instance_count}"
    let node_number++
done;
echo "db_user=${username}"
echo "db_passwd=${password}"
if [ "${no_watchdog}" != "default" ]; then
    echo "no-watchdog=${no_watchdog}"
fi;
if [ "${redundancy}" != "default" ]; then
    echo "redundancy=${redundancy}"
fi;
echo "install_root=/opt/scidb/${SCIDB_VER}"
echo "pluginsdir=/opt/scidb/${SCIDB_VER}/lib/scidb/plugins"
echo "logconf=/opt/scidb/${SCIDB_VER}/share/scidb/log4cxx.properties"
echo "base-path=${base_path}"
echo "base-port=1239"
echo "interface=eth0"
}

# Prepare machine for run SciDB (setup environment, generate config file, etc)
function scidb_prepare_node ()
{
    local username="${1}"
    local password="${2}"
    local hostname=${3}
    remote "${username}" "${password}" ${hostname} "./scidb_prepare.sh ${SCIDB_VER}"
    remote root "" ${hostname} "cat config.ini > /opt/scidb/${SCIDB_VER}/etc/config.ini && chown ${username} /opt/scidb/${SCIDB_VER}/etc/config.ini" `readlink -f ./config.ini`
}

# Prepare SciDB cluster
function scidb_prepare ()
{
    local username="${1}"
    local password="${2}"
    local db_user=${3}
    local db_passwd="${4}"
    local database=${5}
    local base_path=${6}
    local instance_count=${7}
    local no_watchdog=${8}
    local redundancy=${9}
    local coordinator=${10}
    shift 10

    # grab coordinator public key
    local coordinator_key=`remote_no_password "${username}" "${password}" "${coordinator}" "${SSH} ${username}@${coordinator}  \"cat ~/.ssh/id_rsa.pub\"" | tail -1`

    # generate config.ini locally
    scidb_config ${db_user} "${db_passwd}" ${database} ${base_path} ${instance_count} ${no_watchdog} ${redundancy} ${coordinator} "$@" | tee ./config.ini

    # deposit config.ini to coordinator

    local hostname
    for hostname in ${coordinator} $@; do
        # generate scidb environment for username
	scidb_prepare_node "${username}" "${password}" ${hostname} # not ideal to modify the environment
	provide_password_less_ssh_access ${username} "${password}" "${coordinator_key}" ${hostname}
    done;
    rm -f ./config.ini
    remote root "" ${coordinator} "./scidb_prepare_coordinator.sh ${username} ${database} ${SCIDB_VER}"
}

# Start SciDB
function scidb_start ()
{
    local username="${1}"
    local password="${2}"
    local database=${3}
    local coordinator=${4}
    remote "${username}" "${password}" ${coordinator} "./scidb_start.sh ${database} ${SCIDB_VER}"
}

# Stop SciDB
function scidb_stop ()
{
    local username="${1}"
    local password="${2}"
    local database=${3}
    local coordinator=${4}
    remote "${username}" "${password}" ${coordinator} "./scidb_stop.sh ${database} ${SCIDB_VER}"
}

# Install & configure Apache (required for CDash on build machines)
function prepare_httpd_cdash ()
{
    local username=${1}
    local build_machine=${2}
    remote root "" ${build_machine} "./prepare_httpd_cdash.sh ${username}"
}

if [ $# -lt 1 ]; then
    print_usage_exit 1
fi

echo "Executing: $@"
echo

case ${1} in
    help)
        if [ $# -gt 2 ]; then
            print_usage_exit 1
        fi
        print_help
        ;;
    usage)
        if [ $# -gt 2 ]; then
            print_usage_exit 1
        fi
        print_usage
        ;;
    access)
	if [ $# -lt 5 ]; then
	    print_usage_exit 1
	fi
	username="${2}"
	password="${3}"
	key="${4}"
	shift 4
	if [ "${key}" == "" ]; then
	    key="`cat ~/.ssh/id_rsa.pub`"
	fi
        if [ "${password}" == "" ]; then
           get_password "${username}"
        fi
	for hostname in $@; do
	    provide_password_less_ssh_access "${username}" "${password}" "${key}" "${hostname}"
	done;
	;;
    push_source)
	if [ $# -lt 4 ]; then
	    print_usage_exit 1
	fi
	username=${2}
	remote_path=${3}
	shift 3
	for hostname in $@; do
	    push_source ${username} ${hostname} ${source_path} ${remote_path}
        done;
	;;
    pull_packages)
	if [ $# -lt 5 ]; then
	    print_usage_exit 1
	fi
	path_local=`readlink -f ${2}`
	username=${3}
	path_remote="${4}"
	shift 4
	for hostname in $@; do
	    push_and_pull_packages ${path_local} ${username} ${hostname} ${path_remote} 0
	done;
	;;
    push_packages)
	if [ $# -lt 5 ]; then
	    print_usage_exit 1
	fi
	path_local=`readlink -f ${2}`
	username=${3}
	path_remote="${4}"
	shift 4
	for hostname in $@; do
	    push_and_pull_packages ${path_local} ${username} ${hostname} ${path_remote} 1
	done;
	;;
    prepare_toolchain)
	if [ $# -lt 2 ]; then
	    print_usage_exit 1
	fi
	shift 1

	for hostname in $@; do
	    prepare_toolchain "${hostname}"
	done;
	;;
    prepare_coordinator)
	if [ $# -lt 2 ]; then
	    print_usage_exit 1
	fi
	shift 1

	for hostname in $@; do
	    prepare_coordinator "${hostname}"
	done;
	;;
    setup_ccache)
	if [ $# -lt 3 ]; then
	    print_usage_exit 1
	fi
	username="${2}"
	shift 2

        # get password from stdin
        get_password "${username}"

	for hostname in $@; do
	    setup_ccache "${username}" "${password}" ${hostname}
	done;
	;;
    prepare_chroot)
	if [ $# -lt 3 ]; then
	    print_usage_exit 1
	fi
	username="${2}"
	shift 2

        # get password from stdin
        get_password "${username}"

	for hostname in $@; do
	    prepare_chroot "${username}" "${password}"  "${hostname}"
	done;
	;;
    prepare_postgresql)
	if [ $# -ne 5 ]; then
	    print_usage_exit 1
	fi
	username=${2}
	password="${3}"
	network=${4}
	hostname=${5}
	install_and_configure_postgresql ${username} "${password}" ${network} ${hostname}
	;;
    build)
	if [ $# -ne 3 ]; then
	    print_usage_exit 1
	fi
	package_build_type=${2}
	packages_path=${3}
	build_scidb_packages "${packages_path}" "chroot ${package_build_type}"
	;;
    build_fast)
	if [ $# -ne 2 ]; then
	    print_usage_exit 1
	fi
	packages_path=${2}
	build_scidb_packages "${packages_path}" "insource"
	;;
    build_deps)
	if [ $# -ne 2 ]; then
	    print_usage_exit 1
	fi
	packages_path=${2}
	echo "TODO build SciDB dependencies packages"
	;;
    scidb_install)
	if [ $# -lt 3 ]; then
	    print_usage_exit 1
	fi
	path_or_ver=${2}
	coordinator=${3}
	echo "Coordinator IP: ${coordinator}"
	shift 3
	if [[ ${path_or_ver} =~ ^[0-9\.]+$ ]]; then
	    # Its an install from release:
	    releaseNum=${path_or_ver}
	    scidb_install_release ${releaseNum} ${coordinator} 1
	    for hostname in $@; do
		scidb_install_release ${releaseNum} ${hostname} 0
	    done;
	else
	    # Its an install from a package directory
	    packages_path=${path_or_ver}
	    scidb_install ${packages_path} ${coordinator} 1
	    for hostname in $@; do
		scidb_install ${packages_path} ${hostname} 0
	    done;
	fi
	;;
    scidb_remove)
	if [ $# -lt 3 ]; then
	    print_usage_exit 1
	fi
	path_or_ver=${2}
	coordinator=${3}
	echo "Coordinator IP: ${coordinator}"
	shift 3
	if [[ ${path_or_ver} =~ ^[0-9\.]+$ ]]; then
	    # Its remove a release:
	    releaseNum=${path_or_ver}
	    scidb_remove_release ${releaseNum} ${coordinator} 1
	    for hostname in $@; do
		scidb_remove_release ${releaseNum} ${hostname} 0
	    done;
	else
	    # Its package remove packages in package directory
	    packages_path=${path_or_ver}
	    scidb_remove ${packages_path} ${coordinator}
	    for hostname in $@; do
		scidb_remove ${packages_path} ${hostname}
	    done;
	fi
	;;
    scidb_prepare)
	if [ $# -lt 11 ]; then
	    print_usage_exit 1
	fi
        username=${2}
        password="${3}"
        db_user=${4}
        db_passwd="${5}"
        database=${6}
        base_path=${7}
        instance_count=${8}
        no_watchdog=${9}
        redundancy=${10}
        coordinator=${11}
        shift 11

        # get password from stdin if not given on cmd
        if [ "${password}" == "" ]; then
           get_password "${username}"
        fi
	scidb_prepare ${username} "${password}" ${db_user} "${db_passwd}" ${database} ${base_path} ${instance_count} ${no_watchdog} ${redundancy} ${coordinator} $@
	;;
    scidb_start)
	if [ $# -lt 4 ]; then
	    print_usage_exit 1
	fi
	username="${2}"
	database=${3}
	coordinator="${4}"
        shift 4

        # get password from stdin
        get_password "${username}"

        scidb_start "${username}" "${password}" ${database} ${coordinator}
	;;
    scidb_stop)
	if [ $# -lt 4 ]; then
	    print_usage_exit 1
	fi
	username="${2}"
	database=${3}
	coordinator="${4}"
        shift 4

        # get password from stdin
        get_password "${username}"

        scidb_stop "${username}" "${password}" ${database} ${coordinator}
	;;
    prepare_httpd_cdash)
	if [ $# -lt 3 ]; then
	    print_usage_exit 1
	fi;
	username=${2}
	shift 2
	for hostname in $@; do
	    prepare_httpd_cdash ${username} ${hostname}
	done;
	;;
    *)
	print_usage_exit 1
	;;
esac
exit 0
