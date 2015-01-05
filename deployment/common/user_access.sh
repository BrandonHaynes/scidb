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

user=${1}
key=${2}
OS=`./os_detect.sh`
update=0

function add_public_key ()
{
    local new_key="${1}"
    if [ "0" == `cat ${HOME}/.ssh/authorized_keys | grep "${new_key}" | wc -l || true` ]; then 
	echo "${new_key}" >> ${HOME}/.ssh/authorized_keys
	update=1
    fi;
}

# Otherwise, ssh connect would/can ask about the "adding the host to the known host list"
function disable_host_checking ()
{
    echo "Host *" > ~/.ssh/config
    echo "   StrictHostKeyChecking no" >> ~/.ssh/config
    echo "   UserKnownHostsFile=/dev/null" >> ~/.ssh/config
}

function selinux_home_ssh ()
{
    if [ selinuxenabled ]; then
        chcon -R -v -t user_ssh_home_t ~/.ssh || true
    fi
}

# Update right to ~/.ssh directory
function update_rights ()
{
    disable_host_checking
    chmod go-rwx,u+rwx ${HOME}/.ssh
    chmod a-x,go-rw,u+rw ${HOME}/.ssh/*
    if [ "${OS}" = "CentOS 6" ]; then
	selinux_home_ssh
    fi 

    if [ "${OS}" = "RedHat 6" ]; then
	selinux_home_ssh
    fi
}

mkdir -p ${HOME}/.ssh
private=${HOME}/.ssh/id_rsa
public=${HOME}/.ssh/id_rsa.pub

if [[ ("1" != `ls ${private} | wc -l || true`) || ("1" != `ls ${public} | wc -l || true`)]]; then
    rm -f ${private}
    rm -f ${public}
    echo "" | ssh-keygen -t rsa
fi;

add_public_key "${key}"
update_rights

exit 0
