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

#
# Script for creating simple APT/YOUM deb repo in current directory
#

#
# Layout of repo should be "distro/release" e.g.
#
# oneiric/
#   redqueen
#   cheshire/
# precise/
#   redqueen
#   cheshire/
#

#
# To use APT repo with such layout you should create file like this:
#
# /etc/apt/sources.list.d/scidb.list
#
# with contents:
#
# deb http://scidb.org/apt/ precise/cheshire/
# deb-src http://scidb.org/apt/ precise/cheshire/
#

function usage
{
    echo "Usage: $0 <apt|yum> <gpg key id> [<distro/release/>]"
    exit 1
}

[ $# -lt 2 ] && usage

repotype="${1,,}"

#
# Check directory if it have files for building repo
# Params:
#   $1 - directory for checking
# Return codes:
#   0 - all ok
#   1 - skip this dir
#
function check_apt_dir
{
    if [ -f "$1/.skip" ]; then
        echo "Found '$1/.skip'. Will not scan '$1'."
        return 1
    fi
    if [ "`find "$1" -maxdepth 1 -type f -iname \*.deb | wc -l`" = 0 ]; then
        echo "ERROR: Can not find .deb files in '$1'. Can not create repository here. Consider delete this directory or create .skip file"
        exit 1
    fi
    return 0
}

function check_yum_dir
{
    if [ -f "$1/.skip" ]; then
        echo "Found '$1/.skip'. Will not scan '$1'."
        return 1
    fi
    if [ "`find "$1" -maxdepth 1 -type f -iname \*.rpm | wc -l`" = 0 ]; then
        echo "ERROR: Can not find .rpm files in '$1'. Can not create repository here. Consider delete this directory or create .skip file"
        exit 1
    fi
    return 0
}

function die
{
    echo "ERROR: Something went wrong in '$1'! Aborting process!"
    exit 1
}

function expect_rpmsign_script
{
cat << ENDOFSCRIPT
spawn rpm --define "_gpg_name $2" --addsign "$1"
expect -exact "Enter pass phrase: "
send -- "Secret passphrase\r"
expect eof
catch wait result
exit [lindex \$result 3]
ENDOFSCRIPT
}

if [ "$3" != "" ]; then
    dirs="$3"
else
    dirs="`ls */*/ -d`"
fi

if [ "$repotype" == "apt" ]; then
    for release_dir in $dirs; do
        echo Checking dir $release_dir
        check_apt_dir "$release_dir"
        if [ "$?" = "0" ]; then
            echo Building repo in "$release_dir"
            echo Cleanup old repo files
            rm -f "$release_dir"/{Packages*,Sources*,*Release*,Contents*}
            echo Scanning files
            dpkg-scanpackages "$release_dir" > "$release_dir"/Packages || die dpkg-scanpackages
            dpkg-scansources "$release_dir" > "$release_dir"/Sources || die dpkg-scansources
            apt-ftparchive contents "$release_dir" > "$release_dir"/Contents || die apt-ftparchive
            echo Codename: `echo $release_dir|sed 's/\/$//'`> "$release_dir"/Release
            apt-ftparchive release "$release_dir" >> "$release_dir"/Release || die apt-ftparchive
            echo Signing repo
            gpg -u "$2" -abs -o "$release_dir"/Release.gpg "$release_dir"/Release || die gpg
            gpg -u "$2" --clearsign -o "$release_dir"/InRelease "$release_dir"/Release || die gpg
        fi
    done
elif [ "$repotype" == "yum" ]; then
    for release_dir in $dirs; do
        echo Checking dir $release_dir
        check_yum_dir "$release_dir"
        if [ "$?" = "0" ]; then
            for f in $release_dir/*.rpm; do
                echo Signing package $f
                expect_rpmsign_script "$f" "$2" | /usr/bin/expect - || die expect_rpmsign_script
            done

            echo Building repo inside $release_dir
            rm -rf "$release_dir/repodata"
            createrepo "$release_dir"
            echo Signing repo $release_dir
            gpg -u "$2" --detach-sign --armor "$release_dir/repodata/repomd.xml"         
        fi
    done
else
    echo Unknown repo type \'$repotype\'
    usage
fi

echo Done! Have a nice day!
