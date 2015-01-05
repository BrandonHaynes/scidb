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
# Script for preparing SciDB catalog database 
#

if [ 4 != $# ]; then
	echo "Usage: $0 owner_name database_name owner's_database_password";
	exit 1
fi

PD_DIR=$(dirname $(readlink -f $0))
owner=$1
database=$2
password=$3
pgPort=$4

function die()
{
    echo "$1" 1>&2
    exit 1
}

function read_yn()
{
    while [ 1 ]; do
        read -p "$1 (y/n): " res
        case "$res" in        
            y|Y) 
                return 0
                ;;
            n|N)
                return 1
                ;;
        esac
    done
}

function user_exists()
{
    for u in $(echo "select u.usename from pg_catalog.pg_user u" | psql -p $pgPort -q postgres|tail -n+3|head -n-2); do
	if [[ "$u" = "$1" ]]; then
	    echo "$u"
	fi 
    done
}

function db_exists()
{
    for d in $(echo "select d.datname from pg_catalog.pg_database d" | psql -p $pgPort -q postgres|tail -n+3|head -n-2); do
	if [[ "$d" = "$1" ]]; then
	    echo "$d"
	fi 
    done
}

function db_adduser()
{
    echo "create role $1 with login password '$2'" | psql -p $pgPort postgres
}

function plpgsql_exists()
{
    if [ "`echo "select count(*) from pg_language where lanname = 'plpgsql'" | psql -p $pgPort $1|tail -n+3|head -n-2`" -eq 1 ]; then
        echo 1
    else
        echo 0
    fi
}

function db_init() 
{
    local owner=$1
    local database=$2
    local password=$3

    createdb --owner "$owner" "$database" || die
    if [ `plpgsql_exists $database` = "0" ]; then
        echo "Creating language plpgsql for database $catalog_name..."
        createlang plpgsql "$database" || die
    fi
    echo "update pg_language set lanpltrusted = true where lanname = 'c'" | psql -p $pgPort "$database" || die
    echo "grant usage on language c to $owner;" | psql -p $pgPort "$database" || die
    
#    export PGPASSWORD=$password
#    echo metadata $metadata
#    psql -h localhost -f "$metadata" -U "$owner" "$database" || die
}

[ "`whoami`" = "postgres" ] || die "You must run this script as owner of PostgreSQL!"

echo "$(db_exists $database) is the result"

if [[ $(db_exists $database) = $database ]]; then
    echo "Deleting $database..."
    psql -p $pgPort postgres -c "drop database $database;" || die
fi

if [[ $(user_exists $owner) != $owner ]]; then
    echo "Adding user $owner..."
    db_adduser $owner $password || die
fi

db_init $owner $database $password

echo Sample of connection string:
echo host=localhost port="$pgPort" dbname="$database" user="$owner" password="$password"

