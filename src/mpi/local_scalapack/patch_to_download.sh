#!/bin/bash
################################################################
# Modify all files in installer
# substituting NETLIB_MIRROR for NETLIB_URL
#
# ARGUMENTS
SETUP_DIR=$1
NETLIB_SITE=$2
MIRROR_SITE=$3
################################################################
cd ${SETUP_DIR}
find . -type f -exec sed -i "s@${2}@${3}@g" {} \;
#
# Also if NETLIB_SITE has prefix www. stripit and search for that also
#
find . -type f -exec sed -i "s@${2#www.}@${3#www.}@g" {} \;
