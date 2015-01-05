#!/usr/bin/python
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
# This file generage header for embedding SQL script for initializing and upgrading catalog metadata
#

import sys

def main(argc, argv):
    input = []
    output = ''

    argn = 1
    while argn < argc:
        if argv[argn] == '-o':
            output = argv[argn + 1]
            argn += 1
        else:
            input.append(argv[argn])
        argn += 1

    if input == []:
        raise Exception('Input files not defined')

    if output == '':
        raise Exception('Output file not defined')

    outFile = open(output, 'w')

    outFile.write('\n//Do not edit manually!\n')
    outFile.write('//This file was generated automatically by ' + argv[0] + ' \n\n')

    upgradesList = []
    for fileno in range(0, len(input)):
        inFile = open(input[fileno], 'r')
        text = inFile.read()
        inFile.close()

        varName = ('METADATA_UPGRADE_' + str(fileno), 'CURRENT_METADATA')[fileno == 0]

        if fileno:
            upgradesList.append('&'+varName+'[0]');

        outFile.write('const char %s[] = %s;\n' % (varName, StrToHex(text)));

    outFile.write('const char *METADATA_UPGRADES_LIST[] = {0, ' + reduce(lambda x,y:x+', '+y, upgradesList) + '};\n')

    outFile.close()

def StrToHex(s):
    lst = []
    for ch in s:
        hv = hex(ord(ch))
        if len(hv) == 1:
            hv = '0'+hv
        lst.append(hv)
    
    return '{' + reduce(lambda x,y:x+', '+y, lst) + ', 0x0' + '}'

if __name__ == '__main__':
    main(len(sys.argv), sys.argv)
