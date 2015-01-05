#!/usr/bin/python
#
# BEGIN_COPYRIGHT
#
# This file is part of SciDB.
# Copyright (C) 2014 SciDB, Inc.
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
"""This script removes all SciDB arrays satisfying given conditions.

@author Donghui Zhang

Assumptions:
  - SciDB is running.
  - 'iquery' is in your path.
"""

#
# Whether to print traceback, upon an exception
#
_print_traceback_upon_exception = True

import argparse
import sys
import re
import traceback
import scidblib
from scidblib import scidb_afl
from scidblib import scidb_psf
        
def main():
    """The main function gets command-line argument as a pattern, and removes all arrays with that pattern.

    @exception AppError if something goes wrong.
    @note If print_traceback_upon_exception (defined at the top of the script) is True,
          stack trace will be printed. This is helpful during debugging.
    """
    parser = argparse.ArgumentParser(
                                     description='Remove all SciDB arrays whose names match a given pattern.',
                                     epilog=
                                     'assumptions:\n' +
                                     '  - iquery is in your path.',
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-f', '--force', action='store_true',
                        help='Forced removal of the arrays without asking for confirmation.')
    parser.add_argument('-c', '--host',
                        help='Host name to be passed to iquery.')
    parser.add_argument('-p', '--port',
                        help='Port number to be passed to iquery.')
    parser.add_argument('-t', '--temp-only', action='store_true',
                        help='Limiting the candidates to temp arrays.')
    parser.add_argument('regex', metavar='REGEX', type=str, nargs='?', default='.*',
                        help='''Regular expression to match against array names.
                        The utility will remove arrays whose names match the regular expression.
                        Default is '.+', meaning to remove all arrays, because the pattern matches all names.
                        The regular expression must match the full array name.
                        For instance, '.*s' will match array 'dogs' because it ends with 's',
                        but will not match array 'moose' because it does not end with 's'.'''
                        )
    args = parser.parse_args()

    try:
        names = scidb_afl.get_array_names(temp_only=args.temp_only)
        iquery_cmd = scidb_afl.get_iquery_cmd(args)
        names_to_remove = []

        for name in names:
            match_name = re.match('^'+args.regex+'$', name)
            if match_name:
                names_to_remove.append(name)

        if not names_to_remove:
            print 'There is no array to remove.'
            sys.exit(0)

        if not args.force:
            print 'The following arrays are about to be removed:'
            print names_to_remove
            proceed = scidb_psf.confirm(prompt='Are you sure you want to remove?', resp=False)
            if not proceed:
                sys.exit(0)

        for name in names_to_remove:
            scidb_afl.afl(iquery_cmd, 'remove('+name+')')

        print 'Number of arrays removed =', len(names_to_remove)
    except Exception, e:
        print >> sys.stderr, '------ Exception -----------------------------'
        print >> sys.stderr, e

        if _print_traceback_upon_exception:
            print >> sys.stderr, '------ Traceback (for debug purpose) ---------'
            traceback.print_exc()

        print >> sys.stderr, '----------------------------------------------'
        sys.exit(-1)  # upon an exception, throw -1

    # normal exit path
    sys.exit(0)

### MAIN
if __name__ == "__main__":
   main()
### end MAIN
