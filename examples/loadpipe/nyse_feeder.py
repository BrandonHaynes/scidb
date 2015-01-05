#!/usr/bin/python

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

'''
Feed historical trade data to stdout at a rate approximating the
actual trade frequency.

The program input is expected to be a TAQ format file of historical
trading data, such as the sample data obtainable at

  http://www.nyxdata.com/Data-Products/NYSE-Trades-EOD

The file format is described by http://www.nyxdata.com/doc/76994 .

(Real-time trading data is probably not formatted in this way, but for
this demo all that matters is that we emit some trade-related data at
time intervals approximating actual trading.)

The raw TAQ data is not suitable for immediate loading insto SciDB, so
this script performs two kinds of data conditioning:

 1. The monotonically increasing SendTime field must be made strictly
    increasing, so that it can be used as an array dimension.  To do
    this, its resolution is changed from milliseconds to microseconds
    and a sequence number is injected as a microsecond interval to
    distinguish trades that are logged at the same millisecond.

 2. We would like to use the stock ticker symbol as an array
    dimension, but SciDB does not (yet!) support string-valued
    dimensions.  We therefore decorate each trade with a symbol_id
    field based on a hash of the stock symbol.  WARNING: This hash
    mapping is likely not robust enough for a real-world application!
'''

import argparse
import csv
import hashlib
import os
import sys
import time
from nyse_tool import sym_to_id

_args = None                    # Globally visible argparse namespace.
_csv_writer = None              # For CSV output formats only (obviously).

index_of = dict()

class Usage(Exception):
    pass

def now_millis():
    return int(time.time() * 1000)

def debug(*args):
    if _args.verbose:
        print >>sys.stderr, ' '.join(map(str, args))

def prt(row, _field_count=[0]):
    '''Print list of fields to output file.'''
    global _csv_writer
    if _csv_writer:
        _csv_writer.writerow(row)
    elif isinstance(row, basestring):
        print >>_args.output, row
    else:
        print >>_args.output, _args.delim.join(map(str, row))

def usecs_since_midnite(last_msm, msm, _seqno_count=[0]):
    '''Strictly increasing microsecond timestamp for single day's trades.

    Time values in the TAQ format input file are expressed in
    milliseconds since midnight ("MSM") on the start of the trading
    day.  The SendTime input field is monotonically increasing, but
    *not* guaranteed unique: several trades can have the same
    SendTime.  (The other TAQ timestamp, SourceTime, is not
    monotonic.)  To use time as a dimension in the desired SciDB
    array, we must therefore append a sequence number to the SendTime
    in what would be the microseconds position.

    Obviously these logical timestamps are not unique across multiple
    trading days, but they are convenient for debugging this demo.  An
    application for processing data from multiple trading days would
    use a timestamp based on "microseconds" (i.e. sequence numbered
    milliseconds) since the epoch.
    '''
    if last_msm is None:
        return msm * 1000  # millis to mikes
    usm = msm * 1000
    if last_msm == msm:
        _seqno_count[0] += 1
        assert _seqno_count[0] < 1000
        usm += _seqno_count[0]
    else:
        _seqno_count[0] = 0
    return usm

def feed_file(F):
    '''Emit trade data at a rate linearly scaled to the actual data rate.

    When the --scale=S option is specified, an n-millisecond interval
    between trades becomes an (S*n)-millisecond interval.
    '''
    SENDTIME = None         # Row index of SendTime field
    MSM_0 = None            # "Millis since midnight" of first trade
    START_TIME = None       # Actual program start time in millis
    prev_msm = None         # MSM of previous trade
    lineno = 0

    # First line provides field names, not data.
    for i, field_name in enumerate(F.readline().strip().split('|')):
        index_of[field_name] = i
    SENDTIME = index_of['SendTime']
    debug("# SendTime in column", SENDTIME)

    # Delay data lines appropriately based on scaling factor.
    for line in F:

        lineno += 1
        if (_args.max_trades > 0 and lineno > _args.max_trades):
            return

        # We need to split the pipe-delimited input to get the
        # inter-transaction delays.
        row = line.strip().split('|')

        # We decorate the row with a computed symbol_id, since (for
        # now) SciDB does not allow string-valued dimensions, and we'd
        # like to have a "by stock" dimension.
        row.append(str(sym_to_id(row[index_of['Symbol']])))

        # We replace SendTime with strictly increasing "microseconds
        # since midnight", see above.
        msm = int(row[SENDTIME])
        row[SENDTIME] = str(usecs_since_midnite(prev_msm, msm))
        prev_msm = msm

        # First trade: set time baseline values and print immediately.
        if START_TIME is None:
            START_TIME = now_millis()
            MSM_0 = msm
            prt(row)
            continue

        if _args.no_delay:
            prt(row)
        else:
            # How many logical msecs along in the trading day is this trade?
            logical_time = msm - MSM_0

            # Scaling that, how long would that be since actual program start?
            ready_time = (logical_time * _args.scale) + START_TIME

            # Wait 'til then if we need to, then print the line.
            now = now_millis()
            if ready_time > now:
                # It's a float, so sleeping at millisecond resolution.
                snooze_secs = float(ready_time - now) / 1000
                debug("# sleep({0})".format(snooze_secs))
                time.sleep(snooze_secs)
            prt(row)

def main(argv=None):
    if argv is None:
        argv = sys.argv

    parser = argparse.ArgumentParser(
        description='''Feed historical trade data to stdout at a rate
            approximating the actual trade frequency.''')
    parser.add_argument('infile', type=argparse.FileType('r'),
                        help='Input file.')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-d', '--delim', default='|',
                       help="Output field delimiter.  (Default = '|')")
    group.add_argument('-c', '--csv', default=False, action='store_true',
                       help='Generate CSV output.')
    parser.add_argument('-m', '--max-trades', default=0, type=int,
                        help='Maximum data lines to process.  (Default = all)')
    parser.add_argument('-o', '--output', default=sys.stdout,
                        type=argparse.FileType('w'),
                        help='Output file. (Default = stdout)')
    parser.add_argument('-v', '--verbose', default=False, action='store_true',
                        help='Print debug info to stderr.')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-s', '--scale', default=1, type=int,
                        help='''
        Time scaling factor. An N-millisecond interval in the input
        file translates to (N*scale) milliseconds of actual elapsed
        time.  (Default = 1 (no scaling))''')
    group.add_argument('-n', '--no-delay', default=False, action='store_true',
                       help='''
        Do not insert delays between trades.  Useful if you just want
        to have a look at the generated output.''')

    global _args
    _args = parser.parse_args(argv[1:])
    if _args.scale < 1:
        raise Usage('Scale factor must be greater than zero.')

    # Buffer the output by line, in case it is a pipe (e.g. a pipe to nc(1)).
    _args.output = os.fdopen(_args.output.fileno(), 'w', 1)

    global _csv_writer
    if _args.csv:
        _csv_writer = csv.writer(_args.output, lineterminator='\n',
                                 quoting=csv.QUOTE_NONNUMERIC)

    ret = 0
    try:
        feed_file(_args.infile)
    except KeyboardInterrupt:
        ret = 1
    return ret

if __name__ == '__main__':
    try:
        sys.exit(main())
    except Usage as e:
        print >>sys.stderr, e
        sys.exit(2)
