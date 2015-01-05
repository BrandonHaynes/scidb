#!/usr/bin/python

'''
Analyze sample historical data from ftp.nyxdata.com.

The data format is described by http://www.nyxdata.com/doc/76994 .

Also provides some utility functions to the rest of the demo.
'''

import argparse
import os
import sys
import hashlib

DEFAULT_INFILE = os.path.expanduser('~/Downloads/nysetrades20130404')
index_of = {
    'MsgSeqNum': 0,
    'MsgType': 1,
    'OriginalTradeRefNum': 2,
    'SourceSeqNum': 3,
    'SourceSessionId': 4,
    'SendTime': 5,
    'SourceTime': 6,
    'Symbol': 7,
    'PriceNumerator': 8,
    'PriceScaleCode': 9,
    'Volume': 10,
    'ExchangeID': 11,
    'SecurityType': 12,
    'LinkID': 13,
    'TradeCond1': 14,
    'TradeCond2': 15,
    'TradeCond3': 16,
    'TradeCond4': 17,
    'SymbolID': 18          # Added by nyse_feeder.py, not part of TAQ format
}

_args = None
g_lineno = 0

class Usage(Exception):
    pass

def sym_to_id(sym):
    '''Generate numeric id based on hash of modified ticker symbol.
    Crude, but effective for our purposes.

    WARNING: When working with a full list of NYSE ticker symbols, you
    will need *at least* one more digit from the hexdigest to avoid
    collisions.  This "truncated hash" approach is probably not best
    for real applications; the symbol_id dimension of your array is
    likely too sparse.

    A helpful article on doing this sort of thing for real can be
    found at: http://www.scidb.org/forum/viewtopic.php?f=18&t=1280
    '''
    #return int(hashlib.sha1(sym).hexdigest()[0:7], 16) # nope, collisions
    #return int(hashlib.md5(sym).hexdigest()[0:6], 16)  # nope, collisions
    #return int(hashlib.sha1('%{0}&{0}!{0}@'.format(sym)).hexdigest()[0:5], 16) # nope, collisions
    return int(hashlib.sha1('%'.join((sym,sym,sym))).hexdigest()[0:5], 16) # Alright!!!

class SymCounter(object):
    '''Count number of times a stock symbol appears in the input file,
    and print symbol and count sorted by count.'''
    def __init__(self, by_count=True):
        self.sym2count = dict()
        self.maxlen = 0
        if by_count:
            self.sort_key = lambda x: x[1]
        else:
            self.sort_key = lambda x: x[0]
    def analyze(self, row):
        global index_of
        sym = row[index_of['Symbol']]
        if len(sym) > self.maxlen:
            self.maxlen = len(sym)
        try:
            self.sym2count[sym] += 1
        except KeyError:
            self.sym2count[sym] = 1
    def summarize(self):
        print '=== Symbol counts ==='
        for sym, cnt in sorted(self.sym2count.items(), key=self.sort_key):
            print sym, (' ' * (self.maxlen - len(sym))), ':', cnt

class Partitioner(object):
    '''Partition ticker symbols into two sets, each with a roughly
    equal number of trades.'''
    def __init__(self):
        self.sym2count = dict()
        self.group = [[], []]
        self.count = [0, 0]
    def analyze(self, row):
        '''Count trades by symbol.'''
        global index_of
        sym = row[index_of['Symbol']]
        try:
            self.sym2count[sym] += 1
        except KeyError:
            self.sym2count[sym] = 1
    def summarize(self):
        '''Parition ticker symbols into groups.'''
        for sym, cnt in sorted(self.sym2count.items(),
                               reverse=True,
                               key=lambda x: x[1]):
            if self.count[0] < self.count[1]:
                self.group[0].append(sym)
                self.count[0] += cnt
            else:
                self.group[1].append(sym)
                self.count[1] += cnt
        for grp in (0, 1):
            print 'group%d = [\t\t# %d total trades' % (grp, self.count[grp])
            for sym in self.group[grp]:
                print '\t"%s",\t\t# %d trades' % (sym, self.sym2count[sym])
            print ']'

class IdAssignment(object):
    '''Test the sym_to_id function on our data: does it produce collisions?'''
    def __init__(self, by_count=True):
        self.id2sym = dict()
    def analyze(self, row):
        global index_of
        sym = row[index_of['Symbol']]
        sym_id = sym_to_id(sym)
        if sym_id in self.id2sym:
            if sym not in self.id2sym[sym_id]:
                # Collision, sigh.
                self.id2sym[sym_id].append(sym)
        else:
            self.id2sym[sym_id] = [sym]
    def summarize(self):
        print '=== Id conflicts ==='
        collisions = 0
        for sym_id in self.id2sym:
            if len(self.id2sym[sym_id]) > 1:
                print sym_id, '-->', self.id2sym[sym_id]
                collisions += 1
        print '===', collisions, 'collisions ==='

class TimeWarpCounter(object):
    '''Identify places where SendTime jumps backwards.'''
    def __init__(self):
        self.line_nbr = 0
        self.prev_send_msm = 0       # msm - millis since midnight
        self.send_warps = []
        self.prev_src_msm = 0       # msm - millis since midnight
        self.src_warps = []
    def analyze(self, row):
        global index_of
        self.line_nbr += 1

        # Check for SendTime time warps.
        msm = int(row[index_of['SendTime']])
        if msm > self.prev_send_msm:
            self.prev_send_msm = msm
        elif msm < self.prev_send_msm:
            self.send_warps.append(self.line_nbr)

        # Check for SourceTime time warps.
        msm = int(row[index_of['SourceTime']])
        if msm > self.prev_src_msm:
            self.prev_src_msm = msm
        elif msm < self.prev_src_msm:
            self.src_warps.append(self.line_nbr)

    def summarize(self):
        NUM_PRINT = 5
        print '===', len(self.send_warps), 'SendTime warps ==='
        for i in xrange(0, min(NUM_PRINT, len(self.send_warps))):
            print 'Line', self.send_warps[i]
        if len(self.send_warps) > NUM_PRINT:
            print '...etc...'
        print '===', len(self.src_warps), 'SourceTime warps ==='
        for i in xrange(0, min(NUM_PRINT, len(self.src_warps))):
            print 'Line', self.src_warps[i]
        if len(self.src_warps) > NUM_PRINT:
            print '...etc...'

class TypeFlipFlop(object):
    '''Check whether the type of any field appears to flip-flop.'''
    def __init__(self):
        self.ftypes = []
        self.nullable = []
        self.flips = 0
    @staticmethod
    def type_of(field):
        if not field:
            return None
        elif field.isdigit():
            val = int(field)
            return 'int64' if val > ((2 ** 32) - 1) else 'int32'
        else:
            return 'string'
    def analyze(self, row):
        global index_of
        if not self.ftypes:
            # First row, set baseline types.
            for field in row:
                typ = self.type_of(field)
                self.ftypes.append(typ)
                self.nullable.append(typ is None)
            assert len(self.ftypes) == len(self.nullable)
        else:
            for i, field in enumerate(row):
                typ = self.type_of(field)
                if typ is None:
                    if not self.nullable[i]:
                        print 'Field', i, 'became nullable at line', g_lineno
                        self.nullable[i] = True
                        self.flips += 1
                elif typ != self.ftypes[i]:
                    if typ == 'int64' and self.ftypes[i] == 'int32':
                        self.ftypes[i] = typ
                    elif type == 'int32' and self.ftypes[i] == 'int64':
                        pass
                    else:
                        print 'Field %d flipped from %s to %s at line %d' % (
                            i, self.ftypes[i], typ, g_lineno)
                        self.ftypes[i] = typ
                        self.flips += 1
    def summarize(self):
        print '===', self.flips, 'type flips ==='

analyzers = {
    "warp": TimeWarpCounter(),
    "syms": SymCounter(by_count=False), # by name; long output
    "syms-by-count": SymCounter(by_count=True), # long output
    "hash-ids": IdAssignment(),
    "type-flip-check": TypeFlipFlop(),
    "partition": Partitioner()
}

def analyze(F):
    global _args, g_lineno
    # First line provides field names, not data.
    if _args.header_line:
        for i, field_name in enumerate(F.readline().strip().split('|')):
            index_of[field_name] = i
        g_lineno = 1
    # Build list of selected analyzers.
    if 'all' == _args.analyzer:
        ana_list = sorted(analyzers.keys())
    else:
        ana_list = [_args.analyzer]
    # Pass each data line to analyzers.
    for line in F:
        g_lineno += 1
        row = line.strip().split('|')
        for ana in ana_list:
            analyzers[ana].analyze(row)
    # Analyzers, report your findings!
    for ana in ana_list:
        analyzers[ana].summarize()

def main(argv=None):
    if argv is None:
        argv = sys.argv

    global _args, g_lineno

    ana_list = ['all']
    ana_list.extend(sorted(analyzers.keys()))

    parser = argparse.ArgumentParser()
    parser.add_argument('-a', '--analyze', dest='analyzer', choices=ana_list,
                        help='Analyzer to run.')
    parser.add_argument('-H', '--header', dest='header_line', default=False,
                        action='store_true',
                        help='Specify this if input has a header line.')
    parser.add_argument('-i', '--symbol-id', dest='symbols', action='append',
                        help='Print symbol_id value for ticker symbol.')
    parser.add_argument('taqfile', nargs='*', type=argparse.FileType('r'),
                        help='Print analysis of the input TAQ file.')
    _args = parser.parse_args(argv[1:])

    if _args.symbols:
        for sym in _args.symbols:
            s = sym.upper()
            print s, sym_to_id(s)
    elif _args.taqfile:
        for fil in _args.taqfile:
            analyze(fil)
    else:
        parser.print_help()

if __name__ == '__main__':
    try:
        sys.exit(main())
    except Usage as e:
        print >>sys.stderr, e
        sys.exit(2)
