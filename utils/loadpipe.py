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

"""
Incrementally load a SciDB array from stdin.

This script is intended to work with nc(1) (-aka- netcat) to allow
lines of comma- or tab-separated values read from a socket to be
batched up and periodically inserted into a SciDB array.  For example:

    $ nc -d -k -l 8080 | loadpipe.py <options> -- <loadcsv_options>

For information about command line options, run "loadpipe.py --help".

Array Insertion
===============

Loadpipe reads data line by line from stdin and batches it up until
one of the following criteria are met:

 - The number of lines specified by --batch-lines have been read
   from stdin,

 - The number of bytes specified by --batch-size is exceeded,

 - The batch is non-empty and N seconds have elapsed since the last
   'insert'.  (This timeout value is settable with the --timeout
   option.)

When any of those conditions have been met, the loadcsv module is
invoked with '-z IRI' to inject the batched values into the target
array using an "insert(redimension(input(...)))" query.

Signal Handling
===============

The 'loadpipe' script responds to the following signals:

  SIGUSR1 - immediately insert any batched data into the array.
  SIGUSR2 - print a statistics summary to the log.
  SIGTERM - immediately insert batched data into the array and exit.

Open Issues
===========

 - No precaution is taken to prevent an 'insert' from clobbering
   pre-existing array values.

 - How to handle format errors in the input, e.g. records with too few
   or too many fields, field X varying in type from record to record,
   etc.
"""

import argparse
import csv
import errno
import fcntl
import loadcsv                  # from SciDB Python library
import os
import Queue
import select
import signal
import sys
import syslog
import threading
import time
import traceback

# We need the non-C version so that isinstance(x, pyStringIO) works.
from StringIO import StringIO as pyStringIO

_args = None                    # soon to be an argparse.Namespace
_pgm = ''                       # soon to be the program name
_loader = None                  # soon to be the worker thread object

_start_gmtime = time.gmtime()
_start_iso8601 = time.strftime('%FT%TZ', _start_gmtime)

class Usage(Exception):
    """For flagging script usage errors."""
    pass

# A map of (option, argcount) pairs describing which loadcsv options
# should be ignored if passed to us in the loadcsv_args list.
_DISALLOWED_LOADCSV_OPTIONS = {
    '-i': 1                     # Input file is what *I* say it is!
    , '-n': 1                   # In mid-stream, so no skipping.
    , '-X': 0                   # Never remove target array.
    , '-z': 1                   # Loading transform belongs to *me*!

    # These options are always specified by us, but we strip them out
    # so as not to specify them twice.
    , '-x': 0                   # Always same load/shadow arrays; always remove
    # , '-q': 0                 # Not stripped, but added if wanted and missing
    }

def set_nonblock(file_):
    """Set the os.O_NONBLOCK flag on an fd or file object.

    @param file_  file object or file descriptor to modify.
    @return previous value of fcntl(2) flags.
    """
    try:
        fd = file_.fileno()
    except AttributeError:
        fd = file_
    oflags = fcntl.fcntl(fd, fcntl.F_GETFL)
    flags = oflags | os.O_NONBLOCK
    rc = fcntl.fcntl(fd, fcntl.F_SETFL, flags)
    if rc < 0:
        raise Usage('Cannot set non-blocking mode on fd %d' % fd)
    return oflags

_saw_sigusr1 = False
_saw_sigusr2 = False
_saw_sigterm = False

def _sigusr1_handler(signo, frame):
    """Arrange to post the current batch immediately."""
    global _saw_sigusr1
    _saw_sigusr1 = True

def _sigusr2_handler(signo, frame):
    """Arrange to log statistics."""
    global _saw_sigusr2
    _saw_sigusr2 = True

def _sigterm_handler(signo, frame):
    """Prepare to shutdown!"""
    global _saw_sigterm
    _saw_sigterm = True

def check_for_signals():
    """Respond to signals in the main thread's non-signal context.

    @return True iff we detected SIGUSR1, informing the caller that
            the current batch should be posted.
    """
    global _saw_sigusr1, _saw_sigusr2, _saw_sigterm
    ret = False
    if _saw_sigusr1:
        # By returning True here the current batch will be posted.
        debuglog("saw sigusr1")
        ret = True
        _saw_sigusr1 = False
    if _saw_sigusr2:
        log_statistics()
        _saw_sigusr2 = False
    if _saw_sigterm:
        critlog('Caught SIGTERM, shutting down')
        raise KeyboardInterrupt('Caught SIGTERM')
    return ret

def number(s):
    """Convert a string to a number, allowing suffixes like KiB, M, etc.

    Currently only useful for integers (we had high aspirations).

    @param s    string to be converted.
    @return integer value of string.
    """
    # Doesn't handle negative values very consistently, oh well.
    assert isinstance(s, basestring)
    sufat = None
    for i, ch in enumerate(s):
        if not ch.isdigit():
            sufat = i
            break
    if not sufat:
        return int(s)
    num = int(s[0:sufat])
    suffix = s[sufat:].strip().lower()
    if suffix in ('k', 'kb', 'kib'):
        return num * 1024
    if suffix in ('m', 'mb', 'mib'):
        return num * 1024 * 1024
    if suffix in ('g', 'gb', 'gib'):
        return num * 1024 * 1024 * 1024
    raise Usage('Unrecognized numeric suffix "{0}"'.format(suffix))

# For argparse below.
_facility_choices = ['kern', 'user', 'mail', 'daemon', 'auth', 'lpr', 'news',
                     'uucp', 'cron', 'local0', 'local1', 'local2', 'local3',
                     'local4', 'local5', 'local6', 'local7']

# Short strings for logging levels.
_strlevel = {
    syslog.LOG_DEBUG: "DEBG",
    syslog.LOG_INFO: "INFO",
    syslog.LOG_NOTICE: "NOTE",
    syslog.LOG_WARNING: "WARN",
    syslog.LOG_ERR: "ERR ",
    syslog.LOG_CRIT: "CRIT"
}

# Wrappers for syslog(3) logging.
def debuglog(*args):
    return _log(syslog.LOG_DEBUG, args)
def infolog(*args):
    return _log(syslog.LOG_INFO, args)
def notelog(*args):
    return _log(syslog.LOG_NOTICE, args)
def warnlog(*args):
    return _log(syslog.LOG_WARNING, args)
def errlog(*args):
    return _log(syslog.LOG_ERR, args)
def critlog(*args):
    return _log(syslog.LOG_CRIT, args)
def _log(level, *args):
    tname = threading.current_thread().name[:4]
    msg = ' '.join((tname, _strlevel[level], ' '.join(map(str, *args))))
    syslog.syslog(level, msg)
    if _args.verbosity > 2:
        utc = time.strftime('%FT%TZ', time.gmtime())
        print utc, _pgm[:9], msg
    return None

def setup_logging(_initialized=[False]):
    """Initialize logging infrastructure."""
    # Takes no arguments; see http://effbot.org/zone/default-values.htm
    if _initialized[0]:
        syslog.closelog()
    opts = syslog.LOG_PID | syslog.LOG_NDELAY
    facility = syslog.LOG_LOCAL0
    if _args.facility:
        fac_name = ''.join(('LOG_', _args.facility.upper()))
        # We can assert this because we limit choices to _facilities_choices.
        assert fac_name in dir(syslog)
        facility = syslog.__dict__[fac_name]
    syslog.openlog(_pgm[:8], opts, facility)
    if _args.verbosity == 0:
        syslog.setlogmask(syslog.LOG_UPTO(syslog.LOG_NOTICE))
    if _args.verbosity == 1:
        syslog.setlogmask(syslog.LOG_UPTO(syslog.LOG_INFO))
    elif _args.verbosity > 1:
        syslog.setlogmask(syslog.LOG_UPTO(syslog.LOG_DEBUG))
    _initialized[0] = True
    return None

def preen_loadcsv_args(args):
    """Drop all disallowed loadcsv arguments from an argument list.

    @param args a list of loadcsv.py options and arguments.
    @return preened list of permitted loadcsv.py options and arguments.
    """
    skip = 0
    result = []
    if args and args[0] == '--':
        args = args[1:]
    for arg in args:
        if skip:
            skip -= 1
            continue
        if arg in _DISALLOWED_LOADCSV_OPTIONS:
            skip = _DISALLOWED_LOADCSV_OPTIONS[arg]
            warnlog('Option "{0}" and {1} arguments ignored'.format(arg, skip))
        else:
            result.append(arg)
    return result

def log_statistics():
    """Write a statistics summary to the log."""
    global _loader
    stats = [Batch.get_statistics()]
    if _loader is not None:
        stats.extend(_loader.get_statistics())
    notelog('=== Statistics since %s' % _start_iso8601)
    notelog('Created %d batches, %d bytes, %d total lines' % stats[0])
    if _loader is not None:
        notelog('Loaded %d batches, %d bytes, %d total lines' % stats[1])
        notelog('Failed %d batches, %d bytes, %d total lines' % stats[2])
    notelog('=== End statistics summary')
    return None

class Batch(object):

    """Hoard lines of input until they are ready to be posted."""

    # Class parameters.
    max_lines = 0
    max_bytes = 0

    # Class statistics.
    total_batches = 0
    total_bytes = 0
    total_lines = 0

    def __init__(self):
        """Create an empty Batch object."""
        self.nbytes = 0        # bytes in batch so far
        self.nlines = 0        # lines in batch so far
        self.data = pyStringIO()
        Batch.total_batches += 1     # sequentially number these
        self.batchno = Batch.total_batches
        self.csv_writer = csv.writer(self.data) if _args.delim else None

    @classmethod
    def set_max_lines(cls, n):
        """Set the maximum number of lines a Batch may contain."""
        if n < 0:
            raise Usage('--batch-lines value must be >= 0')
        cls.max_lines = n

    @classmethod
    def set_max_bytes(cls, n):
        """Set the (approximate) maximum size in bytes of a Batch."""
        if n < 0:
            raise Usage('--max-bytes value must be >= 0')
        cls.max_bytes = n

    @classmethod
    def get_statistics(cls):
        """Get batch-related statistics.

        @return a tuple of (total_batches, total_bytes, total_lines).
        """
        return cls.total_batches, cls.total_bytes, cls.total_lines

    def full(self):
        """Return True iff batch exceeds the size or line count threshold."""
        if Batch.max_lines and self.nlines >= Batch.max_lines:
            return True
        if Batch.max_bytes and self.nbytes > Batch.max_bytes:
            return True
        return False

    def empty(self):
        """Return True iff the batch is empty."""
        return self.nbytes == 0

    def size(self):
        """Return number of bytes in this batch."""
        return self.nbytes

    def lines(self):
        """Return number of lines in this batch."""
        return self.nlines

    def add(self, data_line):
        """Add a datum to the batch."""
        self.nbytes += len(data_line)
        Batch.total_bytes += len(data_line)
        self.nlines += 1
        Batch.total_lines += 1
        if self.csv_writer:
            self.csv_writer.writerow(data_line.rstrip().split(_args.delim))
        else:
            self.data.write(data_line)

    def get_file(self):
        """Return a StringIO object containing the batch data."""
        self.data.seek(0)       # Get ready for reads.
        return self.data

    def __repr__(self):
        """Return a string representation for logging purposes."""
        # See http://stackoverflow.com/questions/1436703/difference-between-str-and-repr-in-python
        return '%s(id=%d, bytes=%d, lines=%d)' % (
            self.__class__.__name__, self.batchno, self.nbytes, self.nlines)

class CsvLoaderThread(threading.Thread):

    """Worker thread, periodically calls loadcsv.py with a Batch of input."""

    max_queue = 0

    @classmethod
    def set_max_queue(cls, n):
        """Set upper limit on batch queue size.

        Must be called before the worker thread instance is created.
        """
        if n < 0:
            raise Usage('--max-queue must be >= 0')
        cls.max_queue = n
        return None

    def __init__(self):
        """Initialize a worker thread."""
        threading.Thread.__init__(self, name='CsvLoader')
        self.queue = Queue.Queue(maxsize=CsvLoaderThread.max_queue)
        self.failure = [0, 0, 0] # batches, bytes, lines
        self.success = [0, 0, 0] # ditto (alpha ordered!)

    def post(self, batch):
        """Post a batch of data to the work queue.

        Intended to be called from the main thread only.
        @param batch object to be sent to worker, @i usually a Batch
        """
        # Simple if unlimited queue or no debug logging!
        if CsvLoaderThread.max_queue == 0 or _args.verbosity < 2:
            self.queue.put(batch)
        # Tricky if we have a queue limit and debug logging is enabled.
        try:
            self.queue.put(batch, block=False)
        except Queue.Full:
            debuglog("Blocked on full worker queue (len=%d)" %
                     CsvLoaderThread.max_queue)
            self.queue.put(batch)
            debuglog("Unblocked, worker queue size %d" % self.queue.qsize())

    def run(self):
        """Call loadcsv.py for each queued Batch.

        This is the worker thread's service loop.
        """
        notelog(self.name, 'worker thread running')
        while True:
            batch = self.queue.get()
            if isinstance(batch, basestring):
                notelog('Command string:', batch)
                break
            if self._call_loadcsv(batch):
                self.success[0] += 1
                self.success[1] += batch.size()
                self.success[2] += batch.lines()
            else:
                self.failure[0] += 1
                self.failure[1] += batch.size()
                self.failure[2] += batch.lines()
            del batch
        notelog(self.name, 'worker thread exiting')
        return None

    def get_statistics(self):
        """Return statistics for posted batches.

        Does not perform any locking, oh well.

        @return a 2-tuple of (total_batches, total_bytes, total_lines)
                3-tuples: the first 3-tuple is for successfully loaded
                batches; the second is for failed batches.  (The same
                kind of 3-tuple is returned by Batch.get_statistics().)
        """
        return (tuple(self.success), tuple(self.failure))

    def _call_loadcsv(self, batch):
        """Invoke loadcsv.py script to insert this batch of data.

        Returns True on success, False on failure.
        """
        cmd = ['loadcsv',       # argv[0]
               '-x',            # remove last batch from load array
               '-z', 'IRI',     # we want insert(redim(input(...))) behavior
               '-i', batch.get_file() ] # input is batch's StringIO object!
        cmd.extend(_args.loadcsv_args)
        if os.environ.has_key('IQUERY_HOST'):
            cmd.extend(['-d', os.environ['IQUERY_HOST']])
        if os.environ.has_key('IQUERY_PORT'):
            cmd.extend(['-p', os.environ['IQUERY_PORT']])
        if not _args.verbosity and '-q' not in cmd:
            cmd.append('-q')
        infolog(batch, 'load:', ' '.join(map(str, cmd)))
        if _args.no_load:
            infolog("Option --no-load in effect, all done.")
            return True

        # XXX It would be nice to capture loadcsv stdout/stderr in
        # StringIOs since it's a very chatty program.  Unfortunately
        # loadcsv makes a lot of calls (mostly Popen-related) that
        # assume these are real files with file descriptors, so that's
        # not possible just yet.  Too bad, because in case of error
        # we'd really like that stuff to appear in the log file.

        try:
            rc = loadcsv.main(cmd)
        except Exception as e:
            errlog('Exception escaped from loadcsv.main(): %s' % str(e))
            for line in traceback.format_exc().split('\n'):
                errlog(line)
            rc = 42             # ...the most random return code

        if rc == 0:
            infolog("Done loading %s, %d failures so far" % (
                    batch, self.failure[0]))
            return True

        errlog('loadcsv.main() exited with status', rc)
        errlog('Problems with batch', batch)
        if _args.verbosity > 3: # batch could be large...
            debuglog('--- begin bad %s ---' % str(batch))
            for i, line in enumerate(batch.get_file()):
                debuglog('[%d] %s' % (i, line.rstrip()))
            debuglog('--- end bad batch ---')
        return False

def get_next_line(file_, tmo_seconds):
    """A generator that produces lines, timeouts, or EOF.

    @param file_        a readable file object.
    @param tmo_seconds  timeout value in seconds.

    @retval '' (an empty string) indicates a timeout.
    @retval None indicates EOF.
    @retval <data> a full line of data.
    """
    poll = select.poll()
    set_nonblock(file_)
    poll.register(file_.fileno())
    partial_line = ''
    while True:
        try:
            ready = poll.poll(tmo_seconds *  1000) # millis
        except select.error as e:
            if e[0] == errno.EINTR:
                yield ''        # pretend timeout allows for signal checking
                continue
            raise
        if not ready:
            yield ''            # timeout
        else:
            while True:
                try:
                    buf = file_.read(4096)
                except IOError as e:
                    if e.errno == errno.EWOULDBLOCK:
                        break
                    raise
                if not buf:
                    debuglog("eof on input")
                    yield None
                else:
                    lines = buf.split('\n')
                    start = 0
                    if partial_line:
                        yield ''.join((partial_line, lines[0], '\n'))
                        partial_line = ''
                        start = 1
                    for line in lines[start:-1]:
                        yield ''.join((line, '\n'))
                    partial_line = lines[-1]
                    if _args.verbosity > 3 and partial_line:
                        debuglog('Partial line: %s' % partial_line)

def load_stream_loop():
    """Read lines from stdin, batch them up, and call loadcsv.py.

    Lines are batched until either (a) the --batch-lines threshold is
    exceeded, (b) the --batch-size threshold is exceeded, or (c) the
    timeout has expired on a non-empty batch.  Then post the batch to
    the CsvLoaderThread, where loadcsv.main() will be called.

    @retval 0 EOF was reached on input.
    @retval 1 Terminated by signal.
    """
    global _loader
    _loader = CsvLoaderThread()
    _loader.start()
    ret = 0
    batch = Batch()
    try:
        for line in get_next_line(sys.stdin, _args.timeout):
            if check_for_signals() and not batch.empty():
                debuglog("post on signal:", batch)
                _loader.post(batch)
                batch = Batch()
            if line is None:
                # eof
                if batch.empty():
                    del batch
                else:
                    # Last one!
                    debuglog("post final", batch)
                    _loader.post(batch)
                    batch = None
                break
            if not line:
                # timeout
                if not batch.empty():
                    # Timeout expired.
                    debuglog("timeout posts", batch)
                    _loader.post(batch)
                    batch = Batch()
            else:
                #debuglog("read:", line.rstrip()) # boring
                batch.add(line)
                if batch.full():
                    debuglog("post full", batch)
                    _loader.post(batch)
                    batch = Batch()
    except KeyboardInterrupt:
        ret = 1
    # Wait for worker thread to finish.
    _loader.post("Goodbye")
    _loader.join()
    notelog("Farewell")
    return ret

def main(argv=None):
    if argv is None:
        argv = sys.argv

    global _pgm
    _pgm = os.path.basename(argv[0])

    # Long options only, to avoid overlap with the options provided by
    # loadcsv.py (for good or ill that script uses only short
    # options).  Better to not use short options at all than be forced
    # to use non-intuitive short options.
    parser = argparse.ArgumentParser(
        description='Incrementally load a SciDB array from stdin.',
        epilog="Type 'pydoc {0}' for more information.".format(_pgm))
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--batch-size', default='16KiB', help='''
        Insert into array when buffered data reaches this size.  Multiplier
        suffixes are understood, e.g. 64K, 10MiB, 2G.  (Default = 16KiB)
        ''')
    group.add_argument('--batch-lines', default=0, type=int, help='''
        Insert into array whenever this many input lines have been batched.''')
    parser.add_argument('--log', dest='facility', default='local0',
                        choices=_facility_choices, help='''
        Specify syslog(3) logging facility. (Default = local0)''')
    parser.add_argument('--no-load', default=False, action='store_true',
                        help='''
        Do not actually call loadcsv.  Used for debugging.''')
    parser.add_argument('--delim', default=None,
                        help='''
        Input field delimiter.  By default, input is assumed to be in
        a CSV format acceptable to loadcsv.py.  If this option is set,
        however, input lines are split on the specified delimiter and
        then reassembled using the Python csv module.''')
    parser.add_argument('--max-queue', type=int, default=0, help='''
        Never let array loading fall behind by more than this many batches.
        Reading from stdin ceases until array insertion catches up.
        (Default = 0 (unlimited queue size))''')
    parser.add_argument('--timeout', type=int, default=10, help='''
        Maximum time to wait (in seconds) before performing an 'insert'
        of batched data.''')
    # Our one conflict with loadcsv options: short -v means increase log level.
    parser.add_argument('-v', '--verbosity', default=0, action='count',
                    help='Increase debug logging. 1=info, 2=debug, 3=debug+')
    parser.add_argument('loadcsv_args', nargs=argparse.REMAINDER, help='''
        Unrecognized options and positional arguments are passed
        through when calling loadcsv.py .  If the first argument to be
        passed to loadcsv begins with '-' (and it almost always does),
        use '--' as the first pass-thru argument to mark the beginning
        of loadcsv options.
        ''')

    global _args
    _args = parser.parse_args(argv[1:])

    signal.signal(signal.SIGUSR1, _sigusr1_handler)
    signal.signal(signal.SIGUSR2, _sigusr2_handler)
    signal.signal(signal.SIGTERM, _sigterm_handler)
    setup_logging()

    Batch.set_max_lines(_args.batch_lines)
    Batch.set_max_bytes(number(_args.batch_size))
    CsvLoaderThread.set_max_queue(_args.max_queue)
    _args.loadcsv_args = preen_loadcsv_args(_args.loadcsv_args)

    notelog(' '.join(('Starting loadpipe, max batch %s, max batch lines %d,',
                      'timeout %ds, max queue %d')) % (
           _args.batch_size, _args.batch_lines, _args.timeout, _args.max_queue))
    infolog('Info logging is enabled (verbosity=%d)' % _args.verbosity)
    debuglog('Debug logging is enabled')

    return load_stream_loop()

if __name__ == '__main__':
    try:
        sys.exit(main())
    except Usage as e:
        print >>sys.stderr, e
        sys.exit(2)
