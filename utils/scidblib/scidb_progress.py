#!/usr/bin/env python

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

import sys
import os
import datetime
import re
import scidblib

def datetime_as_str(the_datetime = None, format = '%Y-%m-%d %H:%M:%S'):
    """Generate a string representation of a datetime object.

    @param the_datetime  an object of type datetime.datetime.
    @param format        the desired format of the generated string.
    """
    if the_datetime == None:
        the_datetime = datetime.datetime.now()
    return the_datetime.strftime(format)

def timedelta_total_seconds(timedelta):
    """Get total_seconds out of a timedelta object.
    
    @param timedelta  an object of type datatime.timedelta.
    @return total_seconds.
    @note timedelta.total_seconds() is supported since Python 2.7.
    """
    return timedelta.seconds + (timedelta.days * 24 * 3600) + (timedelta.microseconds*0.000001)

class VersionAndDate:
    """A class that makes it easy to parse, generate, or compare version and date.

    @example the last modification version/date of a script may be '14.6.7400 (2014-6-17)'

    Details of public attributes:
      - re_whole: (static) the regular expression callers may use to cut out a version and date from a longer string.
      - major:    major number
      - minor:    minor number
      - revision: revision number
      - year:     year
      - month:    month
      - day:      day
    """
    re_whole = r'\b\d+\.\d+\.\d+\s\(\d+-\d+-\d+\)'

    def __init__(self, s):
        """Given a string, parse and set data members.

        @param s  a string of form '14.6.7400 (2014-6-17)'.
        """
        self.major = None
        self.minor = None
        self.revision = None
        self.year = None
        self.month = None
        self.day = None

        re_components = r'(\d+)\.(\d+)\.(\d+)\s\((\d+)-(\d+)-(\d+)\)'
        match_components = re.match(re_components, s)
        if match_components:
            self.major = int(match_components.group(1))
            self.minor = int(match_components.group(2))
            self.revision = int(match_components.group(3))
            self.year = int(match_components.group(4))
            self.month = int(match_components.group(5))
            self.day = int(match_components.group(6))

    def valid(self):
        """Whether the stored values constitute a valid VersionAndDate.

        @return whether all six components are non-negative integers and the date is valid.
        """
        if not (isinstance(self.major, (int, long)) and self.major>=0):
            return False
        if not (isinstance(self.minor, (int, long)) and self.minor>=0):
            return False
        if not (isinstance(self.revision, (int, long)) and self.revision>=0):
            return False
        if not (isinstance(self.year, (int, long)) and self.year>=0):
            return False
        if not (isinstance(self.month, (int, long)) and self.month>=0):
            return False
        if not (isinstance(self.day, (int, long)) and self.day>=0):
            return False
        try:
            a = datetime.datetime(self.year, self.month, self.day)
        except ValueError:
            return False
        return True

    def __str__(self):
        """To String.

        @return a string form of the version and date.
        @exception AppError if the VersionAndDate is not valid.
        """
        if not self.valid():
            raise scidblib.AppError('The VersionAndDate cannot be turned to string because it is not valid.')
        return '{0}.{1}.{2} ({3}-{4}-{5})'.format(self.major, self.minor, self.revision,
                                                  self.year, self.month, self.day)

    def earlier_than(self, another):
        """Comparing two versions.

        @param another  another VersionAndDate object.
        @return whether the current VersionAndDate is earlier than the other one.
        @exception AppError if any of the two objects is not valid.
        """
        if not self.valid() or not another.valid():
            raise scidblib.AppError('I cannot compare invalid VersionAndDate objects.')

        if self.major < another.major:
            return True
        elif self.major > another.major:
            return False

        if self.minor < another.minor:
            return True
        elif self.minor > another.minor:
            return False

        return self.revision < another.revision

class ProgressTracker:
    """A class that prints the "progress bar".

    A typical usage pattern is:
      - Call register_step() to register all steps of an algorithm.
      - Call start_step() at the beginning of each step.
      - Call end_step() at the end of each step.
    """
    def __init__(self, out=sys.stdout, name='',
                 if_print_start=True, if_print_end=True, if_print_skip=True,
                 prefix_start='*** ', prefix_end='*** ', prefix_skip='*** ',
                 suffix_start=' ***', suffix_end=' ***', suffix_skip=' ***'):
        """Configure a ProgressTracker object with some optional configurations.

        @param out            where to print output to. Default is sys.stdout.
        @param name           a name to be printed along with every step. Default is ''.
        @param if_print_start whether to print a message when a step has started. Default is True.
        @param if_print_end   whether to print a message when a step has ended. Default is True.
        @param if_print_skip  whether to print a message when a step is skipped. Default is True.
        @param prefix_start   a prefix string for a start message. Default is '*** '.
        @param prefix_end     a prefix string for a end message.   Default is '*** '.
        @param prefix_skip    a prefix string for a skip message.  Default is '*** '.
        @param suffix_start   a suffix string for a start message. Default is ' ***'.
        @param suffix_end     a suffix string for a end message.   Default is ' ***'.
        @param suffix_skip    a suffix string for a skip message.  Default is ' ***'.
        """
        self._out = out
        self._name = name
        self._if_print = {}   # a dict mapping ('start' | 'end' | 'skip') to a boolean telling whether to print.
        self._prefix = {}     # a dict mapping ('start' | 'end' | 'skip') to a prefix string.
        self._suffix = {}     # a dict mapping ('start' | 'end' | 'skip') to a suffix string.
        self._verb = {}       # a dict mapping ('start' | 'end' | 'skip') to a verb string.
        self._id_2_name = {}  # a dict mapping step_id to step_name.
        self._id_2_index = {} # a dict mapping step_id to step number.
        self._start_time = {} # a dict mapping step_id to start time of the step.
        self._end_time = {}   # a dict mapping step_id to end time of the step.

        self._if_print['start'] = if_print_start
        self._if_print['end'] =   if_print_end
        self._if_print['skip'] =  if_print_skip

        self._prefix['start'] = prefix_start
        self._prefix['end'] =   prefix_end
        self._prefix['skip'] =  prefix_skip

        self._suffix['start'] = suffix_start
        self._suffix['end'] =   suffix_end
        self._suffix['skip'] =  suffix_skip

        self._verb['start'] = 'has started'
        self._verb['end'] =   'has ended'
        self._verb['skip'] =  'is skipped'

    def register_step(self, step_id, step_name):
        """Register a step.

        @param step_id    an identifier to be used later when a step starts/ends.
        @param step_name  what the step does.
        """
        if step_id in self._id_2_index:
            raise scidblib.AppError('The step_id, \'' + step_id + '\', was already registered.')
        self._id_2_name[step_id] = step_name
        self._id_2_index[step_id] = len(self._id_2_name)

    def _print(self, what, step_id):
        """A helper function, servicing all of start_step(), end_step(), and skip_step().

        @param what     a string out of 'start', 'end', or 'skip'.
        @param step_id  the previously-registered step_id.
        @exception AssertException if 'what' is not understood.
        @exception AppError if the step_id was not registered.
        """
        assert what in self._if_print
        if not step_id in self._id_2_index:
            raise scidblib.AppError('The step_id, \'' + step_id + '\', was not registered in ProgressTracker.')

        if self._if_print[what]:
            s = self._prefix[what]
            if self._name:
                s += self._name + ': '
            s += 'Step ' + str(self._id_2_index[step_id]) + ' of ' + str(len(self._id_2_index)) + ' ' + self._verb[what]

            # In 'start' and 'skip' messages, print the step name;
            # In 'end' messages, print the elapsed time for the step.
            if what=='start' or what=='skip':
                s += '. (' + self._id_2_name[step_id] + ')'  # print the step name
            elif step_id in self._start_time and step_id in self._end_time and self._end_time[step_id] > self._start_time[step_id]:
                timedelta = self._end_time[step_id]-self._start_time[step_id]
                seconds = timedelta_total_seconds(timedelta)
                s += ' after ' + str(seconds) + ' s.'
            else:
                s += '.'
            s += self._suffix[what]

            print >> self._out, s

    def start_step(self, step_id):
        """A step started.

        @param step_id: the Id of the step.
        """
        self._start_time[step_id] = datetime.datetime.now()
        if step_id in self._end_time:
            del self._end_time[step_id]
        self._print('start', step_id)

    def end_step(self, step_id):
        """A step ended.

        @param step_id: the Id of the step.
        """
        self._end_time[step_id] = datetime.datetime.now()
        self._print('end', step_id)

    def skip_step(self, step_id):
        """A step was skipped.

        @param step_id: the Id of the step.
        """
        self._print('skip', step_id)

