/*
**
* BEGIN_COPYRIGHT
*
* This file is part of SciDB.
* Copyright (C) 2008-2014 SciDB, Inc.
*
* SciDB is free software: you can redistribute it and/or modify
* it under the terms of the AFFERO GNU General Public License as published by
* the Free Software Foundation.
*
* SciDB is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
* INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
* NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
* the AFFERO GNU General Public License for the complete license terms.
*
* You should have received a copy of the AFFERO GNU General Public License
* along with SciDB.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
*
* END_COPYRIGHT
*/

/**
 * Timing.h
 *
 *  Created on: Jun 1, 2012
 *      Author: Donghui
 */

#ifndef TIMING_H_
#define TIMING_H_

#include <stdio.h>
#include <stdint.h>
#include <sys/time.h>
#include <sstream>
#include <log4cxx/logger.h>

namespace scidb
{

using namespace std;

/**
 * A utility class that returns the #elapsed milliseconds between start() and elapsed() calls.
 */
class ElapsedMilliSeconds {
public:
    /**
     * The Constructor records the start time unless told not to do so.
     */
    ElapsedMilliSeconds(bool nostart = false) {
        if (!nostart)
        {
            restart();
        }
    }

    /**
     * Re-record the start time.
     */
    void restart() {
        gettimeofday(&_startTime, NULL);
    }

    /**
     * Return the elapsed time, in milliseconds, since the last call to restart(), or the constructor if no restart() was called.
     */
    uint64_t elapsed() {
        timeval endTime;
        gettimeofday(&endTime, NULL);
        uint64_t s = _startTime.tv_sec*1000 + _startTime.tv_usec/1000;
        uint64_t e = endTime.tv_sec*1000 + endTime.tv_usec/1000;
        if (s < e) {
            return e-s;
        }
        return 0;
    }

    /**
     * Print a timing in log (DEBUG level) and restart the timer.
     */
    void logTiming(log4cxx::LoggerPtr const& logger, char const*const what, bool restartTiming = true) {
        if (logger->isDebugEnabled()) {
            uint64_t e = elapsed();
            LOG4CXX_DEBUG(logger, what << " took " << e << " ms, or " << ElapsedMilliSeconds::toString(e));
            if (restartTiming) {
                restart();
            }
        }
    }

    /**
     * Change millisecond to a string in the format of:
     *   "1 hour 11 seconds 500 milliseconds"
     *
     * Some other examples:
     * - if spaceAfterNumber=",":
     *   "1 hour, 11 seconds, 500 milliseconds"
     *
     * - if pluralAppend="", spaceAfterNumber="", spaceAfterUnit=";", H="H", M="M", S="S", MS="MS":
     *   "1H;11S;500MS
     *
     * - if pluralAppend="", spaceAfterNumber="", spaceAfterUnit=":", H="", M="", S="", noMSWhatSoEver=true, omitZeroUnits=false:
     *   "1:0:11"
     */
    static std::string toString(uint64_t in,
                                std::string pluralAppend="s",
                                std::string spaceAfterNumber=" ",
                                std::string spaceAfterUnit=" ",
                                std::string H = "hour",
                                std::string M = "minute",
                                std::string S = "second",
                                std::string MS="millisecond",
                                bool noMSWhatSoEver=false,
                                bool omitZeroUnits=true)
    {
        ostringstream os;
        uint64_t h, m, s, ms;
        h = in/(1000*3600);
        in -= h*(1000*3600);
        m = in/(1000*60);
        in -= m*(1000*60);
        s = in/1000;
        in -= s*1000;
        ms = in;

        bool showed=false;  // whether something was shown so far

        if (h>0 || !omitZeroUnits) {
            os << h << spaceAfterNumber << H;
            if (h>1) {
                os << pluralAppend;
            }
            bool willShowAfter = (!omitZeroUnits) || m>0 || s>0 || (!noMSWhatSoEver && ms>0);
            if (willShowAfter) {
                os << spaceAfterUnit;
            }
            showed = true;
        }

        if (m>0 || !omitZeroUnits) {
            os << m << spaceAfterNumber << M;
            if (m>1) {
                os << pluralAppend;
            }
            bool willShowAfter = (!omitZeroUnits) || s>0 || (!noMSWhatSoEver && ms>0);
            if (willShowAfter) {
                os << spaceAfterUnit;
            }
            showed = true;
        }

        if (s>0 || !omitZeroUnits || (noMSWhatSoEver && !showed)) {
            os << s << spaceAfterNumber << S;
            if (s>1) {
                os << pluralAppend;
            }
            bool willShowAfter = (!noMSWhatSoEver) && ((!omitZeroUnits) || ms>0);
            if (willShowAfter) {
                os << spaceAfterUnit;
            }
            showed = true;
        }

        if ((!noMSWhatSoEver) && (!omitZeroUnits || ms>0 || !showed)) {
            os << ms << spaceAfterNumber << MS;
            if (ms>1) {
                os << pluralAppend;
            }
        }

        return os.str();
    }

protected:
    timeval _startTime;
};


}

#endif /* TIMING_H_ */
