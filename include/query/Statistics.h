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
 * @file Statistic.h
 *
 * @author roman.simakov@gmail.com
 *
 * @brief Runtime statistic
 */

#ifndef STATISTIC_H_
#define STATISTIC_H_

#include <string>
#include <stdint.h>
#include <boost/shared_ptr.hpp>

namespace scidb
{

/**
 * The class describes statistics of query execution for every operator.
 * Every operator will have a field of this type and provides it for
 * operations.
 */
class Statistics
{
public:
    uint64_t executionTime; /**< In milliseconds */
    std::string explainPhysical; /**< Every executed physical plan separated by ';' */

    // network
    volatile uint64_t sentSize;  /**< A number of sent bytes */
    volatile uint64_t sentMessages; /**< A number of sent messages */
    volatile uint64_t receivedSize; /**< A number of received bytes */
    volatile uint64_t receivedMessages; /**< A number of received messages */

    // disk
    volatile uint64_t writtenSize;  /**< A number of written bytes to disk */
    volatile uint64_t writtenChunks; /**< A number of written chunks to disk */
    volatile uint64_t readSize; /**< A number of read bytes from disk */
    volatile  uint64_t readChunks; /**< A number of read chunks from disk */

    // cache
    volatile uint64_t pinnedSize;  /**< A number of pinned bytes */
    volatile uint64_t pinnedChunks; /**< A number of pinned chunks */

    // allocation
    volatile uint64_t allocatedSize;  /**< A number of allocated bytes */
    volatile uint64_t allocatedChunks; /**< A number of allocated chunks */

    Statistics(): executionTime(0),
        sentSize(0), sentMessages(0), receivedSize(0), receivedMessages(0),
        writtenSize(0), writtenChunks(0), readSize(0), readChunks(0),
        pinnedSize(0), pinnedChunks(0),
        allocatedSize(0), allocatedChunks(0)
    {
    }
};

std::ostream& writeStatistics(std::ostream& os, const Statistics& s, size_t tab);

extern __thread Statistics* currentStatistics;

class StatisticsScope
{
private:
    Statistics* _prevStatistics;

public:
    static Statistics systemStatistics;

    StatisticsScope(Statistics* statistics = NULL): _prevStatistics(currentStatistics)
    {
        currentStatistics = statistics ? statistics : &systemStatistics;
    }

    ~StatisticsScope()
    {
        currentStatistics = _prevStatistics;
    }
};

class SelfStatistics
{
public:
    SelfStatistics(): _statistics(currentStatistics)
    {
    }

protected:
    Statistics* _statistics;
};

/**
 * This class is an interface to monitor of statistics.
 * Implementations defines how statistics will be processed.
 * That could be output to log file, append to postgres database, xml, etc
 */
class Query;

const size_t smLogger = 1; /**< Output into postgres database. String in create is logger name. */
const size_t smPostgres = 2; /**< Output into postgres database. String in create is connection string. */

class StatisticsMonitor
{
public:
    virtual ~StatisticsMonitor() {}
    virtual void pushStatistics(const Query& query) = 0;
    static boost::shared_ptr<StatisticsMonitor> create(size_t type, const std::string& params = "");
};

}

#endif
