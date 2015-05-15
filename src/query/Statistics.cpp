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
 * @file Statistic.cpp
 *
 * @author roman.simakov@gmail.com
 *
 * @brief Implementation of statistic gatharing class
 */

#include <boost/make_shared.hpp>
#include <log4cxx/logger.h>

#include <pqxx/connection>
#include <pqxx/transaction>
#include <pqxx/prepared_statement>
#include <pqxx/except>
#include <libpq-fe.h>

#include "query/Statistics.h"
#include "query/Query.h"

using namespace std;
using namespace boost;
using namespace pqxx;
using namespace pqxx::prepare;

namespace scidb
{

Statistics StatisticsScope::systemStatistics;

__thread Statistics* currentStatistics = &StatisticsScope::systemStatistics;

inline size_t printSize(size_t size)
{
    if (size < 2*KiB) {
        return size;
    }
    if (size < 2*MiB) {
        return size / KiB;
    }
    return size / MiB;
}

inline const char* printSizeUnit(size_t size)
{
    if (size < 2*KiB) {
        return "B";
    }
    if (size < 2*MiB) {
        return "KiB";
    }
    return "MiB";
}

#ifndef SCIDB_CLIENT
std::ostream& writeStatistics(std::ostream& os, const Statistics& s, size_t tab)
{
    string tabStr(tab*4, ' ');
    os <<
        tabStr << "Sent " << printSize(s.sentSize) << printSizeUnit(s.sentSize) << " (" << s.sentMessages << " messages)" << endl <<
        tabStr << "Recieved " << printSize(s.receivedSize) << printSizeUnit(s.receivedSize) << " (" << s.receivedMessages << " messages)" << endl <<
        tabStr << "Written " << printSize(s.writtenSize) << printSizeUnit(s.writtenSize) << " (" << s.writtenChunks << " chunks)" << endl <<
        tabStr << "Read " << printSize(s.readSize) << printSizeUnit(s.readSize) << " (" << s.readChunks << " chunks)" << endl <<
        tabStr << "Pinned " << printSize(s.pinnedSize) << printSizeUnit(s.pinnedSize) << " (" << s.pinnedChunks << " chunks)" << endl <<
        tabStr << "Allocated " << printSize(s.allocatedSize) << printSizeUnit(s.allocatedSize) << " (" << s.allocatedChunks << " chunks)" << endl;

    return os;
}

// S T A T I S T I C S   M O N I T O R

class PostgresStatisticsMonitor: public StatisticsMonitor
{
public:
    virtual ~PostgresStatisticsMonitor() {}
    PostgresStatisticsMonitor(const string& connectionString)
    {
        if (!PQisthreadsafe())
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_LIBPQ_NOT_THREADSAFE);

        try
        {
            _connection = new pqxx::connection(connectionString);

            work tr(*_connection);
            result query_res = tr.exec("select count(*) from pg_tables where tablename = 'scidb_stat'");
            const bool _initialized = query_res[0].at("count").as(bool());

            if (!_initialized) {
                tr.exec("create table \"scidb_stat\" (query_id bigint, ts timestamp, query_str varchar, query_stat varchar)");
                tr.commit();
            }
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query()
                << e.what();
        }
        catch (const PGSTD::runtime_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_CANT_CONNECT_PG) << e.what();
        }
        catch (const Exception &e)
        {
            throw;
        }
        catch (const std::exception &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
        catch (...)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) <<
                "Unknown exception when connecting to system catalog";
        }
    }

    void pushStatistics(const Query& query)
    {
        try
        {
            work tr(*_connection);
            string sql = "insert into \"scidb_stat\"(query_id, ts, query_str, query_stat)"
                    " values ($1, now(), $2, $3)";
            _connection->prepare("append_stat", sql)
                    ("bigint", treat_direct)
                    ("varchar", treat_string)
                    ("varchar", treat_string);

            stringstream ss;
            query.writeStatistics(ss);

            tr.prepared("append_stat")
                    (query.getQueryID())
                    (query.queryString)
                    (ss.str()).exec();
            _connection->unprepare("append_stat");
            tr.commit();
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query()
                << e.what();
        }
        catch (const PGSTD::runtime_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_CANT_CONNECT_PG) << e.what();
        }
        catch (const Exception &e)
        {
            throw;
        }
        catch (const std::exception &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
        catch (...)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) <<
                "Unknown exception when connecting to system catalog";
        }
    }

private:
    pqxx::connection *_connection;

};

class LoggerStatisticsMonitor: public StatisticsMonitor
{
public:
    virtual ~LoggerStatisticsMonitor() {}
    LoggerStatisticsMonitor(const string& loggerName):
        _logger(log4cxx::Logger::getLogger(loggerName == "" ? "scidb.statistics" : loggerName))
    {
    }

    void pushStatistics(const Query& query)
    {
        stringstream ss;
        query.writeStatistics(ss);
        LOG4CXX_INFO(_logger, "Statistics of query " << query.getQueryID() << ": " << ss.str())
    }

private:
    log4cxx::LoggerPtr _logger;
};

shared_ptr<StatisticsMonitor> StatisticsMonitor::create(size_t type, const string& params)
{
    switch (type)
    {
    case smPostgres:
        return shared_ptr<StatisticsMonitor>(new PostgresStatisticsMonitor(params));
    case smLogger:
    default:
        return shared_ptr<StatisticsMonitor>(new LoggerStatisticsMonitor(params));
    }
}

#endif

} // namespace
