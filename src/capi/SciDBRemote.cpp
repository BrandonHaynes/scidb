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

/*
 * @file SciDBRemote.cpp
 *
 * @author roman.simakov@gmail.com
 * @author smirnoffjr@gmail.com
 *
 * @brief SciDB API implementation to communicate with instance by network
 */

#include <stdlib.h>
#include <string>
#include <boost/shared_ptr.hpp>
#include <boost/make_shared.hpp>
#include <log4cxx/logger.h>
#include <log4cxx/basicconfigurator.h>

#include "SciDBAPI.h"
#include "network/Connection.h"
#include "network/MessageUtils.h"
#include "array/StreamArray.h"
#include "system/Exceptions.h"
#include "util/PluginManager.h"
#include "util/Singleton.h"

using namespace std;
using namespace boost;

namespace scidb
{
/**
 * Initialize instance
 */
class __Init
{
public:
    __Init()
    {
        log4cxx::BasicConfigurator::configure();
        log4cxx::LoggerPtr rootLogger(log4cxx::Logger::getRootLogger());
        rootLogger->setLevel(log4cxx::Level::toLevel("ERROR"));
    }
} __init;

/**
 * Class which associating active queries with warnings queues on client, so it easy to add new
 * warning from anywhere when it received from server
 */
class SciDBWarnings: public Singleton<SciDBWarnings>
{
public:
    void postWarning(QueryID queryId, const Warning &warning)
    {
    	ScopedMutexLock _lock(_mapLock);
        assert(_resultsMap.find(queryId) != _resultsMap.end());
        _resultsMap[queryId]->_postWarning(warning);
    }

    void associateWarnings(QueryID queryId, QueryResult *res)
    {
    	ScopedMutexLock _lock(_mapLock);
    	_resultsMap[queryId] = res;
    }

    void unassociateWarnings(QueryID queryId)
    {
    	ScopedMutexLock _lock(_mapLock);
    	_resultsMap.erase(queryId);
    }

private:
    std::map<QueryID, QueryResult*> _resultsMap;
    Mutex _mapLock;
};

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.services.network"));

boost::asio::io_service ioService;


/**
 * ClientArray inherits StreamArray and implement nextChunk method by requesting network
 */
class ClientArray: public StreamArray
{
public:
    ClientArray( BaseConnection* connection, const ArrayDesc& arrayDesc, QueryID queryID, QueryResult& queryResult):
    StreamArray(arrayDesc), _connection(connection), _queryID(queryID), _queryResult(queryResult)
    {
    }

protected:
    // overloaded method
    ConstChunk const* nextChunk(AttributeID attId, MemChunk& chunk);

private:
    BaseConnection* _connection;
    QueryID _queryID;
    QueryResult& _queryResult;
};

std::string getModuleFileName()
{
    // read the full name, e.g. '/tmp/dir/myfile'
    const size_t maxLength = PATH_MAX+NAME_MAX;
    char exepath[maxLength+1] = {0};
    ssize_t len = readlink("/proc/self/exe", exepath, maxLength);
    if (len==-1 || len==0) return exepath;
    exepath[len]=0;
    return exepath;
}

std::string getCommandLineOptions()
{
    stringstream cmdline;
    FILE *f = fopen("/proc/self/cmdline", "rb");
    char *arg = NULL;
    size_t size = 0;
    bool first = true;
    while(getdelim(&arg, &size, 0, f) != -1) {
        if (!first) {
            cmdline << arg << " ";
        } else {
            first = false;
        }
    }
    free(arg);
    fclose(f);
    return cmdline.str();
}

void fillProgramOptions(std::string& programOptions)
{
    programOptions = getModuleFileName() + " " + getCommandLineOptions();
}

/**
 * Remote implementation of the SciDBAPI interface
 */
class SciDBRemote: public scidb::SciDB
{
public:
    virtual ~SciDBRemote() {}
    void* connect(const std::string& connectionString, uint16_t port) const
    {
        StatisticsScope sScope;
        BaseConnection* connection = new  BaseConnection(ioService);
        connection->connect(connectionString, port);
        return connection;
    }

    void disconnect(void* connection) const
    {
        StatisticsScope sScope;
        BaseConnection* bc =  (BaseConnection*)connection;
        if (bc) {
            bc->disconnect();
            delete bc;
        }
    }

    void prepareQuery(const std::string& queryString, bool afl, const std::string&, QueryResult& queryResult, void* connection) const
    {
        StatisticsScope sScope;
        boost::shared_ptr<MessageDesc> queryMessage = boost::make_shared<MessageDesc>(mtPrepareQuery);
        queryMessage->getRecord<scidb_msg::Query>()->set_query(queryString);
        queryMessage->getRecord<scidb_msg::Query>()->set_afl(afl);

        std::string programOptions;
        fillProgramOptions(programOptions);
        queryMessage->getRecord<scidb_msg::Query>()->set_program_options(programOptions);

        LOG4CXX_TRACE(logger, "Send " << (afl ? "AFL" : "AQL") << " for preparation " << queryString);

        boost::shared_ptr<MessageDesc> resultMessage = (( BaseConnection*)connection)->sendAndReadMessage<MessageDesc>(queryMessage);

        if (resultMessage->getMessageType() != mtQueryResult) {
            assert(resultMessage->getMessageType() == mtError);

            makeExceptionFromErrorMessageAndThrow(resultMessage);
        }

        boost::shared_ptr<scidb_msg::QueryResult> queryResultRecord =
            resultMessage->getRecord<scidb_msg::QueryResult>();

        SciDBWarnings::getInstance()->associateWarnings(resultMessage->getQueryID(), &queryResult);
        for (int i = 0; i < queryResultRecord->warnings_size(); i++)
        {
            const ::scidb_msg::QueryResult_Warning& w = queryResultRecord->warnings(i);
            SciDBWarnings::getInstance()->postWarning(
                        resultMessage->getQueryID(),
                        Warning(
                            w.file().c_str(),
                            w.function().c_str(),
                            w.line(),
                            w.strings_namespace().c_str(),
                            w.code(),
                            w.what_str().c_str(),
                            w.stringified_code().c_str())
                        );
        }

         // Processing result message
        queryResult.queryID = resultMessage->getQueryID();
        if (queryResultRecord->has_exclusive_array_access()) {
            queryResult.requiresExclusiveArrayAccess = queryResultRecord->exclusive_array_access();
        }

        LOG4CXX_TRACE(logger, "Result for query " << queryResult.queryID);
    }

    void executeQuery(const std::string& queryString, bool afl, QueryResult& queryResult, void* connection) const
    {
        StatisticsScope sScope;
        boost::shared_ptr<MessageDesc> queryMessage = boost::make_shared<MessageDesc>(mtExecuteQuery);
        queryMessage->getRecord<scidb_msg::Query>()->set_query(queryString);
        queryMessage->getRecord<scidb_msg::Query>()->set_afl(afl);
        std::string programOptions;
        fillProgramOptions(programOptions);
        queryMessage->getRecord<scidb_msg::Query>()->set_program_options(programOptions);
        queryMessage->setQueryID(queryResult.queryID);

        if (!queryResult.queryID) {
            LOG4CXX_TRACE(logger, "Send " << (afl ? "AFL" : "AQL") << " for execution " << queryString);
        }
        else {
            LOG4CXX_TRACE(logger, "Send prepared query " << queryResult.queryID << " for execution");
        }

        boost::shared_ptr<MessageDesc> resultMessage = static_cast<BaseConnection*>(connection)->sendAndReadMessage<MessageDesc>(queryMessage);

        if (resultMessage->getMessageType() != mtQueryResult) {
            assert(resultMessage->getMessageType() == mtError);

            makeExceptionFromErrorMessageAndThrow(resultMessage);
        }

        // Processing result message
        boost::shared_ptr<scidb_msg::QueryResult> queryResultRecord = resultMessage->getRecord<scidb_msg::QueryResult>();

        queryResult.queryID = resultMessage->getQueryID();

        LOG4CXX_TRACE(logger, "Result for query " << queryResult.queryID);

        queryResult.selective = queryResultRecord->selective();
        if (queryResult.selective)
        {
            Attributes attributes;
            for (int i = 0; i < queryResultRecord->attributes_size(); i++)
            {
                Value defaultValue;
                if (queryResultRecord->attributes(i).default_missing_reason() >= 0) {
                    defaultValue.setNull(queryResultRecord->attributes(i).default_missing_reason());
                } else {
                    defaultValue.setData(queryResultRecord->attributes(i).default_value().data(),  queryResultRecord->attributes(i).default_value().size());
                }
                attributes.push_back(AttributeDesc(
                        queryResultRecord->attributes(i).id(),
                        queryResultRecord->attributes(i).name(),
                        queryResultRecord->attributes(i).type(),
                        queryResultRecord->attributes(i).flags(),
                        queryResultRecord->attributes(i).default_compression_method(),
                        std::set<std::string>(),
                        0, &defaultValue)
                );
            }
            queryResult.mappingArrays.resize(queryResultRecord->dimensions_size());

            Dimensions dimensions;
            for (int i = 0; i < queryResultRecord->dimensions_size(); i++)
            {
                DimensionDesc dim(
                        queryResultRecord->dimensions(i).name(),
                        queryResultRecord->dimensions(i).start_min(),
                        queryResultRecord->dimensions(i).curr_start(),
                        queryResultRecord->dimensions(i).curr_end(),
                        queryResultRecord->dimensions(i).end_max(),
                        queryResultRecord->dimensions(i).chunk_interval(),
                        queryResultRecord->dimensions(i).chunk_overlap());

                dimensions.push_back(dim);
            }

            SciDBWarnings::getInstance()->associateWarnings(resultMessage->getQueryID(), &queryResult);
            for (int i = 0; i < queryResultRecord->warnings_size(); i++)
            {
                const ::scidb_msg::QueryResult_Warning& w = queryResultRecord->warnings(i);
                SciDBWarnings::getInstance()->postWarning(
                            resultMessage->getQueryID(),
                            Warning(
                                w.file().c_str(),
                                w.function().c_str(),
                                w.line(),
                                w.strings_namespace().c_str(),
                                w.code(),
                                w.what_str().c_str(),
                                w.stringified_code().c_str())
                            );
            }

            queryResult.executionTime = queryResultRecord->execution_time();
            queryResult.explainLogical = queryResultRecord->explain_logical();
            queryResult.explainPhysical = queryResultRecord->explain_physical();

            const ArrayDesc arrayDesc(queryResultRecord->array_name(), attributes, dimensions);

            queryResult.array = boost::shared_ptr<Array>(new ClientArray(( BaseConnection*)connection,
                                                                         arrayDesc, queryResult.queryID, queryResult));
        }
    }

    void cancelQuery(QueryID queryID, void* connection) const
    {
        StatisticsScope sScope;
        boost::shared_ptr<MessageDesc> cancelQueryMessage = boost::make_shared<MessageDesc>(mtCancelQuery);
        cancelQueryMessage->setQueryID(queryID);

        LOG4CXX_TRACE(logger, "Canceling query for execution " << queryID);

        boost::shared_ptr<MessageDesc> resultMessage = (( BaseConnection*)connection)->sendAndReadMessage<MessageDesc>(cancelQueryMessage);

        //  assert(resultMessage->getMessageType() == mtError);
        if (resultMessage->getMessageType() == mtError) {
            boost::shared_ptr<scidb_msg::Error> error = resultMessage->getRecord<scidb_msg::Error>();
            if (error->short_error_code() != SCIDB_E_NO_ERROR)
                makeExceptionFromErrorMessageAndThrow(resultMessage);
        } else {
            throw USER_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_MESSAGE_TYPE2) << resultMessage->getMessageType();
        }
    }

    void completeQuery(QueryID queryID, void* connection) const
    {
        StatisticsScope sScope;
        boost::shared_ptr<MessageDesc> completeQueryMessage = boost::make_shared<MessageDesc>(mtCompleteQuery);
        completeQueryMessage->setQueryID(queryID);

        LOG4CXX_TRACE(logger, "Completing query for execution " << queryID);

        boost::shared_ptr<MessageDesc> resultMessage = (( BaseConnection*)connection)->sendAndReadMessage<MessageDesc>(completeQueryMessage);

        if (resultMessage->getMessageType() == mtError) {
            boost::shared_ptr<scidb_msg::Error> error = resultMessage->getRecord<scidb_msg::Error>();
            if (error->short_error_code() != SCIDB_E_NO_ERROR)
                makeExceptionFromErrorMessageAndThrow(resultMessage);
        } else {
            throw USER_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_MESSAGE_TYPE2) << resultMessage->getMessageType();
        }
    }

} _sciDB;


/**
 * C L I E N T   A R R A Y
 */
ConstChunk const* ClientArray::nextChunk(AttributeID attId, MemChunk& chunk)
{
    StatisticsScope sScope;
    LOG4CXX_TRACE(logger, "Fetching next chunk of " << attId << " attribute");
    boost::shared_ptr<MessageDesc> fetchDesc = boost::make_shared<MessageDesc>(mtFetch);
    fetchDesc->setQueryID(_queryID);
    shared_ptr<scidb_msg::Fetch> fetchDescRecord = fetchDesc->getRecord<scidb_msg::Fetch>();
    fetchDescRecord->set_attribute_id(attId);
    fetchDescRecord->set_array_name(getArrayDesc().getName());

    boost::shared_ptr<MessageDesc> chunkDesc = _connection->sendAndReadMessage<MessageDesc>(fetchDesc);

    if (chunkDesc->getMessageType() != mtChunk) {
        assert(chunkDesc->getMessageType() == mtError);

        makeExceptionFromErrorMessageAndThrow(chunkDesc);
    }

    boost::shared_ptr<scidb_msg::Chunk> chunkMsg = chunkDesc->getRecord<scidb_msg::Chunk>();

    if (!chunkMsg->eof())
    {
        LOG4CXX_TRACE(logger, "Next chunk message was received");
        const int compMethod = chunkMsg->compression_method();
        const size_t decompressedSize = chunkMsg->decompressed_size();

        Address firstElem;
        firstElem.attId = attId;
        for (int i = 0; i < chunkMsg->coordinates_size(); i++) {
            firstElem.coords.push_back(chunkMsg->coordinates(i));
        }

        chunk.initialize(this, &desc, firstElem, compMethod);
        boost::shared_ptr<CompressedBuffer> compressedBuffer = dynamic_pointer_cast<CompressedBuffer>(chunkDesc->getBinary());
        compressedBuffer->setCompressionMethod(compMethod);
        compressedBuffer->setDecompressedSize(decompressedSize);
        chunk.decompress(*compressedBuffer);

        for (int i = 0; i < chunkMsg->warnings_size(); i++)
        {
            const ::scidb_msg::Chunk_Warning& w = chunkMsg->warnings(i);
            SciDBWarnings::getInstance()->postWarning(
                        _queryID,
                        Warning(
                            w.file().c_str(),
                            w.function().c_str(),
                            w.line(),
                            w.strings_namespace().c_str(),
                            w.code(),
                            w.what_str().c_str(),
                            w.stringified_code().c_str())
                        );
        }

        LOG4CXX_TRACE(logger, "Next chunk was initialized");
        return &chunk;
    }
    else
    {
        LOG4CXX_TRACE(logger, "There is no new chunks");
        return NULL;
    }
}

QueryResult::~QueryResult()
{
    SciDBWarnings::getInstance()->unassociateWarnings(queryID);
}

bool QueryResult::hasWarnings()
{
	ScopedMutexLock lock(_warningsLock);
	return !_warnings.empty();
}

Warning QueryResult::nextWarning()
{
	ScopedMutexLock lock(_warningsLock);
	Warning res = _warnings.front();
	_warnings.pop();
	return res;
}

void QueryResult::_postWarning(const Warning &warning)
{
	ScopedMutexLock lock(_warningsLock);
	_warnings.push(warning);
}

/**
 * E X P O R T E D   F U N C T I O N
 */
EXPORTED_FUNCTION const SciDB& getSciDB()
{
    return _sciDB;
}

}
