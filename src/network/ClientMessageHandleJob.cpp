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
 * MessageHandleJob.cpp
 *
 *  Created on: Jan 12, 2010
 *      Author: roman.simakov@gmail.com
 */

#include "log4cxx/logger.h"
#include <boost/make_shared.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <time.h>

#include "ClientMessageHandleJob.h"
#include <system/Exceptions.h>
#include <query/QueryProcessor.h>
#include <network/NetworkManager.h>
#include <network/MessageUtils.h>
#include <query/Serialize.h>
#include <array/Metadata.h>
#include <query/executor/SciDBExecutor.h>

using namespace std;
using namespace boost;

namespace scidb
{

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.services.network"));

ClientMessageHandleJob::ClientMessageHandleJob(boost::shared_ptr< Connection > connection,
                                               const boost::shared_ptr<MessageDesc>& messageDesc)
: MessageHandleJob(messageDesc), _connection(connection)
{
    assert(connection); //XXX TODO: convert to exception
}

void ClientMessageHandleJob::run()
{
   assert(_messageDesc->getMessageType() < mtSystemMax);
   MessageType messageType = static_cast<MessageType>(_messageDesc->getMessageType());
   LOG4CXX_TRACE(logger, "Starting client message handling: type=" << messageType)

   assert(_currHandler);
   _currHandler();

   LOG4CXX_TRACE(logger, "Finishing client message handling: type=" << messageType)
}

string ClientMessageHandleJob::getProgramOptions(std::string const& programOptions) const
{
    stringstream ip;
    boost::system::error_code ec;
    boost::asio::ip::tcp::endpoint endpoint = _connection->getSocket().remote_endpoint(ec);
    if (!ec) {
        ip << endpoint.address().to_string() << ":" << endpoint.port();
    }
    ip << programOptions;
    return ip.str();
}

void
ClientMessageHandleJob::executeSerially(shared_ptr<WorkQueue>& serialQueue,
                                        weak_ptr<WorkQueue>& initialQueue,
                                       const scidb::Exception* error)
{
    static const char *funcName="ClientMessageHandleJob::handleReschedule: ";

    if (dynamic_cast<const scidb::ClientMessageHandleJob::CancelChunkFetchException*>(error)) {
        serialQueue->stop();
        LOG4CXX_TRACE(logger, funcName << "Serial queue "<<serialQueue.get()<<" is stopped");
        serialQueue.reset();
        if (shared_ptr<WorkQueue> q = initialQueue.lock()) {
            q->unreserve();
        }
        return;
    }

    if (error) {
        LOG4CXX_ERROR(logger, funcName << "Error: "<<error);
        getQuery()->handleError(error->copy());
    }

    shared_ptr<Job> fetchJob(shared_from_this());
    WorkQueue::WorkItem work = boost::bind(&Job::executeOnQueue, fetchJob, _1, _2);
    assert(work);
    try
    {
        serialQueue->enqueue(work);
    }
    catch (const WorkQueue::OverflowException& e)
    {
        // as long as there is at least one item in the queue, we are OK
        LOG4CXX_TRACE(logger, funcName << "Serial queue is full, dropping request");
    }
}

ClientMessageHandleJob::RescheduleCallback
ClientMessageHandleJob::getSerializeCallback(shared_ptr<WorkQueue>& serialQueue)
{
    shared_ptr<WorkQueue> thisQ(_wq.lock());
    ASSERT_EXCEPTION(thisQ, "ClientMessageHandleJob::getSerializeCallback: current work queue is deallocated");
    shared_ptr<ClientMessageHandleJob> thisJob(boost::dynamic_pointer_cast<ClientMessageHandleJob>(shared_from_this()));

    const uint32_t cuncurrency = 1;
    const uint32_t depth = 2;
    serialQueue = NetworkManager::getInstance()->createWorkQueue(cuncurrency, depth);
    serialQueue->stop();

    ClientMessageHandleJob::RescheduleCallback func =
       boost::bind(&ClientMessageHandleJob::executeSerially, thisJob,
                   serialQueue, _wq, _1);

    thisQ->reserve(thisQ);
    return func;
}

void
ClientMessageHandleJob::handleQueryError(RescheduleCallback& cb,
                                         Notification<scidb::Exception>::MessageTypePtr errPtr)
{
    assert(!dynamic_cast<const scidb::ClientMessageHandleJob::CancelChunkFetchException*>(errPtr.get()));
    assert(cb);
    if (errPtr->getQueryId() != _query->getQueryID()) {
        return;
    }
    cb(errPtr.get());
}

void
ClientMessageHandleJob::fetchChunk()
{
    static const char *funcName="ClientMessageHandleJob::fetchChunk: ";
    const QueryID queryID = _messageDesc->getQueryID();
    try
    {
        _query = Query::getQueryByID(queryID);
        _query->validate();

        boost::shared_ptr<scidb_msg::Fetch> fetchRecord = _messageDesc->getRecord<scidb_msg::Fetch>();

        ASSERT_EXCEPTION((fetchRecord->has_attribute_id()), funcName);
        AttributeID attributeId = fetchRecord->attribute_id();
        const string arrayName = fetchRecord->array_name();

        LOG4CXX_TRACE(logger, funcName << "Fetching chunk attId= " << attributeId << ", queryID=" << queryID );

        boost::shared_ptr<Array> fetchArray = _query->getCurrentResultArray();

        const uint32_t invalidArrayType(~0);
        validateRemoteChunkInfo(fetchArray.get(),
                                _messageDesc->getMessageType(),
                                invalidArrayType,
                                attributeId,
                                CLIENT_INSTANCE);

        shared_ptr<RemoteMergedArray> mergedArray = boost::dynamic_pointer_cast<RemoteMergedArray>(fetchArray);
        if (mergedArray != NULL) {
            shared_ptr<WorkQueue> serialQueue;
            Notification<scidb::Exception>::ListenerID queryErrorListenerID;
            // Set up this job for async execution
            RemoteMergedArray::RescheduleCallback cb;
            try {
                // create a functor which serializes the execution(s) of this job
                cb = getSerializeCallback(serialQueue);
                assert(cb);
                assert(serialQueue);
                assert(!serialQueue->isStarted());

                // create and register a listener that will kick off this job if query error happens
                Notification<scidb::Exception>::PublishListener listener =
                   boost::bind(&ClientMessageHandleJob::handleQueryError, this, cb, _1);
                queryErrorListenerID = Notification<scidb::Exception>::addPublishListener(listener);
                _query->validate(); // to make sure we have not just missed the notification

                // prepare this job for the next execution
                _currHandler = boost::bind(&ClientMessageHandleJob::fetchMergedChunk, this, mergedArray,
                                           attributeId, queryErrorListenerID);
                assert(_currHandler);

                // register the functor with the array so that it can kick it off when remote messages arrive
                mergedArray->resetCallback(attributeId, cb);
                // finally enqueue & run this job ...
                cb(NULL);
                serialQueue->start();
            } catch (const Exception& e) {
                // well ... undo everything
                Notification<scidb::Exception>::removePublishListener(queryErrorListenerID);
                mergedArray->resetCallback(attributeId);
                if (cb) {
                    CancelChunkFetchException ccfe(REL_FILE, __FUNCTION__, __LINE__);
                    cb(&ccfe);
                }
                throw;
            }
            return;
        }

        boost::shared_ptr<MessageDesc> chunkMsg;
        boost::shared_ptr< ConstArrayIterator> iter = fetchArray->getConstIterator(attributeId);
        if (!iter->end()) {
            const ConstChunk* chunk = &iter->getChunk();
            assert(chunk);
            populateClientChunk(arrayName, attributeId, chunk, chunkMsg);
            ++(*iter);
        } else {
            populateClientChunk(arrayName, attributeId, NULL, chunkMsg);
        }

        _query->validate();
        _connection->sendMessage(chunkMsg);

        LOG4CXX_TRACE(logger, funcName << "Chunk of arrayName= "<< arrayName
                     <<", attId="<< attributeId
                     << " queryID=" << queryID << " sent to client");
    }
    catch (const Exception& e)
    {
        LOG4CXX_ERROR(logger, funcName << "Client's fetchChunk failed to complete queryID="<<queryID<<" : " << e.what()) ;
        if (_query) {
            _query->handleError(e.copy());
        }
        boost::shared_ptr<MessageDesc> msg(makeErrorMessageFromException(e, queryID));
        sendMessageToClient(msg);
    }
}

void ClientMessageHandleJob::fetchMergedChunk(boost::shared_ptr<RemoteMergedArray>& fetchArray,
                                              AttributeID attributeId,
                                              Notification<scidb::Exception>::ListenerID queryErrorListenerID)
{
    static const char *funcName="ClientMessageHandleJob::fetchMergedChunk: ";
    const QueryID queryID = _messageDesc->getQueryID();
    RemoteMergedArray::RescheduleCallback cb;
    try
    {
        ASSERT_EXCEPTION((queryID == _query->getQueryID()),
                         "Query ID mismatch in fetchMergedChunk");
        _query->validate();

        const string arrayName = _messageDesc->getRecord<scidb_msg::Fetch>()->array_name();
        boost::shared_ptr<MessageDesc> chunkMsg;

        LOG4CXX_TRACE(logger,
                      funcName << "Processing chunk of arrayName= " << arrayName
                      <<", attId="<< attributeId
                      << " queryID=" << queryID);
        try
        {
            boost::shared_ptr< ConstArrayIterator> iter = 
                fetchArray->getConstIterator(attributeId);
            if (!iter->end()) {
                const ConstChunk* chunk = &iter->getChunk();
                assert(chunk);
                populateClientChunk(arrayName, attributeId, chunk, chunkMsg);
            } else {
                populateClientChunk(arrayName, attributeId, NULL, chunkMsg);
            }
        }
        catch (const scidb::MultiStreamArray::RetryException& )
        {
            LOG4CXX_TRACE(logger,
                          funcName << " reschedule arrayName= " << arrayName 
                          << ", attId="<<attributeId
                          <<" queryID="<<queryID);
            return;
        }

        // This is the last execution of this job, tear down the async execution setup
        CancelChunkFetchException e(REL_FILE, __FUNCTION__, __LINE__);
        Notification<scidb::Exception>::removePublishListener(queryErrorListenerID);
        cb = fetchArray->resetCallback(attributeId);
        assert(cb);
        cb(&e);
        cb.clear();

        _query->validate();
        _connection->sendMessage(chunkMsg);

        LOG4CXX_TRACE(logger, funcName << "Chunk of arrayName= "<< arrayName
                     <<", attId="<< attributeId
                     << " queryID=" << queryID
                     << " sent to client");
    }
    catch (const Exception& e)
    {
        LOG4CXX_ERROR(logger, funcName << "Client's fetchChunk failed to complete "
                      <<" queryID="<<queryID<<" : " << e.what()) ;

        // Async setup teardown
        Notification<scidb::Exception>::removePublishListener(queryErrorListenerID);
        if (!cb) {
            cb = fetchArray->resetCallback(attributeId);
        }
        if (cb) {
            CancelChunkFetchException ccfe(REL_FILE, __FUNCTION__, __LINE__);
            cb(&ccfe);
        }
        if (_query) {
            _query->handleError(e.copy());
        }
        boost::shared_ptr<MessageDesc> msg(makeErrorMessageFromException(e, queryID));
        sendMessageToClient(msg);
    }
}

void ClientMessageHandleJob::populateClientChunk(const std::string& arrayName,
                                                 AttributeID attributeId,
                                                 const ConstChunk* chunk,
                                                 boost::shared_ptr<MessageDesc>& chunkMsg)
{
    static const char *funcName="ClientMessageHandleJob::populateClientChunk: ";
    boost::shared_ptr<scidb_msg::Chunk> chunkRecord;
    if (chunk)
    {
        checkChunkMagic(*chunk);
        boost::shared_ptr<CompressedBuffer> buffer = boost::make_shared<CompressedBuffer>();
        boost::shared_ptr<ConstRLEEmptyBitmap> emptyBitmap;
        chunk->compress(*buffer, emptyBitmap);
        chunkMsg = boost::make_shared<MessageDesc>(mtChunk, buffer);
        chunkRecord = chunkMsg->getRecord<scidb_msg::Chunk>();
        chunkRecord->set_eof(false);
        chunkRecord->set_compression_method(buffer->getCompressionMethod());
        chunkRecord->set_attribute_id(chunk->getAttributeDesc().getId());
        chunkRecord->set_decompressed_size(buffer->getDecompressedSize());
        chunkMsg->setQueryID(_query->getQueryID());
        chunkRecord->set_count(chunk->isCountKnown() ? chunk->count() : 0);
        const Coordinates& coordinates = chunk->getFirstPosition(false);
        for (size_t i = 0; i < coordinates.size(); i++) {
            chunkRecord->add_coordinates(coordinates[i]);
        }
    }
    else
    {
        chunkMsg = boost::make_shared<MessageDesc>(mtChunk);
        chunkRecord = chunkMsg->getRecord<scidb_msg::Chunk>();
        chunkMsg->setQueryID(_query->getQueryID());
        chunkRecord->set_eof(true);
        LOG4CXX_DEBUG(logger, funcName << "Prepared message with information that there are no unread chunks (EOF)"
                      <<", arrayName= "<< arrayName
                      <<", attId="<< attributeId
                      <<", queryID="<<_query->getQueryID());
    }

    if (_query->getWarnings().size())
    {
        //Propagate warnings gathered on coordinator to client
        vector<Warning> v = _query->getWarnings();
        for (vector<Warning>::const_iterator it = v.begin(); it != v.end(); ++it)
        {
            ::scidb_msg::Chunk_Warning* warn = chunkRecord->add_warnings();
            warn->set_code(it->getCode());
            warn->set_file(it->getFile());
            warn->set_function(it->getFunction());
            warn->set_line(it->getLine());
            warn->set_what_str(it->msg());
            warn->set_strings_namespace(it->getStringsNamespace());
            warn->set_stringified_code(it->getStringifiedCode());
        }
        _query->clearWarnings();
    }
}

void ClientMessageHandleJob::prepareClientQuery()
{
    scidb::QueryResult queryResult;
    const scidb::SciDB& scidb = getSciDBExecutor();
    try
    {
        queryResult.queryID = Query::generateID();
        assert(queryResult.queryID > 0);
        assert(_connection);
        _connection->attachQuery(queryResult.queryID);

        // Getting needed parameters for execution
        boost::shared_ptr<scidb_msg::Query> record = _messageDesc->getRecord<scidb_msg::Query>();
        const string queryString = record->query();
        bool afl = record->afl();
        const string programOptions = record->program_options();

        assert(queryResult.queryID > 0);
        try
        {
            scidb.prepareQuery(queryString, afl, getProgramOptions(programOptions), queryResult);
        }
        catch (const scidb::SystemCatalog::LockBusyException& e)
        {
            _currHandler=boost::bind(&ClientMessageHandleJob::retryPrepareQuery, this, queryResult/*copy*/);
            assert(_currHandler);
            reschedule(Query::getLockTimeoutNanoSec()/1000);
            return;
        }
        postPrepareQuery(queryResult);
    }
    catch (const Exception& e)
    {
        LOG4CXX_ERROR(logger, "prepareClientQuery failed to complete: " << e.what())
        const scidb::SciDB& scidb = getSciDBExecutor();
        handleExecuteOrPrepareError(e, queryResult, scidb);
    }
}

void ClientMessageHandleJob::retryPrepareQuery(scidb::QueryResult& queryResult)
{
    assert(queryResult.queryID > 0);
    const scidb::SciDB& scidb = getSciDBExecutor();
    try {
        // Getting needed parameters for execution
        boost::shared_ptr<scidb_msg::Query> record = _messageDesc->getRecord<scidb_msg::Query>();
        const string queryString = record->query();
        bool afl = record->afl();
        const string programOptions = record->program_options();
        try
        {
            scidb.retryPrepareQuery(queryString, afl, getProgramOptions(programOptions), queryResult);
        }
        catch (const scidb::SystemCatalog::LockBusyException& e)
        {
            _currHandler=boost::bind(&ClientMessageHandleJob::retryPrepareQuery, this, queryResult/*copy*/);
            assert(_currHandler);
            assert(_timer);
            reschedule(Query::getLockTimeoutNanoSec()/1000);
            return;
        }
        postPrepareQuery(queryResult);
    }
    catch (const Exception& e)
    {
        LOG4CXX_ERROR(logger, "prepareClientQuery failed to complete: " << e.what())
        const scidb::SciDB& scidb = getSciDBExecutor();
        handleExecuteOrPrepareError(e, queryResult, scidb);
    }
}

void ClientMessageHandleJob::postPrepareQuery(scidb::QueryResult& queryResult)
{
    assert(queryResult.queryID > 0);
    _timer.reset();

    // Creating message with result for sending to client
    shared_ptr<MessageDesc> resultMessage = make_shared<MessageDesc>(mtQueryResult);
    shared_ptr<scidb_msg::QueryResult> queryResultRecord = resultMessage->getRecord<scidb_msg::QueryResult>();
    resultMessage->setQueryID(queryResult.queryID);
    queryResultRecord->set_explain_logical(queryResult.explainLogical);
    queryResultRecord->set_selective(queryResult.selective);
    queryResultRecord->set_exclusive_array_access(queryResult.requiresExclusiveArrayAccess);

    vector<Warning> v = Query::getQueryByID(queryResult.queryID)->getWarnings();
    for (vector<Warning>::const_iterator it = v.begin(); it != v.end(); ++it)
    {
        ::scidb_msg::QueryResult_Warning* warn = queryResultRecord->add_warnings();

        cout << "Propagate warning during prepare" << endl;
        warn->set_code(it->getCode());
        warn->set_file(it->getFile());
        warn->set_function(it->getFunction());
        warn->set_line(it->getLine());
        warn->set_what_str(it->msg());
        warn->set_strings_namespace(it->getStringsNamespace());
        warn->set_stringified_code(it->getStringifiedCode());
    }
    Query::getQueryByID(queryResult.queryID)->clearWarnings();

    for (vector<string>::const_iterator it = queryResult.plugins.begin();
         it != queryResult.plugins.end(); ++it)
    {
        queryResultRecord->add_plugins(*it);
    }
    sendMessageToClient(resultMessage);
    LOG4CXX_DEBUG(logger, "The result preparation of query is sent to the client")
}

void ClientMessageHandleJob::handleExecuteOrPrepareError(const Exception& err,
                                                         const scidb::QueryResult& queryResult,
                                                         const scidb::SciDB& scidb)
{
    assert(_connection);
    if (queryResult.queryID != 0) {
        try {
            scidb.cancelQuery(queryResult.queryID);
            _connection->detachQuery(queryResult.queryID);
        } catch (const scidb::SystemException& e) {
            if (e.getLongErrorCode() != SCIDB_LE_QUERY_NOT_FOUND
                && e.getLongErrorCode() != SCIDB_LE_QUERY_NOT_FOUND2) {
                try { _connection->disconnect(); } catch (...) {}
                throw;
            }
        }
    }
    shared_ptr<MessageDesc> msg(makeErrorMessageFromException(err));
    sendMessageToClient(msg);
}

void ClientMessageHandleJob::sendMessageToClient(shared_ptr<MessageDesc>& msg)
{
    assert(_connection);
    assert(msg);
    try {
        _connection->sendMessage(msg);
    } catch (const scidb::Exception& e) {
        try { _connection->disconnect(); } catch (...) {}
        throw;
    }
}

void ClientMessageHandleJob::executeClientQuery()
{
    const scidb::SciDB& scidb = getSciDBExecutor();
    scidb::QueryResult queryResult;
    try
    {
        boost::shared_ptr<scidb_msg::Query> record = _messageDesc->getRecord<scidb_msg::Query>();
        const string queryString = record->query();
        bool afl = record->afl();
        queryResult.queryID = _messageDesc->getQueryID();

        if (queryResult.queryID <= 0) {
            const string programOptions = record->program_options();
            queryResult.queryID = Query::generateID();
            assert(queryResult.queryID > 0);
            _connection->attachQuery(queryResult.queryID);
            try
            {
                scidb.prepareQuery(queryString, afl, getProgramOptions(programOptions), queryResult);
            }
            catch (const scidb::SystemCatalog::LockBusyException& e)
            {
                _currHandler=boost::bind(&ClientMessageHandleJob::retryExecuteQuery, this, queryResult/*copy*/);
                assert(_currHandler);
                reschedule(Query::getLockTimeoutNanoSec()/1000);
                return;
            }
        }
        assert(queryResult.queryID>0);
        assert(Query::getQueryByID(queryResult.queryID)->queryString == queryString);

        scidb.executeQuery(queryString, afl, queryResult);

        postExecuteQueryInternal(queryResult);
    }
    catch (const Exception& e)
    {
       LOG4CXX_ERROR(logger, "executeClientQuery failed to complete: " << e.what())
       handleExecuteOrPrepareError(e, queryResult, scidb);
    }
}

void ClientMessageHandleJob::retryExecuteQuery(scidb::QueryResult& queryResult)
{
    assert(queryResult.queryID>0);
    const scidb::SciDB& scidb = getSciDBExecutor();
    try
    {
        boost::shared_ptr<scidb_msg::Query> record = _messageDesc->getRecord<scidb_msg::Query>();
        const string queryString = record->query();
        bool afl = record->afl();
        const string programOptions = record->program_options();
        try
        {
            scidb.retryPrepareQuery(queryString, afl, getProgramOptions(programOptions), queryResult);
        }
        catch (const scidb::SystemCatalog::LockBusyException& e)
        {
            _currHandler=boost::bind(&ClientMessageHandleJob::retryExecuteQuery, this, queryResult/*copy*/);
            assert(_currHandler);
            assert(_timer);
            reschedule(Query::getLockTimeoutNanoSec()/1000);
            return;
        }
        assert(queryResult.queryID>0);
        assert(Query::getQueryByID(queryResult.queryID)->queryString == queryString);

        scidb.executeQuery(queryString, afl, queryResult);

        postExecuteQueryInternal(queryResult);
    }
    catch (const Exception& e)
    {
       LOG4CXX_ERROR(logger, "executeClientQuery failed to complete: " << e.what())
       handleExecuteOrPrepareError(e, queryResult, scidb);
    }
}

void ClientMessageHandleJob::postExecuteQueryInternal(scidb::QueryResult& queryResult)
{
    _timer.reset();

    assert(queryResult.queryID>0);

    // Creating message with result for sending to client
    boost::shared_ptr<MessageDesc> resultMessage = boost::make_shared<MessageDesc>(mtQueryResult);
    boost::shared_ptr<scidb_msg::QueryResult> queryResultRecord = resultMessage->getRecord<scidb_msg::QueryResult>();
    resultMessage->setQueryID(queryResult.queryID);
    queryResultRecord->set_execution_time(queryResult.executionTime);
    queryResultRecord->set_explain_logical(queryResult.explainLogical);
    queryResultRecord->set_explain_physical(queryResult.explainPhysical);
    queryResultRecord->set_selective(queryResult.selective);

    if (queryResult.selective)
    {
        const ArrayDesc& arrayDesc = queryResult.array->getArrayDesc();
        queryResultRecord->set_array_name(arrayDesc.getName());

        const Attributes& attributes = arrayDesc.getAttributes();
        for (size_t i = 0; i < attributes.size(); i++)
        {
            ::scidb_msg::QueryResult_AttributeDesc* attribute = queryResultRecord->add_attributes();

            attribute->set_id(attributes[i].getId());
            attribute->set_name(attributes[i].getName());
            attribute->set_type(attributes[i].getType());
            attribute->set_flags(attributes[i].getFlags());
            attribute->set_default_compression_method(attributes[i].getDefaultCompressionMethod());
            attribute->set_default_missing_reason(attributes[i].getDefaultValue().getMissingReason());
            attribute->set_default_value(string((char*)attributes[i].getDefaultValue().data(), attributes[i].getDefaultValue().size()));
        }

        const Dimensions& dimensions = arrayDesc.getDimensions();
        for (size_t i = 0; i < dimensions.size(); i++)
        {
            ::scidb_msg::QueryResult_DimensionDesc* dimension = queryResultRecord->add_dimensions();

            dimension->set_name(dimensions[i].getBaseName());
            dimension->set_start_min(dimensions[i].getStartMin());
            dimension->set_curr_start(dimensions[i].getCurrStart());
            dimension->set_curr_end(dimensions[i].getCurrEnd());
            dimension->set_end_max(dimensions[i].getEndMax());
            dimension->set_chunk_interval(dimensions[i].getChunkInterval());
            dimension->set_chunk_overlap(dimensions[i].getChunkOverlap());
        }
    }

    boost::shared_ptr<Query> query = Query::getQueryByID(queryResult.queryID);

    vector<Warning> v = query->getWarnings();
    for (vector<Warning>::const_iterator it = v.begin(); it != v.end(); ++it)
    {
        ::scidb_msg::QueryResult_Warning* warn = queryResultRecord->add_warnings();

        warn->set_code(it->getCode());
        warn->set_file(it->getFile());
        warn->set_function(it->getFunction());
        warn->set_line(it->getLine());
        warn->set_what_str(it->msg());
        warn->set_strings_namespace(it->getStringsNamespace());
        warn->set_stringified_code(it->getStringifiedCode());
    }
    query->clearWarnings();

    for (vector<string>::const_iterator it = queryResult.plugins.begin();
         it != queryResult.plugins.end(); ++it)
    {
        queryResultRecord->add_plugins(*it);
    }

    queryResult.array.reset();

    query->validate();

    sendMessageToClient(resultMessage);
    LOG4CXX_DEBUG(logger, "The result of query is sent to the client")
}

void ClientMessageHandleJob::cancelQuery()
{
    const scidb::SciDB& scidb = getSciDBExecutor();

    const QueryID queryID = _messageDesc->getQueryID();
    try
    {
        scidb.cancelQuery(queryID);
        _connection->detachQuery(queryID);
        boost::shared_ptr<MessageDesc> msg(makeOkMessage(queryID));
        sendMessageToClient(msg);
        LOG4CXX_TRACE(logger, "The query " << queryID << " execution was canceled")
    }
    catch (const Exception& e)
    {
        LOG4CXX_ERROR(logger, e.what()) ;
        boost::shared_ptr<MessageDesc> msg(makeErrorMessageFromException(e, queryID));
        sendMessageToClient(msg);
    }
}

void ClientMessageHandleJob::completeQuery()
{
    const scidb::SciDB& scidb = getSciDBExecutor();

    const QueryID queryID = _messageDesc->getQueryID();
    try
    {
        scidb.completeQuery(queryID);
        _connection->detachQuery(queryID);
        boost::shared_ptr<MessageDesc> msg(makeOkMessage(queryID));
        sendMessageToClient(msg);
        LOG4CXX_TRACE(logger, "The query " << queryID << " execution was completed")
    }
    catch (const Exception& e)
    {
        LOG4CXX_ERROR(logger, e.what()) ;
        boost::shared_ptr<MessageDesc> msg(makeErrorMessageFromException(e, queryID));
        sendMessageToClient(msg);
    }
}

void ClientMessageHandleJob::dispatch(boost::shared_ptr<WorkQueue>& requestQueue,
                                      boost::shared_ptr<WorkQueue>& workQueue)
{
    assert(workQueue);
    assert(requestQueue);
    assert(_messageDesc->getMessageType() < mtSystemMax);
    MessageType messageType = static_cast<MessageType>(_messageDesc->getMessageType());
    LOG4CXX_TRACE(logger, "Dispatching client message type=" << messageType);
    const QueryID queryID = _messageDesc->getQueryID();
    try {
        switch (messageType)
        {
        case mtPrepareQuery:
        {
            _currHandler=boost::bind(&ClientMessageHandleJob::prepareClientQuery, this);
            // can potentially block
            enqueue(requestQueue);
        }
        break;
        case mtExecuteQuery:
        {
            _currHandler=boost::bind(&ClientMessageHandleJob::executeClientQuery, this);
            // can potentially block
            enqueue(requestQueue);
        }
        break;
        case mtFetch:
        {
            _currHandler=boost::bind(&ClientMessageHandleJob::fetchChunk, this);
            // can potentially block
            enqueue(requestQueue);
        }
        break;
        case mtCompleteQuery:
        {
            _currHandler=boost::bind(&ClientMessageHandleJob::completeQuery, this);
            enqueueOnErrorQueue(queryID);
        }
        break;
        case mtCancelQuery:
        {
            _currHandler=boost::bind(&ClientMessageHandleJob::cancelQuery, this);
            enqueueOnErrorQueue(queryID);
            break;
        }
        break;
        default:
        {
            LOG4CXX_ERROR(logger, "Unknown message type " << messageType);
            throw SYSTEM_EXCEPTION(SCIDB_SE_NETWORK, SCIDB_LE_UNKNOWN_MESSAGE_TYPE) << messageType;
        }
        }
        LOG4CXX_TRACE(logger, "Client message type=" << messageType <<" dispatched");
    }
    catch (const Exception& e)
    {
        LOG4CXX_ERROR(logger, "Dropping message of type=" <<  _messageDesc->getMessageType()
                      << ", for queryID=" << _messageDesc->getQueryID()
                      << ", from CLIENT"
                      << " because "<<e.what());
        boost::shared_ptr<MessageDesc> msg(makeErrorMessageFromException(e, queryID));
        sendMessageToClient(msg);
    }
}

// Note: No operations mutating this object are allowed to be called
// after enqueue() returns.
void ClientMessageHandleJob::enqueue(boost::shared_ptr<WorkQueue>& q)

{
    LOG4CXX_TRACE(logger, "ClientMessageHandleJob::enqueue message of type="
                  <<  _messageDesc->getMessageType()
                  << ", for queryID=" << _messageDesc->getQueryID()
                  << ", from CLIENT");

    shared_ptr<Job> thisJob(shared_from_this());
    WorkQueue::WorkItem work = boost::bind(&Job::executeOnQueue, thisJob, _1, _2);
    assert(work);
    try
    {
        q->enqueue(work);
    }
    catch (const WorkQueue::OverflowException& e)
    {
        LOG4CXX_ERROR(logger, "Overflow exception from the message queue ("
                      << q.get() << "): " << e.what());
        boost::shared_ptr<MessageDesc> msg(makeErrorMessageFromException(e, _messageDesc->getQueryID()));
        sendMessageToClient(msg);
    }
}

void ClientMessageHandleJob::enqueueOnErrorQueue(QueryID queryID)
{
    boost::shared_ptr<Query> query = Query::getQueryByID(queryID);
    boost::shared_ptr<WorkQueue> q = query->getErrorQueue();
    if (!q) {
        // if errorQueue is gone, the query must be deallocated at this point
        throw SYSTEM_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_QUERY_NOT_FOUND) << queryID;
    }
    LOG4CXX_TRACE(logger, "Error queue size=" << q->size()
                  << " for query ("<< queryID <<")");
    enqueue(q);
}

} // namespace
