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
 * @file MessageUtils.cpp
 *
 * @author roman.simakov@gmail.com
 */

#include <log4cxx/logger.h>
#include <boost/shared_ptr.hpp>
#include <boost/make_shared.hpp>

#include <system/Cluster.h>
#include <network/MessageUtils.h>
#include <query/ParsingContext.h>

using namespace boost;

namespace scidb
{

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.services.network"));

#ifndef SCIDB_CLIENT

boost::shared_ptr<MessageDesc> makeErrorMessageFromException(const Exception& e, QueryID queryID)
{
    boost::shared_ptr<MessageDesc> errorMessage = boost::make_shared<MessageDesc>(mtError);
    boost::shared_ptr<scidb_msg::Error> errorRecord = errorMessage->getRecord<scidb_msg::Error>();
    errorMessage->setQueryID(queryID);

    errorRecord->set_cluster_uuid(Cluster::getInstance()->getUuid());

    errorRecord->set_file(e.getFile());
    errorRecord->set_function(e.getFunction());
    errorRecord->set_line(e.getLine());
    errorRecord->set_errors_namespace(e.getErrorsNamespace());
    errorRecord->set_short_error_code(e.getShortErrorCode());
    errorRecord->set_long_error_code(e.getLongErrorCode());
    errorRecord->set_stringified_short_error_code(e.getStringifiedShortErrorCode());
    errorRecord->set_stringified_long_error_code(e.getStringifiedLongErrorCode());
    errorRecord->set_what_str(e.getWhatStr());

    if (dynamic_cast<const SystemException*>(&e) != NULL)
    {
        errorRecord->set_type(1);
    }
    else if (dynamic_cast<const UserException*>(&e) != NULL)
    {
        errorRecord->set_type(2);
    }
    else if (dynamic_cast<const UserQueryException*>(&e) != NULL)
    {
        errorRecord->set_type(3);
        const shared_ptr<ParsingContext> &ctxt = ((const UserQueryException&) e).getParsingContext();
        ::scidb_msg::Error_ParsingContext *mCtxt = errorRecord->mutable_parsing_context();
        mCtxt->set_query_string(ctxt->getQueryString());
        mCtxt->set_line_start(ctxt->getLineStart());
        mCtxt->set_col_start(ctxt->getColStart());
        mCtxt->set_line_end(ctxt->getLineEnd());
        mCtxt->set_col_end(ctxt->getColEnd());
    }
    else
    {
        assert(0);
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR);
    }

    return errorMessage;
}

boost::shared_ptr<MessageDesc> makeOkMessage(QueryID queryID)
{
    boost::shared_ptr<MessageDesc> okMessage = boost::make_shared<MessageDesc>(mtError);
    boost::shared_ptr<scidb_msg::Error> okRecord = okMessage->getRecord<scidb_msg::Error>();
    okMessage->setQueryID(queryID);
    okRecord->set_cluster_uuid(Cluster::getInstance()->getUuid());
    okRecord->set_type(0);
    okRecord->set_errors_namespace("scidb");
    okRecord->set_short_error_code(SCIDB_E_NO_ERROR);
    okRecord->set_long_error_code(SCIDB_E_NO_ERROR);

    return okMessage;
}

static bool parseInstanceList(shared_ptr<InstanceLiveness>& queryLiveness,
                          const scidb_msg::PhysicalPlan_InstanceList& instanceList,
                          const bool isDeadList)

{
   assert(queryLiveness);

   const google::protobuf::RepeatedPtrField<scidb_msg::PhysicalPlan_InstanceListEntry>&  instances = instanceList.instance_entry();
   for(  google::protobuf::RepeatedPtrField<scidb_msg::PhysicalPlan_InstanceListEntry>::const_iterator instanceIter = instances.begin();
         instanceIter != instances.end(); ++instanceIter) {

      const scidb_msg::PhysicalPlan_InstanceListEntry& entry = (*instanceIter);
      if(!entry.has_instance_id()) {
         assert(false);
         return false;
      }
      if(!entry.has_gen_id()) {
         assert(false);
         return false;
      }
      InstanceLiveness::InstancePtr instanceEntry(new InstanceLivenessEntry(entry.instance_id(), entry.gen_id(), isDeadList));
      bool rc = queryLiveness->insert(instanceEntry);
      if (!rc) {
         assert(false);
         return false;
      }
   }
   return true;
}

boost::shared_ptr<MessageDesc> makeAbortMessage(QueryID queryID)
{
   boost::shared_ptr<MessageDesc> msg = boost::make_shared<MessageDesc>(mtAbort);
   boost::shared_ptr<scidb_msg::DummyQuery> record = msg->getRecord<scidb_msg::DummyQuery>();
   msg->setQueryID(queryID);
   record->set_cluster_uuid(Cluster::getInstance()->getUuid());
   return msg;
}

boost::shared_ptr<MessageDesc> makeCommitMessage(QueryID queryID)
{
   boost::shared_ptr<MessageDesc> msg = boost::make_shared<MessageDesc>(mtCommit);
   boost::shared_ptr<scidb_msg::DummyQuery> record = msg->getRecord<scidb_msg::DummyQuery>();
   msg->setQueryID(queryID);
   record->set_cluster_uuid(Cluster::getInstance()->getUuid());
   return msg;
}

boost::shared_ptr<MessageDesc> makeWaitMessage(QueryID queryID)
{
    boost::shared_ptr<MessageDesc> msg = boost::make_shared<MessageDesc>(mtWait);
    boost::shared_ptr<scidb_msg::DummyQuery> record = msg->getRecord<scidb_msg::DummyQuery>();
    msg->setQueryID(queryID);
    record->set_cluster_uuid(Cluster::getInstance()->getUuid());
    return msg;
}

boost::shared_ptr<MessageDesc> makeNotifyMessage(QueryID queryID)
{
    boost::shared_ptr<MessageDesc> msg = boost::make_shared<MessageDesc>(mtNotify);
    boost::shared_ptr<scidb_msg::DummyQuery> record = msg->getRecord<scidb_msg::DummyQuery>();
    msg->setQueryID(queryID);
    record->set_cluster_uuid(Cluster::getInstance()->getUuid());
    return msg;
}

bool parseQueryLiveness(shared_ptr<InstanceLiveness>& queryLiveness,
                        shared_ptr<scidb_msg::PhysicalPlan>& ppMsg)
{
   assert(ppMsg);
   assert(ppMsg->IsInitialized());

   if (!ppMsg->has_view_id()) {
      assert(false);
      return false;
   }

   queryLiveness =
   shared_ptr<scidb::InstanceLiveness>(new scidb::InstanceLiveness(ppMsg->view_id(), 0));

   if (!ppMsg->has_dead_list()) {
      assert(false);
      return false;
   }
   const scidb_msg::PhysicalPlan_InstanceList& deadList = ppMsg->dead_list();

   if (!ppMsg->has_live_list()) {
      assert(false);
      return false;
   }
   const scidb_msg::PhysicalPlan_InstanceList& liveList = ppMsg->live_list();

   if (!parseInstanceList(queryLiveness, deadList, true)) {
      assert(false);
      return false;
   }
   if (!parseInstanceList(queryLiveness, liveList, false)) {
      assert(false);
      return false;
   }
   if (queryLiveness->getNumLive() < 1) {
      assert(false);
      return false;
   }
   return true;
}

bool serializeQueryLiveness(shared_ptr<const InstanceLiveness>& queryLiveness,
                            shared_ptr<scidb_msg::PhysicalPlan>& ppMsg)
{
   assert(ppMsg);
   assert(queryLiveness);

   ppMsg->set_view_id(queryLiveness->getViewId());

   const InstanceLiveness::DeadInstances& deadInstances = queryLiveness->getDeadInstances();
   scidb_msg::PhysicalPlan_InstanceList* deadList = ppMsg->mutable_dead_list();
   assert(deadList);

   for ( InstanceLiveness::DeadInstances::const_iterator iter = deadInstances.begin();
        iter != deadInstances.end(); ++iter) {
      google::protobuf::uint64 id = (*iter)->getInstanceId();
      google::protobuf::uint64 genId = (*iter)->getGenerationId();
      scidb_msg::PhysicalPlan_InstanceListEntry* instanceEntry = deadList->add_instance_entry();
      assert(instanceEntry);
      instanceEntry->set_instance_id(id);
      instanceEntry->set_gen_id(genId);
   }

   const InstanceLiveness::LiveInstances& liveInstances = queryLiveness->getLiveInstances();
   assert(liveInstances.size() > 0);
   scidb_msg::PhysicalPlan_InstanceList* liveList = ppMsg->mutable_live_list();
   assert(liveList);

   for ( InstanceLiveness::LiveInstances::const_iterator iter = liveInstances.begin();
        iter != liveInstances.end(); ++iter) {
      google::protobuf::uint64 id = (*iter)->getInstanceId();
      google::protobuf::uint64 genId = (*iter)->getGenerationId();
      scidb_msg::PhysicalPlan_InstanceListEntry* instanceEntry = liveList->add_instance_entry();
      assert(instanceEntry);
      instanceEntry->set_instance_id(id);
      instanceEntry->set_gen_id(genId);
   }
   return true;
}

#endif //SCIDB_CLIENT

shared_ptr<Exception> makeExceptionFromErrorMessage(const boost::shared_ptr<MessageDesc> &msg)
{
    boost::shared_ptr<scidb_msg::Error> errorRecord = msg->getRecord<scidb_msg::Error>();

    assert(SCIDB_E_NO_ERROR != errorRecord->short_error_code());

    switch (errorRecord->type())
    {
        case 1:
            return shared_ptr<Exception>(new SystemException(errorRecord->file().c_str(), errorRecord->function().c_str(),
                errorRecord->line(), errorRecord->errors_namespace().c_str(), errorRecord->short_error_code(),
                errorRecord->long_error_code(),  errorRecord->what_str().c_str(),
                errorRecord->stringified_short_error_code().c_str(), errorRecord->stringified_long_error_code().c_str(),
                msg->getQueryID()));
        case 2:
            return shared_ptr<Exception>(new UserException(errorRecord->file().c_str(), errorRecord->function().c_str(),
                errorRecord->line(), errorRecord->errors_namespace().c_str(), errorRecord->short_error_code(),
                errorRecord->long_error_code(),  errorRecord->what_str().c_str(),
                errorRecord->stringified_short_error_code().c_str(), errorRecord->stringified_long_error_code().c_str(),
                msg->getQueryID()));
        case 3:
            return shared_ptr<Exception>(new UserQueryException(errorRecord->file().c_str(), errorRecord->function().c_str(),
                errorRecord->line(), errorRecord->errors_namespace().c_str(), errorRecord->short_error_code(),
                errorRecord->long_error_code(),  errorRecord->what_str().c_str(),
                errorRecord->stringified_short_error_code().c_str(), errorRecord->stringified_long_error_code().c_str(),
                make_shared<ParsingContext>(errorRecord->parsing_context().query_string(),
                        errorRecord->parsing_context().line_start(),
                        errorRecord->parsing_context().col_start(),
                        errorRecord->parsing_context().line_end(),
                        errorRecord->parsing_context().col_end()
                ),
                msg->getQueryID()));
        default:
        {
            assert(0);
            const MessageType messageType = static_cast<MessageType>(msg->getMessageType());
            LOG4CXX_ERROR(logger, "Unknown/unexpected message format for type " << messageType);
            throw SYSTEM_EXCEPTION(SCIDB_SE_NETWORK, SCIDB_LE_INVALID_MESSAGE_FORMAT)  << messageType;
        }
    }
    return boost::shared_ptr<Exception>();
}

void makeExceptionFromErrorMessageAndThrow(const boost::shared_ptr<MessageDesc> &msg)
{
    makeExceptionFromErrorMessage(msg)->raise();
}

} // namespace
