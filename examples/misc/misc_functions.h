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
 * @file misc_functions.h
 *
 * @author paul@scidb.org
 *
 * @brief Misc. functions
 *
 *
 */

#ifndef MISC_FUNCTIONS_H
#define MISC_FUNCTIONS_H

#include <unistd.h>
#include <signal.h>
#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <util/Network.h>
#include "query/TypeSystem.h"
#include <query/Query.h>
#include <network/Connection.h>
#include <network/BaseConnection.h>
#include <network/NetworkManager.h>
#include <network/proto/scidb_msg.pb.h>
#include <log4cxx/logger.h>
#include "system/ErrorsLibrary.h"
#include "system/Warnings.h"
#include <util/InjectedError.h>

enum
{
    MISC_FUNCTIONS_ERROR1 = SCIDB_USER_ERROR_CODE_START,
    MISC_FUNCTIONS_WARNING
};

void sleepyInt(const Value** args, Value* res, void*)
{
    res->setInt64(args[0]->getInt64());
    sleep(args[1]->getInt32());
}

void trapOnNotEqual ( const Value** args, Value* res, void*)
{
    long int i1 = args[0]->getInt64();
    long int i2 = args[1]->getInt64();

    if (i1 != i2)
        throw PLUGIN_USER_EXCEPTION("misc_functions", SCIDB_SE_UDO, MISC_FUNCTIONS_ERROR1);

    res->setInt64(i1);
}

void exitOnNotEqual ( const Value** args, Value* res, void*)
{
    long int i1 = args[0]->getInt64();
    long int i2 = args[1]->getInt64();
	if ( i1 == i2 ) {
        res->setInt64(i1);
	} else {
		//
		// Forve an exit
		//
		_exit ( 0 );
	}
}

void netPauseHandler(const boost::shared_ptr<boost::asio::deadline_timer>& t,
                     int32_t duration,
                     const boost::system::error_code& error)
{
   // the timer is runnig on the service_io thread (at least currently)
   // so stop processing network packets for awhile
   sleep(duration);
}
void netPauseOnNotEqual(const Value** args, Value* res, void*)
{
   long int i1 = args[0]->getInt64();
   long int i2 = args[1]->getInt64();
   res->setInt64(i1);
   if ( i1 != i2 ) {
      int32_t duration = (int32_t)args[2]->getInt32();
      assert(duration > 0);
      boost::shared_ptr<boost::asio::deadline_timer> timer
      (new boost::asio::deadline_timer(scidb::getIOService()));
      timer->expires_from_now(posix_time::seconds(0));
      timer->async_wait(boost::bind(&netPauseHandler, timer, duration, _1));
   }
}

void collectQueryIds(std::deque<scidb::QueryID>* idList,
                     const boost::shared_ptr<scidb::Query>& q)
{
    scidb::QueryID queryId = q->getQueryID();
    idList->push_back(queryId);
}

void injectRemoteErrorForQuery(std::deque<scidb::QueryID>& idList, long int errCode)
{
    for (std::deque<scidb::QueryID>::const_iterator i=idList.begin(); i < idList.end(); ++i) {
        scidb::QueryID queryID = *i;

        LOG4CXX_ERROR(log4cxx::Logger::getRootLogger(),
                      "Injecting remote error=" << errCode <<" for query="<<queryID);

        boost::shared_ptr<MessageDesc> errorMessage = boost::make_shared<MessageDesc>(mtError);
        boost::shared_ptr<scidb_msg::Error> errorRecord = errorMessage->getRecord<scidb_msg::Error>();
        errorMessage->setQueryID(queryID);
        errorRecord->set_cluster_uuid(Cluster::getInstance()->getUuid());
        errorRecord->set_type(1);
        errorRecord->set_errors_namespace("scidb");
        errorRecord->set_short_error_code(SCIDB_SE_INJECTED_ERROR);
        errorRecord->set_long_error_code(SCIDB_LE_INJECTED_ERROR);
        errorRecord->set_what_str("Injected error");
        NetworkManager::getInstance()->broadcastPhysical(errorMessage);
    }
}
void injectRemoteError(const Value** args, Value* res, void*)
{
   InstanceID instanceID  = static_cast<InstanceID>(args[0]->getInt64());
   long int errCode = args[1]->getInt64();
   res->setInt64(-1);

   if (Cluster::getInstance()->getLocalInstanceId() != instanceID) {
      return;
   }
   std::deque<scidb::QueryID> idList;

   boost::function<void (const boost::shared_ptr<scidb::Query>&)> f =
       boost::bind(&collectQueryIds, &idList, _1);
   scidb::Query::listQueries(f);

   injectRemoteErrorForQuery(idList, errCode);

   res->setInt64(instanceID);
}

void killInstance(const Value** args, Value* res, void*)
{
   InstanceID instanceID = static_cast<InstanceID>(args[0]->getInt64());
   int   sigNum  = args[1]->getInt32();
   bool  killParent = args[2]->getBool();
   res->setInt64(instanceID);

   if (Cluster::getInstance()->getLocalInstanceId() != instanceID) {
      return;
   }
   if (killParent) {
      kill(getppid(), sigNum);
   }
   kill(getpid(), sigNum);
}

void postWarning(const Value** args, Value* res, void*)
{
    InstanceID instanceID = static_cast<InstanceID>(args[0]->getInt64());
    res->setInt64(instanceID);

    if (Cluster::getInstance()->getLocalInstanceId() == instanceID)
    {
        scidb::Query::getQueryByID(scidb::Query::getCurrentQueryID())->postWarning(
                    SCIDB_PLUGIN_WARNING("misc_functions", MISC_FUNCTIONS_WARNING) << instanceID);
    }
}

void injectError(const Value** args, Value* res, void*)
{
   InstanceID instanceID  = static_cast<InstanceID>(args[0]->getInt64());
   long int errID = args[1]->getInt64();
   res->setInt64(-1);

   if (Cluster::getInstance()->getLocalInstanceId() != instanceID) {
      return;
   }

   boost::shared_ptr<const InjectedError> err = InjectedErrorLibrary::getLibrary()->getError(errID);

   if (!err) {
       return;
   }
   res->setInt64(errID);
   err->inject();
}

void setMemCap(const Value** args, Value* res, void*)
{
   InstanceID instanceID = static_cast<InstanceID>(args[0]->getInt64());
   int64_t  maxMem  = args[1]->getInt32();
   res->setInt64(-1);

   if (Cluster::getInstance()->getLocalInstanceId() != instanceID) {
      return;
   }

   if (maxMem<0) {
       maxMem = RLIM_INFINITY;
   }

   struct rlimit rlim;
   if (getrlimit(RLIMIT_AS, &rlim) != 0) {
       LOG4CXX_ERROR(log4cxx::Logger::getRootLogger(),
                     " getrlimit call failed: " << ::strerror(errno) << " (" <<
                     errno << "); memory cap not set.");
       return;
   }
   rlim.rlim_cur = maxMem;
   if (setrlimit(RLIMIT_AS, &rlim) != 0) {
       LOG4CXX_ERROR(log4cxx::Logger::getRootLogger(),
                     " setrlimit call failed: " << ::strerror(errno) << " (" <<
                     errno << "); memory cap not set.");
       return;
   }
   res->setInt64(instanceID);
}


#endif // MISC_FUNCTIONS_H
