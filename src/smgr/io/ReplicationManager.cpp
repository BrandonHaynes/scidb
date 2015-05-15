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
 * ReplicationManager.cpp
 *
 * Description: Poor man's replication manager that blocks the replicating thread if the network is congested
 */

#include "ReplicationManager.h"

using namespace std;
using namespace boost;
namespace scidb
{
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.replication"));

void ReplicationManager::start(const boost::shared_ptr<JobQueue>& jobQueue)
{
    ScopedMutexLock cs(_repMutex);
    assert(!_lsnrId);
    Notification<NetworkManager::ConnectionStatus>::PublishListener pListener =
    bind(&ReplicationManager::handleConnectionStatus, this, _1); // bare pointer because RM should never go away
    _lsnrId = Notification<NetworkManager::ConnectionStatus>::addPublishListener(pListener);
    assert(_lsnrId);

    // initialize inbound replication queue
    uint64_t size = Config::getInstance()->getOption<int>(CONFIG_REPLICATION_RECEIVE_QUEUE_SIZE);
    assert(size>0);
    size = (size<1) ? 1 : size;
    // this queue is single-threaded because the order of replicas is important (per source)
    // and CachedStorage serializes everything anyway via THE mutex.
    _inboundReplicationQ = boost::make_shared<WorkQueue>(jobQueue, 1, size);
    InjectedErrorListener<ReplicaSendInjectedError>::start();
    InjectedErrorListener<ReplicaWaitInjectedError>::start();
}

void ReplicationManager::stop()
{
    ScopedMutexLock cs(_repMutex);
    Notification<NetworkManager::ConnectionStatus>::removePublishListener(_lsnrId);
    _lsnrId.reset();
    clear();
    InjectedErrorListener<ReplicaSendInjectedError>::stop();
    InjectedErrorListener<ReplicaWaitInjectedError>::stop();
    _repEvent.signal();
}

void ReplicationManager::send(const boost::shared_ptr<Item>& item)
{
    assert(item);
    assert(!item->isDone());
    assert(_lsnrId);

    ScopedMutexLock cs(_repMutex);
    shared_ptr<RepItems>& ri = _repQueue[item->getInstanceId()];
    if (!ri) {
        ri = shared_ptr<RepItems>(new RepItems);
    }
    ri->push_back(item);
    if (ri->size() == 1) {
        assert(ri->front() == item);
        sendItem(*ri);
    }
}

void ReplicationManager::wait(const boost::shared_ptr<Item>& item)
{
    ScopedMutexLock cs(_repMutex);

    assert(_lsnrId);

    if (item->isDone()) {
        item->validate();
        return;
    }

    shared_ptr<RepItems>& ri = _repQueue[item->getInstanceId()];
    assert(ri);

    Event::ErrorChecker ec = bind(&ReplicationManager::checkItemState, item);

    while (true) {

        LOG4CXX_TRACE(logger, "ReplicationManager::wait: about to wait for instance=" << item->getInstanceId()
                      << ", size=" << item->getChunkMsg()->getMessageSize()
                      << ", query (" << item->getChunkMsg()->getQueryID()<<")"
                      << ", queue size="<< ri->size());
        assert(!ri->empty());

        bool mine = (ri->front() == item);
        bool res = sendItem(*ri);
        if (mine && res) {
            assert(item->isDone());
            item->validate();
            return;
        }
        if (!res) {
            try {
                InjectedErrorListener<ReplicaWaitInjectedError>::check();
                _repEvent.wait(_repMutex, ec);
            } catch (Exception& e) {
                item->setDone(e.copy());
                throw;
            }
        }
        if (item->isDone()) {
            item->validate();
            return;
        }
    }
}

void ReplicationManager::handleConnectionStatus(Notification<NetworkManager::ConnectionStatus>::MessageTypePtr connStatus)
{
    assert(connStatus->getPhysicalInstanceId() != INVALID_INSTANCE);

    LOG4CXX_TRACE(logger, "ReplicationManager::handleConnectionStatus: notification for instance="
                  << connStatus->getPhysicalInstanceId()
                  << ", remote receive queue size="
                  << connStatus->getAvailabeQueueSize());

    if (connStatus->getQueueType() != NetworkManager::mqtReplication)
    {
        return;
    }
    if (connStatus->getAvailabeQueueSize() <= 0)
    {
        return;
    }
    ScopedMutexLock cs(_repMutex);

    RepQueue::iterator iter = _repQueue.find(connStatus->getPhysicalInstanceId());
    if (iter == _repQueue.end()) {
        return;
    }

    LOG4CXX_TRACE(logger, "ReplicationManager::handleConnectionStatus: notification for instance="
                  << connStatus->getPhysicalInstanceId()
                  << ", local replication queue size="<< iter->second->size()
                  << ", remote receive queue size="<< connStatus->getAvailabeQueueSize());
    _repEvent.signal();
}

bool ReplicationManager::sendItem(RepItems& ri)
{
    ScopedMutexLock cs(_repMutex);

    const boost::shared_ptr<Item>& item = ri.front();

    if (item->isDone()) {
        ri.pop_front();
        return true;
    }
    try {
        shared_ptr<Query> q(Query::getValidQueryPtr(item->getQuery()));

        boost::shared_ptr<MessageDesc> chunkMsg(item->getChunkMsg());
        NetworkManager::getInstance()->sendPhysical(item->getInstanceId(), chunkMsg,
                                                   NetworkManager::mqtReplication);
        LOG4CXX_TRACE(logger, "ReplicationManager::sendItem: successful replica chunk send to instance="
                      << item->getInstanceId()
                      << ", size=" << item->getChunkMsg()->getMessageSize()
                      << ", query (" << q->getQueryID()<<")"
                      << ", queue size="<< ri.size());
        InjectedErrorListener<ReplicaSendInjectedError>::check();
        item->setDone();
    } catch (NetworkManager::OverflowException& e) {
        assert(e.getQueueType() == NetworkManager::mqtReplication);
        return false;
    } catch (Exception& e) {
        item->setDone(e.copy());
    }
    ri.pop_front();
    return true;
}

void ReplicationManager::clear()
{
    // mutex must be locked
    for (RepQueue::iterator iter = _repQueue.begin(); iter != _repQueue.end(); ++iter) {
        shared_ptr<RepItems>& ri = iter->second;
        assert(ri);
        for (RepItems::iterator i=ri->begin(); i != ri->end(); ++i) {
            (*i)->setDone(SYSTEM_EXCEPTION_SPTR(SCIDB_SE_REPLICATION, SCIDB_LE_UNKNOWN_ERROR));
        }
    }
    _repQueue.clear();
    _repEvent.signal();
}

}

