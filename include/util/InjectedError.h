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
 * @file InjectedError.h
 *
 * @brief 
 *
 */

#ifndef INJECTEDERROR_H_
#define INJECTEDERROR_H_

#include <map>
#include <boost/shared_ptr.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <boost/make_shared.hpp>
#include <boost/bind.hpp>
#include <util/Mutex.h>
#include <util/Notification.h>

namespace scidb
{
/**
 * @class InjectedError - a base class for all injected errors
 *
 */
class InjectedError
{
 public:
    InjectedError() {}
    virtual ~InjectedError() {}
    virtual void inject() const = 0;
    virtual void activate() const = 0;
 private:
    InjectedError(const InjectedError&);
    InjectedError& operator=(const InjectedError&);
};

/**
 * @class WriteChunkInjectedError - a specific error injected into the Storage::writeChunk code path
 *                                  which is triggered at the end of writing the first chunk
 *
 */
class WriteChunkInjectedError
: public InjectedError, public boost::enable_shared_from_this<WriteChunkInjectedError>
{
 public:
    const static long int ID = 1;
    virtual void inject() const;
    virtual void activate() const;

};
typedef Notification<WriteChunkInjectedError> WriteChunkInjectedErrorNotification;
 
/**
 * @class ReplicaSendInjectedError - a specific error injected into the ReplicationManager::sendItem code path
 *
 */
class ReplicaSendInjectedError
: public InjectedError, public boost::enable_shared_from_this<ReplicaSendInjectedError>
{
 public:
    const static long int ID = 2;
    virtual void inject() const;
    virtual void activate() const;

};
typedef Notification<ReplicaSendInjectedError> ReplicaSendInjectedErrorNotification;

/**
 * @class ReplicaWaitInjectedError - a specific error injected into the ReplicationManager::wait code path
 * which is triggered when the ReplicationManager is trying to wait until more buffer/queue space
 * is available in NetworkManager. This path is often taken when replication network flow control is pushing back. 
 *
 */
class ReplicaWaitInjectedError
: public InjectedError, public boost::enable_shared_from_this<ReplicaWaitInjectedError>
{
 public:
    const static long int ID = 3;
    virtual void inject() const;
    virtual void activate() const;

};
typedef Notification<ReplicaWaitInjectedError> ReplicaWaitInjectedErrorNotification;

/**
 * @class OperatorInjectedError - a generic error to be interpreted by a specific operator
 *  store(),redim_store(),sg(),rename() error out after they have done 99% of the work
 *  (99% because a coordinator will error out before creating a new version in the catalog).
 *  remove() errors out before it does any work.
 *
 */
class OperatorInjectedError
: public InjectedError, public boost::enable_shared_from_this<OperatorInjectedError>
{
 public:
    const static long int ID = 4;
    virtual void inject() const;
    virtual void activate() const;
};
typedef Notification<OperatorInjectedError> OperatorInjectedErrorNotification;

/**
 * @class ThreadStartInjectedError - a specific error injected into the ThreadPool::start code path
 * which is triggered when the ThreadPool spawns off its threads. This error should cause one or more scidb::Threads to be created but not started.
 */
class ThreadStartInjectedError
: public InjectedError, public boost::enable_shared_from_this<ThreadStartInjectedError>
{
 public:
    const static long int ID = 5;
    virtual void inject() const;
    virtual void activate() const;

};
typedef Notification<ThreadStartInjectedError> ThreadStartInjectedErrorNotification;

/**
 * @class DataStoreInjectedError - a specific error injected into the DataStore::invalidateFreelist
 * code path.
 */
class DataStoreInjectedError
: public InjectedError, public boost::enable_shared_from_this<DataStoreInjectedError>
{
 public:
    const static long int ID = 6;
    virtual void inject() const;
    virtual void activate() const;

};
typedef Notification<DataStoreInjectedError> DataStoreInjectedErrorNotification;
 
/**
 * @class InjectedErrorLibrary - a library of all injected error identified by their IDs
 *
 */
class InjectedErrorLibrary
{
 public:
    InjectedErrorLibrary();
    virtual ~InjectedErrorLibrary();
    bool registerError(long int id, const boost::shared_ptr<const InjectedError>& err);
    boost::shared_ptr<const InjectedError> getError(long int id);
    static InjectedErrorLibrary* getLibrary()
    {
        return &_injectedErrorLib;
    }
 private:
    typedef std::map<long int, boost::shared_ptr<const InjectedError> > IdToErrorMap;
#ifndef NDEBUG
    IdToErrorMap _registeredErrors;
    Mutex _mutex;
#endif
    static InjectedErrorLibrary _injectedErrorLib;
 };

/**
 * @class InjectedErrorListener - a mixin class to receive and act on the injected errors
 *
 */
template<typename ErrorType>
class InjectedErrorListener
{
 public:
    InjectedErrorListener() {}

    /**
     * Mixin destructor to prevent standalone instanciations
     */
    virtual ~InjectedErrorListener();
    /**
     * Start receiving error notifications
     */
    void start()
    {
#ifndef NDEBUG
        ScopedMutexLock lock(_mutex);
        if (_lsnrID) {
            return;
        }
        typename Notification<ErrorType>::PublishListener listener =
        boost::bind(&InjectedErrorListener<ErrorType>::handle, this, _1);
        _lsnrID = Notification<ErrorType>::addPublishListener(listener);
#endif
    }
    /**
     * Check if an error has been injected and activate the error
     */
    void check()
    {
#ifndef NDEBUG
        ScopedMutexLock lock(_mutex);
        if (!_msg) {
            return;
        }
        typename Notification<ErrorType>::MessageTypePtr msg = _msg;
        _msg.reset();
        msg->activate();
#endif
    }
    /**
     * Must be called before destructing the object
     */
    void stop()
    {
#ifndef NDEBUG
        ScopedMutexLock lock(_mutex);
        Notification<ErrorType>::removePublishListener(_lsnrID);
#endif
    }
 private:
    void handle(typename Notification<ErrorType>::MessageTypePtr msg)
    {
#ifndef NDEBUG
        ScopedMutexLock lock(_mutex);
        _msg = msg;
#endif
    }
#ifndef NDEBUG
    typename Notification<ErrorType>::ListenerID _lsnrID;
    typename Notification<ErrorType>::MessageTypePtr _msg;
    Mutex _mutex;
#endif
 };

/// Mixin destructor
template<typename ErrorType>
InjectedErrorListener<ErrorType>::~InjectedErrorListener() {}

} //namespace scidb

#endif
