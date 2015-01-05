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
 * @file Job.cpp
 *
 * @brief Implementaton of the error injection mechanism
 */

#include <util/InjectedError.h>
#include <system/Exceptions.h>

namespace scidb
{
InjectedErrorLibrary::~InjectedErrorLibrary()
{
}

bool InjectedErrorLibrary::registerError(long int id, const boost::shared_ptr<const InjectedError>& err)
{
#ifndef NDEBUG
    ScopedMutexLock lock(_mutex);
    return _registeredErrors.insert(std::make_pair(id, err)).second;
#endif
    return false;
}

boost::shared_ptr<const InjectedError> InjectedErrorLibrary::getError(long int id)
{
#ifndef NDEBUG
    ScopedMutexLock lock(_mutex);
    IdToErrorMap::const_iterator iter = _registeredErrors.find(id);
    if (iter == _registeredErrors.end()) {
        return boost::shared_ptr<InjectedError>();
    }
    return iter->second;
#endif
    return boost::shared_ptr<const InjectedError>();
}

InjectedErrorLibrary::InjectedErrorLibrary()
{
#ifndef NDEBUG
    bool rc = registerError(WriteChunkInjectedError::ID,
                            boost::shared_ptr<InjectedError>(new WriteChunkInjectedError()));
    assert(rc);
    rc = registerError(ReplicaSendInjectedError::ID,
                       boost::shared_ptr<InjectedError>(new ReplicaSendInjectedError()));
    assert(rc);
    rc = registerError(ReplicaWaitInjectedError::ID,
                       boost::shared_ptr<InjectedError>(new ReplicaWaitInjectedError()));
    assert(rc);
    rc = registerError(OperatorInjectedError::ID,
                       boost::shared_ptr<InjectedError>(new OperatorInjectedError()));
    assert(rc);
    rc = registerError(ThreadStartInjectedError::ID,
                       boost::shared_ptr<InjectedError>(new ThreadStartInjectedError()));
    assert(rc);
    rc = registerError(DataStoreInjectedError::ID,
                       boost::shared_ptr<InjectedError>(new DataStoreInjectedError()));
    assert(rc);
#endif
}

InjectedErrorLibrary InjectedErrorLibrary::_injectedErrorLib;

void WriteChunkInjectedError::inject() const
{
    boost::shared_ptr<const WriteChunkInjectedError> err(shared_from_this());
    Notification<WriteChunkInjectedError> event(err);
    event.publish();
}

void WriteChunkInjectedError::activate() const
{
    throw SYSTEM_EXCEPTION(SCIDB_SE_INJECTED_ERROR, SCIDB_LE_INJECTED_ERROR);
}

void ReplicaSendInjectedError::inject() const
{
    boost::shared_ptr<const ReplicaSendInjectedError> err(shared_from_this());
    Notification<ReplicaSendInjectedError> event(err);
    event.publish();
}

void ReplicaSendInjectedError::activate() const
{
    throw SYSTEM_EXCEPTION(SCIDB_SE_INJECTED_ERROR, SCIDB_LE_INJECTED_ERROR);
}

void ReplicaWaitInjectedError::inject() const
{
    boost::shared_ptr<const ReplicaWaitInjectedError> err(shared_from_this());
    Notification<ReplicaWaitInjectedError> event(err);
    event.publish();
}

void ReplicaWaitInjectedError::activate() const
{
    throw SYSTEM_EXCEPTION(SCIDB_SE_INJECTED_ERROR, SCIDB_LE_INJECTED_ERROR);
}

void OperatorInjectedError::inject() const
{
    boost::shared_ptr<const OperatorInjectedError> err(shared_from_this());
    Notification<OperatorInjectedError> event(err);
    event.publish();
}

void OperatorInjectedError::activate() const
{
    throw SYSTEM_EXCEPTION(SCIDB_SE_INJECTED_ERROR, SCIDB_LE_INJECTED_ERROR);
}

void ThreadStartInjectedError::inject() const
{
    boost::shared_ptr<const ThreadStartInjectedError> err(shared_from_this());
    Notification< ThreadStartInjectedError> event(err);
    event.publish();
}

void  ThreadStartInjectedError::activate() const
{
    throw std::runtime_error("Injected Error");
}

void DataStoreInjectedError::inject() const
{
    boost::shared_ptr<const DataStoreInjectedError> err(shared_from_this());
    Notification<DataStoreInjectedError> event(err);
    event.publish();
}

void  DataStoreInjectedError::activate() const
{
    throw SYSTEM_EXCEPTION(SCIDB_SE_INJECTED_ERROR, SCIDB_LE_INJECTED_ERROR);
}

}
