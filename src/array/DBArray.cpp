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
 * @file
 *
 * @brief Database array implementation
 *
 * @author Konstantin Knizhnik <knizhnik@garret.ru>
 */
#include <log4cxx/logger.h>
#include "array/DBArray.h"
#include "smgr/io/InternalStorage.h"
#include "system/Exceptions.h"
#include "system/SystemCatalog.h"

namespace scidb
{
    static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.services.storage"));
    //
    // DBArray
    //
    string DBArray::getRealName() const
    {
        return SystemCatalog::getInstance()->getArrayDesc(_desc.getId())->getName();
    }

    DBArray::DBArray(ArrayDesc const& desc, const boost::shared_ptr<Query>& query)
    : _desc(desc)
    {
        assert(query);
        _query = query;
        _desc.setPartitioningSchema(SystemCatalog::getInstance()->getPartitioningSchema(desc.getId()));
    }

    DBArray::DBArray(std::string const& name, const boost::shared_ptr<Query>& query)
    {
        assert(query);
        _query = query;
        SystemCatalog::getInstance()->getArrayDesc(name, _desc);
    }

    std::string const& DBArray::getName() const
    {
        return _desc.getName();
    }

    ArrayID DBArray::getHandle() const
    {
        return _desc.getId();
    }

    ArrayDesc const& DBArray::getArrayDesc() const
    {
        return _desc;
    }

    boost::shared_ptr<ArrayIterator> DBArray::getIterator(AttributeID attId)
    {
       LOG4CXX_TRACE(logger, "Getting DB iterator for ID="<<_desc.getId()<<", attrID="<<attId);
       boost::shared_ptr<Query> query(Query::getValidQueryPtr(_query));
       shared_ptr<const Array> arr= boost::dynamic_pointer_cast<const Array>(shared_from_this());
       return StorageManager::getInstance().getArrayIterator(arr, attId, query);
    }

    boost::shared_ptr<ConstArrayIterator> DBArray::getConstIterator(AttributeID attId) const
    {
        LOG4CXX_TRACE(logger, "Getting const DB iterator for ID="<<_desc.getId()<<", attrID="<<attId);
        boost::shared_ptr<Query> query(Query::getValidQueryPtr(_query));
        shared_ptr<const Array> arr= boost::dynamic_pointer_cast<const Array>(shared_from_this());
        return StorageManager::getInstance().getConstArrayIterator(arr, attId, query);
    }

    boost::shared_ptr<CoordinateSet> DBArray::getChunkPositions() const
    {
        boost::shared_ptr<Query> query(Query::getValidQueryPtr(_query));
        boost::shared_ptr<CoordinateSet> result (new CoordinateSet());
        assert(query);
        StorageManager::getInstance().getChunkPositions(_desc, query, *(result.get()));
        return result;
    }
}
