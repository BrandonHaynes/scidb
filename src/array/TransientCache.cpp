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

/****************************************************************************/

#include <util/Mutex.h>                                  // For Mutex etc.
#include <util/Singleton.h>                              // For Singleton
#include <array/MemArray.h>                              // For MemArray
#include <array/TransientCache.h>                        // For interface

/****************************************************************************/
namespace scidb { namespace transient { namespace {
/****************************************************************************/

struct Cache : map<ArrayUAID,MemArrayPtr>, Singleton<Cache>, Mutex
{
    MemArrayPtr lookup(const ArrayDesc&,const QueryPtr&);
};

/****************************************************************************/

MemArrayPtr Cache::lookup(const ArrayDesc& d,const QueryPtr& q)
{
    iterator i = find(d.getUAId());                      // Search for the id

    if (i != end())                                      // Is it in there?
    {
        assert(i->second != 0);                          // ...validate value

        if (d.isInvalid())                               // ...pending removal?
        {
            this->erase(i);                              // ....so free it now
        }
        else                                             // ...array is valid
        {
            struct Temp:MemArray{using MemArray::desc;}; // ....access to desc
            static_cast<Temp*>(i->second.get())->desc=d; // ....update schema
            i->second->setQuery(q);                      // ....update query

            return i->second;                            // ....and return it
        }
    }

 /* The cache contains no such array: perhaps this instance has fallen over
    started back up since the transient array was added... */

    throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE,SCIDB_LE_BAD_TEMP_ARRAY) << d;
}

/****************************************************************************/

class CachePtr : boost::noncopyable, stackonly
{
 public:
                              CachePtr()
                               : _ptr(Cache::getInstance()),
                                 _sml(*_ptr)             {}
    Cache*                    operator->()               {return _ptr;}

 private:
    Cache*              const _ptr;                      // The cache instance
    ScopedMutexLock     const _sml;                      // The lock on it
};

/****************************************************************************/
}
/****************************************************************************/

MemArrayPtr lookup(const ArrayDesc& d,const QueryPtr& q)
{
    return CachePtr()->lookup(d,q);
}

MemArrayPtr record(const MemArrayPtr& p)
{
    return CachePtr()->operator[](p->getArrayDesc().getUAId()) = p;
}

void remove(const ArrayDesc& d)
{
    CachePtr()->erase(d.getUAId());
}

void clear(void)
{
    CachePtr()->clear();
}

/****************************************************************************/
}}
/****************************************************************************/
