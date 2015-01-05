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
 * @file LookupArray.h
 *
 * @brief The implementation of the array iterator for the lookup operator
 *
 */

#ifndef LOOKUP_ARRAY_H_
#define LOOKUP_ARRAY_H_

#include <string>
#include <vector>
#include "array/DelegateArray.h"
#include "array/Metadata.h"
#include "query/FunctionDescription.h"
#include "query/Expression.h"

namespace scidb
{

using namespace std;
using namespace boost;

class LookupArray;
class LookupArrayIterator;
class LookupChunkIterator;


class LookupChunkIterator : public DelegateChunkIterator
{    
public:
    virtual  Value& getItem();
    virtual void operator ++();
    virtual void reset();
    virtual bool setPosition(Coordinates const& pos);
    virtual bool end();
    virtual bool isEmpty();
    LookupChunkIterator(LookupArrayIterator const& arrayIterator,
                        DelegateChunk const* chunk,
                        int iterationMode,
                        boost::shared_ptr<Query> const& query);
    virtual boost::shared_ptr<Query> getQuery() { return _query; }
    
private:
    bool mapPosition();
    vector< boost::shared_ptr<ConstChunkIterator> > templateChunkIterators;
    vector< FunctionPointer> _converters;
    boost::shared_ptr<Query> _query;

    boost::shared_ptr<ConstArrayIterator> sourceArrayIterator;
    boost::shared_ptr<ConstChunkIterator> sourceChunkIterator;
    ArrayDesc const& sourceArrayDesc;
    int iterationMode;
    bool hasCurrent;
};
   
class LookupArrayIterator : public DelegateArrayIterator
{
    friend class LookupChunkIterator;
  public:
    virtual void operator ++();
    virtual void reset();
	virtual bool setPosition(Coordinates const& pos);
    LookupArrayIterator(LookupArray const& array, AttributeID attrID);

  private:
    vector< boost::shared_ptr<ConstArrayIterator> > templateIterators;
    boost::shared_ptr<ConstArrayIterator> sourceIterator;
    ArrayDesc const& sourceArrayDesc;
};

class LookupArray : public DelegateArray
{
    friend class LookupArrayIterator;
    friend class LookupChunkIterator;
  public:
    virtual DelegateChunkIterator* createChunkIterator(DelegateChunk const* chunk, int iterationMode) const;
    virtual DelegateArrayIterator* createArrayIterator(AttributeID id) const;

    LookupArray(ArrayDesc const& desc, boost::shared_ptr<Array> templateArray, boost::shared_ptr<Array> sourceArray, boost::shared_ptr<Query> const& query);

  private:
    boost::shared_ptr<Array> templateArray;
    boost::shared_ptr<Array> sourceArray;
};

}

#endif
