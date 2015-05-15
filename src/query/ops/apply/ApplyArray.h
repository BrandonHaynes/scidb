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
 * @file ApplyArray.h
 *
 * @brief The implementation of the array iterator for the apply operator
 *
 */

#ifndef APPLY_ARRAY_H_
#define APPLY_ARRAY_H_

#include <string>
#include <vector>
#include "array/DelegateArray.h"
#include "array/Metadata.h"
#include "query/LogicalExpression.h"
#include "query/Expression.h"

namespace scidb
{

using namespace std;
using namespace boost;

class ApplyArray;
class ApplyArrayIterator;
class ApplyChunkIterator;


class ApplyChunkIterator : public DelegateChunkIterator
{
public:
    virtual  Value& getItem();
    virtual void operator ++();
    virtual void reset();
    virtual bool setPosition(Coordinates const& pos);
    ApplyChunkIterator(ApplyArrayIterator const& arrayIterator, DelegateChunk const* chunk, int iterationMode);
    bool isNull();
    virtual boost::shared_ptr<Query> getQuery() { return _query; }

private:
    ApplyArray const& _array;
    AttributeID _outAttrId;
    vector<BindInfo> const& _bindings;
    vector< boost::shared_ptr<ConstChunkIterator> > _iterators;
    ExpressionContext _params;
    int _mode;
    Value* _value;
    bool _applied;
    bool _nullable;
    shared_ptr<Query> _query;

};

class ApplyArrayIterator : public DelegateArrayIterator
{
    friend class ApplyChunkIterator;
  public:
    virtual void operator ++();
    virtual void reset();
    virtual bool setPosition(Coordinates const& pos);
    ApplyArrayIterator(ApplyArray const& array, AttributeID attrID, AttributeID inputAttrID);

  private:
    vector< boost::shared_ptr<ConstArrayIterator> > iterators;
    AttributeID inputAttrID;
};

class ApplyArray : public DelegateArray
{
    friend class ApplyArrayIterator;
    friend class ApplyChunkIterator;
  public:
    virtual DelegateChunk* createChunk(DelegateArrayIterator const* iterator, AttributeID id) const;
    virtual DelegateChunkIterator* createChunkIterator(DelegateChunk const* chunk, int iterationMode) const;
    virtual DelegateArrayIterator* createArrayIterator(AttributeID id) const;

    ApplyArray(ArrayDesc const& desc, boost::shared_ptr<Array> const& array,
               vector <shared_ptr<Expression> > expressions,
               const boost::shared_ptr<Query>& query, bool tile);

  private:
    vector <shared_ptr<Expression> > _expressions;
    vector <bool> _attributeNullable;
    vector <bool> _runInTileMode;
    vector <vector<BindInfo> > _bindingSets;

};

}

#endif
