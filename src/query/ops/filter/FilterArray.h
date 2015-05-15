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
 * @file FilterArray.h
 *
 * @brief The implementation of the array iterator for the filter operator
 *
 */

#ifndef FILTER_ARRAY_H_
#define FILTER_ARRAY_H_

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

class FilterArray;
class FilterArrayIterator;
class FilterChunkIterator;


class FilterChunkIterator : public DelegateChunkIterator
{
  protected:
    Value& evaluate();
    Value& buildBitmap();
    bool filter();
    void moveNext();
    void nextVisible();

  public:
    virtual Value& getItem();
    virtual void operator ++();
    virtual void reset();
    virtual bool end();
    virtual bool setPosition(Coordinates const& pos);
    virtual boost::shared_ptr<Query> getQuery() { return _query; }
    FilterChunkIterator(FilterArrayIterator const& arrayIterator, DelegateChunk const* chunk, int iterationMode);

  protected:
    MemChunk shapeChunk;
    FilterArray const& _array;
    vector< boost::shared_ptr<ConstChunkIterator> > _iterators;
    boost::shared_ptr<ConstChunkIterator> emptyBitmapIterator;
    ExpressionContext _params;
    bool _hasCurrent;
    int _mode;
    Value tileValue;
    TypeId _type;
 private:
    boost::shared_ptr<Query> _query;
};


class ExistedBitmapChunkIterator : public FilterChunkIterator
{
public:
    virtual  Value& getItem();

    ExistedBitmapChunkIterator(FilterArrayIterator const& arrayIterator, DelegateChunk const* chunk, int iterationMode);

private:
     Value _value;
};


class NewBitmapChunkIterator : public FilterChunkIterator
{
public:
    virtual  Value& getItem();

    NewBitmapChunkIterator(FilterArrayIterator const& arrayIterator, DelegateChunk const* chunk, int iterationMode);
};


class FilterArrayIterator : public DelegateArrayIterator
{
    friend class FilterChunkIterator;
  public:
    bool setPosition(Coordinates const& pos);
    virtual void reset();
    virtual void operator ++();
    virtual ConstChunk const& getChunk();
    FilterArrayIterator(FilterArray const& array, AttributeID attrID,  AttributeID inputAttrID);

  private:
    vector< boost::shared_ptr<ConstArrayIterator> > iterators;
    boost::shared_ptr<ConstArrayIterator> emptyBitmapIterator;
    AttributeID inputAttrID;
};

class FilterArrayEmptyBitmapIterator : public FilterArrayIterator
{
    FilterArray& array;
  public:
    virtual ConstChunk const& getChunk();
    FilterArrayEmptyBitmapIterator(FilterArray const& array, AttributeID attrID,  AttributeID inputAttrID);
};

class FilterArray : public DelegateArray
{
    friend class FilterArrayIterator;
    friend class FilterChunkIterator;
    friend class NewBitmapChunkIterator;
  public:
    virtual DelegateChunk* createChunk(DelegateArrayIterator const* iterator, AttributeID id) const;
    virtual DelegateChunkIterator* createChunkIterator(DelegateChunk const* chunk, int iterationMode) const;
    virtual DelegateArrayIterator* createArrayIterator(AttributeID id) const;

    boost::shared_ptr<DelegateChunk> getEmptyBitmapChunk(FilterArrayEmptyBitmapIterator* iterator);

    FilterArray(ArrayDesc const& desc, boost::shared_ptr<Array> const& array,
                boost::shared_ptr< Expression> expr, boost::shared_ptr<Query>& query,
                bool tileMode);

  private:
    std::map<Coordinates, boost::shared_ptr<DelegateChunk>, CoordinatesLess > cache;
    Mutex mutex;
    boost::shared_ptr<Expression> expression;
    vector<BindInfo> bindings;
    bool _tileMode;
    size_t cacheSize;
    AttributeID emptyAttrID;

};

}

#endif
