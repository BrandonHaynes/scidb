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
 * @file TileApplyArray.h
 *
 * @brief The implementation of the array iterator for the apply operator
 *
 */

#ifndef TILE_APPLY_ARRAY_H_
#define TILE_APPLY_ARRAY_H_

#include <string>
#include <vector>
#include <array/DelegateArray.h>
#include <array/TileIteratorAdaptors.h>
#include <array/Metadata.h>
#include <query/LogicalExpression.h>
#include <query/Expression.h>
#include <log4cxx/logger.h>

namespace scidb
{

using namespace std;
using namespace boost;

class TileApplyArray;
class TileApplyArrayIterator;
class TileApplyChunkIterator;

/**
 * ChunkIterator for TileApplyArray
 */
class TileApplyChunkIterator : public DelegateChunkIterator, protected CoordinatesMapper
{
public:
    virtual Value& getItem();
    virtual void operator ++();
    virtual void reset();
    virtual bool setPosition(Coordinates const& pos);
    TileApplyChunkIterator(TileApplyArrayIterator const& arrayIterator,
                           DelegateChunk const* chunk,
                           int iterationMode);
    virtual ~TileApplyChunkIterator();
    bool isNull();
    virtual boost::shared_ptr<Query> getQuery() const { return _query; }

    virtual position_t getLogicalPosition();

    virtual bool setPosition(position_t pos);

    virtual operator const CoordinatesMapper* () const { return this; }

    virtual const Coordinates&
    getData(scidb::Coordinates& offset,
            size_t maxValues,
            boost::shared_ptr<BaseTile>& tileData,
            boost::shared_ptr<BaseTile>& tileCoords);

    virtual position_t
    getData(position_t logicalOffset,
            size_t maxValues,
            boost::shared_ptr<BaseTile>& tileData,
            boost::shared_ptr<BaseTile>& tileCoords);

    virtual const Coordinates&
    getData(scidb::Coordinates& offset,
            size_t maxValues,
            boost::shared_ptr<BaseTile>& tileData);

    virtual position_t
    getData(position_t logicalOffset,
            size_t maxValues,
            boost::shared_ptr<BaseTile>& tileData);
private:

    template <typename PosType>
    bool setPositionInternal(PosType const& coords)
    {
        _currPosition = -1;
        _applied = false;
        bool res = false;
        if (inputIterator->setPosition(coords)) {
            for (size_t i = 0, n = _iterators.size(); i < n; i++) {
                if (_iterators[i] && (!_iterators[i]->setPosition(coords))) {
                    throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OPERATION_FAILED) << "setPosition";
                }
            }
            res = !isNull();
        }
        return res;
    }

    position_t
    getDataInternal(position_t logicalOffset,
                    size_t maxValues,
                    boost::shared_ptr<BaseTile>& tileData,
                    boost::shared_ptr<BaseTile>& tileCoords,
                    bool withCoordinates=false);

    const Coordinates&
    getDataInternal(scidb::Coordinates& offset,
                    size_t maxValues,
                    boost::shared_ptr<BaseTile>& tileData,
                    boost::shared_ptr<BaseTile>& tileCoords,
                    bool withCoordinates=false);
    void
    populateTiles(size_t maxValues,
                  boost::shared_ptr<BaseTile>& dataTile,
                  boost::shared_ptr<BaseTile>& coordTile);

    void applyExpression(size_t minTileSize,
                         std::vector< boost::shared_ptr<BaseTile> >& inputDataTiles,
                         boost::shared_ptr<BaseTile>& inputCoordTile,
                         boost::shared_ptr<BaseTile>& dataTile);


    class CoordinatesMapperWrapper : public CoordinatesMapperProvider
    {
    private:
        CoordinatesMapper* _mapper;
    public:
        CoordinatesMapperWrapper(CoordinatesMapper* mapper) : _mapper(mapper)
        { assert(_mapper); }
        virtual ~CoordinatesMapperWrapper() {}
        virtual operator const CoordinatesMapper* () const { return _mapper; }
    };

    TileFactory* _tileFactory; // cached singleton pointer
    position_t _currPosition;
    Coordinates _scratchCoords; // too expensive to create on stack
    static log4cxx::LoggerPtr _logger;

    TileApplyArray const& _array;
    Expression* _exp;
    mutable bool _needCoordinates;
    AttributeID _outAttrId;
    vector<BindInfo> const& _bindings;
    vector<BindInfo> const _fakeBinding;
    vector< boost::shared_ptr<ConstChunkIterator> > _iterators;
    ExpressionContext _params;
    int _mode;
    const Value* _value;
    bool _applied;
    bool _nullable;
    boost::shared_ptr<Query> _query;
};

/**
 * ArrayIterator for TileApplyArray
 */
class TileApplyArrayIterator : public DelegateArrayIterator
{
    friend class TileApplyChunkIterator;
  public:
    virtual void operator ++();
    virtual void reset();
    virtual bool setPosition(Coordinates const& pos);
    TileApplyArrayIterator(TileApplyArray const& array, AttributeID attrID, AttributeID inputAttrID);

  private:
    std::vector< boost::shared_ptr<ConstArrayIterator> > _iterators;
    AttributeID _inputAttrID;
};

/**
 * TileApplyArray generates an additional attribute by applying an expression on the input attributes and/or coordinates.
 * The additional attribute is returned along with the input attributes.
 */
class TileApplyArray : public DelegateArray
{
    friend class TileApplyArrayIterator;
    friend class TileApplyChunkIterator;
  public:
    virtual DelegateChunk* createChunk(DelegateArrayIterator const* iterator, AttributeID id) const;
    virtual DelegateChunkIterator* createChunkIterator(DelegateChunk const* chunk, int iterationMode) const;
    virtual DelegateArrayIterator* createArrayIterator(AttributeID id) const;
    boost::shared_ptr<Query> getQuery() const { return Query::getValidQueryPtr(_query); }

    TileApplyArray(const ArrayDesc& desc,
                   const boost::shared_ptr<Array>& array,
                   const boost::shared_ptr< std::vector< boost::shared_ptr< Expression > > >& expressions,
                   const boost::shared_ptr<Query>& query);

  private:
    boost::shared_ptr<std::vector<boost::shared_ptr<Expression> > > _expressions;
    vector <bool> _attributeNullable;
    vector <bool> _runInTileMode;
    vector <const vector<BindInfo>* > _bindingSets;

};

/**
 * Chunk for TileApplyArray.
 * It wraps TileApplyChunkIterator into BufferedConstChunkIterator
 * to make sure it generates one tile at time (vs one value at at time)
 */

class TileApplyChunk : public DelegateChunk
{
public:
    TileApplyChunk(DelegateArray const& array, DelegateArrayIterator const& iterator, AttributeID attrID, bool isClone)
    : DelegateChunk(array, iterator, attrID, isClone)
    {}
    boost::shared_ptr<ConstChunkIterator> getConstIterator(int iterationMode) const
    {
        const TileApplyArray* myArray = dynamic_cast<const TileApplyArray*>(&array);
        boost::shared_ptr<ConstChunkIterator> iter = DelegateChunk::getConstIterator(iterationMode);
        if (dynamic_cast<TileApplyChunkIterator*>(iter.get())) {
            return boost::make_shared<BufferedConstChunkIterator<
            boost::shared_ptr<ConstChunkIterator> > >(iter, myArray->getQuery());
        }
        return iter;
    }
};

}

#endif
