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

#include <log4cxx/logger.h>
#include <query/Operator.h>
#include <array/Metadata.h>
#include <array/Array.h>
#include <array/Tile.h>
#include <system/Constants.h>
#include "TileApplyArray.h"

namespace scidb
{

using namespace boost;
using namespace std;

log4cxx::LoggerPtr TileApplyChunkIterator::_logger(log4cxx::Logger::getLogger("scidb.array.tileApply"));

inline bool TileApplyChunkIterator::isNull()
{
    return false;
}

void TileApplyChunkIterator::reset()
{
    _applied = false;
    _currPosition = -1;
    inputIterator->reset();
    if (!inputIterator->end())
    {
        for (size_t i = 0, n = _iterators.size(); i < n; i++)
        {
            if (_iterators[i] && _iterators[i] != inputIterator)
            {
                _iterators[i]->reset();
            }
        }
        assert(!isNull());
    }
}

position_t TileApplyChunkIterator::getLogicalPosition()
{
    if (_currPosition >= 0) {
        return _currPosition;
    }
    const CoordinatesMapper* coordMapper = this;

    const Coordinates& coords = getPosition();
    assert(!coords.empty());
    _currPosition = coordMapper->coord2pos(coords);

    return _currPosition;
}

bool TileApplyChunkIterator::setPosition(position_t pos)
{
    assert(pos>=0);
    if (pos>=0 && pos == _currPosition) {
        return true;
    }
    if (setPositionInternal(pos)) {
        _currPosition = pos;
        return true;
    }
    return false;
}

bool TileApplyChunkIterator::setPosition(Coordinates const& coords)
{
    assert(!coords.empty());
    position_t pos(-1);
    const CoordinatesMapper* coordMapper = this;
    if (_currPosition >=0) {
        pos = coordMapper->coord2pos(coords);
        assert(pos>=0);
        if (pos == _currPosition) {
            return true;
        }
    }

    if (setPositionInternal(coords)) {
        if (pos < 0) {
            assert(!coords.empty());
            pos = coordMapper->coord2pos(coords);
        }
        _currPosition = pos;
        assert(_currPosition>=0);
        return true;
    }
    return false;
}

Value& TileApplyChunkIterator::getItem()
{
    if (_applied)
    {
        return *const_cast<Value*>(_value);
    }
    for (size_t i = 0, n = _bindings.size(); i < n; i++)
    {
        BindInfo const& bindInfo = _bindings[i];
        switch (bindInfo.kind)
        {
        case BindInfo::BI_ATTRIBUTE:
        {
            _params[i] = _iterators[i]->getItem();
        }
        break;
        case BindInfo::BI_COORDINATE:
        {
            _params[i].setInt64(inputIterator->getPosition()[bindInfo.resolvedId]);
        }
        break;
        default:
        break;
        }
    }
    _value = static_cast<const Value*> (&_exp->evaluate(_params));
    _applied = true;

    return *const_cast<Value*>(_value); //ugly, getItem() should return Value& const
}

const Coordinates&
TileApplyChunkIterator::getData(scidb::Coordinates& offset,
                                size_t maxValues,
                                boost::shared_ptr<BaseTile>& tileData,
                                boost::shared_ptr<BaseTile>& tileCoords)
{
    return getDataInternal(offset, maxValues, tileData, tileCoords, true);
}

position_t
TileApplyChunkIterator::getData(position_t logicalOffset,
                                size_t maxValues,
                                boost::shared_ptr<BaseTile>& tileData,
                                boost::shared_ptr<BaseTile>& tileCoords)
{
    return getDataInternal(logicalOffset, maxValues, tileData, tileCoords, true);
}

const Coordinates&
TileApplyChunkIterator::getData(scidb::Coordinates& offset,
                                size_t maxValues,
                                boost::shared_ptr<BaseTile>& tileData)
{
    boost::shared_ptr<BaseTile> emptyTileCoords;
    const Coordinates& nextPos = getDataInternal(offset, maxValues, tileData, emptyTileCoords);
    assert(!emptyTileCoords);
    return nextPos;
}

position_t
TileApplyChunkIterator::getData(position_t logicalOffset,
                                size_t maxValues,
                                boost::shared_ptr<BaseTile>& tileData)
{
    boost::shared_ptr<BaseTile> emptyTileCoords;
    position_t nextPos = getDataInternal(logicalOffset, maxValues, tileData, emptyTileCoords);
    assert(!emptyTileCoords);
    return nextPos;
}

position_t
TileApplyChunkIterator::getDataInternal(position_t logicalOffset,
                                        size_t maxValues,
                                        boost::shared_ptr<BaseTile>& tileData,
                                        boost::shared_ptr<BaseTile>& tileCoords,
                                        bool withCoordinates)
{
    assert(! (getMode() & TILE_MODE));

    if (logicalOffset < 0 || !setPosition(logicalOffset)) {
        return position_t(-1);
    }

    const TypeId& dataType = getChunk().getAttributeDesc().getType();
    boost::shared_ptr<BaseTile> dataTile  = _tileFactory->construct(dataType, BaseEncoding::RLE);
    boost::shared_ptr<BaseTile> coordTile;

    if (withCoordinates) {
        const scidb::TypeId& coordTileType = "scidb::Coordinates";
        const CoordinatesMapperWrapper coordMapper(this);
        coordTile = _tileFactory->construct(coordTileType, BaseEncoding::ARRAY, &coordMapper);
    }

    populateTiles(maxValues, dataTile, coordTile);

    position_t pos(-1);
    if (!end()) {
        pos = getLogicalPosition();
        assert(pos>=0);
    }

    assert((!withCoordinates && !coordTile) ||
           (withCoordinates && dataTile->size() == coordTile->size()));

    tileData.swap(dataTile);
    tileCoords.swap(coordTile);
    return pos;
}

const Coordinates&
TileApplyChunkIterator::getDataInternal(scidb::Coordinates& offset,
                                        size_t maxValues,
                                        boost::shared_ptr<BaseTile>& tileData,
                                        boost::shared_ptr<BaseTile>& tileCoords,
                                        bool withCoordinates)
{
    assert(! (getMode() & TILE_MODE));

    if (offset.empty() || !setPosition(offset)) {
        offset.clear();
        return offset;
    }
    const TypeId& dataType = getChunk().getAttributeDesc().getType();

    boost::shared_ptr<BaseTile> dataTile  = _tileFactory->construct(dataType, BaseEncoding::RLE);
    boost::shared_ptr<BaseTile> coordTile;

    if (withCoordinates) {
        const scidb::TypeId& coordTileType = "scidb::Coordinates";
        CoordinatesMapperWrapper coordMapper(this);
        coordTile = _tileFactory->construct(coordTileType, BaseEncoding::ARRAY, &coordMapper);
    }

    populateTiles(maxValues, dataTile, coordTile);

    if (!end()) {
        const Coordinates& coords = getPosition();
        assert(!coords.empty());
        offset = coords;
    } else {
        offset.clear();
    }

    assert((!withCoordinates && coordTile->size()==0) ||
           (withCoordinates && dataTile->size() == coordTile->size()));

    tileData.swap(dataTile);
    tileCoords.swap(coordTile);
    return offset;
}


namespace {
double getTimeInSecs()
{
    struct timespec ts;
    if (::clock_gettime(CLOCK_REALTIME, &ts) == -1) {
        int err = errno;
        stringstream ss;
        ss << "clock_gettime(CLOCK_REALTIME,...) failed: " << ::strerror(err)
           << " (" << err << ")";
        assert(false);
        throw std::runtime_error(ss.str());
    }
    return (ts.tv_sec + ts.tv_nsec*1e-9);
}
void reportTiming(log4cxx::LoggerPtr& logger, const char* info, double newTime, double oldTime)
{
    const bool reportTiming = false;
    if (reportTiming) { // timing
        newTime = getTimeInSecs();
        LOG4CXX_TRACE(logger, info << newTime-oldTime);
        oldTime = newTime;
    }
}
}

void
TileApplyChunkIterator::populateTiles(size_t maxValues,
                                      boost::shared_ptr<BaseTile>& dataTile,
                                      boost::shared_ptr<BaseTile>& coordTile)
{
    double oldTime = getTimeInSecs();
    double newTime = oldTime;

    assert(!_applied);
    assert(dataTile);

    // Get tiles for all expression parameters.
    // The tiles can be of different sizes, so we use the smallest input tile size
    // to generate the output tile. Alternatively, we could keep reading the input
    // until we have maxValues of data, but that approach does not seem any more advantageous (?).

    position_t currPos = getLogicalPosition();
    assert(currPos>=0);
    _currPosition = -1;

    reportTiming(_logger, "TileApplyChunkIterator::populateTiles [pre-getData] took (sec): ", newTime, oldTime);

    boost::shared_ptr<BaseTile> inputDataTile;
    boost::shared_ptr<BaseTile> inputCoordTile;

    bool needCoords = coordTile;
    needCoords = needCoords || _needCoordinates;

    position_t nextPos(-1);
    if (needCoords) {
        inputIterator->getData( currPos, maxValues, inputDataTile, inputCoordTile );
        assert(inputCoordTile);
    } else {
        inputIterator->getData( currPos, maxValues, inputDataTile );
    }

    reportTiming(_logger, "TileApplyChunkIterator::populateTiles [getData] took (sec): ", newTime, oldTime);

    size_t minTileSize = inputDataTile->size();
    assert((inputCoordTile && inputCoordTile->size() == minTileSize) || !needCoords);

    size_t maxTileSize = minTileSize;

    std::vector< boost::shared_ptr<BaseTile> > inputDataTiles(_bindings.size());

    // Get input tiles
    size_t bSize = _bindings.size();
    for (size_t bIndx = 0; bIndx < bSize; ++bIndx)
    {
        if (!_iterators[bIndx])
        {
            // nothing
        } else if (_iterators[bIndx] == inputIterator) {

            inputDataTiles[bIndx] = inputDataTile;

        } else {
            position_t pos = _iterators[bIndx]->getData( currPos, maxValues,
                                                         inputDataTiles[bIndx]);

            const size_t tileSize = inputDataTiles[bIndx]->size();

            if (tileSize < minTileSize) {
                assert(pos < nextPos);
                nextPos = pos;
                minTileSize = tileSize;
            }

            if (tileSize > maxTileSize) {
                assert(pos > nextPos);
                maxTileSize = tileSize;
            }
        }
    }

    reportTiming(_logger, "TileApplyChunkIterator::populateTiles  [pre-expression] took (sec): ", newTime, oldTime);

    dataTile->initialize();
    dataTile->reserve(minTileSize);

    applyExpression(minTileSize, inputDataTiles, inputCoordTile, dataTile);

    reportTiming(_logger, "TileApplyChunkIterator::populateTiles [expression-all] took (sec): ", newTime, oldTime);

    dataTile->finalize();

    if (coordTile) {
        assert(needCoords);
        assert(inputCoordTile);
        assert(typeid(*coordTile) == typeid(*inputCoordTile));
        if (minTileSize == inputCoordTile->size()) {
            // use the input coordinate tile
            coordTile.swap(inputCoordTile);
        } else {
            // copy truncated
            assert(minTileSize < inputCoordTile->size());
            coordTile->initialize();
            coordTile->reserve(minTileSize);
            for (size_t ti = 0; ti < minTileSize; ++ti) {
                Value v;
                inputCoordTile->at(ti,v);
                coordTile->push_back(v);
            }
        }
        assert(dataTile->size() == coordTile->size());
    }
    if (minTileSize != maxTileSize) {
        assert(maxTileSize > minTileSize);
        assert(_currPosition < 0);
        // re-align the input iterators
        bool rc = setPosition(nextPos);
        assert(rc); rc=rc;
    } else {
        _currPosition = nextPos;
    }
    reportTiming(_logger, "TileApplyChunkIterator::populateTiles [exit] took (sec): ", newTime, oldTime);
}

/// Apply the expression on the input tiles
void TileApplyChunkIterator::applyExpression(size_t minTileSize,
                                             std::vector< boost::shared_ptr<BaseTile> >& inputDataTiles,
                                             boost::shared_ptr<BaseTile>& inputCoordTile,
                                             boost::shared_ptr<BaseTile>& dataTile)
{
    assert(dataTile);
    const CoordinatesMapper* coordMapper = this;

    Value value;
    size_t bSize = _bindings.size();
    // assert(bSize > 1);

    for (size_t ti = 0; ti < minTileSize; ++ti) {

        for (size_t bIndx = 0; bIndx < bSize; ++bIndx) {
            BindInfo const& bindInfo = _bindings[bIndx];

            switch (bindInfo.kind)
            {
            case BindInfo::BI_ATTRIBUTE:
            {
                assert(inputDataTiles[bIndx]);
                inputDataTiles[bIndx]->at(ti, _params[bIndx]);
            }
            break;
            case BindInfo::BI_COORDINATE:
            {
                assert(!inputDataTiles[bIndx]);
                assert(inputCoordTile);

                inputCoordTile->at(ti,value);
                assert(value.size() == sizeof(position_t));
                coordMapper->pos2coord(value.get<position_t>(), _scratchCoords);
                _params[bIndx].set<Coordinate>(_scratchCoords[bindInfo.resolvedId]);
            }
            break;
            case BindInfo::BI_VALUE:
            {
                assert(!inputDataTiles[bIndx]);
                assert(_params[bIndx] == bindInfo.value);
            }
            break;
            default:
            assert(false);
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNREACHABLE_CODE)
              << "TileApplyChunkIterator::applyExpression()";
            }
        }
        const Value& result = _exp->evaluate(_params);

        dataTile->push_back(result);
    }
}

void TileApplyChunkIterator::operator ++()
{
    _currPosition = -1;
    _applied = false;

    ++(*inputIterator);
    if (!inputIterator->end())
    {
        for (size_t i = 0, n = _iterators.size(); i < n; i++)
        {
            if (_iterators[i] && _iterators[i] != inputIterator)
            {
                ++(*_iterators[i]);
            }
        }
        assert(!isNull());
    }
}

TileApplyChunkIterator::TileApplyChunkIterator(TileApplyArrayIterator const& arrayIterator,
                                               DelegateChunk const* chunk,
                                               int iterationMode)
    : DelegateChunkIterator(chunk, iterationMode & ~(TILE_MODE |
                                                     INTENDED_TILE_MODE |
                                                     IGNORE_NULL_VALUES |
                                                     IGNORE_DEFAULT_VALUES))
    , CoordinatesMapper(*chunk)
    , _tileFactory(TileFactory::getInstance())
    , _currPosition(-1)
    , _array((TileApplyArray&) arrayIterator.array)
    , _exp ( (*_array._expressions)[arrayIterator.attr].get() )
    , _needCoordinates(false)
    , _bindings(_array._bindingSets[arrayIterator.attr]
                ? (*_array._bindingSets[arrayIterator.attr])
                : _fakeBinding)
    , _iterators(_bindings.size())
    , _params(*(*_array._expressions)[arrayIterator.attr])
    , _mode(iterationMode)
    , _applied(false)
    , _nullable(_array._attributeNullable[arrayIterator.attr])
    , _query(Query::getValidQueryPtr(_array._query))
{
    assert(! (_mode & TILE_MODE));

    // convert iters to tileiters
    boost::shared_ptr<ConstChunkIterator> tmp =
    boost::make_shared< TileConstChunkIterator< boost::shared_ptr<ConstChunkIterator> > > (inputIterator, _query);
    inputIterator.swap(tmp);

    for (size_t i = 0, n = _bindings.size(); i < n; ++i) {
        BindInfo const& bindInfo = _bindings[i];
        switch (bindInfo.kind)
        {
        case BindInfo::BI_COORDINATE:
        {
            _needCoordinates = true;
        }
        break;
        case BindInfo::BI_ATTRIBUTE:
        if (static_cast<AttributeID>(bindInfo.resolvedId) == arrayIterator._inputAttrID)
        {
            _iterators[i] = inputIterator;
        } else {
            const int iterMode = inputIterator->getMode();
            _iterators[i] = boost::make_shared<
               TileConstChunkIterator<boost::shared_ptr<ConstChunkIterator> >
               > (arrayIterator._iterators[i]->getChunk().getConstIterator(iterMode),
               _query);
        }
        break;
        case BindInfo::BI_VALUE:
        {
            _params[i] = bindInfo.value;
        }
        break;
        default:
        {
            assert(false);
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNREACHABLE_CODE)
              << "TileApplyChunkIterator::TileApplyChunkIterator()";

        }
        break;
        }
    }
    assert(!isNull());
}

TileApplyChunkIterator::~TileApplyChunkIterator()
{}

bool TileApplyArrayIterator::setPosition(Coordinates const& pos)
{
    if (inputIterator->setPosition(pos)) {
        for (size_t i = 0, n = _iterators.size(); i < n; ++i) {
            if (_iterators[i]) {
                if (!_iterators[i]->setPosition(pos)) {
                    throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OPERATION_FAILED) << "setPosition";
                }
            }
        }
        return true;
    }
    return false;
}

void TileApplyArrayIterator::reset()
{
    inputIterator->reset();
    for (size_t i = 0, n = _iterators.size(); i < n; ++i) {
        if (_iterators[i] && _iterators[i] != inputIterator) {
            _iterators[i]->reset();
        }
    }
}

void TileApplyArrayIterator::operator ++()
{
    ++(*inputIterator);
    for (size_t i = 0, n = _iterators.size(); i < n; ++i) {
        if (_iterators[i] && _iterators[i] != inputIterator) {
            ++(*_iterators[i]);
        }
    }
}

TileApplyArrayIterator::TileApplyArrayIterator(TileApplyArray const& array,
                                               AttributeID outAttrID,
                                               AttributeID inAttrID)
: DelegateArrayIterator(array, outAttrID, array.getInputArray()->getConstIterator(inAttrID)),
  _iterators(array._bindingSets[outAttrID] ? array._bindingSets[outAttrID]->size() : 0),
  _inputAttrID(inAttrID)
{
    vector<BindInfo> const fakeBinding(0);
    vector<BindInfo> const& bindings = array._bindingSets[outAttrID] ? *array._bindingSets[outAttrID] : fakeBinding;
    boost::shared_ptr<Array> inputArray = array.getInputArray();
    assert(inputArray!=NULL);

    assert(bindings.size() == _iterators.size());

    for (size_t i = 0, n = _iterators.size(); i < n; ++i)
    {
        BindInfo const& bindInfo = bindings[i];
        switch (bindInfo.kind)
        {
        case BindInfo::BI_ATTRIBUTE:
        if ((AttributeID) bindInfo.resolvedId == inAttrID)
        {
            _iterators[i] = inputIterator;
        }
        else
        {
            _iterators[i] = inputArray->getConstIterator(bindInfo.resolvedId);
        }
        break;
        case BindInfo::BI_COORDINATE:
        break;
        default:
        break;
        }
    }
}

DelegateChunkIterator* TileApplyArray::createChunkIterator(DelegateChunk const* chunk, int iterationMode) const
{
    StatisticsScope sScope(_statistics);
    TileApplyArrayIterator const& arrayIterator = (TileApplyArrayIterator const&) chunk->getArrayIterator();
    AttributeDesc const& attr = chunk->getAttributeDesc();
    AttributeID attId = attr.getId();

    assert (!chunk->inTileMode());
    iterationMode &= ~ChunkIterator::TILE_MODE;
    iterationMode &= ~ChunkIterator::INTENDED_TILE_MODE;

    if ( (*_expressions)[attId] ) {
        return new TileApplyChunkIterator(arrayIterator, chunk, iterationMode);
    }
    return new TileDelegateChunkIterator(chunk, iterationMode);
}

DelegateArrayIterator* TileApplyArray::createArrayIterator(AttributeID attrID) const
{
    AttributeID inputAttrID;

    if ( (*_expressions)[attrID] )
    {
        assert(_bindingSets[attrID]);
        vector<BindInfo> const& bindings = *_bindingSets[attrID];
        inputAttrID = 0;

        for (size_t i = 0, n = bindings.size(); i < n; ++i)
        {
            BindInfo const& bindInfo = bindings[i];
            if (bindInfo.kind == BindInfo::BI_ATTRIBUTE)
            {
                inputAttrID = (AttributeID) bindInfo.resolvedId;
                break;
            }
        }
    }
    else if (desc.getEmptyBitmapAttribute() && attrID == desc.getEmptyBitmapAttribute()->getId() )
    {
        inputAttrID = inputArray->getArrayDesc().getEmptyBitmapAttribute()->getId();
    }
    else
    {
        inputAttrID = attrID;
    }

    return new TileApplyArrayIterator(*this, attrID, inputAttrID);
}

DelegateChunk* TileApplyArray::createChunk(DelegateArrayIterator const* iterator, AttributeID attrID) const
{
    bool isClone = !(*_expressions)[attrID];
    DelegateChunk* chunk = new TileApplyChunk(*this, *iterator, attrID, isClone);
    return chunk;
}

TileApplyArray::TileApplyArray(const ArrayDesc& desc,
                               const boost::shared_ptr<Array>& array,
                               const boost::shared_ptr<vector<shared_ptr<Expression> > >& exprs,
                               const boost::shared_ptr<Query>& query)
: DelegateArray(desc, array),
  _expressions(exprs),
  _attributeNullable(desc.getAttributes().size(), false),
  _bindingSets(desc.getAttributes().size(), NULL)
{
    assert(query);
    _query=query;
    vector<shared_ptr<Expression> >& expressions = (*_expressions);
    for(size_t i =0; i<expressions.size(); ++i) {
        _attributeNullable[i] = desc.getAttributes()[i].isNullable();
        if (expressions[i]) {
            _bindingSets[i]= &(expressions[i]->getBindings());
        }
    }
}

}
