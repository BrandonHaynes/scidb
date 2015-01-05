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
#ifndef __TILE_ITERATOR_ADAPTERS__
#define __TILE_ITERATOR_ADAPTERS__

#include <assert.h>         // Lots of asserts in this prototype.
#include <string.h>
#include <stdint.h>         // For the various int types
#include <memory>           // For the unique_ptr
#include <iostream>         // For the operator<< and the dump().
#include <vector>
#include <algorithm>
#include <boost/make_shared.hpp>

#include <array/Array.h>
#include <array/Tile.h>
#include <query/Query.h>
#include <util/CoordinatesMapper.h>
#ifndef SCIDB_CLIENT
#include <system/Config.h>
#endif
#include <array/DelegateArray.h>

namespace scidb {
    /**
     * Templated chunk iterator wrapper that routes getItem() calls to getData() calls.
     * The getItem() method returns an element from an internally buffered tile, in other words this iterator
     * prefetches a tile worth of data at a time.
     */
    template< class TiledChunkIterator >
    class BufferedConstChunkIterator : public ConstChunkIterator
    {
    public:
        BufferedConstChunkIterator(TiledChunkIterator tiledIterator,
                                   boost::shared_ptr<Query> const& query);

        Value& getItem();
        Coordinates const& getPosition();
        position_t getLogicalPosition();
        bool setPosition(Coordinates const& pos);
        bool setPosition(position_t pos);
        void operator ++();
        void reset();
        bool end();

        virtual int getMode()  { return _tiledChunkIterator->getMode(); }
        virtual bool isEmpty() { return _tiledChunkIterator->isEmpty(); }
        virtual const scidb::ConstChunk& getChunk() { return _tiledChunkIterator->getChunk(); }

        /// @see ConstChunkIterator
        virtual const Coordinates& getData(scidb::Coordinates& logicalStart /*IN/OUT*/,
                                           size_t maxValues,
                                           boost::shared_ptr<BaseTile>& tileData,
                                           boost::shared_ptr<BaseTile>& tileCoords);
        /// @see ConstChunkIterator
        virtual position_t getData(position_t logicalStart,
                                   size_t maxValues,
                                   boost::shared_ptr<BaseTile>& tileData,
                                   boost::shared_ptr<BaseTile>& tileCoords);
        /// @see ConstChunkIterator
        virtual const Coordinates&
        getData(scidb::Coordinates& offset,
                size_t maxValues,
                boost::shared_ptr<BaseTile>& tileData);

        /// @see ConstChunkIterator
        virtual position_t
        getData(position_t logicalOffset,
                size_t maxValues,
                boost::shared_ptr<BaseTile>& tileData);

        /// @see ConstChunkIterator
        virtual operator const CoordinatesMapper* () const
        {
            return (*_tiledChunkIterator);
        }

    private:
        void clearCurrentLPosInTile(bool clearTiles=false)
        {
            _currLPosInTile = -1;
            _currTileIndex  = 0;
            if (clearTiles && (_tileData || _tileCoords)) {
                _tileData.reset();
                _tileCoords.reset();
            }
        }
        bool isCurrentLPosNotInTile()
        {
            return (_currLPosInTile < 0);
        }
        static bool findPosition(const boost::shared_ptr<BaseTile>& tileCoords, position_t pos, size_t& index);
        static const uint64_t DEFAULT_TILE_SIZE=10000;
        static log4cxx::LoggerPtr _logger;
        static const bool _sDebug = false;

        TiledChunkIterator _tiledChunkIterator;
        uint64_t     _tileSize;
        boost::shared_ptr<BaseTile> _tileData;
        boost::shared_ptr<BaseTile> _tileCoords;
        position_t _currLPosInTile;
        size_t     _currTileIndex;
        Coordinates _currPos;
        Value      _value;
    };

    template<typename TiledChunkIterator>
    BufferedConstChunkIterator<TiledChunkIterator>::BufferedConstChunkIterator(TiledChunkIterator tiledIterator,
                                                                               boost::shared_ptr<Query> const& query)

    : _tiledChunkIterator(tiledIterator),
    _tileSize(DEFAULT_TILE_SIZE),
    _currLPosInTile(-1),
    _currTileIndex(0)
    {
        assert(_tiledChunkIterator);
        assert(! (_tiledChunkIterator->getMode() & TILE_MODE));

#ifndef SCIDB_CLIENT
        int tsize = Config::getInstance()->getOption<int>(CONFIG_TILE_SIZE);
        _tileSize = (tsize<=0) ? DEFAULT_TILE_SIZE : uint64_t(tsize);
#endif
    }

    template<typename TiledChunkIterator>
    bool BufferedConstChunkIterator<TiledChunkIterator>::setPosition(Coordinates const& coord)
    {
        assert(! (_tiledChunkIterator->getMode() & TILE_MODE));
        clearCurrentLPosInTile();
        return _tiledChunkIterator->setPosition(coord);
    }

    template<typename TiledChunkIterator>
    bool BufferedConstChunkIterator<TiledChunkIterator>::setPosition(position_t coord)
    {
        assert(! (_tiledChunkIterator->getMode() & TILE_MODE));
        clearCurrentLPosInTile();
        return _tiledChunkIterator->setPosition(coord);
    }

    template<typename TiledChunkIterator>
    bool
    BufferedConstChunkIterator<TiledChunkIterator>::end()
    {
        if ( isCurrentLPosNotInTile()) {
            return _tiledChunkIterator->end();
        }
        if (_sDebug) {
            const char * func = "BufferedConstChunkIterator<TiledChunkIterator>::end";
            LOG4CXX_TRACE(_logger, func << " this="<<this << " KNOWN _currLPosInTile="<<_currLPosInTile
                          << " tile coord size="<<_tileCoords->size()<<" tile index="<< _currTileIndex);
        }
        return false;
    }

    template<typename TiledChunkIterator>
    Coordinates const&
    BufferedConstChunkIterator<TiledChunkIterator>::getPosition()
    {
        assert(! (_tiledChunkIterator->getMode() & TILE_MODE));
        if ( isCurrentLPosNotInTile()) {
            return _tiledChunkIterator->getPosition();
        }
        assert(_tileCoords);
        assert(_tileData);
        const CoordinatesMapper* coordMapper = (*_tiledChunkIterator);
        coordMapper->pos2coord(_currLPosInTile, _currPos);
        return _currPos;
    }

    template<typename TiledChunkIterator>
    position_t
    BufferedConstChunkIterator<TiledChunkIterator>::getLogicalPosition()
    {
        assert(! (_tiledChunkIterator->getMode() & TILE_MODE));
        if ( isCurrentLPosNotInTile()) {
            return _tiledChunkIterator->getLogicalPosition();
        }
        assert(_tileCoords);
        assert(_tileData);
        return _currLPosInTile;
    }

    template<typename TiledChunkIterator>
    void
    BufferedConstChunkIterator<TiledChunkIterator>::operator ++()
    {
        assert(! (_tiledChunkIterator->getMode() & TILE_MODE));

        if ( isCurrentLPosNotInTile()) {
            _tiledChunkIterator->operator++();
            return;
        }

        if (_sDebug) {
            const char * func = "BufferedConstChunkIterator<TiledChunkIterator>::operator++";
            LOG4CXX_TRACE(_logger, func << " this="<<this << " KNOWN _currLPosInTile="<<_currLPosInTile
                          << " tile coord size="<<_tileCoords->size()<<" tile index="<< _currTileIndex);
        }

        assert(_tileCoords && _currTileIndex < _tileCoords->size());
        if (_currTileIndex+1 >=_tileCoords->size()) {
            clearCurrentLPosInTile();
            return;
        }

        ++_currTileIndex;
        assert(_tileCoords && _currTileIndex < _tileCoords->size());

        Value newPos(sizeof(position_t));
        _tileCoords->at(_currTileIndex, newPos);

        assert(newPos.get<position_t > () > _currLPosInTile);
        _currLPosInTile = newPos.get<position_t>();
    }

    template<typename TiledChunkIterator>
    void
    BufferedConstChunkIterator<TiledChunkIterator>::reset()
    {
        assert(! (_tiledChunkIterator->getMode() & TILE_MODE));
        clearCurrentLPosInTile();
        _tiledChunkIterator->reset();
    }

    template<typename TiledChunkIterator>
    Value&
    BufferedConstChunkIterator<TiledChunkIterator>::getItem()
    {
        const char * func = "BufferedConstChunkIterator<TiledChunkIterator>::getItem";
        assert(! (_tiledChunkIterator->getMode() & TILE_MODE));

        if ( isCurrentLPosNotInTile()) {

            assert(_tileSize>0);
            size_t index(0);
            position_t lPos = _tiledChunkIterator->getLogicalPosition();

            // check if we already have the tile
            if (!_tileCoords || !findPosition(_tileCoords, lPos, index)) {

                position_t nextTilePos = _tiledChunkIterator->getData(lPos, _tileSize, _tileData, _tileCoords);
                assert(nextTilePos!=lPos);
                nextTilePos = nextTilePos;
            } else {
                // make sure the logical position of the parent is right after the current tile
                assert(_tileCoords);
                assert(_tileCoords->size()>0);
                Value lastTilePos(sizeof(position_t));
                _tileCoords->at(_tileCoords->size()-1,lastTilePos);

                bool rc = _tiledChunkIterator->setPosition(lastTilePos.get<position_t>());
                assert(rc); rc=rc;
                _tiledChunkIterator->operator++();
            }
            assert(_tileCoords);
            assert(_tileData);

            Value newPos(sizeof(position_t));
            _tileCoords->at(index,newPos);
            _currLPosInTile = newPos.get<position_t>();
            assert(_currLPosInTile>=lPos);
            _currTileIndex = index;

            if (_sDebug) {
                LOG4CXX_TRACE(_logger, func << " this="<<this << " NEW _currLPosInTile="<<_currLPosInTile
                              << " tile "<<_tileData<<" data["<< index<<"]");
            }
            assert(_tileCoords && _currTileIndex < _tileCoords->size());
            _tileData->at(index,_value);

        } else {

            assert(_tileCoords);
            assert(_tileData);
            if (_sDebug) {
                LOG4CXX_TRACE(_logger, func << " this="<<this << " KNOWN _currLPosInTile="<<_currLPosInTile
                              << " tile "<<_tileData<<" data["<< _currTileIndex <<"]");
            }

            assert(_tileCoords && _currTileIndex < _tileCoords->size());
            _tileData->at(_currTileIndex,_value);
        }
        if (_sDebug) {
            LOG4CXX_TRACE(_logger, func << " this="<<this << " return val="<<_value);
        }
        return _value;
    }

    template<typename TiledChunkIterator>
    const Coordinates&
    BufferedConstChunkIterator<TiledChunkIterator>::getData(scidb::Coordinates& offset,
                                                            size_t maxValues,
                                                            boost::shared_ptr<BaseTile>& tileData,
                                                            boost::shared_ptr<BaseTile>& tileCoords)
    {
        clearCurrentLPosInTile(true);
        return _tiledChunkIterator->getData(offset,
                                            maxValues,
                                            tileData,
                                            tileCoords);
    }

    template<typename TiledChunkIterator>
    position_t
    BufferedConstChunkIterator<TiledChunkIterator>::getData(position_t logicalOffset,
                                                            size_t maxValues,
                                                            boost::shared_ptr<BaseTile>& tileData,
                                                            boost::shared_ptr<BaseTile>& tileCoords)
    {
        clearCurrentLPosInTile(true);
        return _tiledChunkIterator->getData(logicalOffset,
                                            maxValues,
                                            tileData,
                                            tileCoords);
    }

    template<typename TiledChunkIterator>
    const Coordinates&
    BufferedConstChunkIterator<TiledChunkIterator>::getData(scidb::Coordinates& offset,
                                                            size_t maxValues,
                                                            boost::shared_ptr<BaseTile>& tileData)
    {
        clearCurrentLPosInTile(true);
        return _tiledChunkIterator->getData(offset,
                                            maxValues,
                                            tileData);
    }

    template<typename TiledChunkIterator>
    position_t
    BufferedConstChunkIterator<TiledChunkIterator>::getData(position_t logicalOffset,
                                                            size_t maxValues,
                                                            boost::shared_ptr<BaseTile>& tileData)
    {
        clearCurrentLPosInTile(true);
        return _tiledChunkIterator->getData(logicalOffset,
                                            maxValues,
                                            tileData);
    }

    /**
     * Find the tile index for a given logical position
     * @note on implemetation: Tile should provide an std-like iterator,
     * and this method should use an std-like binary search like std::lower_bound internally
     * for now it is a hand-crafted binary search
     */
    template<typename TiledChunkIterator>
    bool
    BufferedConstChunkIterator<TiledChunkIterator>::findPosition(const boost::shared_ptr<BaseTile>& tileCoords,
                                                                 position_t pos,
                                                                 size_t& index)
    {
        size_t low  = 0;
        size_t high = tileCoords->size();
        if (high == 0) {
            return false;
        }
        assert(high>0);
        --high;

        Value curr(sizeof(position_t));
        position_t pcurr(-1);

        // check hint
        if (high >0) {
            size_t hint = (index % high) + 1;
            tileCoords->at(hint, curr);
            pcurr = curr.get<position_t>();
            if (pcurr == pos) {
                index = hint;
                return true;
            }
        }

        // check right side
        tileCoords->at(high, curr);
        pcurr = curr.get<position_t>();
        if (pcurr == pos) {
            index = high;
            return true;
        }
        if (pos > pcurr) {
            return false;
        }
        if (high==0) {
            assert(low==high);
            return false;
        }
        --high;

        // check left side
        tileCoords->at(low, curr);
        pcurr = curr.get<position_t>();
        if (pcurr == pos) {
            index = low;
            return true;
        }
        if (pos < pcurr) {
            return false;
        }
        ++low;

        // binary search
        while (low <= high) {

            size_t mid = (low + high) / 2;
            tileCoords->at(mid, curr);
            pcurr = curr.get<position_t>();

            if (pos == pcurr) {
                index = mid;
                return true;
            } else if ( pos < pcurr ) {
                assert(mid>0);
                high = mid-1;
            } else {
                low = mid+1;
            }
        }
        return false;
    }

    template<typename TiledChunkIterator>
    log4cxx::LoggerPtr BufferedConstChunkIterator<TiledChunkIterator>::_logger(log4cxx::Logger::getLogger("scidb.array.tile"));

    /**
     * Templated chunk iterator wrapper that routes getData() calls to a series of getData() calls.
     */
    template< class ItemChunkIterator >
    class TileConstChunkIterator : public ConstChunkIterator
    {
    public:
        TileConstChunkIterator(ItemChunkIterator tiledIterator,
                               boost::shared_ptr<Query> const& query);

        Value& getItem() { return _itemChunkIterator->getItem(); }

        Coordinates const& getPosition()         { return _itemChunkIterator->getPosition(); }
        position_t getLogicalPosition();
        bool setPosition(Coordinates const& pos) { return _itemChunkIterator->setPosition(pos); }
        bool setPosition(position_t pos);

        void operator ++() { _itemChunkIterator->operator++(); }
        void reset()       { _itemChunkIterator->reset(); }
        bool end()         { return _itemChunkIterator->end(); }

        virtual int getMode()  { return _itemChunkIterator->getMode(); }
        virtual bool isEmpty() { return _itemChunkIterator->isEmpty(); }
        virtual const scidb::ConstChunk& getChunk() { return _itemChunkIterator->getChunk(); }

        /// @see ConstChunkIterator
        virtual const Coordinates& getData(scidb::Coordinates& logicalStart /*IN/OUT*/,
                                           size_t maxValues,
                                           boost::shared_ptr<BaseTile>& tileData,
                                           boost::shared_ptr<BaseTile>& tileCoords);
        /// @see ConstChunkIterator
        virtual position_t getData(position_t logicalStart,
                                   size_t maxValues,
                                   boost::shared_ptr<BaseTile>& tileData,
                                   boost::shared_ptr<BaseTile>& tileCoords);
        /// @see ConstChunkIterator
        virtual const Coordinates& getData(scidb::Coordinates& offset,
                                           size_t maxValues,
                                           boost::shared_ptr<BaseTile>& tileData);
        /// @see ConstChunkIterator
        virtual position_t getData(position_t logicalOffset,
                                   size_t maxValues,
                                   boost::shared_ptr<BaseTile>& tileData);
        /// @see ConstChunkIterator
        virtual operator const CoordinatesMapper* () const
        {
            return &_itemChunkCoordMapper;
        }

    private:

        /// Container for a CoordinatesMapper to be used with Coordinate tiles
        class CoordinatesMapperWrapper : public CoordinatesMapperProvider
        {
        private:
            const CoordinatesMapper* _mapper;
        public:
        CoordinatesMapperWrapper(const CoordinatesMapper* mapper) : _mapper(mapper)
            {
                assert(_mapper);
            }
            virtual ~CoordinatesMapperWrapper() {}
            virtual operator const CoordinatesMapper* () const
            {
                return _mapper;
            }
        };

        position_t
        getDataInternal(position_t logicalOffset,
                        size_t maxValues,
                        boost::shared_ptr<BaseTile>& tileData,
                        boost::shared_ptr<BaseTile>& tileCoords,
                        BaseTile::Context* coordCtx);
        const Coordinates&
        getDataInternal(scidb::Coordinates& offset,
                        size_t maxValues,
                        boost::shared_ptr<BaseTile>& tileData,
                        boost::shared_ptr<BaseTile>& tileCoords,
                        BaseTile::Context* coordCtx);
        /**
         * Populate supplied tiles with data and coordinates
         * using the internal _itemChunkIterator
         * @param maxValues add upto this number of elements into the tiles
         * @param dataTile may not be NULL
         * @param coordTile may be NULL
         */
        void populateTiles(size_t maxValues,
                      boost::shared_ptr<BaseTile>& dataTile,
                      boost::shared_ptr<BaseTile>& coordTile);

        static log4cxx::LoggerPtr _logger;

        /// iterator that may not support getData() interface
        ItemChunkIterator _itemChunkIterator;
        /// CoordinatesMapper for the current chunk
        CoordinatesMapper _itemChunkCoordMapper;
        /// cached singleton pointer
        TileFactory* _tileFactory;
        /// if _itemChunkIterator supports getData() calls, _passTrhu is true
        bool _passThru;
    };

    template<typename ItemChunkIterator>
    TileConstChunkIterator<ItemChunkIterator>::TileConstChunkIterator(ItemChunkIterator itemIterator,
                                                                      boost::shared_ptr<Query> const& query)
    : _itemChunkIterator(itemIterator),
    _itemChunkCoordMapper(itemIterator->getChunk()),
    _tileFactory(TileFactory::getInstance()),
    _passThru(true)
    {
        assert(_itemChunkIterator);
        assert(! (_itemChunkIterator->getMode() & TILE_MODE));
    }

    template<typename ItemChunkIterator>
    const Coordinates&
    TileConstChunkIterator<ItemChunkIterator>::getData(scidb::Coordinates& offset,
                                                       size_t maxValues,
                                                       boost::shared_ptr<BaseTile>& tileData,
                                                       boost::shared_ptr<BaseTile>& tileCoords)
    {
        if (_passThru) {
            try {
                return _itemChunkIterator->getData(offset, maxValues, tileData, tileCoords);
            } catch (const scidb::SystemException& e) {
                if (e.getLongErrorCode() != SCIDB_LE_UNREACHABLE_CODE) {
                    throw;
                }
                _passThru = false;
            }
        }
        CoordinatesMapperWrapper coordMapper(&_itemChunkCoordMapper);
        return getDataInternal(offset, maxValues,
                               tileData, tileCoords,
                               &coordMapper);
    }

    template<typename ItemChunkIterator>
    position_t
    TileConstChunkIterator<ItemChunkIterator>::getData(position_t logicalOffset,
                                                       size_t maxValues,
                                                       boost::shared_ptr<BaseTile>& tileData,
                                                       boost::shared_ptr<BaseTile>& tileCoords)
    {
        if (_passThru) {
            try {
                return _itemChunkIterator->getData(logicalOffset, maxValues, tileData, tileCoords);
            } catch (const scidb::SystemException& e) {
                if (e.getLongErrorCode() != SCIDB_LE_UNREACHABLE_CODE) {
                    throw;
                }
                _passThru = false;
            }
        }
        CoordinatesMapperWrapper coordMapper(&_itemChunkCoordMapper);
        return getDataInternal(logicalOffset, maxValues,
                               tileData, tileCoords,
                               &coordMapper);
    }

    template<typename ItemChunkIterator>
    position_t TileConstChunkIterator<ItemChunkIterator>::getLogicalPosition()
    {
        if (_passThru) {
            try {
                return _itemChunkIterator->getLogicalPosition();
            } catch (const scidb::SystemException& e) {
                if (e.getLongErrorCode() != SCIDB_LE_UNREACHABLE_CODE) {
                    throw;
                }
                _passThru = false;
            }
        }
        const CoordinatesMapper* mapper = (*this);
        const Coordinates& coords = getPosition();
        assert(!coords.empty());

        position_t pos = mapper->coord2pos(coords);
        assert(pos>=0);
        return pos;
    }

    template<typename ItemChunkIterator>
    bool TileConstChunkIterator<ItemChunkIterator>::setPosition(position_t pos)
    {
        if (_passThru) {
            try {
                return _itemChunkIterator->setPosition(pos);
            } catch (const scidb::SystemException& e) {
                if (e.getLongErrorCode() != SCIDB_LE_UNREACHABLE_CODE) {
                    throw;
                }
                _passThru = false;
            }
        }
        assert(pos>=0);
        const CoordinatesMapper* mapper = (*this);
        Coordinates coords;
        mapper->pos2coord(pos, coords);
        assert(!coords.empty());

        return setPosition(coords);
    }

    template<typename ItemChunkIterator>
    const Coordinates&
    TileConstChunkIterator<ItemChunkIterator>::getData(scidb::Coordinates& offset,
                                                       size_t maxValues,
                                                       boost::shared_ptr<BaseTile>& tileData)
    {
        if (_passThru) {
            try {
                return _itemChunkIterator->getData(offset, maxValues, tileData);
            } catch (const scidb::SystemException& e) {
                if (e.getLongErrorCode() != SCIDB_LE_UNREACHABLE_CODE) {
                    throw;
                }
                _passThru = false;
            }
        }
        boost::shared_ptr<BaseTile> tileCoords;
        return getDataInternal(offset, maxValues,
                               tileData, tileCoords,
                               NULL);
    }

    template<typename ItemChunkIterator>
    position_t
    TileConstChunkIterator<ItemChunkIterator>::getData(position_t logicalOffset,
                                                       size_t maxValues,
                                                       boost::shared_ptr<BaseTile>& tileData)
    {
        if (_passThru) {
            try {
                return _itemChunkIterator->getData(logicalOffset, maxValues, tileData);
            } catch (const scidb::SystemException& e) {
                if (e.getLongErrorCode() != SCIDB_LE_UNREACHABLE_CODE) {
                    throw;
                }
                _passThru = false;
            }
        }
        boost::shared_ptr<BaseTile> tileCoords;
        return getDataInternal(logicalOffset, maxValues,
                               tileData, tileCoords,
                               NULL);
    }

    template<typename ItemChunkIterator>
    position_t
    TileConstChunkIterator<ItemChunkIterator>::getDataInternal(position_t logicalOffset,
                                                               size_t maxValues,
                                                               boost::shared_ptr<BaseTile>& tileData,
                                                               boost::shared_ptr<BaseTile>& tileCoords,
                                                               BaseTile::Context* coordCtx)
    {
        assert(! (getMode() & TILE_MODE));

        const CoordinatesMapper* coordMapper = &_itemChunkCoordMapper;
        {
            Coordinates coords;
            coordMapper->pos2coord(logicalOffset, coords);
            assert(!coords.empty());

            if (!_itemChunkIterator->setPosition(coords)) {
                // Reset OUT pointers: there's no data here, move along.
                tileData.reset();
                tileCoords.reset();
                return position_t(-1);
            }
        }
        const TypeId& dataType = _itemChunkIterator->getChunk().getAttributeDesc().getType();

        boost::shared_ptr<BaseTile> dataTile  = _tileFactory->construct(dataType, BaseEncoding::RLE);
        boost::shared_ptr<BaseTile> coordTile;

        if (coordCtx) {
            const scidb::TypeId& coordTileType = "scidb::Coordinates";
            coordTile = _tileFactory->construct(coordTileType, BaseEncoding::ARRAY, coordCtx);
        }

        populateTiles(maxValues, dataTile, coordTile);

        position_t pos(-1);
        if (!_itemChunkIterator->end()) {

            const Coordinates& coords = _itemChunkIterator->getPosition();
            assert(!coords.empty());

            pos = coordMapper->coord2pos(coords);
            assert(pos>=0);
        }

        tileData.swap(dataTile);
        if (coordCtx) {
            assert(tileData->size() == coordTile->size());
            tileCoords.swap(coordTile);
        }
        return pos;
    }

    template<typename ItemChunkIterator>
    const Coordinates&
    TileConstChunkIterator<ItemChunkIterator>::getDataInternal(scidb::Coordinates& offset,
                                                               size_t maxValues,
                                                               boost::shared_ptr<BaseTile>& tileData,
                                                               boost::shared_ptr<BaseTile>& tileCoords,
                                                               BaseTile::Context* coordCtx)
    {
        assert(! (getMode() & TILE_MODE));

        if (offset.empty() || !_itemChunkIterator->setPosition(offset)) {
            // Reset OUT pointers: there's no data here, move along.
            tileData.reset();
            tileCoords.reset();
            offset.clear();
            return offset;
        }
        const TypeId& dataType = _itemChunkIterator->getChunk().getAttributeDesc().getType();

        boost::shared_ptr<BaseTile> dataTile  = _tileFactory->construct(dataType, BaseEncoding::RLE);
        boost::shared_ptr<BaseTile> coordTile;
        if (coordCtx) {
            const scidb::TypeId& coordTileType = "scidb::Coordinates";
            coordTile = _tileFactory->construct(coordTileType, BaseEncoding::ARRAY, coordCtx);
        }
        populateTiles(maxValues, dataTile, coordTile);

        if (!_itemChunkIterator->end()) {
            const Coordinates& coords = _itemChunkIterator->getPosition();
            assert(!coords.empty());
            offset = coords;
        } else {
            offset.clear();
        }

        tileData.swap(dataTile);
        if (coordCtx) {
            assert(tileData->size() == coordTile->size());
            tileCoords.swap(coordTile);
        }
        return offset;
    }

    template<typename ItemChunkIterator>
    void
    TileConstChunkIterator<ItemChunkIterator>::populateTiles(size_t maxValues,
                                                             boost::shared_ptr<BaseTile>& dataTile,
                                                             boost::shared_ptr<BaseTile>& coordTile)
    {
        assert(dataTile);

        dataTile->initialize();
        dataTile->reserve(maxValues);
        if (coordTile) {
            coordTile->initialize();
            coordTile->reserve(maxValues);
        }
        const CoordinatesMapper* coordMapper = &_itemChunkCoordMapper;

        for (size_t n=0; ! _itemChunkIterator->end() &&
             n < maxValues; ++(*_itemChunkIterator), ++n) {

            const Value& val = _itemChunkIterator->getItem();
            dataTile->push_back(val);
            if (coordTile) {
                const Coordinates& coords = _itemChunkIterator->getPosition();
                assert(!coords.empty());

                position_t pos = coordMapper->coord2pos(coords);
                assert(pos>=0);

                Value posVal(sizeof(position_t));
                posVal.set<position_t>(pos);
                coordTile->push_back(posVal);
            }
        }
        dataTile->finalize();
        if (coordTile) {
            assert(dataTile->size() == coordTile->size());
            coordTile->finalize();
        }
    }

    template<typename ItemChunkIterator>
    log4cxx::LoggerPtr
    TileConstChunkIterator<ItemChunkIterator>::_logger(log4cxx::Logger::getLogger("scidb.array.tile"));

    /**
     * A type of DelegateChunkIterator that supports the getData() interface (by delegating)
     * @note DelegateChunkIterator should not provide the same interface because
     *       its children may not be written in such a way to correctly intercept these methods
     */
    class TileDelegateChunkIterator : public DelegateChunkIterator
    {
    public:
        TileDelegateChunkIterator(DelegateChunk const* sourceChunk, int iterationMode)
        : DelegateChunkIterator(sourceChunk, iterationMode)
        {
        }
        virtual ~TileDelegateChunkIterator()
        {
        }

        /// @see ConstChunkIterator
        virtual const Coordinates&
        getData(scidb::Coordinates& offset,
                size_t maxValues,
                boost::shared_ptr<BaseTile>& tileData,
                boost::shared_ptr<BaseTile>& tileCoords)
        {
            return inputIterator->getData(offset,
                                          maxValues,
                                          tileData,
                                          tileCoords);
        }

        /// @see ConstChunkIterator
        virtual position_t
        getData(position_t logicalOffset,
                size_t maxValues,
                boost::shared_ptr<BaseTile>& tileData,
                boost::shared_ptr<BaseTile>& tileCoords)
        {
            return inputIterator->getData(logicalOffset,
                                          maxValues,
                                          tileData,
                                          tileCoords);
        }

        /// @see ConstChunkIterator
        virtual const Coordinates&
        getData(scidb::Coordinates& offset,
                size_t maxValues,
                boost::shared_ptr<BaseTile>& tileData)
        {
            return inputIterator->getData(offset,
                                          maxValues,
                                          tileData);
        }

        /// @see ConstChunkIterator
        virtual position_t
        getData(position_t logicalOffset,
                size_t maxValues,
                boost::shared_ptr<BaseTile>& tileData)
        {
            return inputIterator->getData(logicalOffset,
                                          maxValues,
                                          tileData);
        }

        /// @see ConstChunkIterator
        virtual operator const CoordinatesMapper* () const
        {
            return (*inputIterator);
        }

        /// @see ConstChunkIterator
        virtual position_t getLogicalPosition()
        {
            return inputIterator->getLogicalPosition();
        }

        /// @see ConstChunkIterator
        virtual bool setPosition(position_t pos)
        {
            return inputIterator->setPosition(pos);
        }
    };

} //scidb namespace
#endif //__TILE_ITERATOR_ADAPTERS__
