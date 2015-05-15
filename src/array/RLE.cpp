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

#include <array/Array.h>
#include <query/TypeSystem.h>
#include <system/Config.h>
#include <system/SciDBConfigOptions.h>
#include <system/Utils.h>

using namespace boost;
using namespace std;

namespace scidb
{

    const uint64_t RLE_EMPTY_BITMAP_MAGIC = 0xEEEEAAAA00EEBAACLL;
    const uint64_t RLE_PAYLOAD_MAGIC = 0xDDDDAAAA000EAAACLL;

    void checkChunkMagic(ConstChunk const& chunk)
    {
        PinBuffer scope(chunk);
        uint64_t* magic = static_cast<uint64_t*>(chunk.getData());

        SCIDB_ASSERT(magic != 0);
        SCIDB_ASSERT(*magic == (chunk.getAttributeDesc().isEmptyIndicator() ?
                          RLE_EMPTY_BITMAP_MAGIC :
                          RLE_PAYLOAD_MAGIC));
    }

    void ConstRLEEmptyBitmap::pack(char* dst) const {
        Header* hdr = (Header*)dst;
        hdr->_magic = RLE_EMPTY_BITMAP_MAGIC;
        hdr->_nSegs = _nSegs;
        hdr->_nNonEmptyElements = _nNonEmptyElements;
        memcpy(hdr+1, _seg, _nSegs*sizeof(Segment));
    }

    ConstRLEEmptyBitmap::~ConstRLEEmptyBitmap()
    {
        if (_chunk && _chunkPinned) {
            _chunk->unPin();
        }
    }

    ConstRLEEmptyBitmap::ConstRLEEmptyBitmap(ConstChunk const& bitmapChunk) : _chunk(NULL)
    {
        _chunkPinned = bitmapChunk.pin();
        char const* src = (char const*)bitmapChunk.getConstData();
        if (src != NULL) {
            Header const* hdr = (Header const*)src;
            assert(hdr->_magic == RLE_EMPTY_BITMAP_MAGIC);
            _nSegs = hdr->_nSegs;
            _nNonEmptyElements = hdr->_nNonEmptyElements;
            _seg = (Segment const*)(hdr+1);
            _chunk = &bitmapChunk;
        } else {
            _nSegs = 0;
            _nNonEmptyElements = 0;
            bitmapChunk.unPin();
            _chunkPinned = false;
        }
    }


    ConstRLEEmptyBitmap::ConstRLEEmptyBitmap(char const* src) : _chunk(NULL)
    {
        if (src != NULL) {
            Header* hdr = (Header*)src;
            assert(hdr->_magic == RLE_EMPTY_BITMAP_MAGIC);
            _nSegs = hdr->_nSegs;
            _nNonEmptyElements = hdr->_nNonEmptyElements;
            _seg = (Segment*)(hdr+1);
        } else {
            _nSegs = 0;
            _nNonEmptyElements = 0;
        }
    }

    ostream& operator<<(ostream& stream, ConstRLEEmptyBitmap const& map)
    {
        if (map.nSegments() == 0)
        {
            stream<<"[empty]";
        }

        for (size_t i=0; i<map.nSegments(); i++)
        {
            stream<<"["<<map.getSegment(i)._lPosition<<","<<map.getSegment(i)._pPosition<<","<<map.getSegment(i)._length<<"];";
        }
        return stream;
    }


    RLEEmptyBitmap::RLEEmptyBitmap(RLEPayload& payload)
    {
        Segment bs;
        bs._lPosition = 0;
        bs._pPosition = 0;
        bs._length = 0;
        reserve(payload.nSegments());
        for (size_t i = 0, n = payload.nSegments(); i < n; i++) {
            ConstRLEPayload::Segment const& ps = payload.getSegment(i);
            size_t bit = ps._valueIndex;
            size_t len = ps.length();
            if (ps._same) {
                if (payload.checkBit(bit)) {
                    if (bs._lPosition + bs._length == ps._pPosition) {
                        bs._length += len;
                    } else {
                        if (bs._length != 0) {
                            _container.push_back(bs);
                            bs._pPosition += bs._length;
                        }
                        bs._lPosition = ps._pPosition;
                        bs._length = len;
                    }
                }
            } else {
                for (size_t j = 0; j < len; j++) {
                    if (payload.checkBit(bit+j)) {
                        if (size_t(bs._lPosition + bs._length) == ps._pPosition + j) {
                            bs._length += 1;
                        } else {
                            if (bs._length != 0) {
                                _container.push_back(bs);
                                bs._pPosition += bs._length;
                            }
                            bs._lPosition = ps._pPosition + j;
                            bs._length = 1;
                        }
                    }
                }
            }
        }
        if (bs._length != 0) {
            _container.push_back(bs);
        }
        _seg = &_container[0];
        _nSegs = _container.size();
        _nNonEmptyElements = bs._pPosition + bs._length;
    }

    position_t RLEEmptyBitmap::addRange(position_t lpos, position_t ppos, uint64_t sliceSize, size_t level, Coordinates const& chunkSize, Coordinates const& origin, Coordinates const& first, Coordinates const& last)
    {
        sliceSize /= chunkSize[level];
        lpos += (first[level] - origin[level])*sliceSize;
        if (level+1 < origin.size()) {
            for (Coordinate beg = first[level], end = last[level]; beg <= end; beg++) {
                ppos = addRange(lpos, ppos, sliceSize, level+1, chunkSize, origin, first, last);
                lpos += sliceSize;
            }
        } else {
            assert(sliceSize == 1);
            size_t len =  last[level] - first[level] + 1;
            if (_container.size() > 0 && _container.back()._lPosition + _container.back()._length == lpos) {
                _container.back()._length += len;
            } else {
                Segment segm;
                segm._lPosition = lpos;
                segm._pPosition = ppos;
                segm._length = len;
                _container.push_back(segm);
            }
            ppos += len;
        }
        return ppos;
    }

    /** /brief BitmapReplicator managed copier of bitmap (replicator). */
    class BitmapReplicator : boost::noncopyable
    {
      public:
        typedef ConstRLEEmptyBitmap::Segment Segment;

      private:
        /** result bitmap */
        boost::shared_ptr<RLEEmptyBitmap> _resultBitmap;
        /** current result segment (not added to result yet) */
        Segment                           _resultSegment;

        /** array with source bitmap segments */
        Segment const * _sourceArray;
        size_t          _sourceCount;
        size_t          _sourceIndex;
        Segment         _sourceSegment;


      private:
        /** flush - add (if available) current result segment to result bitmap  */
        void _flush()
        {
            if (_resultSegment._length > 0) {
                SCIDB_ASSERT(_resultSegment._lPosition != static_cast<position_t>(-1));
                SCIDB_ASSERT(_resultSegment._pPosition != static_cast<position_t>(-1));
                _resultBitmap->addSegment(_resultSegment);
                _resultSegment._length = 0;
            }
        }

      public:
        BitmapReplicator(Segment const* sourceArray, size_t sourceCount) :
            _sourceArray(sourceArray),
            _sourceCount(sourceCount),
            _sourceIndex(0)
        {
            _resultBitmap = boost::make_shared<RLEEmptyBitmap>();
            _resultSegment._lPosition = static_cast<position_t>(-1);
            _resultSegment._pPosition = static_cast<position_t>(-1);
            _resultSegment._length = 0;
            if (!end()) {
                _sourceSegment = _sourceArray[0];
                _resultSegment._lPosition = _sourceSegment._lPosition;
                _resultSegment._pPosition = _sourceSegment._pPosition;
            }
        }

        /** logical position of current source segment. */
        position_t position() const
        {
            SCIDB_ASSERT(!end());
            return _sourceSegment._lPosition;
        }

        /** length of current source segment. */
        position_t length() const
        {
            SCIDB_ASSERT(!end());
            return _sourceSegment._length;
        }

        /** skip @param count positions from current source segment. */
        void skip(position_t count)
        {
            SCIDB_ASSERT(!end());
            SCIDB_ASSERT(count <= _sourceSegment._length);
            _sourceSegment._lPosition += count;
            _sourceSegment._pPosition += count;
            _sourceSegment._length -= count;
        }

        /**
          * copy @param count positions from current source segment
          * to current result segment.
          */
        void copy(position_t count)
        {
            SCIDB_ASSERT(!end());
            if (_resultSegment._length > 0) {
                position_t source = _sourceSegment._lPosition;
                position_t result = _resultSegment._lPosition + _resultSegment._length;
                SCIDB_ASSERT(result <= source);
                if (result < source) {
                    _flush();
                }
            }
            SCIDB_ASSERT(count <= _sourceSegment._length);
            if (_resultSegment._length == 0) {
                _resultSegment._lPosition = _sourceSegment._lPosition;
                _resultSegment._pPosition = _sourceSegment._pPosition;
            }
            _resultSegment._length += count;
            skip(count);
        }

        /** move to next source segment */
        void next()
        {
            SCIDB_ASSERT(!end());
            SCIDB_ASSERT(_sourceSegment._length == 0);
            ++_sourceIndex;
            if (!end()) {
                _sourceSegment = _sourceArray[_sourceIndex];
            }
        }

        /** source completed */
        bool end() const
        {
            SCIDB_ASSERT(_sourceIndex <= _sourceCount);
            return _sourceIndex == _sourceCount;
        }

        /** get result bitmap */
        boost::shared_ptr<RLEEmptyBitmap> result()
        {
            _flush();
            boost::shared_ptr<RLEEmptyBitmap> result;
            _resultBitmap.swap(result);
            return result;
        }
    };

    /** /brief Cut manager of replicator which cut the area outbound subarray */
    class Cut : boost::noncopyable
    {
      private:
        position_t _prefix;
        position_t _suffix;
        position_t _interval;
        position_t _main;
        boost::shared_ptr<Cut> _nested;

      private:
        Cut()
        {
        }
        friend boost::shared_ptr<Cut> boost::make_shared<Cut>();

        position_t init(Coordinates const& lowerOrigin,
                        Coordinates const& upperOrigin,
                        Coordinates const& lowerResult,
                        Coordinates const& upperResult,
                        size_t index)
        {
            SCIDB_ASSERT(lowerOrigin[index] <= lowerResult[index]);
            SCIDB_ASSERT(lowerResult[index] <= upperResult[index]);
            SCIDB_ASSERT(upperResult[index] <= upperOrigin[index]);
            _prefix = lowerResult[index] - lowerOrigin[index];
            _suffix = upperOrigin[index] - upperResult[index];
            _interval = upperOrigin[index] + 1 - lowerOrigin[index];
            ++index;
            position_t multiplier = 1;
            if (index < lowerOrigin.size()) {
                _nested = boost::make_shared<Cut>();
                multiplier = _nested->init(lowerOrigin, upperOrigin,
                                           lowerResult, upperResult,
                                           index);
                if (_nested->_prefix == 0 && _nested->_suffix == 0 && !_nested->_nested) {
                    _nested.reset();
                }
            }
            _prefix *= multiplier;
            _suffix *= multiplier;
            _interval *= multiplier;
            _main = _interval - _prefix - _suffix;
            return _interval;
        }

      public:
        /**
         * Constructor for Cut algorithm
         *
         * @param lowerOrigin lower coordinates of original array.
         * @param upperOrigin upper coordinates of original array.
         * @param lowerResult lower coordinates of subarray.
         * @param lowreResult lower coordinates of subarray.
         *
         */
        Cut(Coordinates const& lowerOrigin,
            Coordinates const& upperOrigin,
            Coordinates const& lowerResult,
            Coordinates const& upperResult)
        {
            size_t const n(lowerOrigin.size());
            SCIDB_ASSERT(n > 0);
            SCIDB_ASSERT(n == upperOrigin.size());
            SCIDB_ASSERT(n == lowerResult.size());
            SCIDB_ASSERT(n == upperResult.size());
            init(lowerOrigin, upperOrigin, lowerResult, upperResult, 0);
        }

        /**
          *  DataReplicator - class without following interface:
          *    - position_t position() const - logical position where source starts.
          *    - position_t length() const - length of source.
          *    - void skip(position_t count) - skip the count positions from source.
          *    - void copy(position_t count) - copy the count positions from source
          *        to result.
          *
          *  @param replicator should provide four described method
          */
        template< typename DataReplicator >
        void operator()(DataReplicator& replicator) const
        {
            process(replicator);
        }

      private:
        template< typename DataReplicator >
        position_t process(DataReplicator& replicator) const
        {
            position_t result = 0;

            if (replicator.length() == 0) {
                /* source segment completed */
                return result;
            }

            /*
             * Where source starts (relative to dimension begin)?
             *
             *                       source
             *                        <==>
             * PREFIX & MAIN & SUFFIX [__|__]
             *                           ^
             *                           |
             *                         source segment logical position
             */
            position_t source = replicator.position() % _interval;

            /*
             * How many position absent in MAIN (not overlap) area?
             *
             *                          skipMain
             *                          <==>
             * PREFIX[___] MAIN & SUFFIX[__|__]
             *                             ^
             *                             |
             *                         source segment logical position
             */
            position_t skipMain;

            if (source < _prefix) {
                /*
                 * Source starts inside PREFIX.
                 *
                 * How many position absent in MAIN (not overlap) area?
                 *          skipPrefix
                 *          <==>
                 * PREFIX[__|__] MAIN[___] SUFFIX [___]
                 *          ^
                 *          |
                 *        source segment logical position
                 */

                position_t skipPrefix = min(_prefix - source, replicator.length());
                result += skipPrefix;

                /* skip tail of PREFIX */
                replicator.skip(skipPrefix);

                if (replicator.length() == 0) {
                    /* source segment completed */
                    return result;
                }

                /*
                 * Now we exactly on begin of MAIN, and skipMain equal zero.
                 *
                 * PREFIX[___] MAIN[|__] SUFFIX[___]
                 *                  ^
                 *                  |
                 *                source segment logical position
                 */
                skipMain = 0;
            } else {
                /*
                 * Source starts in MAIN or SUFFIX.
                 *
                 * PREFIX[___] MAIN & SUFFIX [__|__]
                 *                              ^
                 *                              |
                 *                            source
                 */
                skipMain = source - _prefix;
            }

            SCIDB_ASSERT(replicator.length() > 0);

            if (skipMain < _main) {
                /*
                 * Source starts in MAIN area.
                 *
                 * How many position should we copy?
                 *                       copy
                 *                       <==>
                 * PREFIX[___] MAIN[____|____] SUFFIX[___]
                 *                      ^
                 *                      |
                 *                    source
                 */
                position_t copy = min(_main - skipMain, replicator.length());

                if (_nested) {
                    /* We have nested dimensions (not fully included) */
                    while(copy > 0) {
                        SCIDB_ASSERT(replicator.length() > 0);
                        /* Process one line from nested dimension */
                        position_t step = _nested->process(replicator);
                        copy -= step;
                        source += step;
                        result += step;
                    }
                } else {
                    /*
                      We do not have nested dimensions
                      (or their fully included).
                    */
                    position_t step = min(copy, replicator.length());
                    replicator.copy(step);
                    source += step;
                    result += step;
                }

                /*
                 * We are leaving MAIN area
                 *
                 *                      copy
                 *                   <==========>
                 * PREFIX[___] MAIN[_|_] SUFFIX[|__]
                 *                   ^          ^
                 *                   |          |
                 *                 source      source+copy
                 */
            }

            if (replicator.length() == 0) {
                /* source segment completed */
                return result;
            }

            /*
             * Source now in SUFFIX.
             *
             * PREFIX[___] MAIN[___] SUFFIX[_|_]
             *                               ^
             *                               |
             *                             source
             */
            position_t suffixLeft = min(_interval - source, replicator.length());
            replicator.skip(suffixLeft);

            result += suffixLeft;

            return result;

        }
    };

    boost::shared_ptr<RLEEmptyBitmap> ConstRLEEmptyBitmap::cut(
            Coordinates const& lowerOrigin,
            Coordinates const& upperOrigin,
            Coordinates const& lowerResult,
            Coordinates const& upperResult) const
    {
        BitmapReplicator replicator(_seg, _nSegs);

        /* I prevent creation of "Cut" on empty bitmaps (for fast work) */
        if (replicator.end()) {
            return replicator.result();
        }

        Cut cut(lowerOrigin, upperOrigin, lowerResult, upperResult);

        while(!replicator.end()) {
            cut(replicator);
            SCIDB_ASSERT(replicator.length() == 0);
            replicator.next();
        }

        return replicator.result();
    }

    RLEEmptyBitmap::RLEEmptyBitmap(ValueMap& vm, bool all)
    {
        Segment segm;
        segm._pPosition = 0;
        segm._length = 0;
        segm._lPosition = 0;
        reserve(vm.size());
        for (ValueMap::const_iterator i = vm.begin(); i != vm.end(); ++i) {
            assert(i->first >= segm._lPosition + segm._length);
            if (all || i->second.getBool()) {
                if (i->first != segm._lPosition + segm._length) { // hole
                    if (segm._length != 0) {
                        _container.push_back(segm);
                        segm._pPosition += segm._length;
                        segm._length = 0;
                    }
                    segm._lPosition = i->first;
                }
                segm._length += 1;
            }
        }
        if (segm._length != 0) {
            _container.push_back(segm);
        }
        _nSegs = _container.size();
        _nNonEmptyElements = segm._pPosition + segm._length;
        _seg = &_container[0];
    }

    RLEEmptyBitmap::RLEEmptyBitmap(ConstChunk const& chunk)
    {
        Segment segm;
        segm._pPosition = 0;
        segm._length = 0;
        segm._lPosition = 0;
        Coordinates origin = chunk.getFirstPosition(false);
        Dimensions const& dims = chunk.getArrayDesc().getDimensions();
        size_t nDims = dims.size();
        Coordinates chunkSize(nDims);
        for (size_t i = 0; i < nDims; i++) {
            origin[i] -= dims[i].getChunkOverlap();
            chunkSize[i] = dims[i].getChunkOverlap()*2 + dims[i].getChunkInterval();
        }
        boost::shared_ptr<ConstChunkIterator> it = chunk.getConstIterator(ConstChunkIterator::IGNORE_EMPTY_CELLS);
        assert(!(it->getMode() & ConstChunkIterator::TILE_MODE));
        while (!it->end()) {
            Coordinates const& coord = it->getPosition();
            position_t pos = 0;
            for (size_t i = 0; i < nDims; i++) {
                pos *= chunkSize[i];
                pos += coord[i] - origin[i];
            }
            assert(pos >= segm._lPosition + segm._length);
            if (pos != segm._lPosition + segm._length) { // hole
                if (segm._length != 0) {
                    _container.push_back(segm);
                    segm._pPosition += segm._length;
                    segm._length = 0;
                }
                segm._lPosition = pos;
            }
            segm._length += 1;
            ++(*it);
        }
        if (segm._length != 0) {
            _container.push_back(segm);
        }
        _nSegs = _container.size();
        _nNonEmptyElements = segm._pPosition + segm._length;
        _seg = &_container[0];
    }

    RLEEmptyBitmap::RLEEmptyBitmap(char* data, size_t numBits)
    {
        Segment segm;
        segm._pPosition = 0;
        for (size_t i = 0; i < numBits; i++) {
            if ((data[i >> 3] & (1 << (i & 7))) != 0) {
                segm._lPosition = i;
                while (++i < size_t(numBits) && (data[i >> 3] & (1 << (i & 7))) != 0);
                segm._length = i - segm._lPosition;
                _container.push_back(segm);
                segm._pPosition += segm._length;
            }
        }
        _nNonEmptyElements = segm._pPosition;
        _nSegs = _container.size();
        _seg = &_container[0];
    }

    bool ConstRLEEmptyBitmap::iterator::skip(size_t n)
    {
        assert(!end());
        _currLPos += n;
        if (_currLPos >= _cs->_lPosition + _cs->_length) {
            position_t ppos = getPPos();
            size_t l = 0, r = _bm->_nSegs;
            ConstRLEEmptyBitmap::Segment const* seg = _bm->_seg;
            while (l < r) {
                size_t m = (l + r) >> 1;
                if (seg[m]._pPosition + seg[m]._length <= ppos) {
                    l = m + 1;
                } else {
                    r = m;
                }
            }
            if (r == _bm->_nSegs) {
                return false;
            }
            _currSeg = r;
            _cs = &seg[r];
            _currLPos = _cs->_lPosition + ppos - _cs->_pPosition;
        }
        return true;
    }


    //
    // Const Payload
    //

    void ConstRLEPayload::getValueByIndex(Value& value, size_t index) const
    {
        size_t fixedSize = _elemSize == 0 ? sizeof(int) : _elemSize;

        if (_isBoolean)
        {
            value.setBool( _payload[index>>3] & (1 << (index&7)));
            return;
        }

        char* rawData = _payload + index*fixedSize;
        if (_elemSize == 0) { // varying size
            int offs = *(int*)rawData;
            char* src = _payload + _varOffs + offs;

            size_t sizeHeader, sizeDatum;
            getSizeOfVarPartForOneDatum(src, sizeHeader, sizeDatum);

            value.setData(src + sizeHeader, sizeDatum);
        } else {
            value.setData(rawData, fixedSize);
        }
    }

    void ConstRLEPayload::getCoordinates(ArrayDesc const& array, size_t dim, Coordinates const& chunkPos, Coordinates const& tilePos, boost::shared_ptr<Query> const& query, Value& value, bool withOverlap) const
    {
        Dimensions const& dims = array.getDimensions();
        size_t nDims = dims.size();
        Value buf;

        if (value.getTile() == 0)
        {
            value = Value(TypeLibrary::getType(TID_INT64),Value::asTile);
        }

        RLEPayload::append_iterator appender(value.getTile());
        if (array.getEmptyBitmapAttribute() != NULL) {
            Coordinates origin(nDims);
            Coordinates currPos(nDims);
            Coordinates chunkIntervals(nDims);
            Coordinates overlapBegin(nDims);
            Coordinates overlapEnd(nDims);
            position_t  startPos = 0;

            for (size_t i = 0; i < nDims; i++) {
                //Coordinate of first chunk element, including overlaps
                origin[i] = max(dims[i].getStartMin(), chunkPos[i] - dims[i].getChunkOverlap());
                //Coordinate of last chunk element, including overlaps
                Coordinate terminus = min(dims[i].getEndMax(), chunkPos[i] + dims[i].getChunkInterval() + dims[i].getChunkOverlap() - 1);
                chunkIntervals[i] = terminus - origin[i] + 1;
                startPos *= chunkIntervals[i];
                startPos += tilePos[i] - origin[i];
                overlapBegin[i] = max(dims[i].getStartMin(), chunkPos[i]);
                overlapEnd[i] = min(dims[i].getEndMax(), chunkPos[i] + dims[i].getChunkInterval() - 1);
            }

            iterator it(this);
            if (withOverlap) {
                while (!it.end()) {
                    position_t pPos = it.getPPos();
                    if (it.checkBit()) {
                        position_t pos = startPos + pPos;
                        for (size_t i = nDims; i-- != dim;) {
                            currPos[i] = origin[i] + (pos % chunkIntervals[i]);
                            pos /= chunkIntervals[i];
                        }
                        buf.setInt64(currPos[dim]);
                        appender.add(buf);
                        ++it;
                    } else {
                        it += it.getRepeatCount();
                    }
                }
            } else {
                while (!it.end()) {
                    position_t pPos = it.getPPos();
                    if (it.checkBit()) {
                        bool skip = false;
                        position_t pos = startPos + pPos;
                        for (size_t i = nDims; i-- != dim;) {
                            currPos[i] = origin[i] + (pos % chunkIntervals[i]);
                            if ( ((currPos[i] < overlapBegin[i]) || (currPos[i] > overlapEnd[i])) ) {
                                skip = true;
                                break;
                            }
                            pos /= chunkIntervals[i];
                        }
                        if (!skip) {
                            buf.setInt64(currPos[dim]);
                            appender.add(buf);
                        }
                        ++it;
                    } else {
                        it += it.getRepeatCount();
                    }
                }
            }
        } else {
            Coordinate start;
            Coordinate end;
            uint64_t interval = 1;
            uint64_t offset = 0;
            Coordinates pos = tilePos;
            if (withOverlap) {
                start = max(dims[dim].getStartMin(), Coordinate(chunkPos[dim] - dims[dim].getChunkOverlap()));
                end = min(dims[dim].getEndMax(), Coordinate(chunkPos[dim] + dims[dim].getChunkInterval() + dims[dim].getChunkOverlap() - 1));
                for (size_t i = nDims; i-- != 0; ) {
                    if (pos[i] > dims[i].getEndMax()) {
                        for (size_t j = 0; j < nDims; j++) {
                            pos[j] = max(dims[j].getStartMin(), Coordinate(chunkPos[j] - dims[j].getChunkOverlap()));
                        }
                        if (i != 0) {
                            pos[i-1] += 1;
                        }
                    } else if (pos[i] < dims[i].getStartMin()) {
                        pos[i] = dims[i].getStartMin();
                    }
                }
                for (size_t i = dim; ++i < nDims;) {
                    Coordinate rowStart = max(dims[i].getStartMin(), Coordinate(chunkPos[i] - dims[i].getChunkOverlap()));
                    Coordinate rowEnd = min(dims[i].getEndMax(), Coordinate(chunkPos[i] + dims[i].getChunkInterval() + dims[i].getChunkOverlap() - 1));
                    uint64_t rowLen = rowEnd - rowStart + 1;
                    interval *= rowLen;
                    offset *= rowLen;
                    assert(pos[i] >= rowStart);
                    offset += pos[i] - rowStart;
                }
            } else {
                start = chunkPos[dim];
                end = min(dims[dim].getEndMax(), Coordinate(chunkPos[dim] + dims[dim].getChunkInterval() - 1));
                for (size_t i = nDims; i-- != 0; ) {
                    if (pos[i] > dims[i].getEndMax() || pos[i] >= chunkPos[i] + dims[i].getChunkInterval()) {
                        for (size_t j = 0; j < nDims; j++) {
                            pos[j] = chunkPos[j];
                        }
                        if (i != 0) {
                            pos[i-1] += 1;
                        }
                    } else if (pos[i] < chunkPos[i]) {
                        pos[i] = chunkPos[i];
                    }
                }
                for (size_t i = dim; ++i < nDims;) {
                    Coordinate rowStart = chunkPos[i];
                    Coordinate rowEnd = min(dims[i].getEndMax(), Coordinate(chunkPos[i] + dims[i].getChunkInterval() - 1));
                    uint64_t rowLen = rowEnd - rowStart + 1;

                    interval *= rowLen;
                    offset *= rowLen;
                    assert(pos[i] >= rowStart);
                    offset += pos[i] - rowStart;
                }
            }
            Coordinate curr = pos[dim];
            assert(curr >= start && curr <= end);
            assert(offset < interval);
            uint64_t len = interval - offset;
            uint64_t n = count();
            if (n != 0) {
                while (true) {
                    buf.setInt64(curr);
                    appender.add(buf, len);
                    if (n <= len)  {
                        break;
                    }
                    n -= len;
                    len = interval;
                    if (++curr > end) {
                        curr = start;
                    }
                }
            }
        }
        appender.flush();
    }

    bool ConstRLEPayload::getValueByPosition(Value& value, position_t pos) const {
        size_t l = 0, r = _nSegs;
        while (l < r) {
            size_t m = (l + r) >> 1;
            if (_seg[m+1]._pPosition <= pos) {
                l = m + 1;
            } else {
                r = m;
            }
        }
        if (r == _nSegs) {
            return false;
        }
        if (_seg[r]._null) {
            value.setNull(_seg[r]._valueIndex);
        } else {
            getValueByIndex(value, _seg[r]._valueIndex + (_seg[r]._same ? 0 : pos - _seg[r]._pPosition));
        }
        return true;
    }

    void ConstRLEPayload::pack(char* dst) const
    {
        Header* hdr = (Header*)dst;
        hdr->_magic = RLE_PAYLOAD_MAGIC;
        hdr->_nSegs = _nSegs;
        hdr->_elemSize = _elemSize;
        hdr->_dataSize = _dataSize;
        hdr->_varOffs = _varOffs;
        hdr->_isBoolean = _isBoolean;
        dst += sizeof(Header);
        if (_seg != NULL) { // in case of tile append payload may stay without termination element
            memcpy(dst, _seg, (_nSegs+1)*sizeof(Segment));
        } else {
            assert(_nSegs == 0);
            ((Segment*)dst)->_pPosition = 0;
        }
        dst += (_nSegs+1)*sizeof(Segment);
        memcpy(dst, _payload, _dataSize);
    }

    char* ConstRLEPayload::getRawVarValue(size_t index, size_t& size) const
    {
        if (_elemSize == 0) { // varying size
            int offs = *(int*)(_payload + index * 4);
            char* src = _payload + _varOffs + offs;
            size_t sizeHeader;
            getSizeOfVarPartForOneDatum(src, sizeHeader, size);
            return src + sizeHeader;
        } else if (_isBoolean)
        {
            size = 1;
            return &(_payload[index>>3]);
        }
        else
        {
            size = _elemSize;
            return _payload + index * _elemSize;
        }
    }

    ConstRLEPayload::ConstRLEPayload(char const* src)
    {
        if (src == NULL) {
            _nSegs = 0;
            _elemSize = 0;
            _dataSize = 0;
            _varOffs = 0;
            _isBoolean = false;
            _seg = NULL;
            _payload = NULL;
        } else {
            Header* hdr = (Header*)src;
            assert(hdr->_magic == RLE_PAYLOAD_MAGIC);
            _nSegs = hdr->_nSegs;
            _elemSize = hdr->_elemSize;
            _dataSize = hdr->_dataSize;
            _varOffs = hdr->_varOffs;
            _isBoolean = hdr->_isBoolean;
            _seg = (Segment*)(hdr+1);
            _payload = (char*)(_seg + _nSegs + 1);
        }
    }

    ConstRLEPayload::iterator::iterator(ConstRLEPayload const* payload):
            _payload(payload)
    {
        reset();
    }

    bool ConstRLEPayload::iterator::isDefaultValue(Value const& defaultValue)
    {
        assert(!end());
        if (defaultValue.isNull()) {
            return _cs->_null && defaultValue.getMissingReason() == _cs->_valueIndex;
        } else if (_cs->_null || !_cs->_same) {
            return false;
        }
        size_t index = _cs->_valueIndex;
        size_t valSize;
        char* data = _payload->getRawVarValue(index, valSize);
        return _payload->_isBoolean
            ? defaultValue.getBool() == ((*data & (1 << (index&7))) != 0)
            : defaultValue.size() == valSize && memcmp(data, defaultValue.data(), valSize) == 0;
    }

    void ConstRLEPayload::iterator::getItem(Value& item)
    {
        assert(!end());
        if(_cs->_null)
        {
            item.setNull(_cs->_valueIndex);
        }
        else
        {
            size_t index;
            size_t valSize;
            if(_cs->_same)
            {
                index = _cs->_valueIndex;
            }
            else
            {
                index = _cs->_valueIndex + _currPpos - _cs->_pPosition;
            }

            char* data = _payload->getRawVarValue(index, valSize);
            if(_payload->_isBoolean)
            {
                item.setBool((*data) & (1 << (index&7)));
            }
            else
            {
                item.setData(data, valSize);
            }
        }
    }

    ostream& operator<<(ostream& stream, ConstRLEPayload const& payload)
    {
        if (payload.nSegments() == 0)
        {
            stream<<"[empty]";
        }

        stream<<"eSize "<<payload.elementSize()<<" dSize "<<payload.payloadSize()<<" segs ";
        for (size_t i=0; i<payload.nSegments(); i++)
        {
            stream<<"["<<payload.getSegment(i)._pPosition<<","<<payload.getSegment(i)._same<<","<<payload.getSegment(i)._null<<","<<payload.getSegment(i)._valueIndex<<","<<payload.getSegment(i).length()<<"];";
        }
        return stream;
    }

    //
    // Payload
    //

    void RLEPayload::setVarPart(char const* varData, size_t varSize)
    {
        _varOffs = _data.size();
        _data.resize(_varOffs + varSize);
        memcpy(&_data[_varOffs], varData, varSize);
        _dataSize = _data.size();
        _payload = &_data[0];
        _elemSize = 0;
    }

    void RLEPayload::setVarPart(vector<char>& varPart)
    {
        _varOffs = _data.size();
        _data.insert(_data.end(), varPart.begin(), varPart.end());
        _dataSize = _data.size();
        _payload = &_data[0];
    }


    void RLEPayload::appendValue(vector<char>& varPart, Value const& val, size_t valueIndex)
    {
        if (_isBoolean)
        {
            assert(val.size() == 1);
            if (valueIndex % 8 == 0)
            {
                _data.resize(++_dataSize);
                _data[_dataSize-1]=0;
            }

            if(val.getBool())
            {
                _data[_dataSize-1] |= (1 << (valueIndex&7));
            }
        }
        else
        {
            const size_t fixedSize = _elemSize == 0 ? sizeof(int) : _elemSize;
            _data.resize(_dataSize + fixedSize);
            if (_elemSize == 0)
            {
                int offs = varPart.size();
                *(int*)&_data[_dataSize] = offs;
                appendValueToTheEndOfVarPart(varPart, val);
            }
            else
            {
                if (val.size() > _elemSize) {
                    throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_TRUNCATION) << val.size() << fixedSize;
                }
                memcpy(&_data[_dataSize], val.data(), val.size());
            }
            _dataSize += fixedSize;
        }
        _payload = &_data[0];
    }

    /**
     * Copy a block of boolean values from a given valueIndex in a source buffer to a given valueIndex in a dst buffer.
     * In each buffer, every byte stores up to eight boolean values.
     * A valueIndex has a byte address = (valueIndex >> 3), and a bit address = (valueIndex & 7).
     * For instance, the bit with valueIndex = 0 is stored at bit 0 of byte 0;
     * the bit with valueIndex = 10 is stored at bit 2 of byte 1, and so on.
     *
     * @param[inout] dstData         the beginning of dst data buffer
     * @param[in]    srcData         the beginning of the src data buffer
     * @param[in]    valueIndexInDst the valueIndex in dst
     * @param[in]    valueIndexInSrc the valueIndex in src
     * @param[in]    length          how many bits to copy
     */
    void copyRLEBoolValues(char* dstData, char* srcData, uint32_t valueIndexInDst, uint32_t valueIndexInSrc, position_t length)
    {
        for (position_t i=0; i<length; ++i) {
            uint32_t srcByte = (valueIndexInSrc+i) >> 3;
            uint32_t srcBit = (valueIndexInSrc+i) & 7;
            uint32_t dstByte = (valueIndexInDst+i) >> 3;
            uint32_t dstBit = (valueIndexInDst+i) & 7;

            if (srcData[srcByte] & (1 << srcBit)) {
                dstData[dstByte] |= (1 << dstBit);
            } else {
                dstData[dstByte] &= ~(1 << dstBit);
            }
        }
    }

    void RLEPayload::appendAPartialSegmentOfValues(Segment const& dstSegmentToAppend, std::vector<char>& varPart,
            ConstRLEPayload& srcPayload, uint32_t valueIndexInSrc, position_t realLength)
    {
        assert(!dstSegmentToAppend._same || realLength<=1);  // if _same, we should *only* append one real value
        assert(!dstSegmentToAppend._null || realLength==0);  // if _null, we should not append any real value

        // Append the segment.
        addSegment(dstSegmentToAppend);

        // It is possible (when the segment is NULL) that no real value needs to be appended.
        if (realLength==0) {
            return;
        }

        // Append the values.
        if (_isBoolean)
        {
            addBoolValues(realLength);
            copyRLEBoolValues(_payload, srcPayload._payload, _valuesCount - realLength, valueIndexInSrc, realLength);
        }
        else
        {
            // Copy the fixed part.
            const size_t fixedSizeOne = _elemSize == 0 ? sizeof(int) : _elemSize;
            const size_t fixedSizeAll = fixedSizeOne * realLength;
            _data.resize(_dataSize + fixedSizeAll);
            memcpy(&_data[_dataSize], srcPayload._payload + (valueIndexInSrc*fixedSizeOne), fixedSizeAll);

            // How many values?
            assert(_valuesCount == _dataSize/fixedSizeOne);

            // If variable-length...
            if (_elemSize == 0) {
                // The var-part offset of the first item to copy in src, and the total size of the var part to copy.
                int varOffsetOfFirstToCopyInSrc = *(((int*)srcPayload._payload) + valueIndexInSrc);
                int varOffsetOfLastToCopyInSrc = *(((int*)srcPayload._payload) + valueIndexInSrc + realLength - 1);
                int varSizeOfLastToCopyInSrc = srcPayload.getSizeOfVarPartForOneDatum(varOffsetOfLastToCopyInSrc);
                int sizeOfVarPartToCopy = varOffsetOfLastToCopyInSrc - varOffsetOfFirstToCopyInSrc + varSizeOfLastToCopyInSrc;

                // Update all the pointers stored in the fixed part.
                // The first appended element needs to store an offset = varPart.size() in dst;
                // yet what's copied from the source was storing an offset = varOffsetOfFirstToCopyInSrc.
                int offsetAdjustment = static_cast<int>(varPart.size()) - varOffsetOfFirstToCopyInSrc;
                if (offsetAdjustment!=0) {
                    for (int i=0; i<realLength; ++i) {
                        *(((int*)&_data[0]) + _valuesCount+i) += offsetAdjustment;
                    }
                }

                // Copy the var part.
                size_t oldVarSize = varPart.size();
                varPart.resize(oldVarSize + sizeOfVarPartToCopy);
                memcpy(&varPart[0] + oldVarSize, srcPayload.getVarData()+varOffsetOfFirstToCopyInSrc, sizeOfVarPartToCopy);
            }

            // Update _valuesCount and _dataSize
            _dataSize += fixedSizeAll;
            _valuesCount += realLength;
        }

        // Update _payload to the new address.
        _payload = &_data[0];
    }

    RLEPayload::RLEPayload(): ConstRLEPayload(), _valuesCount(0)
    {
    }

    RLEPayload::RLEPayload(ValueMap& vm, size_t nElems, size_t elemSize, Value const& defaultVal, bool isBoolean,  bool subsequent)
    {
        Value const* currVal = NULL;
        Segment currSeg;
        _data.reserve(isBoolean ? vm.size()/8 : vm.size()*(elemSize==0 ? 4 : elemSize));
        _container.reserve(vm.size());
        currSeg._same = true;
        currSeg._pPosition = 0;

        vector<char> varPart;
        this->_elemSize = elemSize;
        this->_isBoolean = isBoolean;

        // write default value
        _dataSize = 0;
        size_t valueIndex = 0;
        size_t segLength = 0;
        if (!defaultVal.isNull()) {
            appendValue(varPart, defaultVal, valueIndex);
            valueIndex += 1;
        }
        for (ValueMap::const_iterator i = vm.begin(); i != vm.end(); ++i) {
            position_t pos = i->first;
            Value const& val = i->second;
            if (subsequent) {
                pos = currSeg._pPosition + segLength;
            } else {
                assert(pos >= position_t(currSeg._pPosition + segLength));
                if (val == defaultVal) { // ignore explicitly specified default values
                    continue;
                }
            }
            if (currVal == NULL // first element
                || !currSeg._same // sequence of different values
                || *currVal != val // new value is not the same as in the current segment
                || pos != position_t(currSeg._pPosition + segLength)) // hole
            {
                int carry = 0;
                if (pos != position_t(currSeg._pPosition + segLength)) { // hole
                    if (segLength != 0) {
                        _container.push_back(currSeg); // complete current sequence
                    }
                    // .. and insert sequence of default values
                    if (defaultVal.isNull()) {
                        currSeg._null = true;
                        currSeg._valueIndex = defaultVal.getMissingReason();
                    } else {
                        currSeg._null = false;
                        currSeg._valueIndex = 0;
                    }
                    currSeg._same = true;
                    currSeg._pPosition += segLength;
                    _container.push_back(currSeg);
                } else if (segLength != 0) { // subsequent element
                    if ((!currSeg._same || segLength == 1) && !val.isNull() && !currVal->isNull()) {
                        if (*currVal == val) {  // sequence of different values is termianted with the same value as new one: cut this value from the sequence and form separate sequence of repeated values
                            assert(!currSeg._same);
                            carry = 1;
                            segLength -= 1;
                        } else {  // just add value to the sequence of different values
                            currSeg._same = false;
                            segLength += 1;
                            appendValue(varPart, val, valueIndex);
                            valueIndex += 1;
                            currVal = &val;
                            continue;
                        }
                    }
                    _container.push_back(currSeg); // complete current sequence
                }
                if (val.isNull()) {
                    currSeg._null = true;
                    currSeg._valueIndex = val.getMissingReason();
                } else {
                    currSeg._null = false;
                    if (carry) {
                        currSeg._valueIndex = valueIndex-1;
                    } else {
                        appendValue(varPart, val, valueIndex);
                        currSeg._valueIndex = valueIndex++;
                    }
                }
                currSeg._same = true;
                currSeg._pPosition = pos - carry;
                segLength = 1 + carry;
                currVal = &val;
            } else { // same subsequent value
                segLength += 1;
            }
        }
        if (segLength != 0) {
            _container.push_back(currSeg); // save current segment
        }
        if (subsequent) {
            nElems = currSeg._pPosition + segLength;
        } else if (currSeg._pPosition + segLength != nElems) {
            // tail sequence of default values
            if (defaultVal.isNull()) {
                currSeg._null = true;
                currSeg._valueIndex = defaultVal.getMissingReason();
            } else {
                currSeg._null = false;
                currSeg._valueIndex = 0;
            }
            currSeg._same = true;
            currSeg._pPosition += segLength;
            _container.push_back(currSeg);
        }
        _nSegs = _container.size();
        currSeg._pPosition = nElems;
        _container.push_back(currSeg); // terminating segment (needed to calculate size)

        _seg = &_container[0];
        _data.resize(_dataSize + varPart.size());
        memcpy(&_data[_dataSize], &varPart[0], varPart.size());
        _payload = &_data[0];
        _varOffs = _dataSize;
        _dataSize += varPart.size();
        _valuesCount = valueIndex;
    }

    RLEPayload::RLEPayload(Value const& defaultVal, size_t logicalSize, size_t elemSize, bool isBoolean)
    {
        Segment currSeg;
        currSeg._same = true;
        currSeg._pPosition = 0;

        vector<char> varPart;
        this->_elemSize = elemSize;
        this->_isBoolean = isBoolean;

        // write default value
        _dataSize = 0;
        size_t valueIndex = 0;
        if (!defaultVal.isNull()) {
            appendValue(varPart, defaultVal, valueIndex);
            valueIndex += 1;
        }

        // generate one segment of default values
        if (defaultVal.isNull()) {
            currSeg._null = true;
            currSeg._valueIndex = defaultVal.getMissingReason();
        } else {
            currSeg._null = false;
            currSeg._valueIndex = 0;
        }
        _container.push_back(currSeg);

        // generate the terminating segment
        _nSegs = _container.size();
        currSeg._pPosition = logicalSize;
        _container.push_back(currSeg);

        _seg = &_container[0];
        _data.resize(_dataSize + varPart.size());
        memcpy(&_data[_dataSize], &varPart[0], varPart.size());
        _payload = &_data[0];
        _varOffs = _dataSize;
        _dataSize += varPart.size();
        _valuesCount = valueIndex;
    }

    RLEPayload::RLEPayload(char* rawData, size_t rawSize, size_t varOffs, size_t elemSize, size_t nElems, bool isBoolean)
    {
        unpackRawData(rawData, rawSize, varOffs, elemSize, nElems, isBoolean);
    }

    void RLEPayload::unpackRawData(char* rawData, size_t rawSize, size_t varOffs, size_t elemSize, size_t nElems, bool isBoolean)
    {
        clear();
        Segment segm;
        segm._pPosition = 0;
        segm._valueIndex = 0;
        segm._same = false;
        segm._null = false;
        _container.push_back(segm);
        segm._pPosition = nElems;
        _container.push_back(segm);

        _nSegs = 1;
        this->_dataSize = rawSize;
        this->_varOffs  = varOffs;
        this->_elemSize = elemSize;
        this->_isBoolean = isBoolean;
        _seg = &_container[0];
        _data.resize(rawSize);
        memcpy(&_data[0], rawData, rawSize);
        _payload = &_data[0];
        _valuesCount = nElems;
    }

    RLEPayload::RLEPayload(const class Type& type): ConstRLEPayload(),
        _valuesCount(0)
    {
        _elemSize = type.byteSize();
        _isBoolean = type.bitSize() == 1;
    }

    RLEPayload::RLEPayload(size_t bitSize): ConstRLEPayload(),
        _valuesCount(0)
    {
        _elemSize = (bitSize + 7) >> 3;
        _isBoolean = bitSize == 1;
    }

    void RLEPayload::clear()
    {
        _container.clear();
        _data.clear();
        _nSegs = 0;
        _dataSize = 0;
        _valuesCount = 0;
    }

    void RLEPayload::append(RLEPayload& payload)
    {
        assert(_isBoolean == payload._isBoolean);
        assert(_elemSize == payload._elemSize);
        if (payload.count() < 1 || payload._container.empty()) {
            return;
        }
        position_t lastHeadPosition = 0;
        if (!_container.empty()) { // remove terminator segment
            lastHeadPosition = _container.back()._pPosition;
            _container.pop_back();
        }
        size_t headSegments = _container.size();
        if (headSegments == 0) {
            _container = payload._container;
            _data = payload._data;
            _varOffs = payload._varOffs;
        } else {
            _container.insert(_container.end(), payload._container.begin(), payload._container.end());
            size_t headItems;
            if (!_isBoolean) {
                if (_elemSize == 0) { // varying size typed; adjust offsets
                    _data.insert(_data.begin() + _varOffs, payload._data.begin(), payload._data.begin() + payload._varOffs);
                    int* p = (int*)&_data[_varOffs];
                    int* end = (int*)&_data[_varOffs + payload._varOffs];
                    size_t varHead = _dataSize - _varOffs;
                    while (p < end) {
                        *p++ += varHead;
                    }
                    _data.insert(_data.end(), payload._data.begin() + payload._varOffs, payload._data.end());
                    headItems = _varOffs/sizeof(int);
                    _varOffs += payload._varOffs;
                } else {
                    _data.insert(_data.end(), payload._data.begin(), payload._data.end());
                    headItems = _dataSize/_elemSize;
                }
            } else {
                _data.insert(_data.end(), payload._data.begin(), payload._data.end());
                headItems = _dataSize*8;
                _valuesCount += payload._valuesCount;
            }
            for(size_t i=headSegments; i < _container.size(); ++i) {
                Segment& s = _container[i];
                if (!s._null) {
                    s._valueIndex += headItems;
                }
                s._pPosition += lastHeadPosition;
            }
        }
        assert(_container.size()>0);
        _seg = &_container[0];
        _nSegs = _container.size() - 1;

        this->_payload = !_data.empty() ? &_data[0] : NULL;
        _dataSize += payload._dataSize;
    }

    void RLEPayload::unPackTile(const ConstRLEEmptyBitmap& emptyMap, position_t vStart, position_t vEnd)
    {
        clear();
        _data.resize(1);
        _data[0] = 2; //index 0 - false, index 1 - true
        _elemSize = 1;
        _dataSize = 1;
        _isBoolean = true;
        RLEPayload::Segment rs;
        rs._same = true;
        rs._null = false;
        rs._pPosition = 0;
        for (size_t i = emptyMap.findSegment(vStart);
             i < emptyMap.nSegments() && emptyMap.getSegment(i)._lPosition < vEnd;
             i++)
        {
            const RLEEmptyBitmap::Segment& es = emptyMap.getSegment(i);
            const int64_t inStart = max<int64_t>(es._lPosition, vStart);
            const int64_t inEnd = min<int64_t>(es._lPosition + es._length, vEnd);

            if (inStart - vStart != rs._pPosition) {
                rs._valueIndex = 0;
                _container.push_back(rs);
            }
            rs._pPosition = inStart - vStart;
            rs._valueIndex = 1;
            _container.push_back(rs);
            rs._pPosition += inEnd - inStart;
        }
        if (rs._pPosition != vEnd - vStart) {
            rs._valueIndex = 0;
            _container.push_back(rs);
        }
        _nSegs = _container.size();
        rs._pPosition = vEnd - vStart;
        _container.push_back(rs);
        _valuesCount = 2;
        _seg = &_container[0];
        _payload = &_data[0];
    }

    void RLEPayload::unPackTile(const ConstRLEPayload& payload, const ConstRLEEmptyBitmap& emptyMap, position_t vStart, position_t vEnd)
    {
        clear();
        _elemSize = payload.elementSize();
        _isBoolean = payload.isBool();

        RLEPayload::Segment rs;
        rs._pPosition = 0;
        rs._same = true;
        size_t segLength = 0;
        size_t dstValueIndex = 0;
        vector<char> varPart;

        if (emptyMap.count() == payload.count()) { // no gaps in payload
            size_t begin = emptyMap.findSegment(vStart);
            if (begin < emptyMap.nSegments()) {
                size_t end = emptyMap.findSegment(vEnd);
                if (end >= emptyMap.nSegments() || emptyMap.getSegment(end)._lPosition > vEnd) {
                    end -= 1;
                }
                if (end != size_t(-1)) {
                    const RLEEmptyBitmap::Segment& firstSeg = emptyMap.getSegment(begin);
                    const RLEEmptyBitmap::Segment& lastSeg = emptyMap.getSegment(end);
                    const int64_t inStart = max<int64_t>(firstSeg._pPosition, vStart - firstSeg._lPosition + firstSeg._pPosition);
                    const int64_t inEnd = min<int64_t>(lastSeg._pPosition + lastSeg._length, vEnd - lastSeg._lPosition + lastSeg._pPosition);
                    for (size_t j = payload.findSegment(inStart);
                         j < payload.nSegments() && payload.getSegment(j)._pPosition < inEnd;
                         j++)
                    {
                        const RLEPayload::Segment& ps = payload.getSegment(j);
                        // physical start in payload
                        const int64_t resStart = max<int64_t>(inStart, ps._pPosition);
                        // physical end in payload
                        const int64_t resEnd = min<int64_t>(inEnd, ps._pPosition + ps.length());
                        const int64_t length = resEnd - resStart;

                        if (!ps._null) {
                            size_t srcValueIndex = ps._valueIndex + (ps._same ? 0 : resStart - ps._pPosition);
                            size_t nItems = ps._same ? 1 : length;
                            if (_isBoolean) {
                                char* otherData = payload.getFixData();
                                _data.resize((dstValueIndex + nItems + 7) >> 3);
                                for (size_t k = 0; k < nItems; k++) {
                                    if (otherData[(srcValueIndex + k) >> 3] & (1 << ((srcValueIndex + k) & 7))) {
                                        _data[(dstValueIndex + k) >> 3] |= 1 << ((dstValueIndex + k) & 7);
                                    }
                                }
                            } else {
                                if (_elemSize == 0) {
                                    _data.resize(_data.size() + nItems*sizeof(int));
                                    int* dst = (int*)&_data[0] + dstValueIndex;
                                    int* src = (int*)payload.getFixData() + srcValueIndex;
                                    int* end = src + nItems;
                                    while (src < end) {
                                        size_t offs = varPart.size();
                                        *dst++ = offs;
                                        char* body = payload.getVarData() + *src++;
                                        appendValueToTheEndOfVarPart(varPart, body);
                                    }
                                } else {
                                    _data.resize(_data.size() + nItems*_elemSize);
                                    memcpy(&_data[dstValueIndex*_elemSize],
                                           payload.getFixData() + srcValueIndex*_elemSize,
                                           nItems*_elemSize);
                                }
                            }
                            if (segLength > 0 && (!ps._same || ps.length() == 1) && (segLength == 1 || !rs._same)) {
                                // append previous segment
                                segLength += length;
                                rs._same = false;
                            } else {
                                if (segLength != 0) {
                                    _container.push_back(rs);
                                    rs._pPosition += segLength;
                                }
                                rs._same = ps._same;
                                rs._null = false;
                                rs._valueIndex = dstValueIndex;
                                segLength = length;
                            }
                            dstValueIndex += nItems;
                        } else {
                            if (segLength != 0) {
                                _container.push_back(rs);
                                rs._pPosition += segLength;
                            }
                            rs._same = true;
                            rs._null = true;
                            rs._valueIndex = ps._valueIndex;
                            _container.push_back(rs);
                            rs._pPosition += length;
                            segLength = 0;
                        }
                    }
                }
            }
        } else {
            for (size_t i = emptyMap.findSegment(vStart);
                 i < emptyMap.nSegments() && emptyMap.getSegment(i)._lPosition < vEnd;
                 i++)
            {
                const RLEEmptyBitmap::Segment& es = emptyMap.getSegment(i);
                const int64_t inStart = max<int64_t>(es._pPosition, vStart - es._lPosition + es._pPosition);
                const int64_t inEnd = min<int64_t>(es._pPosition + es._length, vEnd - es._lPosition + es._pPosition);
                for (size_t j = payload.findSegment(inStart);
                     j < payload.nSegments() && payload.getSegment(j)._pPosition < inEnd;
                     j++)
                {
                    const RLEPayload::Segment& ps = payload.getSegment(j);
                    // physical start in payload
                    const int64_t resStart = max<int64_t>(inStart, ps._pPosition);
                    // physical end in payload
                    const int64_t resEnd = min<int64_t>(inEnd, ps._pPosition + ps.length());
                    const int64_t length = resEnd - resStart;

                    if (!ps._null) {
                        size_t srcValueIndex = ps._valueIndex + (ps._same ? 0 : resStart - ps._pPosition);
                        size_t nItems = ps._same ? 1 : length;
                        if (_isBoolean) {
                            char* otherData = payload.getFixData();
                            _data.resize((dstValueIndex + nItems + 7) >> 3);
                            for (size_t k = 0; k < nItems; k++) {
                                if (otherData[(srcValueIndex + k) >> 3] & (1 << ((srcValueIndex + k) & 7))) {
                                    _data[(dstValueIndex + k) >> 3] |= 1 << ((dstValueIndex + k) & 7);
                                }
                            }
                        } else {
                            if (_elemSize == 0) {
                                _data.resize(_data.size() + nItems*sizeof(int));
                                int* dst = (int*)&_data[0] + dstValueIndex;
                                int* src = (int*)payload.getFixData() + srcValueIndex;
                                int* end = src + nItems;
                                while (src < end) {
                                    size_t offs = varPart.size();
                                    *dst++ = offs;
                                    char* body = payload.getVarData() + *src++;
                                    appendValueToTheEndOfVarPart(varPart, body);
                                }
                            } else {
                                _data.resize(_data.size() + nItems*_elemSize);
                                memcpy(&_data[dstValueIndex*_elemSize],
                                       payload.getFixData() + srcValueIndex*_elemSize,
                                       nItems*_elemSize);
                            }
                        }
                        if (segLength > 0 && (!ps._same || ps.length() == 1) && (segLength == 1 || !rs._same)) {
                            // append previous segment
                            segLength += length;
                            rs._same = false;
                        } else {
                            if (segLength != 0) {
                                _container.push_back(rs);
                                rs._pPosition += segLength;
                            }
                            rs._same = ps._same;
                            rs._null = false;
                            rs._valueIndex = dstValueIndex;
                            segLength = length;
                        }
                        dstValueIndex += nItems;
                    } else {
                        if (segLength != 0) {
                            _container.push_back(rs);
                            rs._pPosition += segLength;
                        }
                        rs._same = true;
                        rs._null = true;
                        rs._valueIndex = ps._valueIndex;
                        _container.push_back(rs);
                        rs._pPosition += length;
                        segLength = 0;
                    }
                }
            }
        }
        if (segLength != 0) {
            _container.push_back(rs);
            rs._pPosition += segLength;
        }
        _nSegs = _container.size();
        _container.push_back(rs);
        _seg = &_container[0];
        _varOffs = _data.size();
        if (varPart.size() != 0) {
            _data.insert(_data.end(), varPart.begin(), varPart.end());
        }
        _dataSize = _data.size();
        _valuesCount = dstValueIndex;
        this->_payload = &_data[0];
    }

    void RLEPayload::trim(position_t lastPos)
    {
        _container[_nSegs]._pPosition = lastPos;
    }

    void RLEPayload::flush(position_t chunkSize)
    {
        _nSegs = _container.size();
        assert(_nSegs == 0 || _container[_nSegs - 1]._pPosition < chunkSize);
        Segment segm;
        segm._pPosition = chunkSize;
        _container.push_back(segm); // Add terminated segment (needed to calculate length)
        _seg = &_container[0];
    }

    void RLEPayloadAppender::append(Value const& v)
    {
        assert(!_finalized);
        if (_nextSeg == 0 ||
            _payload._container[_nextSeg-1]._null != v.isNull() ||
            (v.isNull() &&
             _payload._container[_nextSeg-1]._valueIndex != v.getMissingReason()))
        {
            _payload._container.resize(_nextSeg+1);
            _payload._container[_nextSeg]._pPosition = _nextPPos;
            _payload._container[_nextSeg]._same = true;
            _payload._container[_nextSeg]._null = v.isNull();

            if(v.isNull())
            {
                _payload._container[_nextSeg]._valueIndex = v.getMissingReason();
            }
            else
            {
                if (v.size() > _payload._elemSize) {
                    throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_TRUNCATION) << v.size() << _payload._elemSize;
                }
                _payload._container[_nextSeg]._valueIndex = _nextValIndex;
                _payload._data.resize((_nextValIndex + 1) * _payload._elemSize);
                memcpy(&_payload._data[(_nextValIndex) * _payload._elemSize], v.data(), v.size());
                _nextValIndex++;
            }
            _nextSeg++;
        }
        else if (!v.isNull())
        {
            bool valuesEqual =
                v.size() == _payload._elemSize &&
                memcmp(&_payload._data[(_nextValIndex-1) * _payload._elemSize],
                       v.data(), _payload._elemSize) == 0;
            if (valuesEqual && !_payload._container[_nextSeg-1]._same)
            {
                _payload._container.resize(_nextSeg+1);
                _payload._container[_nextSeg]._pPosition = _nextPPos-1;
                _payload._container[_nextSeg]._same = true;
                _payload._container[_nextSeg]._null = false;
                _payload._container[_nextSeg]._valueIndex = _nextValIndex-1;
                _nextSeg++;
            }
            else if (!valuesEqual)
            {
                if (v.size() > _payload._elemSize) {
                    throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_TRUNCATION) << v.size() << _payload._elemSize;
                }
                _payload._data.resize((_nextValIndex + 1) * _payload._elemSize);
                memcpy(&_payload._data[(_nextValIndex) * _payload._elemSize], v.data(), _payload._elemSize);
                _nextValIndex++;

                if (_payload._container[_nextSeg-1]._pPosition == _nextPPos-1)
                {
                    _payload._container[_nextSeg-1]._same = false;
                }
                else if (_payload._container[_nextSeg-1]._same)
                {
                    _payload._container.resize(_nextSeg+1);
                    _payload._container[_nextSeg]._pPosition = _nextPPos;
                    _payload._container[_nextSeg]._same = true;
                    _payload._container[_nextSeg]._null = false;
                    _payload._container[_nextSeg]._valueIndex = _nextValIndex-1;
                    _nextSeg++;
                }
            }
        }
        _nextPPos++;
    }

    //
    // Yet another appender: correct handling of boolean and varying size types
    //
    RLEPayload::append_iterator::append_iterator(RLEPayload* dstPayload)
    : result(dstPayload), valueIndex(0), segLength(0)
    {
        assert(dstPayload);
        dstPayload->clear();
        segm._pPosition = 0;
    }

    void RLEPayload::append_iterator::flush()
    {
        if (segLength != 0) {
            result->addSegment(segm);
        }
        result->_valuesCount = valueIndex;
        result->setVarPart(varPart);
        result->flush(segm._pPosition + segLength);
    }

uint64_t RLEPayload::append_iterator::add(iterator& ii, uint64_t limit, bool setupPrevVal)
    {
        uint64_t count = min(limit, ii.available());
        if (ii.isNull()) {
            if (segLength != 0 && (!segm._null || segm._valueIndex != ii.getMissingReason()))  {
                result->addSegment(segm);
                segm._pPosition += segLength;
                segLength = 0;
            }
            segLength += count;
            segm._null = true;
            segm._same = true;
            segm._valueIndex = ii.getMissingReason();
            ii += count;
        } else {
            if (segLength != 0 && (segm._null || (segm._same && segLength > 1) || (ii.isSame() && count > 1))) {
                result->addSegment(segm);
                segm._pPosition += segLength;
                segLength = 0;
            }
            if (segLength == 0) {
                segm._same = (count == 1) || ii.isSame();
                segm._valueIndex = valueIndex;
                segm._null = false;
            } else {
                segm._same = false;
            }
            segLength += count;
            if (!result->_isBoolean && result->_elemSize != 0) {
                size_t size;
                if (segm._same) {
                    size = result->_elemSize;
                    valueIndex += 1;
                } else {
                    size = result->_elemSize*size_t(count);
                    valueIndex += size_t(count);
                }
                result->_data.resize(result->_dataSize + size);
                memcpy(& result->_data[result->_dataSize], ii.getFixedValues(), size);
                result->_dataSize += size;
                if (setupPrevVal) {
                    if (count > 1) {
                        ii += count-1;
                    }
                    ii.getItem(prevVal);
                    ii += 1;
                } else {
                    ii += count;
                }
            } else {
                if (segm._same) {
                    ii.getItem(prevVal);
                    ii += count;
                    result->appendValue(varPart, prevVal, valueIndex++);
                } else {
                    for (uint64_t i = 0; i < count; i++) {
                        ii.getItem(prevVal);
                        result->appendValue(varPart, prevVal, valueIndex++);
                        ++ii;
                    }
                }
            }
        }
        return count;
    }

    void RLEPayload::append_iterator::add(Value const& v, uint64_t count)
    {
        if (v.isNull()) {
            if (segLength != 0 && (!segm._null || segm._valueIndex != v.getMissingReason()))  {
                result->addSegment(segm);
                segm._pPosition += segLength;
                segLength = 0;
            }
            segLength += count;
            segm._null = true;
            segm._same = true;
            segm._valueIndex = v.getMissingReason();
        } else if (segLength != 0 && !segm._null && v == prevVal) {
            if (segm._same) {
                segLength += count;
            } else {
                result->addSegment(segm);
                segm._pPosition += segLength - 1;
                segm._same = true;
                segm._valueIndex = valueIndex-1;
                segLength = 1 + count;
            }
        } else {
            if (segLength == 0 || segm._null || count > 1) {
                if (segLength != 0) {
                    result->addSegment(segm);
                    segm._pPosition += segLength;
                    segLength = 0;
                }
                segm._same = true;
                segm._null = false;
                segm._valueIndex = valueIndex;
            } else {
                if (segLength > 1 && segm._same) {
                    result->addSegment(segm);
                    segm._pPosition += segLength;
                    segLength = 0;
                    segm._valueIndex = valueIndex;
                } else {
                    segm._same = false;
                }
            }
            segLength += count;
            result->appendValue(varPart, v, valueIndex++);
            prevVal = v;
        }
    }

    RLEPayload::append_iterator::~append_iterator()
    {
    }

    void ConstRLEPayload::getSizeOfVarPartForOneDatum(char* const address, size_t& sizeHeader, size_t& sizeDatum)
    {
        if (*address!=0) { // if the first byte is not zero, it stores the length
            sizeHeader = 1;
            sizeDatum = (*address & 0xFF);
        } else {  // if the first byte is zero, the next four bytes store the length
            sizeHeader = 5;
            sizeDatum = *((int*)(address+1));
            assert(sizeDatum==0 || sizeDatum > 0xFF);
        }
    }

    size_t ConstRLEPayload::getSizeOfVarPartForOneDatum(size_t offset)
    {
        assert(_elemSize==0);
        assert(offset+_varOffs<_dataSize);

        char* address = getVarData() + offset;
        size_t sizeHeader, sizeDatum;
        getSizeOfVarPartForOneDatum(address, sizeHeader, sizeDatum);
        size_t sizeTotal = sizeHeader + sizeDatum;
        assert(offset + sizeTotal <= _dataSize-_varOffs); // the var part must hold the whole datum
        return sizeTotal;
    }

    void ConstRLEPayload::appendValueToTheEndOfVarPart(std::vector<char>& varPart, char* const datumInRLEPayload)
    {
        size_t offs = varPart.size();
        size_t len;
        if (*datumInRLEPayload == 0) {
            len = 5 + *(int*)(datumInRLEPayload + 1);
        } else {
            len = 1 + (*datumInRLEPayload & 0xFF);
        }
        varPart.resize(offs + len);
        memcpy(&varPart[offs], datumInRLEPayload, len);
    }

    void ConstRLEPayload::appendValueToTheEndOfVarPart(std::vector<char>& varPart, Value const& value)
    {
        int offs = varPart.size();
        size_t len = value.size();

        if (len==0 || len > 0xFF) {
            varPart.resize(offs + len + 5);
            varPart[offs++] = 0;
            *(int*)&varPart[offs] = len;
            offs += 4;
        } else {
            varPart.resize(offs + len + 1);
            varPart[offs++] = len;
        }
        memcpy(&varPart[offs], value.data(), len);
    }
}
