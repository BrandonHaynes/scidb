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

#ifndef __RLE_H__
#define __RLE_H__

#include <map>
#include <vector>
#include <boost/utility.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/serialization/access.hpp>
#include <boost/serialization/split_member.hpp>
#include <system/Exceptions.h>
#include <array/Coordinate.h>
#include <util/arena/Map.h>
#include <query/Value.h>

namespace scidb
{
class ConstChunk;
class RLEPayload;
class Query;
class ArrayDesc;

typedef mgd::map<position_t, Value> ValueMap;

extern void checkChunkMagic(ConstChunk const& chunk);

class RLEEmptyBitmap;
class ConstRLEEmptyBitmap
{
    friend class RLEEmptyBitmap;
public:
    struct Segment {
        position_t _lPosition;   // start position of sequence of set bits
        position_t _length;  // number of set bits
        position_t _pPosition; // index of value in payload

        Segment()
            : _lPosition(-1),
              _length(-1),
              _pPosition(-1) {}
    };

    // This structure must use platform independent data types with fixed size.
    struct Header {
        uint64_t _magic;
        uint64_t _nSegs;
        uint64_t _nNonEmptyElements;
    };

  protected:
    size_t _nSegs;
    Segment const* _seg;
    uint64_t _nNonEmptyElements;
    ConstChunk const* _chunk;
    bool _chunkPinned;

    /**
     * Default constructor
     */
    ConstRLEEmptyBitmap() {
        _nSegs = 0;
        _nNonEmptyElements = 0;
        _seg = NULL;
        _chunk  = NULL;
    }

  public:

    size_t getValueIndex(position_t pos) const {
        size_t r = findSegment(pos);
        return (r < _nSegs && _seg[r]._lPosition <= pos) ? _seg[r]._pPosition + pos - _seg[r]._lPosition : size_t(-1);
    }

    /**
     * Check if element at specified position is empty
     */
    bool isEmpty(position_t pos) const {
        size_t r = findSegment(pos);
        return r == _nSegs || _seg[r]._lPosition > pos;
    }

    /**
     * Get number of RLE segments
     */
    size_t nSegments() const {
        return _nSegs;
    }

    /**
     * Get next i-th segment corresponding to non-empty elements
     */
    Segment const& getSegment(size_t i) const {
        assert(i < _nSegs);
        return _seg[i];
    }

    /**
     * Find segment of non-empty elements with position greater or equal than specified.
     */
    size_t findSegment(position_t pos) const {
        size_t l = 0, r = _nSegs;
        while (l < r) {
            size_t m = (l + r) >> 1;
            if (_seg[m]._lPosition + _seg[m]._length <= pos) {
                l = m + 1;
            } else {
                r = m;
            }
        }
        return r;
    }

    /**
     * Method to be called to save bitmap in chunk body
     */
    void pack(char* dst) const;

    /**
     * Get size needed to pack bitmap (used to dermine size of chunk)
     */
    size_t packedSize() const
    {
        return sizeof(Header) + _nSegs*sizeof(Segment);
    }

    /**
     * Constructor for initializing Bitmap with raw chunk data
     */
    ConstRLEEmptyBitmap(char const* src);

    ConstRLEEmptyBitmap(ConstChunk const& chunk);

    virtual ~ConstRLEEmptyBitmap();

    class iterator
    {
      private:
        ConstRLEEmptyBitmap const* _bm;
        size_t _currSeg;
        Segment const* _cs;
        position_t _currLPos;

      public:
        iterator(ConstRLEEmptyBitmap const* bm):
        _bm(bm), _currSeg(0), _cs(NULL), _currLPos(-1)
        {
            reset();
        }
        iterator():
        _bm(NULL), _currSeg(0), _cs(NULL), _currLPos(-1)
        {}

        void reset()
        {
            _currSeg = 0;
            if(!end())
            {
                _cs = &_bm->getSegment(_currSeg);
                _currLPos = _cs->_lPosition;
            }
        }

        bool end()
        {
            return _currSeg >= _bm->nSegments();
        }

        position_t const& getLPos()
        {
            assert(!end());
            return _currLPos;
        }

        position_t getPPos()
        {
            assert(!end());
            return _cs->_pPosition + _currLPos - _cs->_lPosition;
        }

        bool setPosition(position_t lPos)
        {
            _currSeg = _bm->findSegment(lPos);
            if (end() || _bm->getSegment(_currSeg)._lPosition > lPos)
            {
                _currSeg = _bm->nSegments();
                return false;
            }
            _cs = &_bm->getSegment(_currSeg);
            _currLPos = lPos;
            return true;
        }

        bool skip(size_t n);

        void operator ++()
        {
            assert(!end());
            if (_currLPos + 1 < _cs->_lPosition + _cs->_length)
            {
                _currLPos ++;
            }
            else
            {
                _currSeg++;
                if (!end())
                {
                    _cs = &_bm->getSegment(_currSeg);
                    _currLPos = _cs->_lPosition;
                }
            }
        }
    };

    /**
     * An iterator that iterates through the segments, rather than the individual non-empty cells.
     * It also helps remember an 'offset' within a segment, enabling the caller to take a partial segment out and treat the remains of the segment as another virtual segment.
     */
    class SegmentIterator
    {
    private:
        ConstRLEEmptyBitmap const* _bm;
        size_t _currSeg;
        position_t _offset;  // offset of _lPosition

    public:
        /**
         * Constructor.
         */
        SegmentIterator(ConstRLEEmptyBitmap const* bm): _bm(bm)
        {
            reset();
        }

        /**
         * Reset to the beginning of the first segment.
         */
        void reset()
        {
            _currSeg = 0;
            _offset = 0;
        }

        /**
         * Whether there is no more segment.
         */
        bool end() const
        {
            return _currSeg >= _bm->nSegments();
        }

        /**
         * Get lPosition, pPosition, and length, for the current virtual segment (could be in the middle of an actual segment).
         * @param[out] ret     the segment to be returned
         */
        void getVirtualSegment(ConstRLEEmptyBitmap::Segment& ret) const
        {
            assert(!end());
            ConstRLEEmptyBitmap::Segment const& segment = _bm->getSegment(_currSeg);
            ret._lPosition = segment._lPosition + _offset;
            ret._pPosition = segment._pPosition + _offset;
            ret._length = segment._length - _offset;
        }

        /**
         * Advance the position, within the same segment.
         * @param stepSize  how many positions to skip
         * @pre stepSize must be LESS than the remaining length of the virtual segment; to advance to the end of the segment you should use operator++()
         */
        void advanceWithinSegment(position_t stepSize)
        {
            assert(stepSize > 0);
            assert(!end());
            assert(_offset + stepSize < _bm->getSegment(_currSeg)._length);
            _offset += stepSize;
        }

        /**
         * Advance to the next segment.
         */
        void operator++()
        {
            assert(!end());
            ++ _currSeg;
            _offset = 0;
        }
    };

    iterator getIterator() const
    {
        return iterator(this);
    }

    uint64_t count() const
    {
        return _nNonEmptyElements;
    }

    /**
     * Extract subregion from bitmap.
     *
     * @param lowerOrigin lower coordinates of original array.
     * @param upperOrigin upper coordinates of original array.
     * @param lowerResult lower coordinates of subarray.
     * @param lowreResult lower coordinates of subarray.
     *
     * @return bitmap with same shape and zeros in (Original MINUS Subarray) areas.
     */
    boost::shared_ptr<RLEEmptyBitmap> cut(
            Coordinates const& lowerOrigin,
            Coordinates const& upperOrigin,
            Coordinates const& lowerResult,
            Coordinates const& upperResult) const;
};

std::ostream& operator<<(std::ostream& stream, ConstRLEEmptyBitmap const& map);

class RLEEmptyBitmap : public ConstRLEEmptyBitmap
{
  private:
    std::vector<Segment> _container;

    position_t addRange(position_t lpos, position_t ppos, uint64_t sliceSize, size_t level, Coordinates const& chunkSize, Coordinates const& origin, Coordinates const& first, Coordinates const& last);

  public:
    void reserve(size_t size) {
        _container.reserve(size);
    }

    void clear()
    {
        _container.clear();
        _seg = NULL;
        _nSegs = 0;
        _nNonEmptyElements = 0;
    }

    void addSegment(Segment const& segm)
    {
        if (_nSegs > 0)
        {
            assert(segm._lPosition >= _container[_nSegs-1]._lPosition + _container[_nSegs-1]._length &&
                   segm._pPosition >= _container[_nSegs-1]._pPosition + _container[_nSegs-1]._length);
        }

        _container.push_back(segm);
        _seg = &_container[0];
        _nNonEmptyElements += segm._length;
        _nSegs++;
    }

    void addPositionPair(position_t const& lPosition, position_t const& pPosition)
    {
        _nNonEmptyElements += 1;
        if (_nSegs > 0 &&
            _container[_nSegs-1]._lPosition + _container[_nSegs-1]._length == lPosition &&
            _container[_nSegs-1]._pPosition + _container[_nSegs-1]._length == pPosition)
        {
            _container[_nSegs-1]._length++;
        }
        else
        {
            Segment ns;
            ns._lPosition=lPosition;
            ns._pPosition=pPosition;
            ns._length=1;
            addSegment(ns);
        }
    }

    RLEEmptyBitmap& operator=(ConstRLEEmptyBitmap const& other)
    {
        _nSegs = other.nSegments();
        _nNonEmptyElements = other._nNonEmptyElements;
        _container.resize(_nSegs);
        memcpy(&_container[0], other._seg, _nSegs*sizeof(Segment));
        _seg = &_container[0];
        return *this;
    }

    RLEEmptyBitmap(ConstRLEEmptyBitmap const& other):
        ConstRLEEmptyBitmap()
    {
        *this = other;
    }

    RLEEmptyBitmap& operator=(RLEEmptyBitmap const& other)
    {
        _nSegs = other._nSegs;
        _nNonEmptyElements = other._nNonEmptyElements;
        _container = other._container;
        _seg = &_container[0];
        return *this;
    }

    RLEEmptyBitmap(RLEPayload& payload);

    RLEEmptyBitmap(RLEEmptyBitmap const& other):
            ConstRLEEmptyBitmap()
    {
        *this = other;
    }

    /**
     * Default constructor
     */
    RLEEmptyBitmap(): ConstRLEEmptyBitmap()
    {}

    /*
     * Create fully dense bitmask of nBits bits
     */
    RLEEmptyBitmap(position_t nBits): ConstRLEEmptyBitmap(), _container(1)
    {
        _container[0]._lPosition = 0;
        _container[0]._length = nBits;
        _container[0]._pPosition = 0;
        _nSegs=1;
        _nNonEmptyElements = nBits;
        _seg = &_container[0];
    }

    /**
     * Constructor of bitmap from ValueMap (which is used to be filled by ChunkIterator)
     */
    RLEEmptyBitmap(ValueMap& vm, bool all = false);

    /**
     * Constructor of RLE bitmap from dense bit vector
     */
    RLEEmptyBitmap(char* data, size_t numBits);

    /**
     * Constructor for initializing Bitmap from specified chunk
     */
    RLEEmptyBitmap(ConstChunk const& chunk);
};

class RLEPayload;

/**
  * class ConstRLEPayload
  * This class stores values in a stride-major-ordered array with RLE-packing of data.
  * We have the payload array where we store values.
  * The payload array is split into separated parts called segments.
  * Each segment has description (struct Segment).  All Segments stored within a container.
  * Every segment has the following fields:
  *  - pPosition:  physical position (stride-major-order) of first value from segment
  *  - valueIndex:  byte number inside the payload array where the data for this segment is
  *      located, or a value for missingReason if the segment is absent (nulls, empty, etc).
  *  - same:  true if all values in the segment are equal
  *  - null:  bit describing valueIndex
  *
  * NOTE: This class doesn't take ownership of passed-in payloads.
  * NOTE: Cannot add values
  */
class ConstRLEPayload
{
friend class boost::serialization::access;
friend class RLEPayload;
public:
    struct Segment {
        position_t _pPosition; // position in chunk of first element
        uint32_t   _valueIndex : 30; // index of element in payload array or missing reason
        uint32_t   _same:1; // sequence of same values
        uint32_t   _null:1; // trigger if value is NULL (missingReason) or normal value (valueIndex)

        Segment()
        : _pPosition(-1),
        _valueIndex(static_cast<uint32_t>(0)),
        _same(uint8_t(0)),
        _null(uint8_t(0)) {}

        Segment(position_t pPos,
                uint32_t vIndex,
                bool isSame,
                bool isNull)
        : _pPosition(pPos),
        _valueIndex(vIndex),
        _same(isSame),
        _null(isNull) {}

        /**
         * NOTE: Danger method implementation!!!
         * If you copy structure to separate variable this method will not work
         * without any warnings.
         * First Idea to remove this method at all and force implement it directly in code.
         * This prevents user from wrong usage.
         */
        uint64_t length() const {
            return this[1]._pPosition - _pPosition;
        }

        template<class Archive>
        void save(Archive & ar, const unsigned int version) const
        {
            position_t pPosition__ = _pPosition;
            uint32_t valueIndex__ = _valueIndex;
            uint8_t same__ = _same;
            uint8_t null__ = _null;
            ar & pPosition__;
            ar & valueIndex__;
            ar & same__;
            ar & null__;
        }
        template<class Archive>
        void load(Archive & ar, const unsigned int version)
        {
            position_t pPosition__;
            uint32_t valueIndex__;
            uint8_t same__;
            uint8_t null__;
            ar & pPosition__;
            ar & valueIndex__;
            ar & same__;
            ar & null__;
            _pPosition = pPosition__;
            _valueIndex = valueIndex__;
            _same = same__;
            _null = null__;
        }
        BOOST_SERIALIZATION_SPLIT_MEMBER()
    } __attribute__ ((packed));

    // This structure must have platform independent data types because we use it in chunk format data structure
    struct Header {
        uint64_t  _magic;
        uint64_t  _nSegs;
        uint64_t  _elemSize;
        uint64_t  _dataSize;
        uint64_t  _varOffs;
        bool      _isBoolean;
    };

  protected:
    uint64_t _nSegs;
    uint64_t _elemSize;
    uint64_t _dataSize;
    uint64_t _varOffs;
    bool   _isBoolean;

    Segment* _seg;
    // case 1:
    // 1,1,1,2,2,3,0,0,0,0,0,5,5,5
    // seg = {0,0,true}, {3,1,true}, {5,2,true}, {6,3,true}, {11 ,4,true}, {14}
    // case 2:
    // 1,2,3,4,5,0,0,0,0
    // seg = {0,0,false}, {5,5,true}, {10}
    char* _payload;

    ConstRLEPayload(): _nSegs(0), _elemSize(0), _dataSize(0), _varOffs(0), _isBoolean(false), _seg(NULL), _payload(NULL)
    {}

  public:

    size_t count() const {
        return _nSegs == 0 ? 0 : _seg[_nSegs]._pPosition;
    }

    bool isBool() const
    {
        return _isBoolean;
    }

    /**
     * Given the beginning byte address of a var-part datum in an RLE payload, get the size of the datum including header size.
     * @param[in]  address    the byte address of the var-part datum
     * @param[out] sizeHeader the size of the header, that stores the size of the actual datum; either 1 or 5
     * @param[out] sizeDatum  the size of the datum (not including the header)
     * @note If the size is less than 256, one byte is used to store the datum length.
     *       Otherwise, five bytes are used to store the length. In particular, the first byte is 0, and the next four bytes stores the length.
     */
    inline static void getSizeOfVarPartForOneDatum(char* const address, size_t& sizeHeader, size_t& sizeDatum);

    /**
     * Given an offset into the var part (the value that is stored in the fixed part), tell how many bytes the var part of the datum has.
     * @pre Must be var-size type.
     * @pre The offset must be within the range of the var part of the payload.
     *
     * @param[in] offset    the offset of the data in the var part
     * @return    #bytes of the var-part of the datum
     */
    inline size_t getSizeOfVarPartForOneDatum(size_t offset);

    /**
     * Given an existing varPart of some RLEPayload, append a var-size value to the end of it.
     * @param[inout] varPart  an existing var part
     * @param[in]    datumInRLEPayload a var-type value, from another RLEPayload, to be appended
     * @note varPart will be resized to include both the (1-or-5-byte) header and the actual value
     */
    inline static void appendValueToTheEndOfVarPart(std::vector<char>& varPart, char* const datumInRLEPayload);

    /**
     * Given an existing varPart of some RLEPayload, append a var-size value to the end of it.
     * @param[inout] varPart  an existing var part
     * @param[in]    value    a new value to be appended
     * @pre the value must be var-size type
     * @note varPart will be resized to include both the (1-or-5-byte) header and the actual value
     */
    inline static void appendValueToTheEndOfVarPart(std::vector<char>& varPart, Value const& value);

    /**
     * Get value data by the given index
     * @param placeholder for exracted value
     * @param index of value obtained through Segment::valueIndex
     */
    void getValueByIndex(Value& value, size_t index) const;

    /**
     * Get pointer to raw value data for the given poistion
     * @param placeholder for exracted value
     * @param pos element position
     * @return true if values exists i payload, false othrwise
     */
    bool getValueByPosition(Value& value, position_t pos) const;

    /**
     * Return pointer for raw data for non-nullable types
     */
    char* getRawValue(size_t index) const {
        return _payload + index*(_elemSize == 0 ? 4 : _elemSize);
    }

    /**
     * Return pointer for raw data of variable size types
     */
    char* getRawVarValue(size_t index, size_t& size) const;

    /**
     * Get number of RLE segments
     */
    size_t nSegments() const {
        return _nSegs;
    }

    /**
     * Get element size (0 for varying size types)
     */
    size_t elementSize() const {
        return _elemSize;
    }

    /**
     * Get payload size in bytes
     */
    size_t payloadSize() const {
        return _dataSize;
        }

    /**
     * Get number of items in payload
     */
    size_t payloadCount() const {
        return _dataSize / (_elemSize == 0 ? 4 : _elemSize);
    }

    /**
     * Get next i-th segment
     */
    Segment const& getSegment(size_t i) const {
        assert(i <= _nSegs); // allow _nSegs, used to cut new Tile's
        assert(_seg);
        return _seg[i];
    }

    /**
     * Find segment containing elements with position greater or equal than specified
     */
    size_t findSegment(position_t pos) const {
        size_t l = 0, r = _nSegs;
        while (l < r) {
            size_t m = (l + r) / 2;
            position_t mpos =_seg[m+1]._pPosition;
            if (mpos == pos) {
                return (m+1);
            } else if (mpos < pos) {
                l = m + 1;
            } else {
                r = m;
            }
        }
        return r;
    }

    /**
     * Method to be called to save payload in chunk body
     */
    void pack(char* dst) const;

    /**
     * Get size needed to pack payload (used to determine size of chunk)
     */
    size_t packedSize() const
    {
        return sizeof(Header) + (_nSegs+1)*sizeof(Segment) + _dataSize;
    }


    /**
     * Constructor for initializing payload with raw chunk data
     */
    ConstRLEPayload(char const* src);

    void getCoordinates(ArrayDesc const& array, size_t dim, Coordinates const& chunkPos, Coordinates const& tilePos, boost::shared_ptr<Query> const& query, Value& dst, bool withOverlap) const;

    bool checkBit(size_t bit) const {
        return (_payload[bit >> 3] & (1 << (bit & 7))) != 0;
    }

    char* getFixData() const {
        return _payload;
    }

    char* getVarData() const {
        return _payload + _varOffs;
    }

    virtual ~ConstRLEPayload()
    {}

    class iterator
    {
    protected:
        ConstRLEPayload const* _payload;
        size_t _currSeg;
        Segment const* _cs;
        position_t _currPpos;

    public:
        //defined in .cpp because of value constructor
        iterator(ConstRLEPayload const* payload);
        iterator() : _payload(NULL), _currSeg(0), _cs(0), _currPpos(-1) {}

        size_t getCurrSeg()
        {
            return _currSeg;
        }

        void reset()
        {
            _currSeg = 0;
            if(!end())
            {
                _cs = &_payload->getSegment(_currSeg);
                _currPpos = _cs->_pPosition;
            }
        }

        bool end() const
        {
            return _currSeg >= _payload->nSegments();
        }

        int getMissingReason() const
        {
            assert(!end());
            return _cs->_valueIndex;
        }

        bool isNull() const
        {
            assert(!end());
            return _cs->_null;
        }

        bool isSame() const
        {
            assert(!end());
            return _cs->_same;
        }

        position_t const& getPPos() const
        {
            assert(!end());
            return _currPpos;
        }

        uint32_t getValueIndex() const
        {
            assert(!end());
            return (_cs->_same || _cs->_null) ? _cs->_valueIndex : _cs->_valueIndex + _currPpos - _cs->_pPosition;
        }

        uint64_t getSegLength() const
        {
            assert(!end());
            return _cs->length();
        }

        uint64_t getRepeatCount() const
        {
            assert(!end());
            return _cs->_same ? _cs->length() - _currPpos + _cs->_pPosition : 1;
        }

        uint64_t available() const
        {
            assert(!end());
            return _cs->length() - _currPpos + _cs->_pPosition;
        }

       bool checkBit() const
        {
            assert(_payload->_isBoolean);
            return _payload->checkBit(_cs->_valueIndex + (_cs->_same ? 0 : _currPpos - _cs->_pPosition));
        }

        void toNextSegment()
        {
            assert(!end());
            _currSeg ++;
            if (!end())
            {
                _cs = &_payload->getSegment(_currSeg);
                _currPpos = _cs->_pPosition;
            }
        }

        char* getRawValue(size_t& valSize)
        {
            size_t index = _cs->_same ? _cs->_valueIndex : _cs->_valueIndex + _currPpos - _cs->_pPosition;
            return _payload->getRawVarValue(index, valSize);
        }

        char* getFixedValues()
        {
            size_t index = _cs->_same ? _cs->_valueIndex : _cs->_valueIndex + _currPpos - _cs->_pPosition;
            return _payload->_payload + index*_payload->_elemSize;
        }

        bool isDefaultValue(Value const& defaultValue);

        //defined in .cpp because of value methods
        void getItem(Value &item);

        void operator ++()
        {
            assert(!end());
            if (_currPpos + 1 < position_t(_cs->_pPosition + _cs->length()))
            {
                _currPpos ++;
            }
            else
            {
                _currSeg ++;
                if(!end())
                {
                    _cs = &_payload->getSegment(_currSeg);
                    _currPpos = _cs->_pPosition;
                }
            }
        }

        bool setPosition(position_t pPos)
        {
            _currSeg = _payload->findSegment(pPos);
            if (end())
            {
                return false;
            }

            assert (_payload->getSegment(_currSeg)._pPosition <= pPos);

            _cs = &_payload->getSegment(_currSeg);
            _currPpos = pPos;
            return true;
        }

        /**
          * Should applied just for bool-typed RLE (bitmap tiles).
          * Skip @param count positions in payload and return number of "1" values.
          * Data Tile just store values without knowledge about physical positions inside tile, while a bitmap helps to understand where which value stay.
          * It is important for skip data from data tile:
          *   you call dataReader += bitmapReader.skip(physicalPositionsCount) and receive the consistent positions inside bitmapReader and dataReader.
          */
        uint64_t skip(uint64_t count)
        {
            uint64_t setBits = 0;
            while (!end()) {
                if (_currPpos + count >= _cs->_pPosition + _cs->length()) {
                    uint64_t tail = _cs->length() - _currPpos + _cs->_pPosition;
                    count -= tail;
                    if (_cs->_same)  {
                        setBits += _payload->checkBit(_cs->_valueIndex) ? tail : 0;
                    }  else {
                        position_t beg = _cs->_valueIndex + _currPpos - _cs->_pPosition;
                        position_t end = _cs->_valueIndex + _cs->length();
                        while (beg < end) {
                            setBits += _payload->checkBit(beg++);
                        }
                    }
                    toNextSegment();
                } else {
                    if (_cs->_same)  {
                        setBits += _payload->checkBit(_cs->_valueIndex) ? count : 0;
                    } else {
                        position_t beg = _cs->_valueIndex + _currPpos - _cs->_pPosition;
                        position_t end = beg + count;
                        while (beg < end) {
                            setBits += _payload->checkBit(beg++);
                        }
                    }
                    _currPpos += count;
                    break;
                }
            }
            return setBits;
        }

        void operator +=(uint64_t count)
        {
            assert(!end());
            _currPpos += count;
            if (_currPpos >= position_t(_cs->_pPosition + _cs->length())) {
                if (++_currSeg < _payload->nSegments()) {
                    _cs = &_payload->getSegment(_currSeg);
                    if (_currPpos < position_t(_cs->_pPosition + _cs->length())) {
                        return;
                    }
                }
                setPosition(_currPpos);
            }
        }
    };

    /**
     * A structure that contains more complete info of a virtual segment, than Segment does.
     */
    class SegmentWithLength: public Segment
    {
    public:
        position_t _length;         // the length of this segment
    };

    /**
     * An iterator that iterates through the segments, rather than the individual non-empty cells.
     * It is a wrapper over the 'iterator' class.
     */
    class SegmentIterator
    {
    private:
        ConstRLEPayload::iterator _it;

    public:
        /**
         * Constructor.
         */
        SegmentIterator(ConstRLEPayload const* payload): _it(payload)
        {
            reset();
        }

        /**
         * Get the segment number of the current segment.
         */
        size_t getCurrSeg()
        {
            return _it.getCurrSeg();
        }

        /**
         * Reset to the beginning of the first segment.
         */
        void reset()
        {
            _it.reset();
        }

        /**
         * Whether there is no more segment.
         */
        bool end() const
        {
            return _it.end();
        }

        /**
         * Get information for SegmentWithLength of the current virtual segment (could be in the middle of an actual segment).
         * @param[out] ret     the segment to be returned
         *
         */
        void getVirtualSegment(ConstRLEPayload::SegmentWithLength& ret) const
        {
            assert(!end());

            ret._pPosition = _it.getPPos();
            ret._length = _it.available();
            ret._same = _it.isSame();
            ret._null = _it.isNull();
            ret._valueIndex = _it.getValueIndex();
        }

        /**
         * Advance the position, within the same segment.
         * @param stepSize  how many positions to skip
         * @pre stepSize must be LESS than the remaining length of the virtual segment; to advance to the end of the segment you should use operator++()
         */
        void advanceWithinSegment(uint64_t stepSize)
        {
            assert(!end());
            assert(_it.available() > stepSize);

            _it.skip(stepSize);
        }

        /**
         * Advance to the next segment.
         */
        void operator++()
        {
            assert(!end());
            _it.toNextSegment();
        }

        /**
         * Advance either to the beginning of the next segment or to a position within the same segment.
         */
        void advanceBy(uint64_t stepSize)
        {
            assert(!end());
            assert(stepSize <= _it.available());
            if (stepSize == _it.available()) {
                _it.toNextSegment();
            } else {
                advanceWithinSegment(stepSize);
            }
        }
    };

    iterator getIterator() const
    {
        return iterator(this);
    }
};

std::ostream& operator<<(std::ostream& stream, ConstRLEPayload const& payload);

/**
 * A class that enables the construction of an RLEPayload object.
 * @note
 *   - For a variable-size type, until setVarPart() is called, _dataSize is the byte size of the fixed-size part (and _payload only contains the fixed-size part);
 *     After setVarPart() is called, _dataSize is the total byte size of the fixed-size part and the variable-size part (and the var part is copied into _payload).
 *   - For a fixed-size type, there is no var part. So _dataSize is both the size of the fixed-size part and the size of all the data.
 *   - For a boolean type, _dataSize is _valuesCount divided by 8 -- just enough to hold all the boolean values.
 *   - Each call to appendValue takes as input a vector of bytes (as the var part of the data), to receive the var part of the new value.
 *   - When constructing an RLEPayload, don't forget the last segment (which helps tell the length of the previous segment). You may call flush().
 *
 * @note
 *   Donghui believes the class should be rewritten in several ways:
 *   - It is better to store the variable part of the data inside the RLEPayload object, not somewhere else.
 *   - getValuesCount() returns _dataSize/elementSize, which is simply wrong after setVarPart() is called.
 *   - It is not clear when _valuesCount can be trusted. Seems that while data are being appended, _valuesCount is trustworthy only if the data type is boolean.  DJG While this is true, you never access _valuesCount directly.  The getValuesCount accessor returns the correct number of elements regardless of whether or not _isBoolean is true.  _valuesCount is an optimization for the _isBoolean = true case only.
 *   - addBoolValues() updates _dataSize and _valuesCount, but addRawValues() and addRawVarValues() do NOT update any of them. This is very inconsistent.
 *   - It is redundant to have both RLEPayload::append_iterator and RLEPayloadAppender.
 *   - append(RLEPayload& other) may set _data = other._data. This seems to violate the theme of the class -- storing its own copy of the data.
 */
class RLEPayload : public ConstRLEPayload
{
  private:
    std::vector<Segment> _container;
    std::vector<char> _data;
    uint64_t _valuesCount;

  public:
    void appendValue(std::vector<char>& varPart, Value const& val, size_t valueIndex);

    /**
     * Append a (partial) segment of values from another RLEPayload.
     *
     * @param[in]    dstSegmentToAppend   a segment to be appended
     * @param[inout] varPart              the variable-size part of the data
     * @param[in]    srcPayload           the source ConstRLEPayload to copy data from
     * @param[in]    valueIndexInSrc      the valueIndex in the src, corresponding to the first value to append
     * @param[in]    realLength           the number of values to append; 1 if dstSegmentToAppend._same==true; 0 if dstSegmentToAppend._null==true
     *
     * @note _valuesCount needs to be accurate before and after the call.
     * @note _dataSize needs to be the byte size of the fixed data before and after the call.
     * @note It is the caller's responsibility to call setVarPart() and flush() at the end.
     */
    void appendAPartialSegmentOfValues(Segment const& dstSegmentToAppend, std::vector<char>& varPart,
            ConstRLEPayload& srcPayload, uint32_t valueIndexInSrc, position_t realLength);

    void setVarPart(char const* data, size_t size);
    void setVarPart(std::vector<char>& varPart);

    friend class RLEPayloadAppender;

    void append(RLEPayload& payload);

    /**
     * Add raw fixed data for non-nullable types
     * @param n a number of new items
     * @return index of the first new item
     */
    size_t addRawValues(size_t n = 1) {
        assert(_elemSize != 0);
        const size_t ret = _dataSize / _elemSize;
        _data.resize(_dataSize += _elemSize * n);
        _payload = &_data[0];
        return ret;
    }

    /**
     * Add raw var data for non-nullable types
     * @param n a number of new items
     * @return index of the first new item
     */
    size_t addRawVarValues(size_t n = 1) {
        assert(_elemSize == 0);
        const size_t fixedSize = sizeof(int);
        const size_t ret = _dataSize / fixedSize;
        _data.resize(_dataSize += fixedSize * n);
        _payload = &_data[0];
        return ret;
    }

    /**
     * Add raw bool data for non-nullable types
     * @param n a number of new items
     * @return index of the first new item
     */
    size_t addBoolValues(size_t n = 1) {
        assert(_elemSize == 1 && _isBoolean);
        size_t ret = _valuesCount;
        _valuesCount += n;
        _dataSize = (_valuesCount >> 3) + 1;
        _data.resize(_dataSize);
        _payload = &_data[0];
        return ret;
    }

    /**
     * @return number of elements
     */
    size_t getValuesCount() const  {
        if (_isBoolean)
            return _valuesCount;
        const size_t fixedSize = _elemSize == 0 ? 4 : _elemSize;
        return _dataSize / fixedSize;
    }

    /**
     * Add new segment
     */
    void addSegment(const Segment& segment) {
        assert(_container.size() == 0 || _container[_container.size() - 1]._pPosition < segment._pPosition);
        _container.push_back(segment);
        _seg = &_container[0];
        _nSegs = _container.size() - 1;
    }

    /**
     * Assign segments pointer from other payload.
     * Sometimes it's safe to just copy pointer but for conversion
     * constant inplace it's impossible for example.
     * That's why copy param is tru by default
     */
    void assignSegments(const ConstRLEPayload& payload, bool copy = true)
    {

        if (copy) {
            _nSegs = payload.nSegments();
            _container.resize(_nSegs + 1);
            memcpy(&_container[0], payload._seg, (_nSegs + 1) * sizeof(Segment));
            _seg = &_container[0];
        } else {
            _seg = payload._seg;
            _nSegs = payload._nSegs;
        }
    }

    /**
     * Assignment operator: deep copy from const payload into non-const
     */
    RLEPayload& operator=(ConstRLEPayload const& other)
    {
        _nSegs = other.nSegments();
        _elemSize = other._elemSize;
        _dataSize = other._dataSize;
        _varOffs = other._varOffs;
        _isBoolean = other._isBoolean;
        _container.resize(_nSegs+1);
        memcpy(&_container[0], other._seg, (_nSegs+1)*sizeof(Segment));
        _seg = &_container[0];
        _data.resize(_dataSize);
        memcpy(&_data[0], other._payload, _dataSize);
        _payload = &_data[0];
        return *this;
    }

    RLEPayload(ConstRLEPayload const& other):
        ConstRLEPayload()
    {
        *this = other;
    }

    RLEPayload& operator=(RLEPayload const& other)
    {
        _nSegs = other._nSegs;
        _elemSize = other._elemSize;
        _dataSize = other._dataSize;
        _varOffs = other._varOffs;
        _isBoolean = other._isBoolean;
        _container = other._container;
        _seg = &_container[0];
        _data = other._data;
        _payload = &_data[0];
        _valuesCount = other._valuesCount;
        return *this;
    }

    /**
     * Copy constructor: deep copy
     */
    RLEPayload(RLEPayload const& other):
        ConstRLEPayload()
    {
        *this = other;
    }

    RLEPayload();

    /**
     * Constructor of bitmap from ValueMap (which is used to be filled by ChunkIterator)
     * @param vm ValueMap of inserted {position,value} pairs
     * @param nElems number of elements present in the chunk
     * @param elemSize fixed size of element (in bytes), 0 for varying size types
     * @param defaultValue default value used to fill holes (elements not specified in ValueMap)
     * @param subsequent all elements in ValueMap are assumed to be subsequent
     */
    RLEPayload(ValueMap& vm, size_t nElems, size_t elemSize, Value const& defaultVal, bool isBoolean, bool subsequent);

    /**
     * Constructor which is used to fill a non-emptyable RLE chunk with default values.
     * @param[in]  defaultVal  the default value of the attribute
     * @param[in]  logicalSize the number of logical cells in the chunk
     * @param[in]  elemSize    fixed size of element (in bytes), 0 for varying size types
     * @param[in]  isBoolean   whether the element is of boolean type
     */
    RLEPayload(Value const& defaultVal, size_t logicalSize, size_t elemSize, bool isBoolean);

    /**
     * Constructor of RLE bitmap from dense non-nullable data
     */
    RLEPayload(char* rawData, size_t rawSize, size_t varOffs, size_t elemSize, size_t nElems, bool isBoolean);
    void unpackRawData(char* rawData, size_t rawSize, size_t varOffs, size_t elemSize, size_t nElems, bool isBoolean);

    RLEPayload(const class Type& type);

    RLEPayload(size_t bitSize);

    //
    // Yet another appender: correct handling of boolean and varying size types
    //
    class append_iterator : boost::noncopyable
    {
        RLEPayload* result;
        std::vector<char> varPart;
        RLEPayload::Segment segm;
        Value prevVal;
        size_t valueIndex;
        size_t segLength;

      public:
        RLEPayload* getPayload() {
            return result;
        }

        explicit append_iterator(RLEPayload* dstPayload);
        void flush();
        void add(Value const& v, uint64_t count = 1);
        /**
         * add not more than @param limit values from @param inputIterator
         * Flag @param setupPrevVal just a workaround for bug with mixed
         * add(iterator&, limit) and add(Value const&) calls - after that payload
         * can be broken.
         * I (Oleg) did not fix bug directly according to potential performance regression
         *
         * @return count of added value from @param inputIterator
         * please note - this method add just single segment from @param inputIterator.
         * if your @param limit can be larger than segmentLength from @param inputIterator
         * you should compare @return and @param limit, and repeat call with
         * (@param limit = @param limit - @return)
         * (if @param inputIterator still has values, of course).
         */
        uint64_t add(iterator& inputIterator, uint64_t limit, bool setupPrevVal = false);
        ~append_iterator();
    private:
        append_iterator();
    };

    /**
     * Clear all data
     */
    void clear();

    /**
     * Use this method to copy payload data according to an empty bitmask and start and stop
     * positions.
     * @param [in] payload an input payload
     * @param [in] emptyMap an input empty bitmap mask according to which data should be extracted
     * @param [in] vStart a logical position of start from data should be copied
     * @param [in] vEnd a logical position of stop where data should be copied
     */
    void unPackTile(const ConstRLEPayload& payload, const ConstRLEEmptyBitmap& emptyMap, position_t vStart, position_t vEnd);

    /**
     * Use this method to copy empty bitmask to payload
     * positions.
     * @param [in] emptyMap an input empty bitmap mask according to which data should be extracted
     * @param [in] vStart a logical position of start from data should be copied
     * @param [in] vEnd a logical position of stop where data should be copied
     */
    void unPackTile(const ConstRLEEmptyBitmap& emptyMap, position_t vStart, position_t vEnd);

     /**
     * Complete adding segments to the chunk
     */
    void flush(position_t chunkSize);

    void trim(position_t lastPos);

    template<class Archive>
    void serialize(Archive& ar, const unsigned int version)
    {
        ar & _nSegs;
        ar & _elemSize;
        ar & _dataSize;
        ar & _varOffs;
        ar & _container;
        ar & _data;
        ar & _isBoolean;
        if (Archive::is_loading::value) {
            _seg = &_container[0];
            _payload = &_data[0];
        }
    }
};

class RLEPayloadAppender
{
private:
    RLEPayload _payload;

    ssize_t _nextSeg;
    ssize_t _nextPPos;
    ssize_t _nextValIndex;
    bool _finalized;

public:
    RLEPayloadAppender(size_t bitSize): _payload(bitSize), _nextSeg(0), _nextPPos(0), _nextValIndex(0), _finalized(false)
    {
        //no boolean yet!
        if (bitSize <= 1)
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_NOT_IMPLEMENTED) << "payload appender for size <= 1";
    }

    ~RLEPayloadAppender()
    {}

    void append(Value const& v);

    void finalize()
    {
        _payload._container.resize(_nextSeg+1);
        _payload._container[_nextSeg]._pPosition = _nextPPos;
        _payload._valuesCount = _nextValIndex;
        _payload._dataSize = _nextValIndex * _payload._elemSize;
        _payload._isBoolean = false;
        _payload._nSegs = _nextSeg;
        _payload._payload = &_payload._data[0];
        _payload._seg = &_payload._container[0];
        _finalized = true;
    }

    RLEPayload const* getPayload()
    {
        assert(_finalized);
        return &_payload;
    }
};

}

#endif
