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
 *    @file Tile.h
 *
 * @note
 * This file contains the classes/templates that we'll use to implement scidb::Tile.
 *    RLEEncoding<> - RLE type-aware encoding implementation
 *    ArrayEncoding<> - identity type-aware encoding implementation
 *
 *    Tile<>      - type-aware data structure that delegates data storage to an encoding implementation
 *    Tile<scidb::Value, ...> specialization
 *    Tile<scidb::Coordinates, ...> specialization
 *    TileBuilder<> - a simple functor for making a tile for a given encoding and data types
 * @see scidb::Tile
 */
#ifndef __TILE__
#define __TILE__

#include <assert.h>         // Lots of asserts in this prototype.
#include <string.h>
#include <stdint.h>         // For the various int types
#include <memory>           // For the unique_ptr
#include <iostream>         // For the operator<< and the dump().
#include <vector>
#include <algorithm>
#include <boost/unordered_map.hpp>
#include <boost/function.hpp>
#include <boost/bind.hpp>
#include <boost/make_shared.hpp>
#include <util/CoordinatesMapper.h>
#include <array/TileInterface.h>
#include <util/Utility.h>

#include <log4cxx/logger.h>

namespace scidb {

    /**
     * The ArrayEncoding simply writes values to the end of the data, and
     * does nothing in the encoding itself. It is an "identity" encoding.
     * @param Type the type of data elements
     */
    template < typename Type >
    class ArrayEncoding : public BaseEncoding, boost::noncopyable
    {
    private:
        std::vector<Type> _data;

    public:
    ArrayEncoding(TypeId typeId)
    : BaseEncoding( BaseEncoding::ARRAY, typeId )
        {}

    ArrayEncoding(TypeId typeId, size_t maxSize)
    : BaseEncoding( BaseEncoding::ARRAY, typeId )
        {
            _data.reserve(maxSize);
        }
        size_t typeSize() const
        {
            return sizeof(Type);
        }
        size_t size() const
        {
            return _data.size();
        }
        bool empty() const
        {
            return _data.empty();
        }
        void reserve(size_t n)
        {
            assert(n>0);
            _data.reserve(n);
        }
        virtual void initialize(const BaseEncoding::Context* ctx)
        {
        }

        void push_back ( const Type& value )
        {
            _data.push_back ( value );
        }
        void finalize()
        {
        }
        const Type& at( size_t where) const
        {
            assert(where<_data.size());
            const Type& value = _data[ where ];
            return value;
        }
        void clear()
        {
            _data.clear();
        }

        std::ostream& insert ( std::ostream& out ) const
        {
            out << "ArrayEncoding ( _typeSize = " << this->typeSize()
                << ", size = " << this->size() << ")" << std::endl;

            for ( size_t i = 0, len = this->size(); i < len; ++i )
            {
                bool isNull(false);
                out << " < [ " << i << " ] " << at(i) <<",null="<< isNull << " >" << std::endl;
            }
            return out;
        }

        friend std::ostream& operator<<( std::ostream& out,
                                         const ArrayEncoding& enc )
        {
            return enc.insert ( out );
        }
    };

    namespace rle
    {
        /**
         * Representation of an RLE segment.
         * An ordered (by _startPosition) list of segments in conjunction with a list of data elements ("payload")
         * is used to represent RLE encoded data.
         * The _dataIndex field points to the first data element inside the payload, which is the first value of the segment.
         * Each segment can either be a "run" or a "literal".
         * In a run segment all values are the same and are represented by a single payload value @ payload [ _dataIndex ].
         * In a literal segment the values may or may not be the same and each value in the segment
         * is represented by a separate value in the payload.
         * In the literal segment Si, the first value is payload [ Si._dataIndex ],
         * the last value is payload [ Si+1._startPosition - Si._startPosition - 1 ].
         * The terminating segment exists strictly for computing the length of the last data segment as in
         * Sterm._startPosition - Slast._startPosition.
         * To accomodate SciDB NULLs with missing codes, there are NULL segments.
         * They dont index into the payload and cannot be literals.
         * They represent runs (possibly of length 1) of NULLs with missing codes recorded in the _dataIndex field.
         */
        class Segment {

        private:

            uint64_t _startPosition;  // position of first cell in segment.
            struct bits {
                uint32_t     _dataIndex : 30; // index of element into payload
                uint32_t     _isRun : 1;      // Is segment run of same value or literal.
                uint32_t     _isNull : 1;    // _dataIndex is missing code
            } __attribute__ ((packed));

            union {
                struct bits _bits;
                uint32_t _allBits;
            };
            static const uint32_t MAX_DATA_INDEX = 0x3FFFFFFF; // 30bits

        public:

            Segment ()
            : _startPosition(0), _allBits(0)
            {
                assert(sizeof(_bits) <= sizeof(_allBits));
            }
            Segment ( const uint64_t position, const uint32_t dataIndex,
                      const uint32_t isRun,    const int32_t isNull )
            : _startPosition(position)
            {
                _bits._dataIndex = dataIndex;
                _bits._isRun = isRun;
                _bits._isNull = isNull;

                assert(dataIndex <= MAX_DATA_INDEX);
                assert(isNull==0 || isNull==1);
                assert(isRun==0 || isRun==1);
            }
            bool isLiteral() const
            {
                return ( 0 == _bits._isRun );
            }
            bool isRun() const
            {
                return !isLiteral();
            }
            bool isNull() const
            {
                return ( 1 == _bits._isNull );
            }
            int32_t getMissingCode() const
            {
                assert(isRun());
                assert(isNull());
                return static_cast<int32_t>(_bits._dataIndex);
            }
            uint32_t getDataIndex() const
            {
                return _bits._dataIndex;
            }
            uint64_t getStartPosition() const
            {
                return _startPosition;
            }
            void setStartPosition(uint64_t pos)
            {
                _startPosition = pos;
            }
            void setMissingCode(int32_t code)
            {
                // the missing code is not stored in payload _data
                // but rather directly in _bits._dataIndex
                assert(isNull());
                assert(isRun());
                assert(code>=0);
                setDataIndex(code);
            }
            void setDataIndex(uint32_t i)
            {
                assert(i <= MAX_DATA_INDEX);
                _bits._dataIndex = i;
            }
            void setRun(bool b)  { _bits._isRun = b; }
            void setNull(bool b) { _bits._isNull = b; }
        };

        std::ostream& operator << ( std::ostream& out,
                                    const scidb::rle::Segment& segment );
        /**
         * A context used to initialize an RLEEncoding object from scidb::ConstRLEPayload
         */
        class RLEPayloadProvider : public BaseEncoding::Context
        {
        public:
            virtual const ConstRLEPayload* getPayload() const = 0;
            virtual position_t getOffset() const = 0;
            virtual size_t getNumElements() const = 0;
        };
    } // namespace rle

    /**
     * Space and time efficient RLE encoding.
     * Physical representation of encoded data:
     *
     *           +-------+-------+-------+-------+-->
     * data      | value | value | value | value |
     *           +-------+-------+-------+-------+-->
     *
     *           +---------+---------+---------+--->
     * encoding  | Segment | Segment | Segment |
     *           +---------+---------+---------+--->
     *                    /                         \
     *           { position, index, run, missing }
     * @see rle::Segment
     * @param Type the type of data elements
     */
    template < typename Type >
    class RLEEncoding : public BaseEncoding, boost::noncopyable
    {
    public:

        /**
         * Constructor
         * @param typeId the type ID of the elements
         */
        RLEEncoding (TypeId typeId)
        : BaseEncoding( BaseEncoding::RLE, typeId ),
        _lastDistinctVal(-1),
        _maxRunlen (static_cast<uint32_t>(sizeof(rle::Segment) / sizeof(Type))+1),
        _nextPosition (0),
        _currSegIndex (0)
        {
        }

        /**
         * Constructor
         * @param typeId the type ID of the elements
         * @param max_size in bytes, the hint to indicate the maximum number of elements
         */
        RLEEncoding ( scidb::TypeId typeId, size_t max_size)
        : BaseEncoding( BaseEncoding::RLE, typeId ),
        _lastDistinctVal(-1),
        _maxRunlen (static_cast<uint32_t>(sizeof(rle::Segment) / sizeof(Type))+1),
        _nextPosition (0),
        _currSegIndex (0)
        {
            _data.reserve( max_size );
            _segments.reserve ( max_size / _maxRunlen + 1);
        }

        /// Reserve space for n elements
        /// @param n number of elements
        void reserve(size_t n)
        {
            assert(n>0);
            _data.reserve(n);
            _segments.reserve ( n / _maxRunlen + 1);
        }

        /// Destructor
        virtual ~RLEEncoding() {}

        /// @return the size of Type in bytes
        size_t typeSize() const
        {
            return sizeof(Type);
        }

        /// @return the number of values in the encoding
        size_t size () const
        {
            size_t result = 0;
            if ( !_segments.empty() )
            {
                result = getLastStartPosition();
                if ( _nextPosition > result )
                {
                    result = _nextPosition;
                }
            }
            return result;
        }

        /// @return true iff the encoding is empty
        bool empty () const
        {
            return _segments.empty();
        }

        /**
         * A hook possibly useful for some encodings
         * @param ctx context required for initialization, can be NULL
         *        If non-NULL, this implementation will cast ctx to
         *        const scidb::rle::RLEPayloadProvider* and use it initialze.
         *        After the call completes (with non-NULL ctx), the object is fully
         *        populated, i.e. push_back() can no longer be used
         * @pre encoding is not finalized
         */
        virtual void initialize(const BaseEncoding::Context* ctx)
        {
            assert(!isFinalized()); //exception ?
            if (ctx==NULL) {
                return; /*nthn*/
            }
            const rle::RLEPayloadProvider *provider =
                safe_dynamic_cast<const rle::RLEPayloadProvider*>(ctx);
            initialize(provider->getPayload(),
                       provider->getOffset(),
                       provider->getNumElements());
        }

        /**
         * Fully initialize this object from ConstRLEPayload
         * @param rlePayload
         * @param off the start offset within the payload
         * @param nElems number of values/elements to extract from the payload
         * @note For internal use only
         * @pre encoding is not finalized
         * @post encoding is finalized
         */
        void initialize(const ConstRLEPayload* rlePayload, position_t off, size_t nElems)
        {
            assert(!isFinalized()); //exception ?
            assert(off>=0);
            assert(rlePayload);
            assert(rlePayload->elementSize()>0);
            assert(!rlePayload->isBool());
            assert(typeSize()>0);

            // terminal segment index
            size_t maxSegIndx   = rlePayload->nSegments();
            // first segment index
            size_t startSegIndx = rlePayload->findSegment(off);
            if (startSegIndx >= maxSegIndx) {
                assert(false);
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNREACHABLE_CODE)
                <<"position not in rlePayload";
            }
            // last segment index
            size_t endSegIndx = maxSegIndx-1;
            if ((off+nElems) < rlePayload->count()) {
                endSegIndx = rlePayload->findSegment(off+nElems-1);
            }

            assert(endSegIndx < maxSegIndx);

            // Initialize segments
            ConstRLEPayload::Segment const* startValueSeg(NULL);

            // +1 to correct length, +1 to allocate the terminal segment
            _segments.resize(endSegIndx-startSegIndx+1+1);

            // first segment containing non-NULL data index
            size_t startValueSegIndx = std::numeric_limits<size_t>::max();
            // last segment containing non-NULL data index
            size_t endValueSegIndx = 0;
            // offset relative to the start of non-NULL data (in case off points into the middle of a litteral segment)
            size_t startValueIndexShift = 0;

            for (size_t i=0, n=_segments.size()-1; i <n; ++i) {

                assert(startSegIndx+i < maxSegIndx);
                ConstRLEPayload::Segment const& srcSeg = rlePayload->getSegment(startSegIndx+i);
                rle::Segment& nextSeg = _segments[i];

                assert( srcSeg._pPosition > off || i==0);
                nextSeg.setStartPosition(srcSeg._pPosition - off);
                assert(nextSeg.getStartPosition()>0 || i==0);

                nextSeg.setRun(srcSeg._same);
                nextSeg.setNull(srcSeg._null);

                // initialize non-NULL segment boundaries & setDataIndex()
                if (srcSeg._null==0) {
                    size_t dataIndexOff = startValueIndexShift;
                    if (startValueSegIndx > startSegIndx+i) {
                        startValueSegIndx = startSegIndx+i;
                        assert(&srcSeg == &rlePayload->getSegment(startValueSegIndx));
                        startValueSeg = &srcSeg;
                        assert(startValueSeg);
                        if (startValueSegIndx==startSegIndx && startValueSeg->_same==0) {
                            // the first segment is non-null and is not compressed (i.e. literal)
                            // the first value we want can be in the middle of the segment
                            assert(off >= startValueSeg->_pPosition);
                            startValueIndexShift = off - startValueSeg->_pPosition;
                            dataIndexOff = 0;
                        }
                    }
                    endValueSegIndx = startSegIndx+i;
                    assert(srcSeg._valueIndex >= (startValueSeg->_valueIndex + dataIndexOff));
                    nextSeg.setDataIndex(srcSeg._valueIndex - startValueSeg->_valueIndex - dataIndexOff);
                } else {
                    // assign the missing code for the null segment
                    nextSeg.setMissingCode(srcSeg._valueIndex);
                }
            }
            assert(_segments.size() > 1);
            _segments[0].setStartPosition(0);
            if (!_segments[0].isNull()) {
                _segments[0].setDataIndex(0);
            }

            if (startValueSeg) {
                assert(startValueSeg->_null==0);
                // We have non-NULL data, so calculate the data boundaries inside rlePaylaod.
                char* startData(NULL);
                char* endData(NULL);
                computePayloadDataBoundaries(rlePayload, off, nElems,
                                             startValueSeg, startValueIndexShift,
                                             startSegIndx, startValueSegIndx,
                                             endSegIndx, endValueSegIndx,
                                             startData, endData);
                assert(rlePayload->getSegment(endValueSegIndx)._null==0);
                assert(rlePayload->getSegment(startValueSegIndx)._null==0);
                assert(endData <= (rlePayload->getRawValue(0) + rlePayload->payloadSize()));
                assert(maxSegIndx >= (endSegIndx+1));

                // make sure that the end of the data buffer does not stretch beyond the offset indicated by endValueSeg
                // (i.e. the last non-null segment we care about)
                assert( ( (maxSegIndx > endSegIndx+1) &&
                          (endData <= ( rlePayload->getRawValue(rlePayload->getSegment(endValueSegIndx)._valueIndex) +
                                        (rlePayload->getSegment(endValueSegIndx+1)._pPosition -
                                         rlePayload->getSegment(endValueSegIndx)._pPosition) * rlePayload->elementSize())) )
                        ||
                        (maxSegIndx == endSegIndx+1)
                        );

                // Initialize data values
                const size_t elemSize = rlePayload->elementSize();
                size_t nVals = (endData - startData) / elemSize;
                assert(nVals<=nElems);
                _data.resize(nVals);

                initializeInternalData(startData, endData, elemSize);
            }

            // finalize
            ConstRLEPayload::Segment const& pastLastSeg = rlePayload->getSegment(endSegIndx+1);
            assert(pastLastSeg._pPosition >= off);
            _nextPosition = size_t(pastLastSeg._pPosition) > (off+nElems) ? nElems : pastLastSeg._pPosition-off;
            assert(_data.size() <= _nextPosition);
            finalizeInternal(true);

            _lastDistinctVal = _data.size()-1;
            _currSegIndex = 0;

            assert(checkConsistency());

            assert(logEncodingContents());
        }

        /**
         * @note For internal use only
         * Fully initialize from the parameters
         * @param segments
         * @param data
         * @param mutate if true parameters can be modified
         * @pre encoding is not finalized
         * @post encoding is finalized
         */
        void initialize(std::vector<rle::Segment>& segments,
                        std::vector<Type>& data,
                        bool mutate=true)
        {
            assert(!isFinalized()); //exception ?
            assert(segments.size() > 1); //exception ?
            if (mutate) {
                _segments.swap(segments);
                _data.swap(data);
            } else {
                _segments = segments;
                _data = data;
            }

            std::vector<rle::Segment>::const_iterator lastSeg = _segments.end()-2;
            _nextPosition = (lastSeg+1)->getStartPosition();
            _lastDistinctVal = _data.size()-1;
            _currSegIndex = 0;

            assert(checkConsistency());
        }

        /**
         *   Append a value to the end.
         * @param value to append
         * @pre encoding is not finalized
         */
        void push_back ( const Type& value )
        {
            // The encoding is a bit tricky in that it needs an additional (final) segment.
            assert(!isFinalized()); //exception ?

            if (!_segments.empty() &&
                getLastSegment()->isNull()) {
                //   Last segment is a 'null' segment so
                //   start a new 'value' segment
                append_data ( value );
                _lastDistinctVal = _data.size()-1; // overflow?
                rle::Segment newSeg( _nextPosition, _lastDistinctVal, 0, 0 );
                append_segment( newSeg );

            } else if ( -1 == _lastDistinctVal ) {
                assert(_segments.empty());

                //  Adding the initial element to the encode.
                _data.push_back ( value );
                const size_t payload_offset = _data.size()-1;
                rle::Segment S( _nextPosition, payload_offset, 0, 0 );
                append_segment ( S );
                _lastDistinctVal = payload_offset;

            } else {

                assert(!_segments.empty());
                //
                //  Not the initial append. So grab the 'last' segment,
                // and the last distinct value.
                rle::Segment* S = getLastSegment();
                assert(S);
                const Type& lastval = read_data(_lastDistinctVal);

                if (lastval == value)
                {
                    //
                    //  Appending a repeated value.
                    if ( true == S->isLiteral() )
                    {
                        //
                        //  Appending a repeat to a literal. Find out how many
                        // repeats there have been. If the number of repeats
                        // is less than the threshhold at which we convert to
                        // a segment, then convert the last few repeats into
                        // a segment.
                        size_t numberofrepeats = lastLiteralRunlength();
                        if ( _maxRunlen <= numberofrepeats )
                        {
                            //
                            //  So. At this point we have determined that we
                            // have a literal containing a series of the
                            // same values, where the length of the series
                            // is such that compacting it into a run
                            // segment saves space.
                            //
                            //   1. Trim the redundant copies of the same
                            //      value out.
                            assert(_data.size()>=_maxRunlen );
                            _data.resize(_data.size()-(_maxRunlen - 1 ));

                            //
                            //   2. There are two possibilities. Either the
                            //      literal contained *only* the series of
                            //      repeated values, or else the series is
                            //      at the *end* of a literal. To figure this
                            //      out, we look at the length of the
                            //      literal that contains the repeat.
                            size_t literalLen = _nextPosition - S->getStartPosition();

                            //
                            //      If the length of the literal is greater
                            //      than the _maxRunlen, then we need to
                            //      create a new segment for the new run.
                            //      Otherwise, we just flip the previous
                            //      Segment into a run, from a literal.
                            if ( literalLen > _maxRunlen )
                            {
                                //
                                //  We have adjusted the data back to remove
                                //  any redundant elements.
                                size_t new_segment_start = _nextPosition - _maxRunlen;
                                size_t new_segment_index = _data.size() - 1;
                                rle::Segment Srun( new_segment_start,
                                                   new_segment_index,
                                                   1, 0 );
                                append_segment ( Srun );
                            } else {
                                //
                                //  So the entire 'literal' so far consists of
                                // a run of the same values. So we don't need a
                                // new segment. Just convert the previous literal
                                // segment to a run.
                                S->setRun(true);
                            }
                        } else {
                            append_data ( value );
                        }
                    }
                } else {
                    //
                    //   Appending a different value.
                    append_data ( value );
                    _lastDistinctVal = _data.size()-1; // overflow?

                    //
                    //   If we previously had a run (not a literal) then
                    //  we need to add a new segment to hold the new
                    //  literal.
                    if (false == S->isLiteral())
                    {
                        //
                        //   Creating a new literal, as we're at the end
                        //  of the previous segment.
                        rle::Segment newSeg( _nextPosition, _lastDistinctVal, 0, 0 );
                        append_segment( newSeg );
                    }
                }
            }
            ++_nextPosition;
        }


        /**
         *   Append a null to the end.
         * @param missingCode of the NULL to append
         * @pre encoding is not finalized
         */
        void push_back_null (const int32_t missingCode)
        {
            assert(!isFinalized()); //exception ?
            assert(missingCode>=0);

            rle::Segment* lastSeg(NULL);
            if ( _segments.empty() ||
                 !(lastSeg = getLastSegment())->isNull() ||
                 lastSeg->getMissingCode() != missingCode ) {
                //   Last segment is a 'value' segment so
                //   start a new 'null' segment
                rle::Segment newSeg( _nextPosition, missingCode, 1, 1 );
                append_segment( newSeg );

            }
            ++_nextPosition;
        }

        /**
         *  Because the RLE encoding needs an additional segment, we need to
         * call finalize() to write the _nextPosition (which is the number at
         * the end) to the
         * @pre encoding is not finalized
         */
        void finalize()
        {
            finalizeInternal();
        }

        /**
         *  This is the basic access method for the RLE encoding.
         *
         *  NOTE: There's an obviously better way to do this, given that
         *        almost all of the searches through the encoding will
         *        be linear. Track the previous index, and the previous
         *        Segment, and only launch into a binary search when
         *        ( a ) the next index != previous index + 1, and
         *        ( b ) the next index isn't on 'this' segment.
         */
        const Type& at( size_t where, int32_t& missing ) const
        {
            assert(isFinalized()); //exception ?
            assert(size()>0);
            assert(where < size());

            size_t mid = findSegmentIndex(where);
            assert(mid < (_segments.size()-1));

            const rle::Segment& Smid = read_segment ( mid );

            if (Smid.isNull()) {
                assert(Smid.isRun());
                missing = static_cast<int32_t>(Smid.getDataIndex());
                return _dummy;
            }

            size_t dataIndex = Smid.getDataIndex();
            if ( Smid.isLiteral() )
            {
                dataIndex += where - Smid.getStartPosition();
            }
            const Type& value = read_data ( dataIndex );
            return value;
        }

        /**
         *  Erase the contents of the encoding.
         *  TODO: Centralize all of the initialization into an
         *        init() function. These changes are all copied
         *        from the constructor.
         */
        void clear()
        {
            _data.clear();
            _segments.clear();
            _lastDistinctVal = -1;
            _nextPosition = 0;
            _currSegIndex = 0;
        }

        /**
         *  Insert the string description of encoded data into a stream
         */
        std::ostream& insert ( std::ostream& out ) const
        {
            out << "["<<pthread_self() <<"] RLEEncoding" << this << " :" << std::endl;
            out << "\tdata ( max_size = " << this->_data.max_size()
                << ", size = " << this->_data.size()
                << " and _nextPosition = " << this->_nextPosition << " ) "
                << std::endl;
            out << "\tencoding ( max_size = " << this->_segments.size()
                << ", segment cnt = " << this->_segments.size() << " ) " << std::endl;
            out << "\tSegments:" << std::endl;

            for (size_t i = 0, len = this->_segments.size(); i < len;i++) {
                const rle::Segment& S = this->read_segment(i);
                out << "[ " << i << " ] => ";
                out << S << std::endl;

                size_t seglength = this->segmentRunlength (i);
                if ( !S.isLiteral() )
                {
                    out << "is run of length = " << seglength << std::endl;
                    if (S.isNull()) {
                        const Type& t = static_cast<Type>(S.getDataIndex());
                        out << " < " << seglength << " x (null) " << t <<  std::endl;
                    } else {
                        const Type& t = this->read_data(S.getDataIndex());
                        out << " < " << seglength << " x " << t <<  std::endl;
                    }
                } else {
                    assert(!S.isNull());
                    out << "is literal of length " << seglength <<  std::endl << "{";
                    for ( size_t j = 0; j < seglength; j++ ) {

                        size_t dataindex = S.getDataIndex() + j;
                        const Type& t = this->read_data(dataindex);
                        if (0 < j )
                        {
                            out << ", ";
                        }
                        out << "< " << dataindex << ", " << t << " >";
                    }
                    out << " }" << std::endl;
                }
            }
            return out;
        }

        typedef RLEEncoding<Type> EncodingType;

        /// STL-style?  forward constant iterator
        class const_iterator
        {
        private:
            const std::vector<Type>* _data;
            std::vector<rle::Segment>::const_iterator _currSeg;
            size_t _currPos;

        public:

        const_iterator(const EncodingType& e, bool start=true)
        : _data(&e._data),
            _currSeg(start ? e._segments.begin() : e._segments.end()-1),
            _currPos(_currSeg->getStartPosition())
            {
                assert(_data);
                assert(!e._segments.empty());
                assert(!start || _currPos == 0);
            }
        const_iterator()
        : _data(NULL), _currPos(0)
            {
            }
            const_iterator(const const_iterator& other)
            {
                _data    = other._data;
                _currSeg = other._currSeg;
                _currPos = other._currPos;
            }
            const_iterator& operator= (const const_iterator& other)
            {
            _data    = other._data;
            _currSeg = other._currSeg;
            _currPos = other._currPos;
            return *this;
            }
            bool isNull ()
            {
                return _currSeg->isNull();
            }
            int32_t getMissingCode()
            {
                assert (_currSeg->isNull());
                return _currSeg->getDataIndex();
            }
            void operator++ ()
            {
                ++_currPos;
                if ( (_currSeg+1)->getStartPosition() <= _currPos) {
                    ++_currSeg;
                }
            }
            bool operator== (const const_iterator& other)
            {
            return _currPos == other._currPos &&
            _currSeg == other._currSeg &&
            _data == other._data;
            }
            bool operator!= (const const_iterator& other)
            {
                return !(*this == other);
            }
            operator const Type* ()
            {
                assert (!_currSeg->isNull());
                size_t off(0);
                if (_currSeg->isLiteral()) {
                    assert(_currPos >= _currSeg->getStartPosition());
                    off = (_currPos - _currSeg->getStartPosition());
                }
                assert(_data);
                assert( (_currSeg->getDataIndex() + off) < (*_data).size() );
                const Type& res = (*_data)[ _currSeg->getDataIndex() + off ];
                return &res;
            }
        };
        friend class const_iterator;

        /// Get a constant value iterator pointing to the first element
        const_iterator begin() const
        {
            assert(isFinalized()); //exception ?
            return const_iterator(*this);
        }
        /// Get a constant value iterator pointing past the last element
        const_iterator end() const
        {
            assert(isFinalized()); //exception ?
            return const_iterator(*this, false);
        }

    private:

        /**
         *  Because the RLE encoding needs an additional segment, we need to
         * call finalizeInternal() to write the _nextPosition (which is the number at
         * the end) as the last segment after every push_back()
         */
        void finalizeInternal(bool preallocated=false)
        {
            assert(!isFinalized()); //exception ?
            if (!preallocated) {
                rle::Segment S( _nextPosition, _data.size() + 1, 0, 0 );
                append_segment ( S );
            } else {
                rle::Segment& finalSeg = _segments[_segments.size()-1];

                assert(finalSeg.getStartPosition() == 0);
                finalSeg.setStartPosition(_nextPosition);

                assert(finalSeg.getDataIndex() == 0);
                finalSeg.setDataIndex(_data.size() + 1);

                assert(!finalSeg.isRun());
                assert(!finalSeg.isNull());
            }
        }

        void computePayloadDataBoundaries(const ConstRLEPayload* rlePayload, const position_t off, const size_t nElems,
                                          ConstRLEPayload::Segment const* startValueSeg,
                                          const size_t startValueIndexShift,
                                          const size_t startSegIndx, const size_t startValueSegIndx,
                                          const size_t endSegIndx,   const size_t endValueSegIndx,
                                          char*& startData,
                                          char*& endData)
        {
            assert(rlePayload);
            assert(off>=0);
            assert(nElems>0);
            assert(startValueSeg);

            // In the presence of NULL segments the start & end segments pointing into the data buffer
            // may be between the first & last segments we are extracting.
            // So, the logic below figures out where the value data (we need) starts and ends
            const size_t elemSize = rlePayload->elementSize();
            startData = rlePayload->getRawValue(startValueSeg->_valueIndex);
            startData += startValueIndexShift*elemSize;
            assert(startData);

            assert(endValueSegIndx<=endSegIndx);
            assert(startValueSegIndx>=startSegIndx);
            assert(endValueSegIndx>=startSegIndx);
            assert(endValueSegIndx>=startValueSegIndx);

            ConstRLEPayload::Segment const& endValSeg = rlePayload->getSegment(endValueSegIndx);
            endData = rlePayload->getRawValue(endValSeg._valueIndex);
            if (endValSeg._same!=0) { //run of same values
                endData += elemSize;
            } else { // literal of different values
                ConstRLEPayload::Segment const& pastEndValSeg = rlePayload->getSegment(endValueSegIndx+1);
                assert(pastEndValSeg ._pPosition > endValSeg._pPosition);
                size_t endValueIndexShift = 0;
                // adjust if we dont need the entire literal
                if (size_t(pastEndValSeg._pPosition) > (off+nElems)) {
                    assert(endSegIndx == endValueSegIndx);
                    endValueIndexShift = pastEndValSeg._pPosition - (off+nElems);
                }
                // advance past the end of literal minus what we dont want
                endData += elemSize*(pastEndValSeg._pPosition - endValSeg._pPosition - endValueIndexShift);
            }

            assert(endData!=NULL);
            assert(startData!=NULL);
            assert(endData > startData);
            assert(size_t(endData-startData) <= rlePayload->payloadSize());
            assert(startData >= rlePayload->getRawValue(startValueSeg->_valueIndex));
            assert(endData   >  rlePayload->getRawValue(rlePayload->getSegment(endValueSegIndx)._valueIndex));
            assert((endData - startData) % elemSize == 0);
        }

        bool checkConsistency()
        { //  sanity checks in Debug only
            const size_t nVals = _data.size();
            size_t lastStart = 0;
            uint32_t lastDataIndex = 0;
            bool foundFirstValueSeg = false;

            assert(_segments[0].getStartPosition() == 0);
            assert(_segments[0].getDataIndex() == 0 || _segments[0].isNull());

            for (size_t i=1, n=_segments.size()-1; i <n; ++i) {
                rle::Segment& nextSeg = _segments[i];
                if (!nextSeg.isNull()) {
                    assert(nextSeg.getStartPosition() > lastStart);
                    assert(nextSeg.getDataIndex() > lastDataIndex ||
                           (lastDataIndex==0 && nextSeg.getDataIndex()==0 && _segments[0].isNull() && !foundFirstValueSeg) );
                    assert(nextSeg.getDataIndex() < nVals);
                    lastDataIndex = nextSeg.getDataIndex();
                    foundFirstValueSeg = true;
                }
                lastStart = nextSeg.getStartPosition();
            }

            assert(_segments.size()>1);
            std::vector<rle::Segment>::const_iterator lastSeg = _segments.end()-2;

            assert( _nextPosition > lastSeg->getStartPosition());
            assert(lastSeg->getDataIndex() < _data.size() || lastSeg->isNull());

            if (!lastSeg->isNull()) {
                if ( lastSeg->isRun()) {
                    assert(lastSeg->getDataIndex() == _lastDistinctVal);
                } else {
                    assert(_lastDistinctVal>=0);
                    assert((lastSeg->getDataIndex() +
                            (_nextPosition - lastSeg->getStartPosition()-1)) == size_t(_lastDistinctVal));
                }
            }
            return true;
        }

        bool logEncodingContents()
        {
            if (!_logger->isTraceEnabled()) {
                return true;
            }
            const char *func = "RLEEncoding<Type>::logEncodingContents";
            for (EncodingType::const_iterator iter = begin(); iter != end(); ++iter) {

                if (iter.isNull()) {
                    LOG4CXX_TRACE(_logger, func << " this =  "<< this
                                  << " isNull = true"
                                  << " missing = " << iter.getMissingCode());
                } else {
                    LOG4CXX_TRACE(_logger, func << " this =  "<< this
                                  << " isNull = false"
                                  << " val = " << *iter);
                }
            }
            return true;
        }

        void initializeInternalData(const char* startData, const char* endData, size_t elemSize);

        /**
         * @return true if the encoding has been finalized
         */
        bool isFinalized() const
        {
            return (!_segments.empty() && getLastStartPosition() == _nextPosition);
        }

        /**
         *  The length of the literal run is the distance between the last
         *  differentvalue and the _data size
         */
        size_t lastLiteralRunlength() const
        {
            assert(! const_cast<EncodingType*>(this)->getLastSegment()->isNull());
            size_t result = 0;
            if ( -1 != _lastDistinctVal ) {
                result = _data.size() - _lastDistinctVal;
            }
            return result;
        }

        /**
         *   Compute the length of the current run. To do this, we need to look
         *  at the difference between the segment at index, and either the
         *  segment at index + 1 (if the index < number of segments) or the
         *  _nextPosition (if the encoding is under construction).
         */
        size_t segmentRunlength( const size_t index ) const
        {
            size_t result = 0;
            assert( index < _segments.size());
            const rle::Segment& seg = read_segment ( index );
            if ( index + 1 < _segments.size())
            {
                const rle::Segment& nextseg = read_segment ( index + 1 );
                assert ( nextseg.getStartPosition() >= seg.getStartPosition() );
                result = nextseg.getStartPosition() - seg.getStartPosition();
            } else {
                result = _nextPosition - seg.getStartPosition();
            }
            return result;
        }

        /**
         * When adding a value, we want the
         * RLE encoding to be able to combine
         * runs of the same value. This ptr
         * refers to the last distinct value.
         */
        int64_t  _lastDistinctVal;

        /**
         * The number of repeated values in
         * a literal at which it saves space
         * to convert to a run.
         */
        const uint32_t _maxRunlen;

        /**
         * Current physical offset into the
         * series of values being encoded.
         */
        uint64_t _nextPosition;

        /// distinct data values
        typedef std::vector<Type> DataType;
        DataType _data;

        /// RLE segments describing the data
        std::vector<rle::Segment> _segments;

        /**
         * The index of a segment from where the data
         * was returned (by at()) last.
         * The idea is that if
         * the index at which you're probing
         * the tile is 'close' to the current,
         * then you can search locally, rather
         * than globally, for the value you want.
         * @note we should probably get rid of this field because the const_iterator can be used
         */
        uint64_t _currSegIndex;

        static log4cxx::LoggerPtr _logger;
        static const Type _dummy;

        /**
         *   (Type *)value points to a typed data value. Append it (copy it) to
         *  the data block. Returns index to data.
         */
        void append_data ( const Type& value )
        {
            _data.push_back ( value );
        }

        /// @return a reference to the data in the block at the specified index.
        const Type& read_data ( size_t index ) const
        {
            assert(index < _data.size());
            const Type& data = _data[ index ];
            return data;
        }

        void append_segment ( const rle::Segment&  segment )
        {
            _segments.push_back ( segment );
        }

        /// @return a reference to a Segment at a specific index.
        const rle::Segment& read_segment ( size_t index ) const
        {
            assert(index < _segments.size());
            return _segments[ index ];
        }


        /**
         * Comparison operator of a segment relative to a position
         */
        struct SegmentLessCmp
        {
            bool operator()(const rle::Segment& seg, size_t where)
            {
                return seg.getStartPosition() < where;
            }
        };

        size_t findSegmentIndex(size_t where ) const
        {
            EncodingType *self = const_cast<EncodingType*>(this);
            assert ( where < this->size() );
            { // fast path
                self->_currSegIndex = _currSegIndex % (_segments.size()-1);
                const rle::Segment& segL = read_segment ( _currSegIndex );
                const rle::Segment& segR = read_segment ( _currSegIndex+1 );

                if (segL.getStartPosition() < where && segR.getStartPosition() > where) {
                    return _currSegIndex;
                }
                if (segL.getStartPosition() == where) {
                    return _currSegIndex;
                }
                if (segR.getStartPosition() == where) {
                    ++self->_currSegIndex;
                    return _currSegIndex;
                }
            }

            SegmentLessCmp comparator;
            std::vector<rle::Segment>::const_iterator iter = std::lower_bound(_segments.begin(),
                                                                              _segments.end(),
                                                                              where, comparator);
            if (iter==_segments.end()) {
                assert(false);
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNREACHABLE_CODE)
                << "segment not found: index too large";
            }

            if (iter==_segments.begin()) {
                if (iter->getStartPosition()!=where) {
                    assert(false);
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNREACHABLE_CODE)
                    <<"segment not found: index too small";
                }
            } else if (iter->getStartPosition()>where) {
                --iter;
                assert(iter->getStartPosition()<where); //exception ?
            } else {
                assert(iter->getStartPosition()==where);
            }
            size_t mid = std::distance(_segments.begin(), iter);

            self->_currSegIndex = mid;
            return _currSegIndex;
        }

    private:

        rle::Segment* getSegment ( size_t index )
        {
            assert(index < _segments.size());
            return &_segments[ index ];
        }

        /// @return a reference to the last segment in the encoding data block
        rle::Segment* getLastSegment ()
        {
            //
            //  Casting away const-ness here because ( a ) didn't want to
            // repeat the entire _segments.read() implementation, but
            // ( b ) from time to time, we will need to adjust the contents
            // of the last segment.
            assert(_segments.size() > 0);
            return getSegment ( _segments.size() - 1);
        }

        ///  Find the start position of the segment that is the current append target.
        size_t getLastStartPosition () const
        {
            EncodingType *self = const_cast<EncodingType*>(this);
            const rle::Segment* S = self->getLastSegment();
            assert(S);
            return S->getStartPosition();
        }
    };
    template<typename Type>
    log4cxx::LoggerPtr RLEEncoding<Type>::_logger = log4cxx::Logger::getLogger("scidb.array.tile");
    template<typename Type>
    const Type RLEEncoding<Type>::_dummy=Type();
    template<>
    void RLEEncoding<scidb::Value>::initializeInternalData(const char* startData, const char* endData, size_t elemSize);
    template<typename Type>
    void RLEEncoding<Type>::initializeInternalData(const char* startData, const char* endData, size_t elemSize)
    {
        for (typename DataType::iterator iter = _data.begin();
             startData != endData;
             ++iter, startData +=elemSize) {
            assert(iter != _data.end());
            Type& el = *iter;
            assert(startData <= (endData-elemSize));
            el = *reinterpret_cast<const Type*>(startData);
        }
    }

    /**
     * Templatized tile class.
     * Tile is a fragment of a SciDB array chunk serialized in a particular order.
     * The only serialization order supported so far is row-major order.
     * Conceptually, Tile contains a (space-efficient) list of data values ordered
     * by their array coordinates (again in row-major order).
     * The coordinates are not stored together with the values,
     * if needed they can be obtained in a separate Tile
     * @see scidb::ConstChunkIterator::getData()
     * Tiles allows for different data represenations such as RLE and
     * can be "plugged in" using the Encoding template parameter
     * @param Type data value type
     * @param Encoding a particular representaion of data
     * @note  Tile can be manipulated using the BaseTile virtual interface
     * or the interface specific to Type & Encoding.
     */
    template < typename Type,
    template<typename T> class Encoding >
    class Tile : public BaseTile {

    private:
        typedef Type ElemType;
        Encoding<ElemType> _encoding;

    public:
        /// Constructor
    Tile(const scidb::TypeId typeId,
         const BaseEncoding::EncodingID encodingId,
         const BaseTile::Context* ctx)
    : _encoding(typeId)
        {
            assert(_encoding.getEncodingID() == encodingId);
        }

        /// Destructor
        virtual ~Tile()
        {
        }

        /// @see BaseTile
        BaseEncoding* getEncoding()
        {
            return &_encoding;
        }

        /// @note for internal use only
        //XXX HACK to enable vectorized execution
        Encoding<ElemType>* getEncoding(ElemType*)
        {
            return &_encoding;
        }
        /// @see BaseTile
        size_t size() const
        {
            return _encoding.size();
        }
        /// @see BaseTile
        bool empty() const
        {
            return _encoding.empty();
        }
        /// @see BaseTile
        size_t typeSize() const
        {
            return _encoding.typeSize();
        }
        /// @see BaseTile
        void reserve(size_t n)
        {
            _encoding.reserve(n);
        }
        /// @see BaseTile
        void initialize()
        {
            _encoding.initialize(NULL);
        }
        /// @see BaseTile
        void finalize()
        {
            _encoding.finalize();
        }
        /// @see BaseTile
        void clear()
        {
            _encoding.clear();
        }

        /**
         * Push back a null value
         * @param missingCode of the null value
         */
        void push_back_null (const int32_t missingCode)
        {
            assert(missingCode>=0);
            _encoding.push_back_null(missingCode);
        }

        /**
         * Push back a non-null value
         * @param val value
         */
        void push_back ( const Type& val )
        {
            _encoding.push_back(val);
        }

        /// @see BaseTile
        void push_back ( const Value& val )
        {
            if (val.isNull()) {
                _encoding.push_back_null(val.getMissingReason());
            } else {
                _encoding.push_back(val.get<Type>());
            }
        }

        /// @see BaseTile
        void at( size_t index, Value& val ) const
        {
            int32_t missing(-1);
            const ElemType& e = _encoding.at(index, missing);
            if (missing>=0) {
                val.setNull(missing);
            } else {
                val.set<Type>(e);
            }
        }

        /**
         * Get value at a given index
         * @param index of the value inside tile
         * @param missing [OUT] missing code if >=0
         * @param val [OUT] value, only valid if missing<0
         */
        void at( size_t index, Type& val, int32_t& missing ) const
        {
            const ElemType& e = _encoding.at(index, missing);
            if (missing < 0) {
                val = e;
            }
        }

        /// iostream output operator
        friend std::ostream& operator<< ( std::ostream& out,
                                          const Tile<Type, Encoding>& tile )
        {
            out << "["<<pthread_self() <<"] Tile<"<< tile._encoding.getTypeID() << ", "
                << tile._encoding.getEncodingID()<<">: [ " << std::endl;
            tile._encoding.insert( out );
            out << " ]";
            return out;
        }
    };

    /**
     * Tile specialization for scidb::Coordinates.
     * The main point of this specialization is to keep the coordinates in a form of logical chunk positions and
     * and to use the information about the chunk to transform the positions into coordinates (and back).
     */
    class CoordinatesMapperProvider : public BaseTile::Context
    {
    public:
        virtual operator const CoordinatesMapper* () const = 0;
    };

    template < template<typename T> class Encoding >
    class Tile< scidb::Coordinates, Encoding >: public BaseTile {

    private:
        /// Encoding of logical positions
        Encoding<position_t> _encoding;
        /// Mapper chunk positions <=> array coordinates
        CoordinatesMapper _coordMapper;

    public:

        /// Constructor
    Tile(const scidb::TypeId typeId,
         const BaseEncoding::EncodingID encodingId,
         const BaseTile::Context* ctx)
    : _encoding(TID_INT64), _coordMapper(Coordinates(1), Coordinates(1))
        {
            assert(ctx);
            assert(_encoding.getEncodingID() == encodingId);

            const CoordinatesMapperProvider *provider = safe_dynamic_cast<const CoordinatesMapperProvider*>(ctx);
            const CoordinatesMapper *mapper = (*provider);
            _coordMapper = *mapper;
        }

        /// Destructor
        virtual ~Tile() {}

        /// @see BaseTile
        BaseEncoding* getEncoding()
        {
            return &_encoding;
        }

        /// @see BaseTile
        size_t size() const
        {
            return _encoding.size();
        }

        /// @see BaseTile
        bool empty() const
        {
            return _encoding.empty();
        }

        /// @see BaseTile
        void reserve(size_t n)
        {
            _encoding.reserve(n);
        }

        /// @see BaseTile
        size_t typeSize() const
        {
            return _encoding.typeSize();
        }

        /// @see BaseTile
        void initialize()
        {
            _encoding.initialize(NULL);
        }

        /// @see BaseTile
        void finalize()
        {
            _encoding.finalize();
        }

        /// @see BaseTile
        void clear()
        {
            _encoding.clear();
        }

        /**
         * Push back a position as a set of coordinates
         * @param coords coordinates
         */
        void push_back ( CoordinateCRange coords )
        {
            assert(!coords.empty());

            position_t pos = _coordMapper.coord2pos(coords);
            assert(pos>=0);
            _encoding.push_back(pos);
        }

        /**
         * Push back a position as a logical position within a chunk serialized in row-major order
         * @param pos position
         */
        void push_back ( position_t pos )
        {
            assert(pos>=0);
            _encoding.push_back(pos);
        }

        /// @see BaseTile
        void push_back ( const Value& val )
        {
            assert(!val.isNull()); //XXX exception ?
            position_t pos = val.get<position_t>();
            assert(pos>=0);
            push_back(pos);
        }

        void at( size_t index, position_t& val ) const
        {
            val = _encoding.at(index);
            assert(val >= 0);
        }

        /// @see BaseTile
        void at( size_t index, Value& val ) const
        {
            const position_t& pos = _encoding.at(index);
            assert(pos>=0);
            val.set<position_t>(pos);
        }

        /**
         * Get a position value at a given index in a form of Coordinates
         * @param index of the value inside tile
         * @param val [OUT] coordinates
         */
        void at( size_t index, scidb::Coordinates& val ) const
        {
            const position_t& pos = _encoding.at(index);
            assert(pos>=0);
            _coordMapper.pos2coord(pos, val);
            assert(val.size() == _coordMapper.getNumDims());
        }

        /**
         * Get a position value at a given index in a form of Coordinates
         * @param index of the value inside tile
         * @param val [OUT] coordinates
         */
        void at( size_t index, CoordinateRange val ) const
        {
            assert(val.size() == _coordMapper.getNumDims());
            position_t pos = _encoding.at(index);
            assert(pos>=0);
            _coordMapper.pos2coord(pos, val);
        }

        /// iostream output operator
        friend std::ostream& operator<< ( std::ostream& out,
                                          const Tile<scidb::Coordinates, Encoding>& tile )
        {
            out << "Tile <"<< tile._encoding.getTypeID() << ", "
                << tile._encoding.getEncodingID()<<">: [ " ;

            for (size_t i=0, n=tile.size() ; i < n; ++i) {
                scidb::Coordinates coords;
                tile.at(i, coords);
                assert(!coords.empty());

                out << " [" << coords << "]" ;
            }
            out << " ]";
            return out;
        }
    };

    /**
     * Tile specialization for scidb::Value.
     * This tile can contain any type including UDT (without knowing the type of course)
     * It is used to contain any variable (or fixed) size type at the expense
     * of not knowing much about the concrete type.
     */
    template < template<typename T> class Encoding >
    class Tile< scidb::Value, Encoding >: public BaseTile {

    private:
        typedef Encoding<scidb::Value> EncodingType;
        EncodingType _encoding;

    public:

        /// Constructor
    Tile(const scidb::TypeId typeId,
         const BaseEncoding::EncodingID encodingId,
         const BaseTile::Context* ctx)
    : _encoding(typeId)
        {
            assert(_encoding.getEncodingID() == encodingId);
        }

        /// Destructor
        virtual ~Tile() {}

        /// @see BaseTile
        BaseEncoding* getEncoding()
        {
            return &_encoding;
        }

        /// @see BaseTile
        size_t size() const
        {
            return _encoding.size();
        }

        /// @see BaseTile
        bool empty() const
        {
            return _encoding.empty();
        }

        /// @see BaseTile
        void reserve(size_t n)
        {
            _encoding.reserve(n);
        }

        /// @see BaseTile
        size_t typeSize() const
        {
            return _encoding.typeSize();
        }

        /// @see BaseTile
        void initialize()
        {
            _encoding.initialize(NULL);
        }

        /// @see BaseTile
        void finalize()
        {
            _encoding.finalize();
        }

        /// @see BaseTile
        void clear()
        {
            _encoding.clear();
        }

        /// @see BaseTile
        void push_back ( const Value& val )
        {
            if (val.isNull()) {
                assert(val.getMissingReason() >= 0);
                _encoding.push_back_null(val.getMissingReason());
            } else {
                _encoding.push_back(val);
            }
        }

        /// @see BaseTile
        void at( size_t index, Value& val ) const
        {
            int32_t missing(-1);
            const Value& elem =_encoding.at(index,missing);
            if (missing >= 0) {
                val.setNull(missing);
            } else {
                val = elem;
            }
        }
    };

    /**
     * Template class used by the tile factory to construct
     * a specific tile.
     * @param T data type
     * @param E encoding type
     */
    template<typename T, template<typename T> class E>
    class TileBuilder
    {
    public:

        /**
         * @param typeId data type ID
         * @param encodingId encoding type ID
         * @param ctx the context required to construct the tile (or NULL)
         * @return a heap-allocated Tile<T,E> based on typeId & encodingId
         */
        boost::shared_ptr<BaseTile> operator() (const scidb::TypeId typeId,
                                                const BaseEncoding::EncodingID encodingId,
                                                const BaseTile::Context* ctx)
        {
            return boost::make_shared< Tile<T, E> > (typeId, encodingId, ctx);
        }
    };

    /// Helper method to register built-in tiles
    template<typename T, template<typename T> class E>
    void TileFactory::registerBuiltin(const scidb::TypeId typeId,
                                      const BaseEncoding::EncodingID encodingId)
    {
        TileBuilder < T, E > tb;
        TileConstructor constructor = boost::bind<boost::shared_ptr<BaseTile> >(tb, _1, _2, _3);
        registerConstructor(typeId, encodingId, constructor);
    }

} // scidb namespace
#endif //__TILE__
