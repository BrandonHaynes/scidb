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
 * @file StreamArray.h
 *
 * @brief Array receiving chunks from abstract stream
 */

#ifndef STREAM_ARRAY_H_
#define STREAM_ARRAY_H_

#include <list>
#include <map>
#include <vector>

#include <array/MemChunk.h>
#include <util/JobQueue.h>
#include <util/Semaphore.h>
#include <util/ThreadPool.h>

namespace scidb
{
    class StreamArrayIterator;

    /**
     * Abstract stream array
     */
    class StreamArray: public virtual Array
    {
        friend class StreamArrayIterator;
      public:
        virtual string const& getName() const;
        virtual ArrayID getHandle() const;

        virtual ArrayDesc const& getArrayDesc() const;

        /**
         * Get the least restrictive access mode that the array supports.
         * @return SINGLE_PASS
         */
        virtual Access getSupportedAccess() const
        {
            return SINGLE_PASS;
        }

        virtual boost::shared_ptr<ArrayIterator> getIterator(AttributeID attId);
        virtual boost::shared_ptr<ConstArrayIterator> getConstIterator(AttributeID attId) const;

        /**
         * Constructor
         * @param arr array schema
         * @param emptyCheck if true, StreamArrayIterator will automatically fetch the emptyBitmap chunk
         *        and set it on other attribute chunks
         * @note XXX WARNING: if emptyCheck==true, the iteration MUST be horizontal across all attributes from 0 to n(-1)
         */
        StreamArray(ArrayDesc const& arr, bool emptyCheck = true);
        StreamArray(const StreamArray& other);
        virtual ~StreamArray() {}
      protected:
        /**
         * Implemented by subclasses for obtaining the next stream chunk
         * @param attId chunk attribute ID
         * @param chunk which can be used to store the next chunk
         * @return the next chunk
         * @note XXX WARNING: the returned chunk pointer does NOT necessarily point to the chunk supplied as the second argument,
         *       in which case the second argument chunk remains unused. Also, it is the responsibility of the subclass to make
         *       sure the returned chunk remains live until the next call to nextChunk() ... uuuuugly!!!
         */
        virtual ConstChunk const* nextChunk(AttributeID attId, MemChunk& chunk) = 0;

        ArrayDesc desc;
        bool emptyCheck;
        vector< boost::shared_ptr<ConstArrayIterator> > _iterators;
        ConstChunk const* currentBitmapChunk;
        size_t nPrefetchedChunks;
    };

    /**
     * Stream array iterator
     */
    class StreamArrayIterator : public ConstArrayIterator
    {
    private:
        StreamArray& array;
        AttributeID attId;
        ConstChunk const* currentChunk;
        MemChunk dataChunk;
        MemChunk bitmapChunk;

        void moveNext();

      public:
        StreamArrayIterator(StreamArray& arr, AttributeID attId);
        ConstChunk const& getChunk();
        bool end();
        /**
         * @note XXX WARNING Because StreamArray/ClientArray advances the emptybitmap iterator behind the scenes
         * increment of all attributes has to happen simulteneously
         */
        void operator ++();
        Coordinates const& getPosition();
    };

#ifndef SCIDB_CLIENT

    //
    // Array implementation materializing current chunk
    //
    class AccumulatorArray : public StreamArray
    {
      public:
        AccumulatorArray(boost::shared_ptr<Array> pipe,
                         boost::shared_ptr<Query>const& query);

      protected:
        virtual ConstChunk const* nextChunk(AttributeID attId, MemChunk& chunk);

      private:
        boost::shared_ptr<Array> pipe;
        vector< boost::shared_ptr<ConstArrayIterator> > iterators;
    };

    /**
     * MultiStreamArray merges chunks from different streams
     * The subclasses implementing the streams via nextChunkPos() and nextChunkBody() are allowed to throw RetryException
     * indicating that nextChunk() should be called again to obtain the chunk
     */
    class MultiStreamArray : public StreamArray
    {
      public:
        /**
         * Exception indicating that an attempt to get the next chunk should be re-tried
         * because it is not yet ready.
         */
        class RetryException: public SystemException
        {
        public:
        RetryException(const char* file, const char* function, int32_t line)
        : SystemException(file, function, line, "scidb",
                          SCIDB_SE_EXECUTION, SCIDB_LE_RESOURCE_BUSY,
                          "SCIDB_SE_EXECUTION", "SCIDB_LE_RESOURCE_BUSY", uint64_t(0))
            {
            }
            ~RetryException() throw () {}
            void raise() const { throw *this; }
            virtual Exception::Pointer copy() const
            {
                Exception::Pointer ep(boost::make_shared<RetryException>(_file.c_str(),
                                                                         _function.c_str(),
                                                                         _line));
                return ep;
            }
        };

        MultiStreamArray(size_t nStreams,
                         size_t localStream,
                         ArrayDesc const& arr,
                         boost::shared_ptr<Query>const& query);

        virtual ~MultiStreamArray() {}

    protected:
        /**
         * @see StreamArray::nextChunk
         * @throws scidb::MultiStreamArray::RetryException if the next chunk is not ready and nextChunk() needs to be called again
         */
        virtual ConstChunk const* nextChunk(AttributeID attId, MemChunk& chunk);
        virtual ConstChunk const* nextChunkBody(size_t stream, AttributeID attId, MemChunk& chunk) = 0;
        virtual bool nextChunkPos(size_t stream,
                                  AttributeID attId,
                                  Coordinates& pos,
                                  size_t& destStream) = 0 ;
        size_t getStreamCount() const { return _nStreams; }
        size_t getLocalStream() const { return _localStream; }

    private:
        /// A pair of a source stream and a destination stream.
        /// The destination stream may not match the local stream.
        /// When it does not, the source stream indicates the position of the next chunk it is sending
        /// (to the specified destination),
        /// so that this array potentially could make progress in selecting/constructing the next output chunk.
        /// Even in the absence of chunk data from the source the next output chunk can be produced if other streams
        /// provide chunks with 'lesser' coordinates.
        class SourceAndDest
        {
        public:
            SourceAndDest(size_t src, size_t dest) : _src(src), _dest(dest) {}
            size_t getSrc() const  { return _src; }
            size_t getDest() const { return _dest; }
        private:
            SourceAndDest();
            // relying on the default destructor, copy constructor & assignment
            size_t _src;
            size_t _dest;
        };
        typedef std::multimap<Coordinates,SourceAndDest,scidb::CoordinatesLess> PositionMap;

        void getAllStreamPositions(PositionMap& readyPos,
                                   std::list<size_t>& notReadyPos,
                                   const AttributeID attId);
        void mergePartialStreams(PositionMap& readyPos,
                                 std::list<size_t>& notReadyPos,
                                 std::list<size_t>& currPartialStreams,
                                 const AttributeID attId);
        void getNextStreamPositions(PositionMap& readyPos,
                                    list<size_t>& notReadyPos,
                                    list<size_t>& currPartialStreams,
                                    const AttributeID attId);

        void mergeChunks(ConstChunk const* dstChunk,
                         ConstChunk const* srcChunk,
                         boost::shared_ptr<ChunkIterator>& dstIterator);

        size_t _nStreams;
        size_t _localStream;
        std::vector<boost::shared_ptr<MemChunk> > _resultChunks;
        vector<boost::shared_ptr<ChunkIterator> > _resultChunkIterators;
        std::vector<PositionMap> _readyPositions;
        std::vector<std::list<size_t> > _notReadyPositions;
        std::vector<std::list<size_t> > _currPartialStreams;
    protected:
        /// A hint to the derived classes specifying the coordinates of the next output chunk (in progress)
        std::vector<Coordinates> _currMinPos;
    };

    /// An interface for an Array which requires a (implementation-dependent) synchronization point
    class SynchableArray : virtual public Array
    {
    public:
        virtual void sync() = 0;
        virtual ~SynchableArray() {}
    protected:
        SynchableArray() {}
    private:
        SynchableArray(const SynchableArray&);
        SynchableArray& operator=(const SynchableArray&);
    };
#endif //SCIDB_CLIENT

}

#endif
