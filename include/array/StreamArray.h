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
#include <queue>
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

        /// Destructor
        virtual ~StreamArray() {}

        /**
         * Exception indicating that an attempt to get the next chunk should be re-tried
         * because it is not yet ready.
         * The common reasons are: the data have not arrived from remote instance(s) or
         * a SINGLE_PASS array is not being consumed horizontally
         * (when the entire "row" of attributes is consumed, the re-try should succeed)
         */
        class RetryException: public SystemException
        {
        public:
        RetryException(const char* file, const char* function, int32_t line)
        : SystemException(file, function, line, "scidb",
                          SCIDB_SE_INTERNAL, SCIDB_LE_RESOURCE_BUSY,
                          "SCIDB_SE_INTERNAL", "SCIDB_LE_RESOURCE_BUSY", uint64_t(0))
            {
                operator<<("StreamArray::RetryException");
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
    private:
        StreamArray(const StreamArray& other);
        StreamArray& operator=(const StreamArray& other);
    };

    /**
     * Stream array iterator
     * @note NOT thread-safe i.e. behavior is undefined if multiple StreamArrayIterators execute concurrently
     */
    class StreamArrayIterator : public ConstArrayIterator
    {
    private:
        StreamArray& _array;
        AttributeID _attId;
        ConstChunk const* _currentChunk;
        MemChunk _dataChunk;
        MemChunk _bitmapChunk;

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
        vector<boost::shared_ptr<ConstArrayIterator> > iterators;
    };

    /**
     * MultiStreamArray merges chunks from different streams
     * The subclasses implementing the streams via nextChunkPos() and nextChunkBody() are allowed to throw RetryException
     * indicating that nextChunk() should be called again to obtain the chunk
     */
    class MultiStreamArray : public StreamArray
    {
      public:

        MultiStreamArray(size_t nStreams,
                         size_t localStream,
                         ArrayDesc const& arr,
                         bool enforceDataIntegrity,
                         boost::shared_ptr<Query>const& query);

        virtual ~MultiStreamArray() {}

        /// @return true if a data collision/unordered data would cause an exception
        bool isEnforceDataIntegrity() { return _enforceDataIntegrity; }

        /**
         * An abstract interface for customizing the way remote partial chunks are merged together
         */
        class PartialChunkMerger
        {
        public:
            virtual ~PartialChunkMerger() {}
            /**
             * Handle a remote partial chunk
             * @param instanceId chunk's instance of origin
             * @param attId chunk's attribute ID
             * @param chunk [in/out] partial chunk to merge. If NULL upon return, the chunk is no longer "owned" by the caller
             * @param query the current query context
             * @return  chunk!=NULL
             */
            virtual bool mergePartialChunk(InstanceID instanceId,
                                           AttributeID attId,
                                           boost::shared_ptr<MemChunk>& chunk,
                                           boost::shared_ptr<Query> const& query) = 0;
            /**
             * Get a complete local chunk after merging all the partial chunks.
             * Upon return this merger must be prepared to handle a partial chunks for a new position.
             * @param attId chunk's attribute ID
             * @param query the current query context
             * @return merged chunk
             */
            virtual boost::shared_ptr<MemChunk> getMergedChunk(AttributeID attId,
                                                           boost::shared_ptr<Query> const& query) = 0;
        protected:
            PartialChunkMerger() {}
        private: // disallow
            PartialChunkMerger(const PartialChunkMerger&);
            PartialChunkMerger& operator=(const PartialChunkMerger&);
        };

        /**
         * A default partial chunk merger which adds new cell values and overwrites the existing ones.
         * It can enforce data integrity checks and error out in case cell collisions (i.e. will not overwrite existing cells).
         * @note If the data integrity checks are not enabled and the data cells collisions are possible,
         * the merged chunk data values are undefined (i.e. any colliding cell value for a given coordinate may be chosen).
         */
        class DefaultChunkMerger : public MultiStreamArray::PartialChunkMerger
        {
        private:
            const bool _isEnforceDataIntegrity;
            // the order of definitions below must be preserved (chunk first, iterator second)
            boost::shared_ptr<MemChunk> _mergedChunk;
            /// true if a data integrity issue has been found
            bool _hasDataIntegrityIssue;
            size_t _numElems;
            size_t _chunkSizeLimit;

            static bool isEmptyBitMap(const boost::shared_ptr<MemChunk>& chunk);

        public:
            /**
             * Constructor
             * @param isEnforceDataIntegrity flag for turning on/off data integrity checks
             */
            DefaultChunkMerger (bool isEnforceDataIntegrity);

            /// Destructor
            virtual ~DefaultChunkMerger () {}

            /// @see MultiStreamArray::PartialChunkMerger::mergePartialChunk
            virtual bool mergePartialChunk(size_t stream,
                                           AttributeID attId,
                                           shared_ptr<MemChunk>& partialChunk,
                                           const boost::shared_ptr<Query>& query);

            /// @see MultiStreamArray::PartialChunkMerger::getMergedChunk
            virtual boost::shared_ptr<MemChunk> getMergedChunk(AttributeID attId,
                                                               const boost::shared_ptr<Query>& query);
        };

        /**
         * Customize the partial chunk merger for a given attribute.
         * @note NOT thread-safe. So, it should be called before starting to use this array.
         * @param attId attribute ID to which apply the merger
         * @param chunkMerger [in/out] merger to set, set to NULL upon return
         */
        void setPartialChunkMerger(AttributeID attId,
                                   boost::shared_ptr<PartialChunkMerger>& chunkMerger)
        {
            assert(chunkMerger);
            assert(attId < _chunkMergers.size());
            _chunkMergers[attId].swap(chunkMerger);
            chunkMerger.reset();
        }

    protected:
        /**
         * @see StreamArray::nextChunk
         * @throws scidb::StreamArray::RetryException if the next chunk is not ready and nextChunk() needs to be called again
         * @note This method is NOT thread-safe.
         * Technically, this method should work correctly if the invocations are serialized per attribute
         * (i.e. different attributes are accessed in different threads).
         * However, MultiStream is a SINGLE_PASS array and has to be scanned horizontally
         * (all attributes at the same position before going to the next position),
         * so accessing different attributes asynchronously (say in different threads) may result in RetryException
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
        SourceAndDest(Coordinates const& coords,
                          size_t src, size_t dest)
        : _coords(coords), _src(src), _dest(dest) {}
            Coordinates const& getCoords() const { return _coords; }
            size_t getSrc() const  { return _src; }
            size_t getDest() const { return _dest; }
            bool operator< (SourceAndDest const& right) const
            {
                scidb::CoordinatesLess comp;
                return comp(right.getCoords(), _coords); // reverse order
            }
        private:
            SourceAndDest();
            // relying on the default destructor, copy constructor & assignment
            Coordinates _coords;
            size_t _src;
            size_t _dest;
        };
        typedef std::priority_queue<SourceAndDest> PositionMap;

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
        void logReadyPositions(PositionMap& readyPos,
                               const AttributeID attId);

        const size_t _nStreams;
        const size_t _localStream;
        const bool   _enforceDataIntegrity;
        std::vector<boost::shared_ptr<MemChunk> > _resultChunks;
        std::vector<boost::shared_ptr<PartialChunkMerger> > _chunkMergers;
        std::vector<PositionMap> _readyPositions;
        std::vector<std::list<size_t> > _notReadyPositions;
        std::vector<std::list<size_t> > _currPartialStreams;

        /// true if a data integrity issue has been found
        bool _hasDataIntegrityIssue;

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

    /// An Array that can enforce the horizontal consumption of attributes
    /// (i.e. all attribute chunks at the same position can be forced to be consumed before continuing to the next position)
    class SinglePassArray : public StreamArray
    {
    public:
        void setEnforceHorizontalIteration(bool on) { _enforceHorizontalIteration=on; }
        bool isEnforceHorizontalIteration() const   { return _enforceHorizontalIteration; }
    protected:
        /**
         * Get the current sequential index of the attribute "row" being consumed
         */
        virtual size_t getCurrentRowIndex() const = 0;

        /**
         * Move to the specified attribute "row", so that the chunks from that row can be consumed.
         * The subcalsses are required to successfully advance forward the row.
         * (if isEnforceHorizontalIteration()==true, only when all attributes of the current row are consumed).
         * @param rowIndex the sequential row index
         * @return false if no more rows/chunks are available (i.e. EOF); true otherwise
         */
        virtual bool moveNext(size_t rowIndex) = 0;

        /**
         * Get chunk for a given attribute from the specified row
         * @param attr attribute ID
         * @param rowIndex attribute row index
         * @return chunk ref
         */
        virtual ConstChunk const& getChunk(AttributeID attr, size_t rowIndex) = 0;

        /// Constructor
        SinglePassArray(ArrayDesc const& arr);

        /// @see Array::getConstIterator
        boost::shared_ptr<ConstArrayIterator> getConstIterator(AttributeID attId) const ;

        /**
         * @see StreamArray::nextChunk
         * @throws scidb::StreamArray::RetryException if setEnforceHorizontalIteration(true) has been called AND
         * nextChunk() needs to be called again because the next chunk(row) is not ready.
         * The next chunk should be ready when all the attribute chunks at the same position are consumed
         * (i.e. the entire attribute "row" is consumed).
         * @note This method is NOT thread-safe.
         */
        virtual ConstChunk const* nextChunk(AttributeID attId, MemChunk& chunk);

        /// @return true if the chunk is considered non-empty
        bool hasValues(const ConstChunk* chunk);
    private:
        bool _enforceHorizontalIteration;
        size_t _consumed;
        std::vector<size_t> _rowIndexPerAttribute;
    private:
        SinglePassArray();
        SinglePassArray(const SinglePassArray&);
        SinglePassArray& operator=(const SinglePassArray&);
    };

#endif //SCIDB_CLIENT

}

#endif
