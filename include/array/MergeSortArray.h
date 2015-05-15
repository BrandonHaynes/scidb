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

/*
 * MergeSortArray.h
 *
 *  Created on: Sep 23, 2010
 */
#ifndef MERGE_SORT_ARRAY_H
#define MERGE_SORT_ARRAY_H

#include <stdio.h>
#include <ctype.h>
#include <inttypes.h>
#include <limits.h>
#include <string>

#include <array/MemArray.h>
#include <array/Metadata.h>
#include <array/StreamArray.h>
#include <array/TupleArray.h>
#include <query/Operator.h>
#include <util/iqsort.h>

namespace scidb
{
    using namespace std;

    class MergeSortArray;

    const size_t CHUNK_HISTORY_SIZE = 2;

    class MergeSortArray : public SinglePassArray
    {
    protected:
        /// @see SinglePass::getCurrentRowIndex()
        virtual size_t getCurrentRowIndex() const { return currChunkIndex; }
        /// @see SinglePass::moveNext()
        virtual bool moveNext(size_t rowIndex);
        /// @see SinglePass::getChunk()
        virtual ConstChunk const& getChunk(AttributeID attr, size_t rowIndex);

    public:

        /**
         * @param query       the query context.
         * @param desc        the result schema.
         * @param inputArrays the input arrays to merge.
         * @param tcom        the tuple comparator.
         * @param offset      the offset to be added to the coordinate of every output cell.
         * @param streamSizes the number of elements from each inputArray.
         */
        MergeSortArray(const boost::shared_ptr<Query>& query,
                       ArrayDesc const& desc,
                       std::vector< boost::shared_ptr<Array> > const& inputArrays,
                       boost::shared_ptr<TupleComparator> tcomp,
                       size_t offset,
                       boost::shared_ptr<std::vector<size_t> > const& streamSizes);

    private:
        size_t currChunkIndex;
        boost::shared_ptr<TupleComparator> comparator;
        Coordinates chunkPos;
        size_t chunkSize;

        struct MergeStream {
            vector< boost::shared_ptr< ConstArrayIterator > > inputArrayIterators;
            vector< boost::shared_ptr< ConstChunkIterator > > inputChunkIterators;
            vector<Value> tuple;
            size_t size;
            bool endOfStream;
        };
        struct ArrayAttribute {
            MemChunk chunks[CHUNK_HISTORY_SIZE];
        };
        std::vector< boost::shared_ptr<Array> > input;
        vector<MergeStream>    streams;
        vector<ArrayAttribute> attributes;
        vector<int>            permutation;

        /// Make sure the MergeSortArray chunk have the empty bitmap chunk set.
        /// For all output attribute chunks in the attributes buffer, set the empty bitmap chunk (nAttrs-1)
        void setEmptyBitmap(size_t nAttrs, size_t chunkIndex);

    public:
        int binarySearch(PointerRange<const Value> tuple);

        int operator()(int i, int j)
        {
            return -comparator->compare(&streams[i].tuple.front(), &streams[j].tuple.front());
        }
    };

} //namespace scidb

#endif /* MUTLIPLY_ARRAY_H */
