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

#include "query/Operator.h"
#include "array/Metadata.h"
#include "array/MemArray.h"
#include "array/TupleArray.h"
#include "util/iqsort.h"

#include <stdio.h>
#include <ctype.h>
#include <inttypes.h>
#include <limits.h>
#include <string>

namespace scidb
{
    using namespace std;

    class MergeSortArray;
    class MergeSortArrayIterator;

    const size_t CHUNK_HISTORY_SIZE = 2;

    class MergeSortArrayIterator : public ConstArrayIterator
    {
        friend class MergeSortArray;
      public:
        virtual ConstChunk const& getChunk();
        virtual bool end();
        virtual void operator ++();
        virtual Coordinates const& getPosition();

        MergeSortArrayIterator(MergeSortArray& array, AttributeID id);

      private:
        MergeSortArray& array;
        AttributeID attr;
        bool hasCurrent;
        size_t currChunkIndex;
    };

    class MergeSortArray : public Array
    {
      public:
        virtual Access getSupportedAccess() const
        {
            return SINGLE_PASS;
        }
        virtual ArrayDesc const& getArrayDesc() const;
        virtual boost::shared_ptr<ConstArrayIterator> getConstIterator(AttributeID attr) const;

        bool moveNext(size_t chunkIndex);
        ConstChunk const& getChunk(AttributeID attr, size_t chunkIndex);

        MergeSortArray(const boost::shared_ptr<Query>& query,
                       ArrayDesc const& desc,
                       std::vector< boost::shared_ptr<Array> > const& inputArrays,
                       boost::shared_ptr<TupleComparator> tcomp, 
                       bool local = false);

      private:
        ArrayDesc desc;
        size_t currChunkIndex;
        boost::shared_ptr<TupleComparator> comparator;
        Coordinates chunkPos;
        size_t chunkSize;
        bool isLocal;

        struct MergeStream {
            vector< boost::shared_ptr< ConstArrayIterator > > inputArrayIterators;
            vector< boost::shared_ptr< ConstChunkIterator > > inputChunkIterators;
            Tuple tuple;
            size_t size;
            bool endOfStream;
        };
        struct ArrayAttribute {
            boost::shared_ptr<MergeSortArrayIterator> iterator;
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
        int binarySearch(Tuple const& tuple);

        int operator()(int i, int j) 
        { 
            return -comparator->compare(streams[i].tuple, streams[j].tuple);
        }
    };

} //namespace scidb

#endif /* MUTLIPLY_ARRAY_H */
