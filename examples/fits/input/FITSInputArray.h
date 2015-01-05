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
#ifndef FITS_INPUT_ARRAY_H
#define FITS_INPUT_ARRAY_H

#include "query/Operator.h"
#include "array/Array.h"
#include "array/Metadata.h"
#include "array/MemArray.h"

#include "../common/FITSParser.h"


namespace scidb
{
using namespace std;

class FITSInputArrayIterator;

/* FITSInputArray */

class FITSInputArray : public Array
{
public:
    FITSInputArray(ArrayDesc const& desc, string const& filePath, uint32_t hdu, boost::shared_ptr<Query>& query);

    virtual ArrayDesc const&                        getArrayDesc() const;
    virtual boost::shared_ptr<ConstArrayIterator>   getConstIterator(AttributeID attr) const;

    /**
     * Get the least restrictive access mode that the array supports.
     * @return SINGLE_PASS
     */
    virtual Access getSupportedAccess() const
    {
        return SINGLE_PASS;
    }

    ConstChunk*                                     getChunkByIndex(size_t index, AttributeID attr);

private:
    // A window is used to keep the last 'kWindowSize' chunks in memory.
    // Its size must be at least 2 because different attribute iterators
    // may be simultaneously requesting chunk N as well as chunk (N - 1).
    static const size_t                             kWindowSize = 2;

    struct CachedChunks {
        MemChunk chunks[kWindowSize];
    };

    void                                            initValueHolders();

    bool                                            validSchema();
    bool                                            validDimensions();

    void                                            initChunkPos();
    bool                                            advanceChunkPos();

    void                                            calculateLength();
    void                                            readChunk();
    void                                            initMemChunks(boost::shared_ptr<Query>& query);
    void                                            flushMemChunks();

    void                                            readShortInts(size_t n);
    void                                            readShortIntsAndScale(size_t n);
    void                                            readInts(size_t n);
    void                                            readIntsAndScale(size_t n);
    void                                            readFloats(size_t n);

    FITSParser                                      parser;
    uint32_t                                        hdu;
    ArrayDesc                                       desc;
    Dimensions const&                               dims;
    size_t                                          nDims;
    size_t                                          nAttrs;
    vector<Value>                                   values;
    vector<CachedChunks>                            chunks;
    vector<boost::shared_ptr<ChunkIterator> >       chunkIterators;
    size_t                                          chunkIndex;
    Coordinates                                     chunkPos;
    size_t                                          nConsecutive;
    size_t                                          nOuter;
    boost::weak_ptr<Query>                          query;
};

/* FITSInputArrayIterator */

class FITSInputArrayIterator : public ConstArrayIterator
{
public:
    FITSInputArrayIterator(FITSInputArray& array, AttributeID attr);

    virtual bool                                    end();
    virtual void                                    operator ++();
    virtual Coordinates const&                      getPosition();
    virtual bool                                    setPosition(Coordinates const& pos);
    virtual void                                    reset();
    virtual ConstChunk const&                       getChunk();

private:
    FITSInputArray&                                 array;
    AttributeID                                     attr;
    ConstChunk const*                               chunk;
    size_t                                          chunkIndex;
    bool                                            chunkRead;
};

}

#endif /* FITS_INPUT_ARRAY_H */

