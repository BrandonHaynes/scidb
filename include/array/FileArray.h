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
 * @file FileArray.h
 *
 * @brief In-Memory (temporary) array implementation
 */

#ifndef FILE_ARRAY_H_
#define FILE_ARRAY_H_

#include <map>
#include <assert.h>

#include <array/MemArray.h>
#include <query/Query.h>
#include <query/Statistics.h>
#include <util/FileIO.h>

using namespace std;
using namespace boost;

namespace scidb
{

class FileArray;
class FileArrayIterator;

struct ChunkHeader {
    uint64_t offset;
    size_t   size;
    bool     sparse;
    bool     rle;
};

class FileChunk : public MemChunk
{
  public:
    bool isTemporary() const;

    virtual void write(boost::shared_ptr<Query>& query);
};


/**
 * Temporary (in-memory) array implementation
 */
class FileArray : public Array
{
    friend class FileArrayIterator;
  public:
    virtual ArrayDesc const& getArrayDesc() const;

    virtual boost::shared_ptr<ArrayIterator> getIterator(AttributeID attId);
    virtual boost::shared_ptr<ConstArrayIterator> getConstIterator(AttributeID attId) const;

    FileArray(ArrayDesc const& arr, const boost::shared_ptr<Query>& query, char const* filePath = NULL);
    FileArray(boost::shared_ptr<Array> input, const boost::shared_ptr<Query>& query,
              bool vertical = true, char const* filePath = NULL);
    ~FileArray();

    void writeChunk(FileChunk* chunk);

    /**
     * @see Array::isMaterialized()
     */
    virtual bool isMaterialized() const
    {
        return true;
    }

  private:
    void init(ArrayDesc const& arr, char const* filePath);

    ArrayDesc desc;
    uint64_t fileSize;
    boost::shared_ptr<File> file;
    AttributeID emptyBitmapID;
    vector< map<Coordinates, ChunkHeader, CoordinatesLess> > chunks;
    map<Coordinates, FileChunk, CoordinatesLess> bitmapChunks;
    AttributeDesc const* bitmapAttr;
};

/**
 * Temporary (in-memory) array iterator
 */
class FileArrayIterator : public ArrayIterator
{
 private:
    map<Coordinates, ChunkHeader, CoordinatesLess>::iterator curr;
    map<Coordinates, ChunkHeader, CoordinatesLess>::iterator last;
    FileArray& array;
    Address addr;
    FileChunk dataChunk;
    FileChunk* currChunk;
    boost::shared_ptr<FileChunk> bitmapChunk;

    void setBitmapChunk();

  public:
    FileArrayIterator(FileArray& arr, AttributeID attId);
    ConstChunk const& getChunk();
    bool end();
    void operator ++();
    Coordinates const& getPosition();
    bool setPosition(Coordinates const& pos);
    void setCurrent();
    void reset();
    Chunk& newChunk(Coordinates const& pos);
    Chunk& newChunk(Coordinates const& pos, int compressionMethod);
    virtual boost::shared_ptr<Query> getQuery() { return Query::getValidQueryPtr(array._query); }
};

 boost::shared_ptr<Array> createTmpArray(ArrayDesc const& arr, boost::shared_ptr<Query>& query);

}

#endif

