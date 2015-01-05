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
#include "system/Utils.h"
#include "TransposeArray.h"

using namespace scidb;

ConstChunk const& TransposeArray::TransposeArrayIterator::getChunk()
{
    if( end() )
    {
        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_CHUNK);
    }
    if (_chunkInitialized)
    {
        return _outputChunk;
    }
    //Initialize our _outputChunk with coordinates and metadata and find the corresponding inputChunk in inputArray
    Address addr(_attributeID, (*_outputChunkPositionsIterator));
    Coordinates inPos = addr.coords;
    _transposeArray->transposeCoordinates(addr.coords, inPos);
    _inputArrayIterator->setPosition(inPos);
    SCIDB_ASSERT(_inputArrayIterator->getPosition() == inPos);
    ConstChunk const& inputChunk = _inputArrayIterator->getChunk();
    shared_ptr<ConstChunkIterator> inputChunkIterator = inputChunk.getConstIterator(ConstChunkIterator::IGNORE_EMPTY_CELLS);
    _outputChunk.initialize(_transposeArray, &_transposeArray->getArrayDesc(), addr, inputChunk.getCompressionMethod());
    _outputChunk.setRLE(true);
    _outputChunk.setSparse(inputChunk.isSparse());
    if (_attributeID != _emptyTagID)
    {
        //this ensures that the _outputChunk will have a filled-in empty bitmasksss
        addr.attId = _emptyTagID;
        _emptyTagChunk.initialize(_transposeArray, &_transposeArray->getArrayDesc(), addr, inputChunk.getCompressionMethod());
        _emptyTagChunk.setRLE(true);
        _emptyTagChunk.setSparse(inputChunk.isSparse());
        _outputChunk.setBitmapChunk(&_emptyTagChunk);
    }
    shared_ptr<Query> localQueryPtr(Query::getValidQueryPtr(_query));
    shared_ptr<ChunkIterator> outputChunkIterator = _outputChunk.getIterator(localQueryPtr, 0);
    //For each value in inputChunk, reorder its coordinates and place it into _outputChunk in the proper order
    while (!inputChunkIterator->end())
    {
        _transposeArray->transposeCoordinates(inputChunkIterator->getPosition(), inPos);
        outputChunkIterator->setPosition(inPos);
        outputChunkIterator->writeItem(inputChunkIterator->getItem());
        ++(*inputChunkIterator);
    }
    outputChunkIterator->flush();
    _chunkInitialized = true;
    return _outputChunk;
}
