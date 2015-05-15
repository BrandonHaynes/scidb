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
#include <algorithm>
#include <vector>

#include "system/Utils.h"
#include "TransposeArray.h"

using namespace scidb;

class CoordSorter {
public:
    CoordSorter(const std::vector<Coordinates>& positions) : _positions(positions) {;}
    bool operator() (size_t i, size_t j) { return _positions[i] < _positions[j]; }
private:
    const std::vector<Coordinates>& _positions;
};

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
    if (_attributeID != _emptyTagID)
    {
        //this ensures that the _outputChunk will have a filled-in empty bitmasksss
        addr.attId = _emptyTagID;
        _emptyTagChunk.initialize(_transposeArray, &_transposeArray->getArrayDesc(), addr, inputChunk.getCompressionMethod());
        _outputChunk.setBitmapChunk(&_emptyTagChunk);
    }
    shared_ptr<Query> localQueryPtr(Query::getValidQueryPtr(_query));

    //
    // std::sort() is about twice as fast as letting the ch
    //
    shared_ptr<ChunkIterator> outputChunkIterator = _outputChunk.getIterator(localQueryPtr, ConstChunkIterator::SEQUENTIAL_WRITE);

    //For each value in inputChunk, reorder its coordinates and place it into _outputChunk in the proper order
    
    // vectors, with sufficent reservation to hold a copy of the elements
    std::vector<Coordinates> positions;
    std::vector<Value> values;
    size_t nCells = inputChunk.count();
    positions.reserve(nCells);
    values.reserve(nCells);

    // get vector of positions and corresponding values
    Coordinates outPos(inPos.size());
    for(; !inputChunkIterator->end(); ++(*inputChunkIterator)) {
        _transposeArray->transposeCoordinates(inputChunkIterator->getPosition(), outPos);

        positions.push_back(outPos);  // always O(1) because of reserve()
        values.push_back(inputChunkIterator->getItem());
    }
    assert(positions.size() >= nCells); // count doesn't seem to include overlap
    nCells = positions.size();  // update for 100% [] safety in NDEBUG mode, below

    //
    // sort a vector of indices rather than the values themselves
    // (less memory bandwidth during exchanges)
    //
    std::vector<size_t> sortOrder;
    sortOrder.reserve(nCells);
    for(size_t ii=0; ii < nCells; ii++) {
        sortOrder.push_back(ii);
    }

    // sort positions
    CoordSorter coordSorter(positions);
    std::sort(sortOrder.begin(), sortOrder.end(), coordSorter);

    // and now do the output in SEQUENTIAL_WRITE order
    for(size_t ii=0; ii < nCells; ii++) {
        size_t index = sortOrder[ii];
        outputChunkIterator->setPosition(positions[index]);
        outputChunkIterator->writeItem(values[index]);
    }

    outputChunkIterator->flush();
    _chunkInitialized = true;
    return _outputChunk;
}
