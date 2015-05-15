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
 * @file SubArray.cpp
 *
 * @brief SubArray implementation
 *
 * @author Konstantin Knizhnik <knizhnik@garret.ru>
 * @author poliocough@gmail.com
 */
#include <log4cxx/logger.h>
#include "SubArray.h"
#include <system/Exceptions.h>

using namespace boost;

namespace scidb
{
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.array.subarray"));
//
// SubArray iterator methods
//
SubArrayIterator::SubArrayIterator(SubArray const& subarray, AttributeID attrID, bool doReset)
: DelegateArrayIterator(subarray, attrID, subarray.inputArray->getConstIterator(attrID)),
  array(subarray),
  outPos(subarray.subarrayLowPos.size()),
  inPos(outPos.size()),
  hasCurrent(false),
  outChunkPos(outPos.size())
{
    if(doReset)
    {
        reset();
    }
}

bool SubArrayIterator::end()
{
    return !hasCurrent;
}

void SubArrayIterator::fillSparseChunk(size_t i)
{
    Dimensions const& dims = array.dims;
    if (i == dims.size()) {
        if (inputIterator->setPosition(inPos)) {
            ConstChunk const& inChunk = inputIterator->getChunk();
            boost::shared_ptr<ConstChunkIterator> inIterator = inChunk.getConstIterator(ConstChunkIterator::IGNORE_OVERLAPS|
                                                                                        ConstChunkIterator::IGNORE_EMPTY_CELLS);

            while (!inIterator->end()) {
                Coordinates const& inChunkPos = inIterator->getPosition();
                array.in2out(inChunkPos, outChunkPos);
                if (outIterator->setPosition(outChunkPos)) {
                    outIterator->writeItem(inIterator->getItem());
                }
                ++(*inIterator);
            }
        }
    } else {
        fillSparseChunk(i+1);

        size_t interval = dims[i].getChunkInterval() - 1;
        inPos[i] += interval;
        fillSparseChunk(i+1);
        inPos[i] -= interval;
    }
}

ConstChunk const& SubArrayIterator::getChunk()
{
    if (!chunkInitialized) {

        chunkInitialized = true;

        ArrayDesc const& desc = array.getArrayDesc();
        Address addr(attr, outPos);
        sparseChunk.initialize(&array, &desc, addr, 0);

        int mode(0);
        AttributeDesc const* emptyAttr = desc.getEmptyBitmapAttribute();
        if (emptyAttr != NULL && emptyAttr->getId() != attr) {
            Address emptyAddr(emptyAttr->getId(), outPos);
            sparseBitmapChunk.initialize(&array, &desc, emptyAddr, 0);
            sparseChunk.setBitmapChunk(&sparseBitmapChunk);
        } 

        outIterator = sparseChunk.getIterator(Query::getValidQueryPtr(array._query), mode);
        fillSparseChunk(0);
        outIterator->flush();

        LOG4CXX_TRACE(logger, "SubArrayIterator::getChunk: "
                      <<" attr=" << attr
                      <<", outCoord=" << outPos
                      <<", chunk isEmpty="<<sparseChunk.isEmpty());
    }
    ASSERT_EXCEPTION(sparseChunk.isInitialized(), "SubArrayIterator::getChunk; ");
    return sparseChunk;
}

bool SubArrayIterator::setInputPosition(size_t i)
{
    Dimensions const& dims = array.dims;
    chunkInitialized = false;
    if (i == dims.size()) {
        return inputIterator->setPosition(inPos);
    }
    if (setInputPosition(i+1)) {
        return true;
    }
    size_t interval = dims[i].getChunkInterval() - 1;
    inPos[i] += interval;
    bool rc = setInputPosition(i+1);
    inPos[i] -= interval;
    return rc;
}

void SubArrayIterator::operator ++()
{
    const Dimensions& dims = array.dims;
    size_t nDims = dims.size();
    chunkInitialized = false;
    while (true) {
        size_t i = nDims-1;
        while ((outPos[i] += dims[i].getChunkInterval()) > dims[i].getEndMax()) {
            if (i == 0) {
                hasCurrent = false;
                return;
            }
            outPos[i--] = 0;
        }
        array.out2in(outPos, inPos);
        if (setInputPosition(0)) {
            hasCurrent = true;
            return;
        }
    }
}

Coordinates const& SubArrayIterator::getPosition()
{
    if (!hasCurrent)
    throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
    return outPos;
}

bool SubArrayIterator::setPosition(Coordinates const& pos)
{
    if( !array.getArrayDesc().contains(pos) )
    {
        return hasCurrent = false;
    }
    outPos = pos;
    array.getArrayDesc().getChunkPositionFor(outPos);
    array.out2in(outPos, inPos);
    return hasCurrent = setInputPosition(0);
}

void SubArrayIterator::reset()
{
    const Dimensions& dims = array.dims;
    size_t nDims = dims.size();
    for (size_t i = 0; i < nDims; i++) {
        outPos[i] = 0;
    }
    chunkInitialized = false;
    outPos[nDims-1] -= dims[nDims-1].getChunkInterval();
    ++(*this);
}

MappedSubArrayIterator::MappedSubArrayIterator(SubArray const& subarray, AttributeID attrID):
SubArrayIterator(subarray, attrID, false)
{
    //need to call this class's reset, not parent's.
    reset();
}

bool MappedSubArrayIterator::setPosition(Coordinates const& pos)
{
    if( !array.getArrayDesc().contains(pos) )
    {
        return hasCurrent = false;
    }

    outPos = pos;
    array.getArrayDesc().getChunkPositionFor(outPos);
    _mIter = array._chunkSet.find(outPos);
    if(_mIter==array._chunkSet.end())
    {
        return hasCurrent = false;
    }
    outPos = *_mIter;
    array.out2in(outPos, inPos);
    return hasCurrent = setInputPosition(0);
}

void MappedSubArrayIterator::operator ++()
{
    do
    {
        _mIter++;
        if(_mIter!=array._chunkSet.end())
        {
            outPos = *_mIter;
            array.out2in(outPos, inPos);
            if(setInputPosition(0))
            {
                hasCurrent = true;
                return;
            }
        }
        else
        {
            hasCurrent = false;
            return;
        }
    } while( true );
}

void MappedSubArrayIterator::reset()
{
    _mIter = array._chunkSet.begin();
    if(_mIter==array._chunkSet.end())
    {
        hasCurrent = false;
    }
    else
    {
        outPos = *_mIter;
        array.out2in(outPos, inPos);
        if (setInputPosition(0))
        {
            hasCurrent = true;
        }
        else
        {
            ++(*this);
        }
    }
}

//
// SubArray methods
//
SubArray::SubArray(ArrayDesc& array, Coordinates lowPos, Coordinates highPos,
                   boost::shared_ptr<Array>& input, const shared_ptr<Query>& query)
: DelegateArray(array, input),
  subarrayLowPos(lowPos),
  subarrayHighPos(highPos),
  dims(desc.getDimensions()),
  inputDims(input->getArrayDesc().getDimensions()),
  _useChunkSet(false)
{
    _query = query;
    aligned = true;
    for (size_t i = 0, n = dims.size(); i < n; i++) {
        if ((lowPos[i] - inputDims[i].getStartMin()) % dims[i].getChunkInterval() != 0) {
            aligned = false;
            break;
        }
    }

    double numChunksInBox = 1;
    ArrayDesc const& inputDesc = input->getArrayDesc();
    for (size_t i=0, n = inputDesc.getDimensions().size(); i<n; i++)
    {
        numChunksInBox *= inputDesc.getNumChunksAlongDimension(i, subarrayLowPos[i], subarrayHighPos[i]);
    }

    if (numChunksInBox > SUBARRAY_MAP_ITERATOR_THRESHOLD)
    {
        _useChunkSet = true;
        buildChunkSet();
    }
}

void SubArray::addChunksToSet(Coordinates outChunkCoords, size_t dim)
{
    //if we are not aligned, then each input chunk can contribute to up to 2^nDims output chunks
    //therefore, the recursion
    for(size_t i= (dim == 0 ? 0 : dim -1), n = outChunkCoords.size(); i<n; i++)
    {
        if (outChunkCoords[i]<dims[i].getStartMin() || outChunkCoords[i]>dims[i].getEndMax())
        {
            return;
        }
    }
    if(aligned || dim == outChunkCoords.size())
    {
        _chunkSet.insert(outChunkCoords);
    }
    else
    {
        addChunksToSet(outChunkCoords, dim+1);
        outChunkCoords[dim]+=dims[dim].getChunkInterval();
        addChunksToSet(outChunkCoords, dim+1);
    }
}

void SubArray::buildChunkSet()
{
    AttributeID inputAttribute = 0;
    if(inputArray->getArrayDesc().getEmptyBitmapAttribute())
    {
        inputAttribute = inputArray->getArrayDesc().getEmptyBitmapAttribute()->getId();
    }
    size_t nDims = inputArray->getArrayDesc().getDimensions().size();
    shared_ptr<ConstArrayIterator> inputIter = inputArray->getConstIterator(inputAttribute);
    Coordinates outChunkCoords(nDims);
    while(!inputIter->end())
    {
        in2out(inputIter->getPosition(), outChunkCoords);
        desc.getChunkPositionFor(outChunkCoords);
        addChunksToSet(outChunkCoords);
        ++(*inputIter);
    }
}

DelegateArrayIterator* SubArray::createArrayIterator(AttributeID attrID) const
{
    if(_useChunkSet)
    {
        return new MappedSubArrayIterator(*this, attrID);
    }
    else
    {
        return new SubArrayIterator(*this, attrID);
    }
}
void SubArray::out2in(Coordinates const& out, Coordinates& in) const
{
    for (size_t i = 0, n = out.size(); i < n; i++) {
        in[i] = out[i] + subarrayLowPos[i];
    }
}

void SubArray::in2out(Coordinates const& in, Coordinates& out) const
{
    for (size_t i = 0, n = in.size(); i < n; i++) {
        out[i] = in[i] - subarrayLowPos[i];
    }
}
}
