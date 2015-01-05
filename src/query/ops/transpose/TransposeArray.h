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
 * @file TransposeArray.h
 *
 * @brief Transpose array implementation
 *
 * @author Konstantin Knizhnik <knizhnik@garret.ru>
 * @author poliocough@gmail.com
 */

#ifndef TRANSPOSE_ARRAY_H
#define TRANSPOSE_ARRAY_H

#include "array/DelegateArray.h"
#include <map>

namespace scidb {

using namespace boost;
using namespace std;

/**
 * Internal structure used by operator transpose. Not documented.
 */
class TransposeArray: public Array
{
public:
    TransposeArray(ArrayDesc const& arrayDesc, shared_ptr<Array>const& input, shared_ptr<CoordinateSet>const& inputChunkPositions, shared_ptr<Query>const& query):
        _arrayDesc(arrayDesc),
        _inputArray(input),
        _nDimensions(input->getArrayDesc().getDimensions().size()),
        _outputChunkPositions(new CoordinateSet())
    {
        assert(query);
        _query=query;
        Coordinates outCoords(_nDimensions);
        for (CoordinateSet::const_iterator iter = inputChunkPositions->begin(); iter != inputChunkPositions->end(); ++iter)
        {
            transposeCoordinates(*iter, outCoords);
            _outputChunkPositions->insert( outCoords );
        }
    }

    virtual ~TransposeArray()
    {}

    virtual ArrayDesc const& getArrayDesc() const
    {
        return _arrayDesc;
    }

    virtual Access getSupportedAccess() const
    {
        return Array::RANDOM;
    }

    virtual bool hasChunkPositions() const
    {
        return true;
    }

    virtual boost::shared_ptr<CoordinateSet> getChunkPositions() const
    {
        return boost::shared_ptr<CoordinateSet> (new CoordinateSet( *_outputChunkPositions ));
    }

    virtual boost::shared_ptr<ConstArrayIterator> getConstIterator(AttributeID attrID) const
    {
        //The TransposeArrayIterator will only fill in the extra empty bitmask if emptyTagID is not the same as attrID.
        //If the array is not emptyable, or if attrID is already the empty bitmask - don't bother.
        AttributeID emptyTagID = attrID;
        if (_arrayDesc.getEmptyBitmapAttribute())
        {
            emptyTagID = _arrayDesc.getEmptyBitmapAttribute()->getId();
        }

        return boost::shared_ptr<ConstArrayIterator>( new TransposeArrayIterator(_outputChunkPositions,
                                                                                 _inputArray->getConstIterator(attrID),
                                                                                 _query,
                                                                                 this,
                                                                                 attrID,
                                                                                 emptyTagID));
    }

    void transposeCoordinates(Coordinates const& in, Coordinates& out) const
    {
        assert(in.size() == _nDimensions && out.size() == _nDimensions);
        for (size_t i = 0; i < _nDimensions; ++i)
        {
            out[_nDimensions-i-1] = in[i];
        }
    }

private:
    ArrayDesc _arrayDesc;
    boost::shared_ptr<Array> _inputArray;
    size_t const _nDimensions;
    boost::shared_ptr<CoordinateSet> _outputChunkPositions;

    class TransposeArrayIterator: public ConstArrayIterator
    {
    public:
        TransposeArrayIterator(boost::shared_ptr<CoordinateSet> const& outputChunkPositions,
                               boost::shared_ptr<ConstArrayIterator> inputArrayIterator,
                               boost::weak_ptr<Query> const& query,
                               TransposeArray const* transposeArray,
                               AttributeID const attributeID,
                               AttributeID const emptyTagID):
            _outputChunkPositions(outputChunkPositions),
            _outputChunkPositionsIterator(_outputChunkPositions->begin()),
            _inputArrayIterator(inputArrayIterator),
            _query(query),
            _transposeArray(transposeArray),
            _attributeID(attributeID),
            _emptyTagID(emptyTagID),
            _chunkInitialized(false)
        {}

        virtual bool end()
        {
            return _outputChunkPositionsIterator == _outputChunkPositions->end();
        }

        virtual void operator ++()
        {
            if( end() )
            {
                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_CHUNK);
            }
            _chunkInitialized = false;
            ++_outputChunkPositionsIterator;
        }

        virtual void reset()
        {
            _chunkInitialized = false;
            _outputChunkPositionsIterator == _outputChunkPositions->begin();
        }

        virtual Coordinates const& getPosition()
        {
            if( end() )
            {
                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_CHUNK);
            }
            return (*_outputChunkPositionsIterator);
        }

        virtual bool setPosition(Coordinates const& pos)
        {
            _chunkInitialized = false;
            Coordinates chunkPosition = pos;
            _transposeArray->getArrayDesc().getChunkPositionFor(chunkPosition);
            _outputChunkPositionsIterator = _outputChunkPositions->find(chunkPosition);
            return !end();
        }

        virtual ConstChunk const& getChunk();

    private:
        boost::shared_ptr<CoordinateSet> _outputChunkPositions;
        CoordinateSet::const_iterator _outputChunkPositionsIterator;
        boost::shared_ptr<ConstArrayIterator> _inputArrayIterator;
        boost::weak_ptr<Query> _query;
        TransposeArray const* _transposeArray;
        AttributeID const _attributeID;
        AttributeID const _emptyTagID;
        bool _chunkInitialized;
        MemChunk _outputChunk;
        MemChunk _emptyTagChunk;
    };
};


}

#endif
