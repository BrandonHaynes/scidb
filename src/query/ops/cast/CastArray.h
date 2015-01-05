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
 * @file
 *
 * @brief Delegate array derivative which can convert attribute to new type
 *
 * @author Artyom Smirnov <smirnoffjr@gmail.com>
 */

#include "query/Operator.h"

namespace scidb
{

/**
 * Class which converts type input chunk
 */
class CastArrayChunkIterator: public DelegateChunkIterator
{
    FunctionPointer _converter;
    Value _result;
public:
    /**
     * Constructor
     *
     * @param chunk Input chunk
     * @param iterationMode Iteration mode
     * @param converter Converter function pointer
     */
    CastArrayChunkIterator(const DelegateChunk *chunk, int iterationMode,
            FunctionPointer converter):
        DelegateChunkIterator(chunk, iterationMode),
        _converter(converter)
    {
    }

    /**
     * Returns value produced after types converting
     *
     * @return Value
     */
    Value& getItem()
    {
        const Value* params[1];
        params[0] = &DelegateChunkIterator::getItem();
        _converter(params, &_result, NULL);
        return _result;
    }

    ~CastArrayChunkIterator()
    {
    }
};

/**
 * This class only create appropriate chunk iterator
 */
class CastArray: public DelegateArray
{
public:
    /**
     * Map for matching attributes which required types conversion
     */
    typedef map<AttributeID, FunctionPointer> CastingMap;

    /**
     * Constructor
     *
     * @param desc Array schema
     * @param inputArray Input array
     * @param castingMap Filled map with casting functions
     */
    CastArray(ArrayDesc const& desc, boost::shared_ptr<Array> &inputArray, CastingMap &castingMap):
        DelegateArray(desc, inputArray),
        _castingMap(castingMap)
    {
    }

    /**
     * Return appropriate chunk iterator:
     *   If types in input and output same schema we return DelegateArray
     *   If types not same we will return CastArrayChunkIterator which will do job
     *
     * @param chunk Chunk
     * @param iterationMode Iteration mode
     * @return Chunk iterator
     */
    DelegateChunkIterator* createChunkIterator(DelegateChunk const* chunk, int iterationMode) const
    {
        CastingMap::const_iterator it = _castingMap.find(chunk->getAttributeDesc().getId());
        if (it != _castingMap.end())
        {
            return new CastArrayChunkIterator(chunk, iterationMode, it->second);
        }
        else
        {
            return DelegateArray::createChunkIterator(chunk, iterationMode);
        }
    }

    ~CastArray()
    {
    }

private:
    CastingMap _castingMap;
};

}
