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
 * @file NormalizeArray.h
 *
 * @brief The implementation of array normalize 
 *
 */

#ifndef NORMALIZE_ARRAY_H_
#define NORMALIZE_ARRAY_H_

#include "array/DelegateArray.h"

namespace scidb
{

using namespace boost;

class NormalizeChunkIterator : public DelegateChunkIterator
{    
public:
    NormalizeChunkIterator(DelegateChunk const* chunk, int iterationMode, double len);
    virtual  Value& getItem();

private:
     Value _value;
    double _len;
};

class NormalizeArray : public DelegateArray
{
public:
    virtual DelegateChunkIterator* createChunkIterator(DelegateChunk const* chunk, int iterationMode) const;
    NormalizeArray(ArrayDesc const& schema, boost::shared_ptr<Array> inputArray, double len);

private:
    double _len;
};

}

#endif
