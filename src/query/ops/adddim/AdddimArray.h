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
 * @file AdddimArray.cpp
 *
 * @brief Adddim array implementation
 *
 * @author Konstantin Knizhnik <knizhnik@garret.ru>
 */

#ifndef ADDDIM_ARRAY_H
#define ADDDIM_ARRAY_H

#include "array/DelegateArray.h"

namespace scidb {

using namespace boost;
using namespace std;

class AdddimArray;
class AdddimArrayIterator;
class AdddimChunk;
class AdddimChunkIterator;

class AdddimChunkIterator : public DelegateChunkIterator
{  
    Coordinates outPos;
    Coordinates inPos;

  public:
    virtual Coordinates const& getPosition();
    virtual bool setPosition(Coordinates const& pos); 

    AdddimChunkIterator(DelegateChunk const* chunk, int iterationMode);
};

class AdddimChunk : public DelegateChunk
{
  private:
    Coordinates _firstPos;
    Coordinates _lastPos;
    Coordinates _firstPosOverlap;
    Coordinates _lastPosOverlap;

  public:
	virtual Coordinates const& getFirstPosition(bool withOverlap) const;
	virtual Coordinates const& getLastPosition(bool withOverlap) const;
    virtual void setInputChunk(ConstChunk const& inputChunk);

    AdddimChunk(AdddimArray const& array, DelegateArrayIterator const& iterator, AttributeID attrID);
};

class AdddimArrayIterator : public DelegateArrayIterator
{
    Coordinates outPos;
    Coordinates inPos;

  public:
    virtual Coordinates const& getPosition();
    virtual bool setPosition(Coordinates const& pos);

	AdddimArrayIterator(AdddimArray const& array, AttributeID attrID, boost::shared_ptr<ConstArrayIterator> inputIterator);
};

class AdddimArray : public DelegateArray
{
  public:
    virtual DelegateChunk* createChunk(DelegateArrayIterator const* iterator, AttributeID id) const;
    virtual DelegateChunkIterator* createChunkIterator(DelegateChunk const* chunk, int iterationMode) const;
    virtual DelegateArrayIterator* createArrayIterator(AttributeID id) const;

    AdddimArray(ArrayDesc const& desc, boost::shared_ptr<Array> const& array);
};

}

#endif
