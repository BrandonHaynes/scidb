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
 * @file ComplementArray.h
 *
 * @brief Array used to megre result of SG with locally available part of array
 *
 * @author Konstantin Knizhnik <knizhnik@garret.ru>
 */

#ifndef COMPLEMENT_ARRAY_H_
#define COMPLEMENT_ARRAY_H_

#include "array/Array.h"

namespace scidb
{

using namespace boost;

class ComplementArrayIterator : public ConstArrayIterator
{
  public:
    ComplementArrayIterator(boost::shared_ptr<ConstArrayIterator> mainArrayIterator, boost::shared_ptr<ConstArrayIterator> compementArrayIterator);

	virtual ConstChunk const& getChunk();

	virtual bool end();
	virtual void operator ++();
	virtual Coordinates const& getPosition();
	virtual bool setPosition(Coordinates const& pos);
	virtual void reset();

  private:
    boost::shared_ptr<ConstArrayIterator> mainArrayIterator;
    boost::shared_ptr<ConstArrayIterator> complementArrayIterator;
    boost::shared_ptr<ConstArrayIterator> currentArrayIterator;
};
                            
class ComplementArray : public Array
{
  public:
	ComplementArray(boost::shared_ptr<Array> mainArray, boost::shared_ptr<Array> compementArray);

	virtual const ArrayDesc& getArrayDesc() const;
	virtual boost::shared_ptr<ConstArrayIterator> getConstIterator(AttributeID id) const;

  private:
    boost::shared_ptr<Array> mainArray;
    boost::shared_ptr<Array> complementArray;
};

}

#endif
