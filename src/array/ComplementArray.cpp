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
 * @file ComplementArray.cpp
 *
 * @brief Array used to megre result of SG with locally available part of array
 *
 * @author Konstantin Knizhnik <knizhnik@garret.ru>
 */

#include "array/ComplementArray.h"

namespace scidb
{
    using namespace boost;


    ComplementArrayIterator::ComplementArrayIterator(boost::shared_ptr<ConstArrayIterator> mainIterator, boost::shared_ptr<ConstArrayIterator> complementIterator)
    :  mainArrayIterator(mainIterator),
       complementArrayIterator(complementIterator),
       currentArrayIterator(mainIterator)
    {
    }

	ConstChunk const& ComplementArrayIterator::getChunk()
    {
        return currentArrayIterator->getChunk();
    }

	bool ComplementArrayIterator::end()
    {
        return mainArrayIterator->end();
    }

	void ComplementArrayIterator::operator ++()
    {
        mainArrayIterator->end();
    }

	Coordinates const& ComplementArrayIterator::getPosition()
    {
        return currentArrayIterator->getPosition();
    }

	bool ComplementArrayIterator::setPosition(Coordinates const& pos)
    {
        if (mainArrayIterator->setPosition(pos)) { 
            currentArrayIterator = mainArrayIterator;
        } else if (complementArrayIterator->setPosition(pos)) { 
            currentArrayIterator = complementArrayIterator;
        } else { 
            return false;
        }
        return true;
    }

	void ComplementArrayIterator::reset()
    {
        mainArrayIterator->reset();
    }

    const ArrayDesc& ComplementArray::getArrayDesc() const
    {
        return mainArray->getArrayDesc();
    }

    boost::shared_ptr<ConstArrayIterator> ComplementArray::getConstIterator(AttributeID id) const
    {
        return boost::shared_ptr<ConstArrayIterator>(new ComplementArrayIterator(mainArray->getConstIterator(id), complementArray->getConstIterator(id)));
    }
}
