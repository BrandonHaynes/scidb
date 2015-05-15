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


/*
 * DBArray.h
 *
 *  Created on: 17.01.2010
 *      Author: knizhnik@garret.ru
 *      Description: Database array implementation
 */

#ifndef DBARRAY_H_
#define DBARRAY_H_

#include <vector>
#include "array/MemArray.h"

namespace scidb
{
    /**
     * Implementation of database array
     */
    class DBArray : public Array, public boost::enable_shared_from_this<DBArray>
    {
        string getRealName() const;
      public:
        virtual string const& getName() const;
        virtual ArrayID getHandle() const;

        virtual ArrayDesc const& getArrayDesc() const;

        virtual boost::shared_ptr<ArrayIterator> getIterator(AttributeID attId);
        virtual boost::shared_ptr<ConstArrayIterator> getConstIterator(AttributeID attId) const;

        /**
         * Returns a flag indicating that this array has an available list of chunk positions
         * @return true unless we don't have a query context
         */
        virtual bool hasChunkPositions() const
        {
            return true;
        }

        /**
         * Build and return a list of the chunk positions.
         * @return the new sorted set of coordinates, containing the first coordinate of every chunk present in the array
         */
        virtual boost::shared_ptr<CoordinateSet> getChunkPositions() const;

        /**
         * @see Array::isMaterialized()
         */
        virtual bool isMaterialized() const
        {
            return true;
        }
        static boost::shared_ptr<DBArray> newDBArray(ArrayDesc const& desc, const boost::shared_ptr<Query>& query)
        {
            return boost::shared_ptr<DBArray>(new DBArray(desc, query));
        }
        static boost::shared_ptr<DBArray> newDBArray(std::string const& name, const boost::shared_ptr<Query>& query)
        {
            return boost::shared_ptr<DBArray>(new DBArray(name, query));
        }

      private:
        DBArray(ArrayDesc const& desc, const boost::shared_ptr<Query>& query);
        DBArray(std::string const& name, const boost::shared_ptr<Query>& query);
        DBArray();
        DBArray(const DBArray& other);
        DBArray& operator=(const DBArray& other);

      private:
        ArrayDesc _desc;
    };
}

#endif
