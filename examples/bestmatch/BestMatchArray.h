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
 * BestMatchArray.h
 *
 *  Created on: Apr 04, 2012
 *      Author: Knizhnik
 */
#ifndef _MATCH_ARRAY_H_
#define _MATCH_ARRAY_H_

#include "query/Operator.h"
#include "array/Metadata.h"
#include "array/DelegateArray.h"
#include "util/Mutex.h"
#include <map>

namespace scidb {

using namespace boost;
using namespace std;

class BestMatchArray;
class BestMatchArrayIterator;

struct BestMatchHash 
{ 
    struct Elem { 
        Coordinates coords;
        int64_t hash;
        Elem* collisionChain;

        Elem(Coordinates const& pos, int64_t h, Elem* next) : coords(pos), hash(h), collisionChain(next) {}
    };
    vector<Elem*> table;
    bool  initialized;
    bool  busy;
    bool  waiting;

    Elem*& collisionChain(int64_t hash) { 
        return table[hash % table.size()];
    }

    Elem* find(int64_t hash) const;
        
    void addCatalogEntry(Coordinates const& pos, size_t i, int64_t hash, int64_t error);

    BestMatchHash();
    BestMatchHash(size_t size);
    ~BestMatchHash();
};


class BestMatchArrayIterator : public DelegateArrayIterator
{
  public: 
	virtual ConstChunk const& getChunk();
    BestMatchArrayIterator(BestMatchArray const& array, AttributeID attrID, boost::shared_ptr<ConstArrayIterator> patIterator, boost::shared_ptr<ConstArrayIterator> catIterator);

  private:
    MemChunk chunk;
    boost::shared_ptr<BestMatchHash> match;
    boost::shared_ptr<ConstArrayIterator> catalogIterator;
};

class BestMatchArray : public DelegateArray
{
    friend class BestMatchArrayIterator;
  public:
    boost::shared_ptr<BestMatchHash> findBestMatch(Coordinates const& chunkPos);
    int64_t getElemPosition(Coordinates const& pos, ConstChunk const& chunk);

    virtual DelegateArrayIterator* createArrayIterator(AttributeID id) const;

    BestMatchArray(ArrayDesc const& desc, boost::shared_ptr<Array> pattern, boost::shared_ptr<Array> catalog, int64_t error);
    
  private:
    Mutex mutex;
    Event event;
    map<Coordinates, boost::weak_ptr<BestMatchHash> > matches;
    boost::shared_ptr<Array> pattern;
    boost::shared_ptr<Array> catalog;
    boost::shared_ptr<ConstArrayIterator> patternIterator;
    boost::shared_ptr<ConstArrayIterator> catalogIterator;

    int64_t error;
    size_t nPatternAttributes;
    size_t nCatalogAttributes;
    AttributeID patternIteratorAttr;
    AttributeID catalogIteratorAttr;
};

}

#endif
