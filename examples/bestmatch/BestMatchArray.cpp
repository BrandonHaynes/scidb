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
 * BestMatchArray.cpp
 *
 *  Created on: Apr 04, 2012
 *      Author: Knizhnik
 */

#include "query/Operator.h"
#include "array/Metadata.h"
#include "array/Array.h"
#include "BestMatchArray.h"

using namespace std;
using namespace boost;

namespace scidb 
{
    const int64_t HASH_MULTIPLIER = 1000003; // prime number, optimal for astronomy RA/DECL->integer conversion
    const size_t HASH_TABLE_RESERVE = 1009; // for overlap area

    BestMatchHash::BestMatchHash(size_t size) : table(size + HASH_TABLE_RESERVE) {}

    BestMatchHash::BestMatchHash() : initialized(false), busy(false), waiting(false) {}

    BestMatchHash::~BestMatchHash() 
    { 
        for (size_t i = 0; i < table.size(); i++) { 
            Elem *curr, *next;
            for (curr = table[i]; curr != NULL; curr = next) {
                next = curr->collisionChain;
                delete curr;
            }
        }
    }

    void BestMatchHash::addCatalogEntry(Coordinates const& pos, size_t i, int64_t hash, int64_t error)
    {
        int64_t from = (pos[i] - error)/error;
        int64_t till = (pos[i] + error)/error; 
        hash *= HASH_MULTIPLIER;
        if (++i < pos.size()) { 
            for (int64_t hi = from; hi <= till; hi++) {
                addCatalogEntry(pos, i, hash ^ hi, error);
            }
        } else { 
            for (int64_t hi = from; hi <= till; hi++) {
                int64_t h = hash ^ hi;
                size_t chain = h % table.size();
                table[chain] = new Elem(pos, h, table[chain]);
            }
        }
    }

    inline BestMatchHash::Elem* BestMatchHash::find(int64_t hash) const
    {
        for (Elem* elem = table[hash % table.size()]; elem != NULL; elem = elem->collisionChain) { 
            if (elem->hash == hash) { 
                return elem;
            }
        }
        return NULL;
    }

    ConstChunk const& BestMatchArrayIterator::getChunk()
    {
        Coordinates const& currPos = inputIterator->getPosition();
        if (chunk.isInitialized() && currPos == chunk.getFirstPosition(false)) {
           return chunk;
        }
        ConstChunk const& srcChunk = inputIterator->getChunk();
        BestMatchArray& array = (BestMatchArray&)this->array;
        match = array.findBestMatch(currPos);
        Address addr(attr, currPos);
        chunk.initialize(&array, &array.getArrayDesc(), addr, 0);

        boost::shared_ptr<Query> emptyQuery;
        boost::shared_ptr<ChunkIterator> dst = chunk.getIterator(emptyQuery, ChunkIterator::SEQUENTIAL_WRITE|ChunkIterator::NO_EMPTY_CHECK);

        if (match->initialized) { 
            boost::shared_ptr<ConstChunkIterator> src = srcChunk.getConstIterator(ConstChunkIterator::IGNORE_EMPTY_CELLS);
            int64_t itemNo = 0;
            if (attr < array.nPatternAttributes) { 
                for (; !src->end(); ++(*src), ++itemNo) { 
                    BestMatchHash::Elem* elem = match->find(itemNo);
                    if (elem != NULL) { 
                        if (!dst->setPosition(src->getPosition())) { 
                            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_POSITION);
                        }
                        dst->writeItem(src->getItem());
                    }
                }
            } else if (attr < array.nPatternAttributes + array.nCatalogAttributes) { 
                if (catalogIterator->setPosition(currPos)) { 
                    boost::shared_ptr<ConstChunkIterator> ci = catalogIterator->getChunk().getConstIterator(ConstChunkIterator::IGNORE_EMPTY_CELLS);
                    for (; !src->end(); ++(*src), ++itemNo) { 
                        BestMatchHash::Elem* elem = match->find(itemNo);
                        if (elem != NULL) { 
                            if (!dst->setPosition(src->getPosition())) { 
                                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_POSITION);
                            }
                            if (!ci->setPosition(elem->coords)) { 
                                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_POSITION);
                            }
                            dst->writeItem(ci->getItem());
                        }
                    }
                }
            } else if (attr < array.nPatternAttributes + array.nCatalogAttributes + currPos.size()) { 
                size_t dimNo = attr - array.nPatternAttributes - array.nCatalogAttributes;
                Value coordValue;
                for (; !src->end(); ++(*src), ++itemNo) { 
                    BestMatchHash::Elem* elem = match->find(itemNo);
                    if (elem != NULL) { 
                        if (!dst->setPosition(src->getPosition())) { 
                            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_POSITION);
                        }
                        coordValue.setInt64(elem->coords[dimNo]);
                        dst->writeItem(coordValue);
                    }
                }            
            } else { 
                Value trueValue;
                trueValue.setBool(true);
                for (; !src->end(); ++(*src), ++itemNo) { 
                    BestMatchHash::Elem* elem = match->find(itemNo);
                    if (elem != NULL) { 
                        if (!dst->setPosition(src->getPosition())) { 
                            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_POSITION);
                        }
                        dst->writeItem(trueValue);
                    }
                }            
            }
        }
        dst->flush();
        return chunk;
    }

    BestMatchArrayIterator::BestMatchArrayIterator(BestMatchArray const& array, AttributeID attrID, boost::shared_ptr<ConstArrayIterator> patIterator, boost::shared_ptr<ConstArrayIterator> catIterator)
    : DelegateArrayIterator(array, attrID, patIterator),
      catalogIterator(catIterator)
    {
    }


    inline int64_t getCatalogHash(Coordinates const& pos, int64_t error) { 
        int64_t hash = 0;
        for (size_t i = 0, n = pos.size(); i < n; i++) { 
            hash *= HASH_MULTIPLIER;
            hash ^= pos[i]/error;
        }
        return hash;
    }
    
    inline uint64_t calculateDistance(Coordinates const& from, Coordinates const& till) { 
        uint64_t dist = 0;
        for (size_t i = 0, n = from.size(); i < n; i++) { 
            dist += (till[i] - from[i])*(till[i] - from[i]);
        }
        return dist;
    }

    boost::shared_ptr<BestMatchHash> BestMatchArray::findBestMatch(Coordinates const& chunkPos)
    {
        boost::shared_ptr<BestMatchHash> strongPtr;
        {
            ScopedMutexLock cs(mutex);
            boost::weak_ptr<BestMatchHash>& weakPtr = matches[chunkPos];
            strongPtr = weakPtr.lock();
            if (strongPtr) { 
                Event::ErrorChecker ec;
                while (strongPtr->busy) {
                    strongPtr->waiting = true;
                    event.wait(mutex, ec);
                }
                return strongPtr;
            } else { 
                weakPtr = strongPtr = boost::shared_ptr<BestMatchHash>(new BestMatchHash());
                strongPtr->busy = true;
            }
        }
        boost::shared_ptr<ConstArrayIterator> patternIterator = pattern->getConstIterator(patternIteratorAttr);
        boost::shared_ptr<ConstArrayIterator> catalogIterator = catalog->getConstIterator(catalogIteratorAttr);
        if (patternIterator->setPosition(chunkPos) && catalogIterator->setPosition(chunkPos))
        {
            ConstChunk const& catalogChunk = catalogIterator->getChunk();
            ConstChunk const& patternChunk = patternIterator->getChunk();
            BestMatchHash catalogHash(catalogChunk.count());
            for (boost::shared_ptr<ConstChunkIterator> ci = catalogChunk.getConstIterator(ConstChunkIterator::IGNORE_EMPTY_CELLS);
                 !ci->end();
                 ++(*ci))
            {
                catalogHash.addCatalogEntry(ci->getPosition(), 0, 0, error);
            }
            BestMatchHash* patternHash = strongPtr.get();
            patternHash->table.resize(patternChunk.count() + HASH_TABLE_RESERVE);
            int64_t item_no = 0;
            for (boost::shared_ptr<ConstChunkIterator> pi = patternChunk.getConstIterator(ConstChunkIterator::IGNORE_EMPTY_CELLS);
                 !pi->end();
                 ++(*pi), item_no++)
            {
                Coordinates const& patternPos = pi->getPosition();
                int64_t hash = getCatalogHash(patternPos, error);
                uint64_t minDistance = (uint64_t)-1;
                uint64_t maxDistance = error*error;
                Coordinates* matchCoords = NULL;

                for (BestMatchHash::Elem* elem = catalogHash.collisionChain(hash); elem != NULL; elem = elem->collisionChain) { 
                    if (elem->hash == hash) {                         
                        uint64_t distance = calculateDistance(patternPos, elem->coords);
                        if (distance <= maxDistance && distance < minDistance) { 
                            minDistance = distance;
                            matchCoords = &elem->coords;
                        }
                    }
                }
                if (matchCoords != NULL) { 
                    BestMatchHash::Elem*& chain = patternHash->collisionChain(item_no);
                    chain = new BestMatchHash::Elem(*matchCoords, item_no, chain);
                }
            }
            strongPtr->initialized = true;        
        }
        {
            ScopedMutexLock cs(mutex);
            strongPtr->busy = false;
            if (strongPtr->waiting) { 
                strongPtr->waiting = false;
                event.signal();
            }
        }
        return strongPtr;
    }

    DelegateArrayIterator* BestMatchArray::createArrayIterator(AttributeID attrID) const
    {                                                                   
        boost::shared_ptr<ConstArrayIterator> patIterator = pattern->getConstIterator(attrID < nPatternAttributes ? attrID : patternIteratorAttr);          
        boost::shared_ptr<ConstArrayIterator> catIterator;
        if (attrID >= nPatternAttributes && attrID < nPatternAttributes + nCatalogAttributes) { 
            catIterator = catalog->getConstIterator(attrID - nPatternAttributes);
        }
        return new BestMatchArrayIterator(*this, attrID, patIterator, catIterator);
    }       
 
    BestMatchArray::BestMatchArray(ArrayDesc const& desc, boost::shared_ptr<Array> patternArr, boost::shared_ptr<Array> catalogArr, int64_t matchError)
    : DelegateArray(desc, patternArr), pattern(patternArr), catalog(catalogArr), error(matchError)
    {
        ArrayDesc const& patternDesc = pattern->getArrayDesc();
        ArrayDesc const& catalogDesc = catalog->getArrayDesc();
        nPatternAttributes = patternDesc.getAttributes(true).size();
        nCatalogAttributes = catalogDesc.getAttributes(true).size();
        patternIteratorAttr = patternDesc.getEmptyBitmapAttribute() != NULL ? patternDesc.getEmptyBitmapAttribute()->getId() : 0;
        catalogIteratorAttr = catalogDesc.getEmptyBitmapAttribute() != NULL ? catalogDesc.getEmptyBitmapAttribute()->getId() : 0;
    }
}
