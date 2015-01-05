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
 * @file FixAlloc.h
 *
 * @brief Fixed size allocator with no individual object free support
 *
 */

#ifndef __FIX_ALLOC_H__
#define  __FIX_ALLOC_H__

#include <stdlib.h>

namespace scidb
{

template<class T>
class Allocator
{
    struct Block {
        Block* next;
        T buf[1];
    };
    Block* first;
    Block* last;
    size_t used;
    size_t blockSize;
        
  public:
    T* allocate() { 
        if (used >= blockSize) { 
            Block* block = (Block*)malloc(sizeof(Block) + sizeof(T)*(blockSize-1));
            if (last == NULL) { 
                first = block;
            } else { 
                last->next = block;
            }
            block->next = NULL;
            last = block;
            used = 0;
        } 
        return &last->buf[used++];
    }

    Allocator(size_t size = 4096)  { 
        used = blockSize = size;
        last = first = NULL;
    }

    ~Allocator() { 
        Block* curr, *next;
        for (curr = first; curr != NULL; curr = next) { 
            next = curr->next;
            free(curr);
        }
    }
};

}

#endif
