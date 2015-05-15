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
 * dmalloc.cpp
 */

#if defined(__GNUC__)

#include <dlfcn.h>
#include <stdlib.h>
#include <util/MallocStats.h>

typedef void* (MallocType) (size_t);
typedef void* (CallocType) (size_t, size_t);
typedef void* (ReallocType) (void *, size_t);
typedef void  (FreeType) (void *);

static MallocType* getTrueMalloc()
{
    // dlsym returns void*, but we need a function pointer
    // casting casuses a warning
    union { MallocType* func; void* ptr; } funcPtr;

    funcPtr.ptr = ::dlsym(RTLD_NEXT, "malloc");
    if (0 == funcPtr.ptr) {
        abort();
    }
    return funcPtr.func;
}

static CallocType* getTrueCalloc()
{
    union { CallocType* func; void* ptr; } funcPtr;

    funcPtr.ptr = ::dlsym(RTLD_NEXT, "calloc");
    if (0 == funcPtr.ptr) {
        abort();
    }
    return funcPtr.func;
}

static ReallocType* getTrueRealloc()
{
    union { ReallocType* func; void* ptr; } funcPtr;

    funcPtr.ptr = ::dlsym(RTLD_NEXT, "realloc");
    if (0 == funcPtr.ptr) {
        abort();
    }
    return funcPtr.func;
}

static FreeType* getTrueFree()
{
    union { FreeType* func; void* ptr; } funcPtr;

    funcPtr.ptr = ::dlsym(RTLD_NEXT, "free");
    if (0 == funcPtr.ptr) {
        abort();
    }
    return funcPtr.func;
}

static void* (*true_malloc) (size_t);
static void* (*true_calloc) (size_t, size_t);
static void* (*true_realloc) (void *, size_t);
static void  (*true_free) (void*);

static void initTrueFuncs()
{
    // the first call to this function
    // should occur before static initialization is complete
    // so it should be thread-safe
    if (!true_malloc) {
        true_malloc = getTrueMalloc();
        true_calloc = getTrueCalloc();
        true_realloc = getTrueRealloc();
        true_free = getTrueFree();
    }
}
static void atomicInc(size_t*tgt, size_t val)
{
    __sync_add_and_fetch(tgt, val);
}
extern "C" {

void* malloc(size_t size)  throw ()
{
    initTrueFuncs();

    void * result = true_malloc (size);

    if (result!=0) {
        atomicInc(&scidb::_mallocStats[scidb::MALLOC] , 1);
    }
    return result;
}

void free (void *ptr)  throw ()
{
    initTrueFuncs();

    true_free (ptr);

    if (ptr!=0) {
        atomicInc(&scidb::_mallocStats[scidb::FREE] , 1);
    }
}

void* realloc(void *ptr, size_t size)  throw ()
{
    initTrueFuncs();

    void *result = true_realloc (ptr,size);

    if (ptr==0 && result!=0) {
        atomicInc(&scidb::_mallocStats[scidb::MALLOC] , 1);
    } else if (ptr!=0 && result==0) {
        atomicInc(&scidb::_mallocStats[scidb::FREE] , 1);
    }
    return result;
}

void *calloc(size_t nmemb, size_t size) throw ()
{
    if (!true_calloc) {
        // dlsym may call calloc internally
        // to prevent an infinite loop just return NULL
        return 0;
    }

    void * result = true_calloc (nmemb, size);

    if (result!=0) {
        atomicInc(&scidb::_mallocStats[scidb::MALLOC] , 1);
    }
    return result;
}
}
#endif
