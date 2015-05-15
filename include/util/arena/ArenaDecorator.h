/*
**
* BEGIN_COPYRIGHT
*
* This file is part of SciDB.
* Copyright (C) 2008-2013 SciDB, Inc.
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

#ifndef UTIL_ARENA_ARENA_DECORATOR_H_
#define UTIL_ARENA_ARENA_DECORATOR_H_

/****************************************************************************/

#include <util/Arena.h>                                  // For Arena

/****************************************************************************/
namespace scidb { namespace arena {
/****************************************************************************/
/**
 *  @brief      Implements the arena interface by forwarding all operations on
 *              to some other %arena.
 *
 *  @details    Class ArenaDecorator provides a common base for a hierarchy of
 *              'decorations', objects that extend the behavior of an existing
 *              %arena at runtime in some way by overriding some or all of the
 *              Arena interface's virtual functions but delegating most of the
 *              actual work on to the %arena that they 'decorate'.
 *
 *              This design allows features such as memory painting and thread
 *              locking to be added to an existing %arena independently of one
 *              another at run time.
 *
 *  @see        http://en.wikipedia.org/wiki/Decorator_pattern.
 *
 *  @author     jbell@paradigm4.com.
 */
class ArenaDecorator : public Arena
{
 public:                   // Construction
                              ArenaDecorator(const ArenaPtr& p) : _arena(p)    {assert(consistent());}

 public:                   // Attributes
    virtual name_t            name()                     const                 {return _arena->name();}
    virtual ArenaPtr          parent()                   const                 {return _arena->parent();}
    virtual size_t            available()                const                 {return _arena->available();}
    virtual size_t            allocated()                const                 {return _arena->allocated();}
    virtual size_t            peakusage()                const                 {return _arena->peakusage();}
    virtual size_t            allocations()              const                 {return _arena->allocations();}
    virtual features_t        features()                 const                 {return _arena->features();}
    virtual void              checkpoint(name_t l)       const                 {return _arena->checkpoint(l);}
    virtual void              insert(std::ostream& o)    const                 {return _arena->insert(o);}

 public:                   // Operations
    virtual void*             allocate(size_t n)                               {return _arena->allocate(n);}
    virtual void*             allocate(size_t n,finalizer_t f)                 {return _arena->allocate(n,f);}
    virtual void*             allocate(size_t n,finalizer_t f,count_t c)       {return _arena->allocate(n,f,c);}
    virtual void              recycle(void* p)                                 {return _arena->recycle(p);}
    virtual void              destroy(void* p,count_t c)                       {return _arena->destroy(p,c);}
    virtual void              reset()                                          {return _arena->reset();}

 public:                   // Implementation
    virtual void*             doMalloc(size_t n)                               {return _arena->doMalloc(n);}
    virtual void              doFree  (void*  p,size_t n)                      {return _arena->doFree(p,n);}

 protected:                // Implementation
            bool              consistent()               const                 {assert(_arena);return true;}

 protected:                // Representation
            ArenaPtr    const _arena;                    // The delegate arena
};
/****************************************************************************/
}}
/****************************************************************************/
#endif
/****************************************************************************/
