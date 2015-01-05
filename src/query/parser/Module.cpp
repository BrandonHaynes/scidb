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

/****************************************************************************/

#include <util/arena/ScopedArena.h>                      // For ScopedArena
#include <util/Singleton.h>                              // For Singleton
#include <util/RWLock.h>                                 // For RWLock
#include "Module.h"                                      // For Module
#include "Table.h"                                       // For Table
#include "AST.h"                                         // For Node etc.

/****************************************************************************/
namespace scidb { namespace parser { namespace {
/****************************************************************************/

/**
 *  @brief      Represents the currently loaded module.
 *
 *  @see        SciDB 'load_module' operator for the public interface to this
 *              data structure.
 *
 *  @author     jbell@paradigm4.com.
 */
class TheModule : public Singleton<TheModule>
{
 public:                   // Construction
                              TheModule();

 public:                   // Operations
            Table*            getTable()    const;

 public:                   // Operations
            void              load(Log&,const Node*);
            void              lock(int);
            void              unlock(int);

 private:                  // Representation
            ScopedArena       _arena;                    // The private arena
            RWLock            _latch;                    // The module latch
            Table*            _table;                    // The module table
};

/**
 *  Create the singleton instance that represents the currently loaded module.
 *
 *  Until loaded, the current module has no actual bindings of its own.
 */
    TheModule::TheModule()
             : _arena("parser::Module"),
               _table(0)
{}

/**
 *  Return (a table that maintains) the bindings associated with the currently
 *  loaded module.
 */
Table* TheModule::getTable() const
{
    static struct empty : Table
    {
        size_t      size()           const {return 0;}   // Has no bindings
        Table*      getParent()      const {return 0;}   // Has no parent
        const Node* get(const Name*) const {return 0;}   // Has no bindings
        void        accept(Visitor&) const {}            // Has no bindings
    } empty;                                             // The default table

    return _table==0 ? &empty : _table;                  // The current table
}

/**
 *  Install the module 'm' as the currently loaded module.
 *
 *  Note that we copy the bindings into our own private arena: we now own them
 *  and will dispose of them when the module is next loaded again.
 */
void TheModule::load(Log& l,const Node* m)
{
    assert(m!=0 && m->is(module));                       // Validate arguments
    assert(_table == 0);                                 // Validate our state

    m = m->get(moduleArgBindings);                       // Only need bindings
    m = Factory(_arena).newCopy(m,fromAnotherArena);     // Copy to local heap

    _table = newTable(_arena,l,getTable(),m->getList()); // Install in a table
}

/**
 *  Lock the current module for subsequent access in the given access mode.
 *
 *  When locking for write access we also reset the module to the state it had
 *  upon construction - in other words, empty - so that the new replacement is
 *  compiled in a 'clean' environment and does not inadvertantly bind to nodes
 *  that are soon to be destroyed.
 */
void TheModule::lock(int mode)
{
    RWLock::ErrorChecker c;                              // We always succeed

    if (mode == Module::write)                           // Lock for writing?
    {
        _latch.lockWrite(c);                             // ...acquire lock
        _table = 0;                                      // ...clear table
        _arena.reset();                                  // ...empty trash
    }
    else                                                 // Lock for reading
    {
        _latch.lockRead(c);                              // ...acquire lock
    }
}

/**
 *  Release a possibly outstanding lock of type 'mode' on the current module.
 */
void TheModule::unlock(int mode)
{
    if (mode == Module::write)                           // Locked to write?
    {
        _latch.unLockWrite();                            // ...free the lock
    }
    else                                                 // Locked to read
    {
        _latch.unLockRead();                             // ...free the lock
    }
}

/****************************************************************************/
}
/****************************************************************************/

/**
 *  Lock the current module for subsequent access in the given access mode.
 */
Module::Module(mode mode)
      : _mode(mode)
{
    TheModule::getInstance()->lock(_mode);               // Acquire the lock
}

/**
 *  Release any outstanding lock on the current module.
 */
Module::~Module()
{
    TheModule::getInstance()->unlock(_mode);             // Release the lock
}

/**
 *
 */
void Module::load(Log& log,const Node* bindings)
{
    assert(_mode == write);                              // Validate the mode

    TheModule::getInstance()->load(log,bindings);        // Load the bindings
}

/**
 *  Return the root table of bindings provided by the currently loaded module.
 */
Table* getTable()
{
    return TheModule::getInstance()->getTable();         // The current table
}

/****************************************************************************/
}}
/****************************************************************************/
