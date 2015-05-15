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
 * Counter.h
 * 
 * A counter/ stat gathering utility
 */

#ifndef COUNTER_H_
#define COUNTER_H_

#include <util/Timing.h>
#include <util/Singleton.h>
#include <util/Mutex.h>
#include <util/Platform.h>

namespace scidb
{
    class ListCounterArrayBuilder;

    /**
     * @brief   Global stat structure for Counter and CounterTimer classes
     * 
     * @details To add a new type of counter, add an entry in the "CounterId"
     *          enum (before the "LastCounter" entry).  Also add a corresponding
     *          string initialization in the constructor so that list('counters')
     *          will have something to print
     */
    class CounterState : public Singleton<CounterState>
    {
    public:

        enum CounterId
        {
            MemArrayChunkWrite = 0,
            MemArrayChunkRead,
            MemArrayCleanSwap,
            LastCounter             // This entry must be last!
        };
        
        struct Entry
        {
            CounterId _id;    // the id --- only set when we are listing w/ builder
            uint64_t  _num;   // the number of hits
            uint64_t  _msecs; // total msecs elapsed (if this is a timer)
            
            Entry() : _id(LastCounter), _num(0), _msecs(0)
                {}
        };
        
        friend class Counter;
        friend class CounterTimer;

        /**
         * Reset all the stats
         */
        void reset()
            {
                Entry e;
                ScopedMutexLock sm(_stateMutex);
                _entries.clear();
                _entries.insert(_entries.begin(), LastCounter, e);
            }

        /**
         * List all stats to the builder
         */
        void listCounters(ListCounterArrayBuilder& builder);
        
        /**
         * Get the name for an id
         */
        std::string getName(const CounterId id)
            {
                return _names[id];
            }

        /**
         * Constructor
         */
        CounterState() :
            _entries(LastCounter),
            _names(LastCounter)
            {
                _names[MemArrayChunkWrite] = "MemArrayChunkWrite";
                _names[MemArrayChunkRead] = "MemArrayChunkRead";
                _names[MemArrayCleanSwap] = "MemArrayCleanSwap";
            }

    private:
        
        Mutex                    _stateMutex;  // protects global data structure
        std::vector<Entry>       _entries;     // global tally of counters/timers
        std::vector<std::string> _names;       // names of counters for output
    };

    /**
     * @brief    Counter class
     *
     * @details  A class that can be instantiated on the stack which automatically
     *           increments a global counter and times the block in which it
     *           is instantiated.  Statically defined integer values are used
     *           to indicate which global counter is being used.  The summary stats of all
     *           global counters can be querried with the "list('counter')" command.
     *           If not in debug mode, counter does nothing by default.  "force"
     *           option to constructor overrides this.
     */
    class Counter
    {
    public:
        
        /**
         * Constructor/Destructor
         */
        Counter(CounterState::CounterId id, bool force = false) :
            _cs(NULL),
            _id(id),
            _ems(true)
            {
                if (isDebug() || force)
                {
                    _cs = CounterState::getInstance();
                    _ems.restart();
                }
            }
        ~Counter()
            {
                if (_cs)
                {
                    ScopedMutexLock m(_cs->_stateMutex);
                    ++(_cs->_entries[_id]._num);
                    _cs->_entries[_id]._msecs += _ems.elapsed();
                }
            }

    private:

        CounterState* _cs;           // ref to the global stat structure
        CounterState::CounterId _id; // id of counter to update
        ElapsedMilliSeconds _ems;    // timer that tracks elapsed time is msecs
    };
}
#endif
