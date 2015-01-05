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
 * @file ThrottledScheduler.h
 *
 * @brief Contains a class that allows to limit how often a particular piece of work is executed.
 *
 */
#ifndef THROTTLEDSCHEDULER_H
#define THROTTLEDSCHEDULER_H

#include <assert.h>
#include <boost/asio.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <util/Mutex.h>
#include <system/ErrorCodes.h>
#include <system/Exceptions.h>
#include <util/Network.h>

namespace scidb
{

 class ThrottledScheduler : public Scheduler, virtual public boost::enable_shared_from_this<ThrottledScheduler>
 {
 public:
    ThrottledScheduler(int64_t maxDelay,
                       Work& work,
                       asio::io_service& io_service)
    : _maxDelay(maxDelay), _work(work), _timer(io_service),
    _lastTime(0), _isScheduled(0), _isRunning(0)
    {
       assert(_work);
    }
    virtual ~ThrottledScheduler() {}
    virtual void schedule()
    {
       ScopedMutexLock lock(_mutex);
       if (_isScheduled) { return; }
       if (_isRunning) {
          _isScheduled = true;
          return;
       }
       _schedule();
    }

 private:

    void _schedule()
    {
       // must be locked
       assert(!_isRunning);

       time_t now = time(NULL);
       if (now < 0)
           throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_CANT_GET_SYSTEM_TIME);

       time_t secToWait = _maxDelay - (now - _lastTime);
       secToWait = (secToWait > 0) ? secToWait : 0;
       assert(secToWait <= _maxDelay);

       size_t n = _timer.expires_from_now(posix_time::seconds(secToWait));
       if (n)
           throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_TIMER_RETURNED_UNEXPECTED_ERROR) << n;
       _timer.async_wait(bind(&ThrottledScheduler::_run,
                              shared_from_this(), asio::placeholders::error));
       _isScheduled = true;
   }

   void _run(const boost::system::error_code& error)
   {
      if (error == boost::asio::error::operation_aborted) {
         return;
      }
      if (error)
          throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_TIMER_RETURNED_UNEXPECTED_ERROR2) << error;
      {
         ScopedMutexLock lock(_mutex);
         assert(_isScheduled);
         assert(!_isRunning);
         _isScheduled = false;
         _isRunning = true;
         _lastTime = time(NULL);
         if (_lastTime < 0)
             throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_CANT_GET_SYSTEM_TIME);
      }
      try {
         if (_work) { _work(); }
      } catch (const scidb::Exception&) {
         _reschedule();
         throw;
      }
      _reschedule();
   }

   void _reschedule()
   {
      ScopedMutexLock lock(_mutex);
      assert(_isRunning);
      _isRunning = false;
      if (_isScheduled) {
         _schedule();
      }
   }

   private:

   ThrottledScheduler(const ThrottledScheduler&);
   ThrottledScheduler& operator=(const ThrottledScheduler&);

   int64_t _maxDelay;
   Work _work;
   boost::asio::deadline_timer _timer;
   int64_t _lastTime;
   bool _isScheduled;
   bool _isRunning;
   Mutex _mutex;
};

} // namespace scidb

#endif
