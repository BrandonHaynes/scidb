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
 * @file JobQueue.h
 *
 * @author roman.simakov@gmail.com
 *
 * @brief The queue of jobs for execution in thread pool
 */

#ifndef JOBQUEUE_H_
#define JOBQUEUE_H_

#include <list>
#include <boost/shared_ptr.hpp>

#include "Job.h"
#include "Mutex.h"
#include "Semaphore.h"
#include "RWLock.h"


namespace scidb
{


class JobQueue
{
private:
    std::list< boost::shared_ptr<Job> > _queue;
	Mutex _queueMutex;
	Semaphore _queueSemaphore;

public:
	JobQueue();

        size_t getSize()
        {
            return _queue.size();
        }

	/// Add new job to the end of queue
	void pushJob(boost::shared_ptr<Job> job);

	// Add new job to the beginning of queue
	void pushHighPriorityJob(boost::shared_ptr<Job> job);

        /**
         * Get next job from the beginning of the queue
         * If there is next element the method waits
         */
        boost::shared_ptr<Job> popJob();
};


} // namespace

#endif /* JOBQUEUE_H_ */
