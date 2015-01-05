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
 * @file Barrier.h
 *
 * @author egor.pugin@gmail.com
 *
 * @brief The Barrier class for synchronization
 */

#ifndef BARRIER_H_
#define BARRIER_H_

#include "Mutex.h"
#include "Semaphore.h"

namespace scidb
{

class Barrier
{
private:
	Mutex _mutex;
	Semaphore _semMainThread;
	Semaphore _semThreads;

	size_t _counter;
	size_t _nThreads;
public:
	Barrier(size_t nThreads) : _counter(nThreads), _nThreads(nThreads) {}

	void sync()
	{
		{
			ScopedMutexLock lock(_mutex);

			if (!--_counter)
			{
				_counter = _nThreads;

				_semThreads.release(_nThreads - 1);
				_semMainThread.enter(_nThreads - 1);

				return;
			}
		}

		_semThreads.enter();
		_semMainThread.release();
	}
};

} //namespace

#endif /* BARRIER_H_ */
