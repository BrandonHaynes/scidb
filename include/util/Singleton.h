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
 * @file Singleton.h
 *
 * @brief Helper template for creating global objects
 *
 * @author Artyom Smirnov <smirnoffjr@gmail.com>
 */

#ifndef SINGLETON_H_
#define SINGLETON_H_

#include "util/Mutex.h"

namespace scidb
{

/**
 * @brief Singleton template
 *
 * Global objects must be derived from this template. For example:
 * @code
 * class MyGlobalObject: public Singleton<MyGlobalObject>
 * {
 * public:
 *   void myMethod();
 * ...
 * }
 * @endcode
 *
 * After this you can access to your global object like this:
 * @code
 * MyGlobalObject::getInstance()->myMethod();
 * @endcode
 */
template<typename Derived>
class Singleton
{
public:
	/**
	 * Construct and return derived class object
	 * @return pointer to constructed object
	 */
	static Derived* getInstance()
	{
		if (!_instance_initialized)
		{
			{
				ScopedMutexLock mutexLock(_instance_mutex);
				if (!_instance)
				{
					_instance = new Derived();
					std::atexit(destroy);
				}
			}
			{
				ScopedMutexLock mutexLock(_instance_mutex);
				_instance_initialized = true;
			}
		}
		return _instance;
	}

protected:
	Singleton()	{}
	virtual ~Singleton() {}

private:
	/**
	 * Destroy global object
	 */
	static void destroy()
	{
		delete _instance;
		_instance = NULL;
	}

	static Derived* volatile _instance;
	static Mutex _instance_mutex;
	static bool _instance_initialized;
};

template <typename Derived>
Derived* volatile Singleton<Derived>::_instance = NULL;

template <typename Derived>
Mutex Singleton<Derived>::_instance_mutex;

template <typename Derived>
bool Singleton<Derived>::_instance_initialized = false;
}

#endif /* SINGLETON_H_ */
