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
 * Lru.h
 *
 *  Created on: May 29, 2012
 *      Author: Donghui
 *
 *  Two versions of LRU are implemented.
 *
 *  The LRU class implements a least-recently used policy.
 *  The class stores a std::list of pushed objects, and a boost::unordered_map to quickly find an item in the list.
 *  To minimize space, the class smartly chooses between storing elements or storing pointers to the elements in the map.
 *  Like other STL containers, this class is not thread safe.
 *
 *  The LRUSecondary class implements a least-recently used policy, where the caller manages space.
 *  A list is used to store the elements (as in LRU), but a map is NOT used to lookup.
 *  Instead, an insertion into LRUSecondary returns an ListIterator, which the caller should store and use in subsequent calls to touch() or erase().
 */
#ifndef LRU_H_
#define LRU_H_

#include <list>
#include <assert.h>
#include <boost/unordered_map.hpp>
#include "Hashing.h"

namespace scidb
{

/**
 * The LRU class that 'owns' the space of the elements.
 */
template<class T, class Hash = boost::hash<T>, class Equal_to = std::equal_to<T> >
class LRU {
protected:
    typedef typename std::list<T>::iterator ListIterator;
    typedef typename boost::unordered_map<const T*, ListIterator, hash_with_ptr<T, Hash>, equal_to_with_ptr<T, Equal_to> > MapBig;
    typedef typename boost::unordered_map<T, ListIterator, Hash, Equal_to> MapSmall;
    typedef typename MapBig::const_iterator MapBigIterator;
    typedef typename MapSmall::const_iterator MapSmallIterator;

    // A double-linked list of all the elements.
    std::list<T> _list;

    // Size of _list. std::list::size() is too expensive.
    size_t _size;

    // Use the big-map if the size of an element is larger than the size of a pointer;
    // Use the small-map otherwise.
    MapBig _mapBigTypeToNode;
    MapSmall _mapSmallTypeToNode;
    bool _useBig;

public:
    /**
     * Constructor.
     */
    LRU(): _size(0), _useBig(sizeof(T) > sizeof(T*)) {
    }

    /**
     * Destructor.
     */
    ~LRU() {
        _mapBigTypeToNode.clear();
        _mapSmallTypeToNode.clear();
        _list.clear();
        _size = 0;
    }

    /**
     * The number of elements.
     */
    size_t size() {
        return _size;
    }

    /**
     * Check if there are some elements
     */
    bool empty() const {
        return _list.empty();
    }

    /**
     * "Touch" an element so it becomes the most recently used.
     * If the element did not exist, push it first.
     * @param   t   The element to touch.
     */
    void touch(const T& t) {
        ListIterator iter = _list.begin();
        if (iter==_list.end()) {
            push(t);
        } else if (*iter != t) {
            erase(t);
            push(t);
        }
    }

    /**
     * Erase an element.
     * @param   t   The element to erase.
     * @return  Whether erased.
     */
    bool erase(const T& t) {
        if (_useBig) {
            MapBigIterator it = _mapBigTypeToNode.find(&t);
            if (it!=_mapBigTypeToNode.end()) {
                ListIterator iter = it->second;
                _mapBigTypeToNode.erase(it);
                _list.erase(iter);
                assert(_size>0);
                -- _size;
                return true;
            }
        } else {
            MapSmallIterator it = _mapSmallTypeToNode.find(t);
            if (it!=_mapSmallTypeToNode.end()) {
                ListIterator iter = it->second;
                _mapSmallTypeToNode.erase(it);
                _list.erase(iter);
                assert(_size>0);
                -- _size;
                return true;
            }
        }
        return false;
    }

    /**
     * Whether an element exist.
     * @param   t   An element.
     * @return Whether exist.
     */
    bool exists(const T& t) {
        if (_useBig) {
            return _mapBigTypeToNode.find(&t)!=_mapBigTypeToNode.end();
        }
        return _mapSmallTypeToNode.find(t)!=_mapSmallTypeToNode.end();
    }
    /**
     * Push an element.
     * @pre The element must NOT exist.
     */
    void push(const T& t) {
        assert(!exists(t));
        ListIterator iter = _list.insert(_list.begin(), t);
        ++ _size;
        if (_useBig) {
            _mapBigTypeToNode.insert(std::pair<T*, ListIterator>(&(*iter), iter));
        } else {
            _mapSmallTypeToNode.insert(std::pair<T, ListIterator>(*iter, iter));
        }
    }

    /**
     * Pop the least-recently used element.
     * @return  whether popped.
     * @param   t   The popped element (if popped).
     */
    bool pop(T& t) {
        if (_list.empty()) {
            return false;
        }
        ListIterator iter = _list.end();
        -- iter;
        assert(iter!=_list.end());
        t = *iter;
        bool erased = erase(t);
        assert(erased);
        return erased;
    }

    /**
     * Peek the next element to pop.
     * @return  The element at the tail.
     * @pre There MUST exist at least one element.
     */
    T& peekNextToPop() {
        ListIterator iter = _list.end();
        -- iter;
        assert(iter!=_list.end());
        return *iter;
    }
};


/**
 * The LRU class that does NOT 'own' the space of the elements.
 */
template<class T>
class LRUSecondary {
protected:

    // A double-linked list of all the elements.
    std::list<T> _list;

    // Size of _list. std::list::size() is too expensive.
    size_t _size;

public:
    typedef typename std::list<T>::iterator ListIterator;

    /**
     * Constructor.
     */
    LRUSecondary(): _size(0) {
    }

    /**
     * Destructor.
     */
    ~LRUSecondary() {
        _list.clear();
        _size = 0;
    }

    /**
     * The number of elements.
     */
    size_t size() {
        return _size;
    }

    /**
     * Check if there are some elements	
     */
    bool empty() const {
        return _list.empty();
    }

    /**
     * Return an iterator to the first element in the LRU.
     * @return the iterator to the first element; end if empty
     */
    ListIterator begin()
    {
        return _list.begin();
    }

    /**
     * The owner of space, if storing this value, has not inserted itself into the LRU yet.
     * @return ListIterator.end()
     */
    ListIterator end()
    {
        return _list.end();
    }

    /**
     * "Touch" an element so it becomes the most recently used.
     * @pre The element *must* exist, because a handle.
     * @param   h   A handle to the element to be touched.
     * @return  A new handle to the element.
     */
    ListIterator touch(ListIterator& h) {
        T t = *h;
        erase(h);
        return push(t);
    }

    /**
     * Erase an element.
     * @param   h   Handle to an existing element.
     */
    void erase(ListIterator& h) {
        assert(h!=_list.end());
        _list.erase(h);
        assert(_size>0);
        -- _size;
    }

    /**
     * Push an element.
     * @pre The element must NOT exist.
     * @return  A handle to the element, which should be used to touch or erase.
     */
    const ListIterator push(const T& t) {
        ++ _size;
        return _list.insert(_list.begin(), t);
    }

    /**
     * Pop the least-recently used element.
     * @return  whether popped.
     * @param   t   The popped element (if popped).
     */
    bool pop(T& t) {
        if (empty()) {
            return false;
        }
        ListIterator iter = _list.end();
        -- iter;
        assert(iter!=_list.end());
        t = *iter;
        erase(iter);
        return true;
    }

    /**
     * Peek the next element to pop.
     * @return  The element at the tail.
     * @pre There MUST exist at least one element.
     */
    const T& peekNextToPop() {
        ListIterator iter = _list.end();
        -- iter;
        assert(iter!=_list.end());
        return *iter;
    }
};

}
#endif /* LRU_H_ */
