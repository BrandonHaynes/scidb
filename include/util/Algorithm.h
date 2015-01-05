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

#ifndef UTIL_ALGORITHM_H_
#define UTIL_ALGORITHM_H_

/****************************************************************************/
namespace scidb {
/****************************************************************************/
/**
 * A 3-sequence version of the std::transform() algorithm.
 */
template<class I,class J,class K,class out,class function>
out transform(I i,I e,J j,K k,out o,function f)
{
    for (; i!=e; ++i,++j,++k,++o)
    {
        *o = f(*i,*j,*k);
    }

    return o;
}

/**
 * A 4-sequence version of the std::transform() algorithm.
 */
template<class I,class J,class K,class L,class out,class function>
out transform(I i,I e,J j,K k,L l,out o,function f)
{
    for ( ; i!=e; ++i,++j,++k,++l,++o)
    {
        *o = f(*i,*j,*k,*l);
    }

    return o;
}

/**
 * Return true if at least one of the elements in the sequence [i,e) satisfies
 * the given predicate. The function should be part of the standard library in
 * C++ 11.
 */
template<class iterator,class predicate>
bool any_of(iterator i,iterator e,predicate pred)
{
    for ( ; i!=e; ++i)
    {
        if (pred(*i))
        {
            return true;
        }
    }

    return false;
}

/**
 * Return  true if every one of  the elements in  the sequence [i,e) satisfies
 * the given predicate. The function should be part of the standard library in
 * C++ 11.
 */
template<class iterator,class predicate>
bool all_of(iterator i,iterator e,predicate pred)
{
    for ( ; i!=e; ++i)
    {
        if (!pred(*i))
        {
            return false;
        }
    }

    return  true;
}

/****************************************************************************/
}
/****************************************************************************/
#endif
/****************************************************************************/
