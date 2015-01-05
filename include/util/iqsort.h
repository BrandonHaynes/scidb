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
** Sorting stuff by Dann Corbit and Pete Filandr.
** (dcorbit@connx.com and pfilandr@mindspring.com)
** Use it however you like.
*/

#ifndef IQSORT_H_
#define IQSORT_H_

//
//  The insertion sort template is used for small partitions.
//  Insertion sort is stable.
//

#include <stddef.h>
#include <vector>
#include <assert.h>
#include <algorithm>

namespace scidb
{

    //
    //  The insertion sort template is used for small partitions.
    //  Insertion sort is stable.
    //

    template<class Elem, class Comparator>
    inline void insertion_sort(Elem* array, size_t nmemb, Comparator& compare)
    {
        Elem temp, *last, *first, *middle;
        if (nmemb > 1)
        {
            first = middle = 1+array;
            last = nmemb - 1+array;
            while (first != last)
            {
                ++first;
                if (compare(*middle, *first) > 0)
                {
                    middle = first;
                }
            }
            if (compare(*array, *middle) > 0)
            {
                temp = *array;
                *array = *middle;
                *middle = temp;
            }
            ++array;
            while (array != last)
            {
                first = array++;
                if (compare(*first, *array) > 0)
                {
                    middle = array;
                    temp = *middle;
                    do
                    {
                        *middle-- = *first--;
                    }
                    while (compare(*first, temp) > 0);
                    *middle = temp;
                }
            }
        }
    }

    //
    // The median estimate is used to choose pivots for the quicksort algorithm
    //

    template<class Elem, class Comparator>
    inline void median_estimate(Elem* array, size_t n, Comparator& compare)
    {
        Elem temp;
        long unsigned lu_seed = 123456789LU;
        const size_t k = (lu_seed = 69069 * lu_seed + 362437) % --n;

        temp = *array;
        *array = array[k];
        array[k] = temp;

        if (compare(array[1], *array) > 0)
        {
            temp = array[1];
            if (compare(array[n], *array) > 0)
            {
                array[1] = * array;
                if (compare(temp, array[n]) > 0)
                {
                    *array = array[n];
                    array[n] = temp;
                }
                else
                {
                    *array = temp;
                }
            }
            else
            {
                array[1] = array[n];
                array[n] = temp;
            }
        }
        else
        {
            if (compare(*array, array[n]) > 0)
            {
                if (compare(array[1], array[n]) > 0)
                {
                    temp = array[1];
                    array[1] = array[n];
                    array[n] = * array;
                    *array = temp;
                }
                else
                {
                    temp = * array;
                    *array = array[n];
                    array[n] = temp;
                }
            }
        }
    }


    //
    // This heap sort is better than average because it uses Lamont's heap.
    //

    template<class Elem, class Comparator>
    inline void heapsort(Elem* array, size_t nmemb, Comparator& compare)
    {
        size_t i, child, parent;
        Elem temp;

        if (nmemb > 1)
        {
            i = --nmemb / 2;
            do
            {
                parent = i;
                temp = array[parent];
                child = parent * 2;
                while (nmemb > child)
                {
                    if (compare(array[child + 1], array[child]) > 0)
                    {
                        child += 1;
                    }
                    if (compare(array[child], temp) > 0)
                    {
                        array[parent] = array[child];
                        parent = child;
                        child *= 2;
                    }
                    else
                    {
                        child -= 1;
                        break;
                    }
                }
                if (nmemb == child && compare(array[child], temp) > 0)
                {
                    array[parent] = array[child];
                    parent = child;
                }
                array[parent] = temp;
            } while (i--);

            temp = * array;
            *array = array[nmemb];
            array[nmemb] = temp;

            for (--nmemb; nmemb; --nmemb)
            {
                parent = 0;
                temp = array[parent];
                child = parent * 2;
                while (nmemb > child)
                {
                    if (compare(array[child + 1], array[child]) > 0)
                    {
                        ++child;
                    }
                    if (compare(array[child], temp) > 0)
                    {
                        array[parent] = array[child];
                        parent = child;
                        child *= 2;
                    }
                    else
                    {
                        --child;
                        break;
                    }
                }
                if (nmemb == child && compare(array[child], temp) > 0)
                {
                    array[parent] = array[child];
                    parent = child;
                }
                array[parent] = temp;

                temp = * array;
                *array = array[nmemb];
                array[nmemb] = temp;
            }
        }
    }

    // 
    // We use this to check to see if a partition is already sorted.
    // 
    template<class Elem, class Comparator>
    inline int sorted(Elem* array, size_t nmemb, Comparator& compare)
    {
        for (--nmemb; nmemb; --nmemb)
        {
            if (compare(*array, array[1]) > 0)
            {
                return 0;
            }
            array += 1;
        }
        return 1;
    }

    // 
    // We use this to check to see if a partition is already reverse-sorted.
    // 
    template<class Elem, class Comparator>
    inline int rev_sorted(Elem* array, size_t nmemb, Comparator& compare)
    {
        for (--nmemb; nmemb; --nmemb)
        {
            if (compare(array[1], * array) > 0)
            {
                return 0;
            }
            array += 1;
        }
        return 1;
    }

    // 
    // We use this to reverse a reverse-sorted partition.
    // 

    template<class Elem>
    inline void rev_array(Elem* array, size_t nmemb)
    {
        Elem temp, *end;
        for (end = array + nmemb - 1; end > array; ++array)
        {
            temp = * array;
            *array = * end;
            *end = temp;
            --end;
        }
    }

    //
    // This is the heart of the quick sort algorithm used here.
    // If the sort is going quadratic, we switch to heap sort.
    // If the partition is small, we switch to insertion sort.
    //

    template<class Elem, class Comparator>
    inline void qloop(Elem* array, size_t nmemb, size_t d, Comparator& compare)
    {
        Elem temp, *first, *last;

        while (nmemb > 50)
        {
            if (sorted(array, nmemb, compare))
            {
                return ;
            }
            if (d-- == 0)
            {
                heapsort(array, nmemb, compare);
                return ;
            }
            median_estimate(array, nmemb, compare);
            first = 1+array;
            last = nmemb - 1+array;

            do
            {
                ++first;
            }
            while (compare(*array, * first) > 0);

            do
            {
                --last;
            }
            while (compare(*last, * array) > 0);

            while (last > first)
            {
                temp = * last;
                *last = * first;
                *first = temp;

                do
                {
                    ++first;
                }
                while (compare(*array, * first) > 0);

                do
                {
                    --last;
                }
                while (compare(*last, * array) > 0);
            }
            temp = * array;
            *array = * last;
            *last = temp;

            qloop(last + 1, nmemb - 1+array - last, d, compare);
            nmemb = last - array;
        }
        insertion_sort(array, nmemb, compare);
    }

    // 
    // Introspective quick sort algorithm user entry point.
    // You do not need to directly call any other sorting template.
    // This sort will perform very well under all circumstances.
    // 
    template<class Elem, class Comparator>
    inline void iqsort(Elem* array, size_t nmemb, Comparator& compare)
    {
        size_t d, n;
        if (nmemb > 1 && !sorted(array, nmemb, compare))
        {
            if (!rev_sorted(array, nmemb, compare))
            {
                n = nmemb / 4;
                d = 2;
                while (n)
                {
                    ++d;
                    n /= 2;
                }
                qloop(array, nmemb, 2* d, compare);
            }
            else
            {
                rev_array(array, nmemb);
            }
        }
    }

    /**
     * Binary search in a sorted vector
     * @param sortedVec vector to search
     * @param val value to search for
     * @param indx array index of the found balue
     * @return true if the value is found and indx is valid; false otherwise
     */
    template<typename ElemT>
    bool bsearch(std::vector<ElemT>& sortedVec, ElemT& val, size_t& indx)
    {
       typename std::vector<ElemT>::iterator iter = std::lower_bound(sortedVec.begin(),sortedVec.end(),val);
       bool found = (iter!=sortedVec.end() && !(val<(*iter)));
       if (found) {
          indx = std::distance(sortedVec.begin(), iter);
          assert(indx>0 || indx==0);
          assert(indx<sortedVec.size());
       }
       return found;
    }
}
#endif
