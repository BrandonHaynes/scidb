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
 * BitManip.h
 *
 *  Created on: Aug 14, 2012
 *      Author: dzhang
 */

#ifndef BITMANIP_H_
#define BITMANIP_H_

template<typename T>
inline T turnOn(T v, T bits) {
    return v | bits;
}

template<typename T>
inline T turnOff(T v, T bits) {
    return v & ~bits;
}

template<typename T>
inline bool isAnyOn(T v, T bits) {
    return (v & bits) != 0;
}

template<typename T>
inline bool isAllOn(T v, T bits) {
    return (v & bits) == bits;
}

template<typename T>
inline bool isAnyOff(T v, T bits) {
    return !isAllOn(v, bits);
}

template<typename T>
inline bool isAllOff(T v, T bits) {
    return !isAnyOn(v, bits);
}

#endif /* BITMANIP_H_ */
