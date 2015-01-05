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
#ifndef SCALAPACK_SLAVE__H
#define SCALAPACK_SLAVE__H

// std C++
#include <ostream>

// SciDB
#include "../../scalapackUtil/scalapackFromCpp.hpp" // home-brew c++ interface to FORTRAN ScaLAPACK

namespace scidb {

/// Scalapack arrays are described by a 9-value descriptor of type slpp::desc_t.
/// most routines also accept an (I,J) offset for the starting point into that
/// array in order to describe a sub-array.
/// ScalapackArrayArgs is a "smart struct" that captures those 11 values 
class ScalapackArrayArgs {
public:
    friend std::ostream& operator<<(std::ostream& os, const ScalapackArrayArgs& a);
    slpp::int_t    I;
    slpp::int_t    J;
    slpp::desc_t   DESC;
};

inline std::ostream& operator<<(std::ostream& os, const ScalapackArrayArgs& a)
{
    os << "I:" << a.I << " J:" << a.J << std::endl ;
    os << "DESC:" << std::endl;
    os <<   a.DESC << std::endl ;
    return os;
}

} // namespace scidb

#endif // SCALAPACK_SLAVE__H

