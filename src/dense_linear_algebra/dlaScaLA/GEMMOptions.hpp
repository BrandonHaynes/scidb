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
#ifndef GEMMOPTIONS_HPP_ 
#define GEMMOPTIONS_HPP_ 

// header groups:
// std C++
#include <string>
// std C
// de-facto standards
// SciDB
// MPI/ScaLAPACK
// local
#include "DLAErrors.h"

namespace scidb
{

struct GEMMOptions {
    /*
     * @param input   the string to be parsed
     */
    GEMMOptions(const std::string& input);  // TODO: change to const &, note: lots of tmplate repercussions

    bool        transposeA, transposeB;         // NOCHECKIN: change this to a contained struct
    double      alpha, beta;                    // NOCHECKIN: so reviewers won't complain about getter/setters
};


} // namespace scidb

#endif // GEMMOPTIONS_HPP_ 
