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
///
/// ScaLAPACKLogical.hpp
///
///

#ifndef SCALAPACKPHYSICAL_HPP_
#define SCALAPACKPHYSICAL_HPP_


// std C
// std C++
// de-facto standards

// SciDB
#include <array/Metadata.h>
#include <query/Query.h>
#include <query/Operator.h>
#include <system/Exceptions.h>
#include <system/BlockCyclic.h>
// MPI/ScaLAPACK
#include <scalapackUtil/dimUtil.hpp>

// locals
#include "DLAErrors.h"
#include "scalapackFromCpp.hpp"   // TODO JHM : rename slpp::int_t


namespace scidb {

/// no actual ScaLAPACKLogical class yet,
/// just helpers for the Logicals are all that are needed so far

/// returns or throws an exception if the input matrices are not suitable for ScaLAPACK
/// processing.
void                      checkScaLAPACKInputs(std::vector<ArrayDesc> schemas, boost::shared_ptr<Query> query,
                                               size_t nMatsMin, size_t nMatsMax);

/// constructs distinct dimension names, from names that may or may not be distinct
std::pair<string, string> ScaLAPACKDistinctDimensionNames(const string& a, const string& b);

void log4cxx_debug_dimensions(const std::string& prefix, const Dimensions& dims);

} // namespace

#endif /* SCALAPACKPHYSICAL_HPP_ */
