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
 * AnalyzeAggregate.cpp
 *
 *  Created on: May 10, 2012
 *      Author: egor.pugin@gmail.com
 */

#include "query/Aggregate.h"
#include "query/FunctionLibrary.h"
#include "query/Expression.h"
#include "query/TileFunctions.h"

#include "MurmurHash/MurmurHash3.h"

#include "PhysicalAnalyze.h"
#include "AnalyzeAggregate.h"
#include <log4cxx/logger.h>

using boost::shared_ptr;
using namespace std;

namespace scidb
{

// Logger for operator. static to prevent visibility of variable outside of file
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.query.ops.approx_dc"));

AnalyzeAggregate::AnalyzeAggregate()
    : Aggregate("ApproxDC", TypeLibrary::getType(TID_VOID), TypeLibrary::getType(TID_UINT64)), k_comp(64 - k), m(1 << k)
{
}

void AnalyzeAggregate::initializeState(Value& state)
{
    state.setVector(m);
    state.setNull(-1);
    memset(state.data(), 0, m);
}

void AnalyzeAggregate::accumulate(Value& state, Value const& input)
{
    const uint32_t seed = 0x5C1DB;
    uint64_t h[2];
    MurmurHash3_x64_128((uint8_t *)input.data(), input.size(), seed, h);
    size_t j = h[0] >> k_comp;

    uint8_t r = 1;
    while ((h[0] & 1) == 0 && r <= k_comp)
    {
        h[0] >>= 1;
        r++;
    }
    
    uint8_t *M = (uint8_t *)state.data();
    M[j] = max(M[j], r);
}

void AnalyzeAggregate::merge(Value& dstState, Value const& srcState)
{
    uint8_t *dest = (uint8_t *)dstState.data();
    uint8_t *src = (uint8_t *)srcState.data();

    for (size_t i = 0; i < m; i++)
    {
        dest[i] = max(dest[i], src[i]);
    }
}

void AnalyzeAggregate::finalResult(Value& result, Value const& state)
{
    if (state.getMissingReason() == 0)
    {
        result.setUint64(0);
        return;
    }

    double alpha_m;
    
    switch (m)
    {
    case 16:
        alpha_m = 0.673;
        break;
    case 32:
        alpha_m = 0.697;
        break;
    case 64:
        alpha_m = 0.709;
        break;
    default:
        alpha_m = 0.7213 / (1 + 1.079 / (double)m);
        break;
    }

    uint8_t *M = (uint8_t *)state.data();

    double c = 0;
    for (size_t i = 0; i < m; i++)
    {
        c += 1 / pow(2., (double)M[i]);
    }
    double E = alpha_m * m * m / c;

    const double pow_2_32 = 0xffffffff;

    //corrections
    if (E <= (5 / 2. * m))
    {
        double V = 0;
        for (size_t i = 0; i < m; i++)
        {
            if (M[i] == 0) V++;
        }

        if (V > 0)
        {
            E = m * log(m / V);
        }
    }
    else if (E > (1 / 30. * pow_2_32))
    {
        E = -pow_2_32 * log(1 - E / pow_2_32);
    }

    result.setUint64(E);
}

} //namespace scidb
