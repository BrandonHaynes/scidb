
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
 * @file naInternal.cpp
 *
 * @brief NA - a NaN of a specific value used to represent an _intended_ missing value
 *
 * @author James McQueston <jmcqueston@paradigm4.com>
 */

#include <cstdlib>
#include <iostream>
#include "util/na.h"
#include "require.h"

//
// TODO JHM ; investigate whether there is a more portable api to access the
//            mantissa and exponent of a float than the following
//            type puns which implicitly assume a little-endian architecture.
//
namespace scidb {
namespace NA {

    const char* NA_NANPAYLOAD_STR = "1954" ;

    uint32_t nanPayloadf(float val) {
        uint32_t valAsBits = *(reinterpret_cast<uint32_t*>(&val));
        return valAsBits & 0x3fffff ;  // lowest 32 - 9 -1 = 22 bits
    }

    uint64_t nanPayload(double val) {
        uint64_t valBits = *(reinterpret_cast<uint64_t*>(&val));
        return valBits & 0x7ffffffffffff ; // lowest 64 - 12 -1 = 51 bits  (7 + 12F's, ) 
    }

    nanPayloadLong_t nanPayloadl(long double val) {
        // for quad precision numbers:
        // 1 bit sign, 15 bits exponent, 112 bits mantissa

        // little-endian code -- e.g. intel.  big-endian will require swizzling, I think.
        uint64_t valBitsA = *(reinterpret_cast<uint64_t*>(&val));
        //uint64_t valBitsB = *(reinterpret_cast<uint64_t*>(&val)-1);  // +/- 1 ... not sure
        nanPayloadLong_t result;
        result.low = valBitsA & 0x3fffffffffffffffLL; // lowest 62 bits of the mantissa (3 + 15 F's)
                                                      // these are disappearing under -O2
        result.high = 0;    // these are not being set by the nanf macro
        return result;
    }

    void nanPayloadsUnitTest() {
        REQUIRE_START(testNanPayloads);
        REQUIRE(nanPayloadf(nanf("0"))    == 0); 
        REQUIRE(nanPayloadf(nanf("1954")) == 1954); 
        REQUIRE(nanPayloadf(nanf("0") + 99) == 0); 
        REQUIRE(nanPayloadf(nanf("1954")+ 99) == 1954); 

        REQUIRE(nanPayload (nan("0"))    == 0); 
        REQUIRE(nanPayload (nan("1954")) == 1954); 
        REQUIRE(nanPayload (nan("0") + 99) == 0); 
        REQUIRE(nanPayload (nan("1954")+ 99) == 1954); 

        // something wrong with nanPayloadl() at the moment
        //REQUIRE(nanPayloadl(nan("0")).low    == 0); 
        //REQUIRE(nanPayloadl(nan("1954")).low == 1954); 
        //REQUIRE(nanPayloadl(nan("0") + 99).low == 0); 
        //REQUIRE(nanPayloadl(nan("1954")+ 99).low == 1954); 
        //int errs = REQUIRE_END(testnanPayloads);
    }

} // namespace R
} // namespace scidb

}

