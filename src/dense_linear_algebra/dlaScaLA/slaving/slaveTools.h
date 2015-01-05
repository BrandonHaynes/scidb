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

#ifndef SLAVE_TOOLS__H
#define SLAVE_TOOLS__H

//
// this file contains routines for supporting the scalapack slaves.
// anything that is done in common between the slaves in terms of using ScaLAPACK
// should be factored to this file.
//

#include <scalapackUtil/scalapackFromCpp.hpp>

namespace scidb {

///
/// a method that returns, for the given ScaLAPACK context (ICTXT), the 5 basic parameters
/// of that context:
/// @param NPROW  number of processes in a row of the process grid
/// @param NPCOL  number of processes in a column of the the process grid
/// @param MYPROW row of this process in the process grid
/// @param MYPCOL column of this process in the process grid
/// @param MYPNUM index of the process in the process grid
///
void getSlaveBLACSInfo(const slpp::int_t ICTXT, slpp::int_t& NPROW, slpp::int_t& NPCOL, slpp::int_t& MYPROW, slpp::int_t& MYPCOL, slpp::int_t& MYPNUM);


} // namespace scidb

///
/// a macro, so that it can be improved by using __LINE__, __FILE__ etc
///
#define SLAVE_ASSERT_ALWAYS(expr)           \
    {                                       \
        if (!(expr)) {                      \
            std::cerr << #expr << "false at: " <<  __FILE__ << " : " << __LINE__  << std::endl; \
            slpp::int_t ICTXT;              \
            blacs_get_(-1, 0, ICTXT);       \
            if(ICTXT < 1) {                 \
                ::abort();                  \
            } else {                        \
                blacs_abort_(ICTXT, 9999);  \
            }                               \
        }                                   \
    }


#endif // SLAVE_TOOLS__H

