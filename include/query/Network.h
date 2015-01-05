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
 * @file network.h
 *
 * @author roman.simakov@gmail.com
 *
 * @brief network functions
 */

#ifndef NETWORK_H
#define NETWORK_H

#include "stdlib.h"
#include "array/Array.h"
#include "query/Query.h"

namespace scidb
{

void Receive(void* ctx, int instance, void* data, size_t size);
void Send(void* ctx, int instance, void const* data, size_t size);

void BufSend(InstanceID target, boost::shared_ptr<SharedBuffer> data, boost::shared_ptr< Query> query);
boost::shared_ptr<SharedBuffer> BufReceive(InstanceID source, boost::shared_ptr< Query> query);


}

#endif // NETWORK_H
