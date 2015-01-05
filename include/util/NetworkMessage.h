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
 * @file NetworkMessage.h
 * @brief Common network related types.
 */

#ifndef NETWORK_MESSAGE_H_
#define NETWORK_MESSAGE_H_

#include <boost/shared_ptr.hpp>
#include <google/protobuf/message.h>

namespace scidb
{
   typedef ::google::protobuf::Message Message;
   typedef boost::shared_ptr<Message> MessagePtr;
   typedef uint16_t MessageID;

   /// Reserved message ID not used for any message
   const MessageID SYSTEM_NONE_MSG_ID = 0;
   /// Message IDs for internal SciDB messages are strictly less than this value
   const MessageID SYSTEM_MAX_MSG_ID = 29;

   /**
    * Messageg types used by SciDB plugins
    */
   enum UserDefinedMessageType
   {
   mtMpiSlaveHandshake=SYSTEM_MAX_MSG_ID,
   mtMpiSlaveResult,
   mtMpiSlaveCommand
   };
}
#endif /* NETWORK_MESSAGE_H_ */
