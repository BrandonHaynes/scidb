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
 * @file Notification.h
 *
 * @brief A publish-subscribe mechanism for receiving arbitrary messages
 *
 */

#ifndef NOTIFICATION_H_
#define NOTIFICATION_H_

#include <boost/function.hpp>
#include <boost/shared_ptr.hpp>
#include <util/Mutex.h>

namespace scidb
{
   /**
    * @class Notification
    *
    * Provides basic functionality for sending and receiving arbitrary memory messages.
    * When invoking any of the methods below NO locks/mutexes must be held.
    * The usual usage is as follows:
    *
    * Publisher:
    * shared_ptr<SomMessageClassInstance> msg = ...
    * <... msg initialization goes here ...>
    * Notification<SomMessageClassInstance> event(msg);
    * event.publish();
    *
    * Subscriber:
    * void receiver(shared_ptr<SomMessageClassInstance> msg)
    * {
    * ...
    * }
    *
    * ListenerID _id;
    * {
    * ...
    * _id = Notification<SomMessageClassInstance>::addPublishListener(&receiver);
    * ...
    * Notification<SomMessageClassInstance>::removePublishListener(_id);
    * ...
    * }
    *
    *
    * Optionally, the publisher may listen for new publish subscriptions as follows:
    *
    * void getBusy()
    * {
    * < ... publish current state ...>
    * }
    * ListenerID id_;
    * {
    * ...
    * id_ = Notification<SomMessageClassInstance>::addSubscribeListener(&getBusy);
    * ...
    * // not publishing any more
    * Notification<SomMessageClassInstance>::removeSubscribeListener(id_);
    * ...
    * }
    *
    */
template <typename MessageType>
class Notification
{
 public:
   /// Message pointer type
   typedef boost::shared_ptr<const MessageType> MessageTypePtr;

   /// A listener functor for published messages
   typedef boost::function<void(MessageTypePtr)> PublishListener;

   /// A listener functor for "subscribe" events
   typedef boost::function<void()> SubscribeListener;

   /// Opaque registered listener ID
   typedef boost::shared_ptr<bool> ListenerID;

   /**
    * Notification constructor
    * @param msgPtr payload message pointer
    */
   Notification(MessageTypePtr& msgPtr)
   : _msgPtr(msgPtr)
   { }

   /// Destructor
   virtual ~Notification() {}

   /**
    * Notify all currently subscribed listeners with this notification
    */
   void publish()
   {
      notifyOnPublish(_msgPtr);
   }

   static ListenerID addPublishListener(PublishListener& lsnr)
   {
      assert(!lsnr.empty());
      ListenerID id(new bool);
      {
         ScopedMutexLock lock(_mutex);
         _publishListeners[id] = lsnr;
      }
      notifyOnSubscribe();
      return id;
   }

   static ListenerID addSubscribeListener(SubscribeListener& lsnr)
   {
      assert(!lsnr.empty());
      ListenerID id(new bool);
      ScopedMutexLock lock(_mutex);
      _subscribeListeners[id] = lsnr;
      return id;
   }

   static bool removePublishListener(ListenerID& id)
   {
      ScopedMutexLock lock(_mutex);
      return (_publishListeners.erase(id) == 1);
   }

   static bool removeSubscribeListener(ListenerID& id)
   {
      ScopedMutexLock lock(_mutex);
      return (_subscribeListeners.erase(id) == 1);
   }

 private:

   static void notifyOnPublish(MessageTypePtr msgPtr)
   {
      PublishListenerVec tmpListeners;
      {
         ScopedMutexLock lock(_mutex);
         tmpListeners.reserve(_publishListeners.size());
         for (typename PublishListenerMap::const_iterator i = _publishListeners.begin();
              i != _publishListeners.end(); ++i) {
            tmpListeners.push_back(i->second);
         }
      }
      for (typename PublishListenerVec::iterator i = tmpListeners.begin();
           i != tmpListeners.end(); ++i) {
         (*i)(msgPtr);
      }
   }

   static void notifyOnSubscribe()
   {
      SubscribeListenerVec tmpListeners;
      {
         ScopedMutexLock lock(_mutex);
         tmpListeners.reserve(_subscribeListeners.size());
         for (typename SubscribeListenerMap::const_iterator i = _subscribeListeners.begin();
              i != _subscribeListeners.end(); ++i) {
            tmpListeners.push_back(i->second);
         }
      }
      for (typename SubscribeListenerVec::iterator i = tmpListeners.begin();
           i != tmpListeners.end(); ++i) {
         (*i)();
      }
   }

   MessageTypePtr _msgPtr;

   typedef std::vector<PublishListener> PublishListenerVec;
   typedef std::map<ListenerID, PublishListener> PublishListenerMap;
   static PublishListenerMap _publishListeners;

   typedef std::vector<SubscribeListener> SubscribeListenerVec;
   typedef std::map<ListenerID, SubscribeListener> SubscribeListenerMap;
   static SubscribeListenerMap _subscribeListeners;

   static Mutex _mutex;
 };

 template <typename MessageType>
 typename Notification<MessageType>::PublishListenerMap Notification<MessageType>::_publishListeners;
 template <typename MessageType>
 typename Notification<MessageType>::SubscribeListenerMap Notification<MessageType>::_subscribeListeners;
 template <typename MessageType>
 Mutex Notification<MessageType>::_mutex;

} //namespace scidb

#endif
