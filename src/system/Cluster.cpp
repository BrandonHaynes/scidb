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
 * @file Cluster.h
 *
 * @brief Contains class for providing information about cluster
 *
 * @author roman.simakov@gmail.com
 */
#include <string>
#include <boost/shared_ptr.hpp>
#include <system/Cluster.h>
#include <array/Metadata.h>
#include <network/NetworkManager.h>
#include <system/SystemCatalog.h>

namespace scidb
{

/**
 * @todo XXX Use NetworkManager until we explicitely start managing the membership
 */
boost::shared_ptr<const InstanceMembership> Cluster::getInstanceMembership()
{
   ScopedMutexLock lock(_mutex);

   if (_lastMembership) {
      return _lastMembership;
   }
   boost::shared_ptr<const Instances> instances = NetworkManager::getInstance()->getInstances();
   _lastMembership = boost::shared_ptr<const InstanceMembership>(new InstanceMembership(0, instances));
   return _lastMembership;
}

boost::shared_ptr<const InstanceLiveness> Cluster::getInstanceLiveness()
{
   boost::shared_ptr<const InstanceLiveness> liveness(NetworkManager::getInstance()->getInstanceLiveness());
   if (liveness) {
      return liveness;
   }
   boost::shared_ptr<const InstanceMembership> membership(getInstanceMembership());
   boost::shared_ptr<InstanceLiveness> newLiveness(new InstanceLiveness(membership->getViewId(), 0));
   for (std::set<InstanceID>::const_iterator i = membership->getInstances().begin();
        i != membership->getInstances().end(); ++i) {
      InstanceID instanceId(*i);
      InstanceLiveness::InstancePtr entry(new InstanceLivenessEntry(instanceId, 0, false));
      newLiveness->insert(entry);
   }
   liveness = newLiveness;
   assert(liveness->getNumLive() > 0);
   return liveness;
}

InstanceID Cluster::getLocalInstanceId()
{
   return NetworkManager::getInstance()->getPhysicalInstanceID();
}

const string& Cluster::getUuid()
{
    if (_uuid.empty()) {
        _uuid = SystemCatalog::getInstance()->getClusterUuid();
    }
    return _uuid;
}

} // namespace scidb

namespace boost
{
   bool operator< (boost::shared_ptr<const scidb::InstanceLivenessEntry> const& l,
                   boost::shared_ptr<const scidb::InstanceLivenessEntry> const& r) {
      if (!l || !r) {
         assert(false);
         return false;
      }
      return (*l.get()) < (*r.get());
   }

   bool operator== (boost::shared_ptr<const scidb::InstanceLivenessEntry> const& l,
                    boost::shared_ptr<const scidb::InstanceLivenessEntry> const& r) {

      if (!l || !r) {
         assert(false);
         return false;
      }
      return (*l.get()) == (*r.get());
   }

   bool operator!= (boost::shared_ptr<const scidb::InstanceLivenessEntry> const& l,
                    boost::shared_ptr<const scidb::InstanceLivenessEntry> const& r) {
       return (!operator==(l,r));
   }
} // namespace boost

namespace std
{
   bool less<boost::shared_ptr<const scidb::InstanceLivenessEntry> >::operator() (
                               const boost::shared_ptr<const scidb::InstanceLivenessEntry>& l,
                               const boost::shared_ptr<const scidb::InstanceLivenessEntry>& r) const
   {
      return l < r;
   }
}
