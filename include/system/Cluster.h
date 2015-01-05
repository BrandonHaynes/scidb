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

#ifndef CLUSTER_H
#define CLUSTER_H

#include <vector>
#include <set>
#include <boost/shared_ptr.hpp>
#include <util/Singleton.h>
#include <array/Metadata.h>
#include <util/Notification.h>

namespace scidb
{
typedef uint64_t ViewID;

/**
 * Class that describes the cluster membership, i.e. all physical SciDB instances.
 */
class InstanceMembership
{
 public:
   InstanceMembership(ViewID viewId) : _viewId(viewId){}
   InstanceMembership(ViewID viewId,
                      boost::shared_ptr<const Instances>& instances)
   : _viewId(viewId), _instanceConfigs(instances)
   {
       assert(instances);
       const Instances& ins = *instances;
       for ( Instances::const_iterator i = ins.begin(); i != ins.end(); ++i) {
           _instances.insert(i->getInstanceId());
       }
   }
   virtual ~InstanceMembership() {}
   const std::set<InstanceID>& getInstances() const { return _instances; }
   ViewID getViewId() const { return _viewId; }
   /**
    * Get configuration information for all registered instances
    */
   const Instances& getInstanceConfigs() const { return *_instanceConfigs; }
   
   bool isEqual(const InstanceMembership& other) const
   {
      return ((_viewId == other._viewId) &&
              (_instances==other._instances));
   }
 private:
   InstanceMembership(const InstanceMembership&);
   InstanceMembership& operator=(const InstanceMembership&);
   
   ViewID _viewId;
   boost::shared_ptr<const scidb::Instances> _instanceConfigs;
   std::set<InstanceID> _instances;
};

/**
 * Class that describes the cluster liveness, i.e. dead/live status of all physical SciDD instances.
 * The view ID associated with a particular liveness must correspond to a particular membership.
 * For example, over the lifetime of a given membership with a given view ID there might be many
 * livenesses corresponding to the membership (via the view ID).
 */
 
 class InstanceLivenessEntry
 {
   public:
      InstanceLivenessEntry() : _generationId(0), _instanceId(INVALID_INSTANCE), _isDead(false)
      {}
      InstanceLivenessEntry(InstanceID instanceId, uint64_t generationId, bool isDead) :
      _generationId(generationId), _instanceId(instanceId), _isDead(isDead) {}
      virtual ~InstanceLivenessEntry() {}
      uint64_t getInstanceId() const { return _instanceId; }
      uint64_t getGenerationId() const { return _generationId; }
      bool isDead() const { return _isDead; }
      void setGenerationId(uint64_t id) {_generationId = id; }
      void setInstanceId(InstanceID id) {_instanceId = id; }
      void setIsDead(bool state) {_isDead = state; }

      bool operator<(const InstanceLivenessEntry& other) const {
         assert(_instanceId != INVALID_INSTANCE);
         assert(other._instanceId != INVALID_INSTANCE);
         return (_instanceId < other._instanceId);
      }
      bool operator==(const InstanceLivenessEntry& other) const {
         assert(_instanceId != INVALID_INSTANCE);
         assert(other._instanceId != INVALID_INSTANCE);
         return ((_instanceId != INVALID_INSTANCE) &&
                 (_instanceId == other._instanceId) &&
                 (_generationId == other._generationId) &&
                 (_isDead == other._isDead));
      }
      bool operator!=(const InstanceLivenessEntry& other) const {
          return !operator==(other);
      }
   private:
      InstanceLivenessEntry(const InstanceLivenessEntry&);
      InstanceLivenessEntry& operator=(const InstanceLivenessEntry&);
      uint64_t _generationId;
      InstanceID _instanceId;
      bool   _isDead;
   };

}  // namespace scidb

namespace boost
{
   bool operator< (boost::shared_ptr<const scidb::InstanceLivenessEntry> const& l,
                   boost::shared_ptr<const scidb::InstanceLivenessEntry> const& r);

   bool operator== (boost::shared_ptr<const scidb::InstanceLivenessEntry> const& l,
                    boost::shared_ptr<const scidb::InstanceLivenessEntry> const& r);

   bool operator!= (boost::shared_ptr<const scidb::InstanceLivenessEntry> const& l,
                    boost::shared_ptr<const scidb::InstanceLivenessEntry> const& r);

} // namespace boost

namespace std
{
   template<>
   struct less<boost::shared_ptr<const scidb::InstanceLivenessEntry> > :
   binary_function <const boost::shared_ptr<const scidb::InstanceLivenessEntry>,
                    const boost::shared_ptr<const scidb::InstanceLivenessEntry>,bool>
   {
      bool operator() (const boost::shared_ptr<const scidb::InstanceLivenessEntry>& l,
                       const boost::shared_ptr<const scidb::InstanceLivenessEntry>& r) const ;
   };
} // namespace std

namespace scidb
{
class InstanceLiveness
{
 public:
   typedef boost::shared_ptr<const InstanceLivenessEntry> InstancePtr;
   typedef std::set<InstancePtr> DeadInstances;
   typedef std::set<InstancePtr> LiveInstances;

   InstanceLiveness(ViewID viewId, uint64_t version) : _viewId(viewId), _version(version) {}
   virtual ~InstanceLiveness() {}
   const LiveInstances& getLiveInstances() const { return _liveInstances; }
   const DeadInstances& getDeadInstances() const { return _deadInstances; }
   ViewID   getViewId()  const { return _viewId; }
   uint64_t getVersion() const { return _version; }
   bool     isDead(const InstanceID& id) const { return find(_deadInstances, id); }
   size_t   getNumDead()  const { return _deadInstances.size(); }
   size_t   getNumLive()  const { return _liveInstances.size(); }
   size_t   getNumInstances() const { return getNumDead()+getNumLive(); }

   bool insert(const InstancePtr& key)
   {
      assert(key);
      if (key->isDead()) {
         if (find(_liveInstances, key)) {
            assert(false);
            return false;
         }
         return _deadInstances.insert(key).second;
      } else {
         if (find(_deadInstances, key)) {
            assert(false);
            return false;
         }
         return _liveInstances.insert(key).second;
      }
      assert(false);
      return false;
   }

   InstancePtr find(const InstanceID& instanceId) const
   {
      const InstancePtr key(new InstanceLivenessEntry(instanceId,0,false));
      InstancePtr val = find(_liveInstances, key);
      if (val) {
         assert(!val->isDead());
         return val;
      }
      val = find(_deadInstances, key);
      assert(!val || val->isDead());
      return val;
   }

   bool isEqual(const InstanceLiveness& other) const
   {
      return ((_viewId == other._viewId) &&
              (_deadInstances==other._deadInstances) &&
              (_liveInstances==other._liveInstances));
   }

 private:
   typedef std::set<InstancePtr> InstanceEntries;
   InstanceLiveness(const InstanceLiveness&);
   InstanceLiveness& operator=(const InstanceLiveness&);
   InstancePtr find(const InstanceEntries& instances, const InstanceID& instanceId) const
   {
      const InstancePtr key(new InstanceLivenessEntry(instanceId,0,false));
      return find(instances, key);
   }
   InstancePtr find(const InstanceEntries& instances, const InstancePtr& key) const
   {
      InstancePtr found;
      InstanceEntries::const_iterator iter = instances.find(key);
      if (iter != instances.end()) {
         found = (*iter);
      }
      return found;
   }
   ViewID _viewId;
   uint64_t _version;
   InstanceEntries _liveInstances;
   InstanceEntries _deadInstances;
};
   
typedef Notification<InstanceLiveness> InstanceLivenessNotification;
 
class Cluster: public Singleton<Cluster>
{
public:
   /**
    * Get cluster membership
    * @return current membership
    */ 
   boost::shared_ptr<const InstanceMembership> getInstanceMembership();

   /**
    * Get cluster liveness
    * @return current liveness
    */ 
   boost::shared_ptr<const InstanceLiveness> getInstanceLiveness();

   /**
    * Get this instances' ID
    */
   InstanceID getLocalInstanceId();

   /// Get the (globally unique?) UUID of this cluster
   const std::string& getUuid();

private:
    friend class Singleton<Cluster>;
    boost::shared_ptr<const InstanceMembership> _lastMembership;
    std::string _uuid;
    Mutex _mutex;
};

} // namespace

#endif // CLUSTER_H
