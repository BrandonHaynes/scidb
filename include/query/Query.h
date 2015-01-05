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
 * @file Query.h
 *
 * @author roman.simakov@gmail.com
 *
 * @brief Query context
 */

#ifndef QUERY_H_
#define QUERY_H_

#include <boost/enable_shared_from_this.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/scoped_array.hpp>
#include <string>
#include <queue>
#include <deque>
#include <stdio.h>
#include <list>
#include <boost/random/mersenne_twister.hpp>
#include <log4cxx/logger.h>

#include <array/Metadata.h>
#include <util/Thread.h>
#include <util/Semaphore.h>
#include <util/Event.h>
#include <util/RWLock.h>
#include <SciDBAPI.h>
#include <util/Arena.h>
#include <util/Atomic.h>
#include <query/Aggregate.h>
#include <query/Statistics.h>
#include <system/BlockCyclic.h>
#include <system/Cluster.h>
#include <system/Warnings.h>
#include <system/SystemCatalog.h>
#include <util/WorkQueue.h>

// XXX TODO: we should not modify std & boost namespaces
namespace boost
{
   bool operator< (boost::shared_ptr<scidb::SystemCatalog::LockDesc> const& l,
                   boost::shared_ptr<scidb::SystemCatalog::LockDesc> const& r);
   bool operator== (boost::shared_ptr<scidb::SystemCatalog::LockDesc> const& l,
                    boost::shared_ptr<scidb::SystemCatalog::LockDesc> const& r);
   bool operator!= (boost::shared_ptr<scidb::SystemCatalog::LockDesc> const& l,
                    boost::shared_ptr<scidb::SystemCatalog::LockDesc> const& r);
} // namespace boost

namespace std
{
   template<>
   struct less<boost::shared_ptr<scidb::SystemCatalog::LockDesc> > :
   binary_function <const boost::shared_ptr<scidb::SystemCatalog::LockDesc>,
                    const boost::shared_ptr<scidb::SystemCatalog::LockDesc>,bool>
   {
      bool operator() (const boost::shared_ptr<scidb::SystemCatalog::LockDesc>& l,
                       const boost::shared_ptr<scidb::SystemCatalog::LockDesc>& r) const ;
   };
} // namespace std

namespace scidb
{

class LogicalPlan;
class PhysicalPlan;
class RemoteArray;
class RemoteMergedArray;
class MessageDesc;
class ReplicationContext;

const size_t MAX_BARRIERS = 2;

/**
 * The query structure keeps track of query execution and manages the resources used by SciDB
 * in order to execute the query. The Query is a state of query processor to make
 * query processor stateless. The object lives while query is used including receiving results.
 */
class Query : public boost::enable_shared_from_this<Query>
{
 public:
    class OperatorContext
    {
      public:
        OperatorContext() {}
        virtual ~OperatorContext() = 0;
    };
    class ErrorHandler
    {
      public:
        virtual void handleError(const boost::shared_ptr<Query>& query) = 0;
        virtual ~ErrorHandler() {}
    };

    /**
     * This struct is used for propper accounting of outstanding requests/jobs.
     * The number of outstanding requests can be incremented when they arrive
     * and decremented when they have been processed.
     * test() indicates the arrival of the last request.
     * From that point on, when the count of outstanding requests drops to zerro,
     * it is an indication that all of the requests have been processed.
     * This mechanism is used for the sync() method used in SG and (for debugging) in replication.
     */
    class PendingRequests
    {
    private:
        Mutex  _mutex;
        size_t _nReqs;
        bool   _sync;
    public:
        size_t increment();
        bool decrement();
        bool test();

        PendingRequests() : _nReqs(0), _sync(false) {}
    };

    typedef boost::function<void(const boost::shared_ptr<Query>&)> Finalizer;

    typedef std::set<boost::shared_ptr<SystemCatalog::LockDesc> >  QueryLocks;

    typedef boost::function<void(const boost::shared_ptr<Query>&,InstanceID)> InstanceVisitor;

 private:

    /**
     * Hold next value for generation query ID
     */
     static uint32_t nextID;

    /**
     * Query identifier to find the query during asynchronous message exchanging.
     */
    QueryID _queryID;

    /**
     * The global list of queries present in the system
     */
    static std::map<QueryID, boost::shared_ptr<Query> > _queries;

    /**
     * Get/insert a query object from/to the global list of queries
     * @param query the query object to insert
     * @return the old query object if it is already on the list;
     *         otherwise, the newly inserted object specified by the argument
     */
    static boost::shared_ptr<Query> insert(const boost::shared_ptr<Query>& query);

    /**
     * Creates new query object detached from global list of queries
     */
    static boost::shared_ptr<Query> createDetached(QueryID queryID);

    /**
     * Initialize a query
     * @param coordID the "physical" coordinator ID (or COORDINATOR_INSTANCE if on coordinator)
     * @param localInstanceID  "physical" local instance ID
     * @param coordinatorLiveness coordinator liveness at the time of query creation
     */
    void init(InstanceID coordID,
              InstanceID localInstanceID,
              boost::shared_ptr<const InstanceLiveness>& coordinatorLiveness);

    void setCoordinatorLiveness(boost::shared_ptr<const InstanceLiveness>& liveness)
    {
       ScopedMutexLock cs(errorMutex);
       _coordinatorLiveness = liveness;
    }

    friend class ServerMessageHandleJob;

    boost::shared_ptr<OperatorContext> _operatorContext;

    /**
     * The physical plan of query. Optimizer generates it for current step of incremental execution
     * from current logical plan. This plan is generated on coordinator and sent out to every instance for execution.
     */
    std::vector< boost::shared_ptr<PhysicalPlan> > _physicalPlans;

    /**
     * Snapshot of the liveness information on the coordiantor
     * The worker instances must fail the query if their liveness view
     * is/becomes different any time during the query execution.
     */
    boost::shared_ptr<const InstanceLiveness> _coordinatorLiveness;

    /// Registration ID for liveness notifications
    InstanceLivenessNotification::ListenerID _livenessListenerID;

    /**
     * The list of instances considered alive for the purposes
     * of this query. It is initialized to the liveness of
     * the coordinator when it starts the query. If any instance
     * detects a discrepancy in its current liveness and
     * this query liveness, it causes the query to abort.
     */
    std::vector<InstanceID> _liveInstances;

    /**
     * A "logical" instance ID of the local instance
     * for the purposes of this query.
     * It is obtained from the "physical" instance ID using
     * the coordinator liveness as the map.
     * Currently, it is the index of the local instance into
     * the sorted list of live instance IDs.
     */
    InstanceID _instanceID;

    /**
     * The "logical" instance ID of the instance responsible for coordination of query.
     * COORDINATOR_INSTANCE if instance execute this query itself.
     */
    InstanceID _coordinatorID;

    std::vector<Warning> _warnings;

    /**
     * Error state
     */
    Mutex errorMutex;

    boost::shared_ptr<Exception> _error;

    // RNG
    static boost::mt19937 _rng;

    friend class UpdateErrorHandler;
    /**
     * This function should only be called if the query is (being) aborted.
     * @return true if the local query execution finished successfully AND the coordinator was notified;
     * false otherwise i.e. the local execution failed before notifying the coordinator,
     * which couldn't possibly commit this query.
     */
    bool isForceCancelled()
    {
       ScopedMutexLock cs(errorMutex);
       assert (_commitState==ABORTED);
       bool result = (_completionStatus == OK);
       return result;
    }
    bool checkFinalState();
    Mutex _warningsMutex;

    /// Query array locks requested by the operators
    std::set<boost::shared_ptr<SystemCatalog::LockDesc> >  _requestedLocks;
    std::deque< boost::shared_ptr<ErrorHandler> > _errorHandlers;
    std::deque<Finalizer> _finalizers; // last minute actions

    /** execution of query completion status */
    enum CompletionStatus
    {
        INIT, // query execute() has not started
        START, // query execute() has not completed
        OK,   // query execute() completed with no errors
        ERROR // query execute() completed with errors
    }  _completionStatus;

    /** Query commit state */
    enum CommitState
    {
        UNKNOWN,
        COMMITTED, // _completionStatus!=ERROR
        ABORTED
    } _commitState;

    /**
     * Queue for MPI-style buffer messages
     */
    boost::shared_ptr<scidb::WorkQueue> _bufferReceiveQueue;

    /**
     * FIFO queue for error messages
     */
    boost::shared_ptr<scidb::WorkQueue> _errorQueue;

    /**
     * FIFO queue for SG messages
     */
    boost::shared_ptr<scidb::WorkQueue> _operatorQueue;

    /**
     * The state required to perform replication during execution
     */
    boost::shared_ptr<ReplicationContext> _replicationCtx;

    /**
     *  Helper to invoke the finalizers with exception handling
     */
    void invokeFinalizers(std::deque<Finalizer>& finalizers);

    /**
     *  Helper to invoke the finalizers with exception handling
     */
    void invokeErrorHandlers(std::deque< boost::shared_ptr<ErrorHandler> >& errorHandlers);

    void destroy();
    static void destroyFinalizer(const boost::shared_ptr<Query>& q)
    {
        assert(q);
        q->destroy();
    }

    /**
     * Destroy specified query context
     */
    static void freeQuery(QueryID queryID);

    /**
     * Acquire a set of SystemCatalog locks
     */
    void acquireLocksInternal(Query::QueryLocks& locks);

    /**
     * The result of query execution. It lives while the client connection is established.
     * In future we can develop more useful policy of result keeping with multiple
     * re-connections to query.
     */
    boost::shared_ptr<Array> _currentResultArray;

    /**
     * TODO: XXX
     */
    boost::shared_ptr<RemoteMergedArray> _mergedArray;

    /**
     * TODO: XXX
     */
    std::vector<boost::shared_ptr<RemoteArray> > _remoteArrays;

    /**
     * Time of query creation;
     */
    time_t _creationTime;

    /**
     * Used counter - increased for every handler which process Query and decreased after.
     * 0 means that client did not fetching data and query was executed.
     */
    int _useCounter;

    void checkNoError() const
    {
        if (SCIDB_E_NO_ERROR != _error->getLongErrorCode())
        {
            // note: error code can be SCIDB_LE_QUERY_ALREADY_COMMITED
            //       because ParallelAccumulatorArray is started
            //       regardless of whether the client pulls the data (even in case of mutating queries).
            //       So, the client can request a commit before PAA is done.
            _error->raise();
        }
    }

    /**
     * true if the query acquires exclusive locks
     */
    bool _doesExclusiveArrayAccess;

    /**
    * cache for the ProGrid, which depends only on numInstances
    */
    mutable ProcGrid* _procGrid; // only access via getProcGrid()

    /**
     * The mutex to serialize access to _queries map.
     */
    static Mutex queriesMutex;

    /**
     * A dedicated arena from which this query can allocate the various resources that
     * it needs to execute.
     */
     arena::ArenaPtr _arena;

 public:

    explicit Query(QueryID querID);
    ~Query();

    // Logger for query processor.
    static log4cxx::LoggerPtr _logger;

    /**
     * @return amount of time to wait before trying to acquire an array lock
     */
    static uint64_t getLockTimeoutNanoSec();

    /**
     * Put this thread to sleep for some time
     * (before trying to acquire a SystemCatalog lock again)
     */
    static void waitForSystemCatalogLock();

    /**
     * Generate unique(?) query ID
     */
    static QueryID generateID();

    /**
     * @return the number of queries currently in the system
     * @param class Observer_tt
     *{
     *  bool operator!();
     *
     *  void operator()(const shared_ptr<scidb::Query>&)
     *}
     * It is not allowed to take any locks.
     */
    template<class Observer_tt>
    static size_t listQueries(Observer_tt& observer);

    /**
     * Add an error handler to run after a query's "main" routine has completed
     * and the query needs to be aborted/rolled back
     * @param eh - the error handler
     */
    void pushErrorHandler(const boost::shared_ptr<ErrorHandler>& eh);

    /**
     * Add a finalizer to run after a query's "main" routine has completed (with any status)
     * and the query is about to be removed from the system
     * @param f - the finalizer
     */
    void pushFinalizer(const Finalizer& f);

    Mutex resultCS; /** < Critical section for SG result */

    bool isDDL;

    /**
     * Program options which is used to run query
     */
    std::string programOptions;

    /**
     * Handle a change in the local instance liveness. If the new livenes is different
     * from this query's coordinator liveness, the query is marked to be aborted.
     */
    void handleLivenessNotification(boost::shared_ptr<const InstanceLiveness>& newLiveness);
    /**
     * Map a "logical" instance ID to a "physical" one using the coordinator liveness
     */
    InstanceID mapLogicalToPhysical(InstanceID instance);
    /**
     * Map a "physical" instance ID to a "logical" one using the coordinator liveness
     */
    InstanceID mapPhysicalToLogical(InstanceID instance);

    /**
     * @return true if a given instance is considered dead
     * @param instance physical ID of a instance
     * @throw scidb::SystemException if this.errorCode is not 0
     */
    bool isPhysicalInstanceDead(InstanceID instance);

    /**
     * Get the "physical" instance ID of the coordinator
     * @return COORDINATOR_INSTANCE if this instance is the coordinator, else the coordinator instance ID
     */
    InstanceID getPhysicalCoordinatorID();

    /**
     * Get the "physical" instance ID of the coordinator
     * @return the coordinator instance ID
     */
    InstanceID getCoordinatorPhysicalInstanceID();

    /**
     * Get logical instance count
     */
    size_t getInstancesCount() const
    {
        return _liveInstances.size();
    }

    /**
     * Return the arena that is owned by this query and from which the various
     * resources it needs in order to execute should be allocated.
     */
    arena::ArenaPtr getArena() const
    {
        return _arena;
    }

    /**
     *  Return true if the query completed successfully and was committed.
     */
    bool wasCommitted() const
    {
        return _commitState == COMMITTED;
    }

    /**
     * Execute a given routine for every live instance
     * @param func routine to execute
     */
    void listLiveInstances(InstanceVisitor& func);

    /**
     * Info needed for ScaLAPACK-compatible chunk distributions
     * Redistribution code and ScaLAPACK-based plugins need this,
     * most operators do not.
     */
    const ProcGrid* getProcGrid() const;

    /**
     * Get logical instance ID
     */
    InstanceID getInstanceID() const
    {
        return _instanceID;
    }
    /**
     * Get coordinator's logical instance ID
     */
    InstanceID getCoordinatorID()
    {
        return _coordinatorID;
    }

    InstanceID getCoordinatorInstanceID()
    {
        return _coordinatorID == COORDINATOR_INSTANCE ? _instanceID : _coordinatorID;
    }

    bool isCoordinator()
    {
        return (_coordinatorID == COORDINATOR_INSTANCE);
    }

    boost::shared_ptr<const InstanceLiveness> getCoordinatorLiveness()
    {
       return _coordinatorLiveness;
    }

    /**
     * The string with query that user want to execute.
     */
    std::string queryString;

    boost::shared_ptr<Array> getCurrentResultArray()
    {
        ScopedMutexLock cs(errorMutex);
        validate();
        return _currentResultArray;
    }

    void setCurrentResultArray(const boost::shared_ptr<Array>& array)
    {
        ScopedMutexLock cs(errorMutex);
        validate();
        _currentResultArray = array;
    }

    boost::shared_ptr<RemoteMergedArray> getMergedArray()
    {
        ScopedMutexLock cs(errorMutex);
        validate();
        return _mergedArray;
    }

    void setMergedArray(const boost::shared_ptr<RemoteMergedArray>& array)
    {
        ScopedMutexLock cs(errorMutex);
        validate();
        _mergedArray = array;
    }


    boost::shared_ptr<RemoteArray> getRemoteArray(const InstanceID& instanceID)
    {
        ScopedMutexLock cs(errorMutex);
        validate();
        assert(!_remoteArrays.empty());
        assert(instanceID < _remoteArrays.size());
        return _remoteArrays[instanceID];
    }

    void setRemoteArray(const InstanceID& instanceID, const boost::shared_ptr<RemoteArray>& array)
    {
        ScopedMutexLock cs(errorMutex);
        validate();
        assert(!_remoteArrays.empty());
        assert(instanceID < _remoteArrays.size());
        _remoteArrays[instanceID] = array;
    }

    Statistics statistics;

    /**
     * The logical plan of query. QueryProcessor generates it by parser only at coordinator instance.
     * Since we use incremental optimization this is the rest of logical plan to be executed.
     */
    boost::shared_ptr<LogicalPlan> logicalPlan;

    /**
     * Request that a given array lock be acquired before the query execution starts
     * @param lock - the lock description
     * @return either the requested lock or the lock that has already been requested for the same array
     *         with a more exclusive mode (RD < WR,CRT,RM,RNF,RNT)
     * @see scidb::SystemCatalog::LockDesc
     */
    boost::shared_ptr<SystemCatalog::LockDesc>
    requestLock(boost::shared_ptr<SystemCatalog::LockDesc>& lock);

    void addPhysicalPlan(boost::shared_ptr<PhysicalPlan> physicalPlan)
    {
        _physicalPlans.push_back(physicalPlan);
    }

    boost::shared_ptr<PhysicalPlan> getCurrentPhysicalPlan()
    {
        return _physicalPlans.back();
    }

    /**
     * Get the queue for delivering buffer-send (mtMPISend) messages
     * @return empty pointer if the query is no longer active
     */
    boost::shared_ptr<scidb::WorkQueue> getBufferReceiveQueue()
    {
        ScopedMutexLock cs(errorMutex);
        validate();
        assert(_bufferReceiveQueue);
        return _bufferReceiveQueue;
    }

    boost::shared_ptr<scidb::WorkQueue> getErrorQueue()
    {
        ScopedMutexLock cs(errorMutex);
        return _errorQueue;
    }

    boost::shared_ptr<scidb::WorkQueue> getOperatorQueue()
    {
        ScopedMutexLock cs(errorMutex);
        validate();
        assert(_operatorQueue);
        return _operatorQueue;
    }

    boost::shared_ptr<scidb::ReplicationContext> getReplicationContext()
    {
        ScopedMutexLock cs(errorMutex);
        validate();
        assert(_replicationCtx);
        return _replicationCtx;
    }

    /**
     *  Context variables to control thread
     */
    Semaphore results;

    /**
     * Semaphores for synchronization SG operations on remote instances
     */
    Semaphore semSG[MAX_BARRIERS];
    Semaphore syncSG;

    std::vector<PendingRequests> chunkReqs;

    /// Create a fake query that does not correspond to a user-generated request
    /// for internal purposes only
    static boost::shared_ptr<Query> createFakeQuery(InstanceID coordID,
                                                    InstanceID localInstanceID,
                                                    boost::shared_ptr<const InstanceLiveness> liveness,
                                                    int32_t *longErrorCode=NULL);
    /// Destroy a query generated by createFakeQuery()
    static void destroyFakeQuery(Query* q);

    /**
     * Creates new query object and generate new queryID
     */
    static boost::shared_ptr<Query> create(QueryID queryId, InstanceID instanceId=COORDINATOR_INSTANCE);

    /**
     * Find query with given queryID in the global query list
     */
    static boost::shared_ptr<Query> getQueryByID(QueryID queryID, bool raise = true);

    /**
     * Validates the pointer and the query it points for errors
     * @throws scidb::SystemException if the pointer is empty or if the query is in error state
     * @return true if no exception is thrown
     */
    static bool validateQueryPtr(const boost::shared_ptr<Query>& query)
    {
#ifndef SCIDB_CLIENT
        if (!query) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_QUERY_NOT_FOUND2);
        }
        return query->validate();
#else
        return true;
#endif
    }

    /**
     * Creates and validates the pointer and the query it points to for errors
     * @throws scidb::SystemException if the pointer is dead or if the query is in error state
     * @return a live query pointer if no exception is thrown
     */
    static boost::shared_ptr<Query> getValidQueryPtr(const boost::weak_ptr<Query>& query)
    {
        boost::shared_ptr<Query> q(query.lock());
        validateQueryPtr(q);
        return q;
    }

    /**
     * Destroys query contexts for every still existing query
     */
    static void freeQueries();

    /**
     * Release all the locks previously acquired by acquireLocks()
     * @param query whose locks to release
     * @throws exceptions while releasing the lock
     */
    static void releaseLocks(const boost::shared_ptr<Query>& query);

    /**
     * Get temporary or persistent array
     * @param arrayName array name
     * @return reference to the array
     * @exception SCIDB_LE_ARRAY_DOESNT_EXIST
     */
    boost::shared_ptr<Array> getArray(std::string const& arrayName);

    /**
     * Associate temporay array with this query
     * @param tmpArray temporary array
     */
    void setTemporaryArray(boost::shared_ptr<Array> const& tmpArray);

    /**
     * Repeatedly execute given work until it either succeeds
     * or throws an unrecoverable exception.
     * @param work to execute
     * @param tries count of tries, -1 to infinite
     * @return result of running work
     */
    template<typename T, typename E>
    static T runRestartableWork(boost::function<T()>& work, int tries = -1);

    /**
     * Acquire all locks requested via requestLock(),
     * @throw scidb::SystemCatalog::LockBusyException if any of the locks are already taken (by other queries).
     *        Any locks that have been successfully acquired remain in that state.
     *        Any subsequent attempts to acquire the remaining locks should be done using retryAcquireLocks()
     * @throws exceptions while acquiring the locks and the same exceptions as validate()
     */
    void acquireLocks();

    /**
     * Acquire all locks requested via requestLock(). This method should be invoked only if
     * a previous call to acquireLocks() has failed with scidb::LockBusyException
     * @throw scidb::SystemCatalog::LockBusyException if any of the locks are already taken (by other queries).
     *        Any locks that have been successfully acquired remain in that state.
     *        Any subsequent attempts to acquire the remaining locks should be done using retryAcquireLocks()
     * @throws exceptions while acquiring the locks and the same exceptions as validate()
     */
    void retryAcquireLocks();

    /**
     * @return  true if the query acquires exclusive locks
     */
    bool doesExclusiveArrayAccess();

    /**
     * Handle a query error.
     * May attempt to invoke error handlers
     */
    void handleError(const boost::shared_ptr<Exception>& unwindException);

    /**
     * Handle a client complete request
     */
    void handleComplete();

    /**
     * Handle a client cancellation request
     */
    void handleCancel();

    /**
     * Handle a coordinator commit request
     */
    void handleCommit();

    /**
     * Handle a coordinator abort request
     */
    void handleAbort();

    /**
     * Retursns query ID
     */
    QueryID getQueryID() const
    {
        return _queryID;
    }

    /**
     * Return current queryID for thread.
     */
    static QueryID getCurrentQueryID();

    /**
     * Set current queryID for thread.
     */
    static void setCurrentQueryID(QueryID queryID);

    /**
     * Set result SG context. Thread safe.
     */
    void setOperatorContext(boost::shared_ptr<OperatorContext> const& opContext,
                            boost::shared_ptr<JobQueue> const& jobQueue = boost::shared_ptr<JobQueue>());

    /**
     * Remove result SG context.
     */
    void unsetOperatorContext();

    /**
     * @return SG Context. Thread safe.
     * wait until context is not NULL,
     */
    boost::shared_ptr<OperatorContext> getOperatorContext(){
        ScopedMutexLock lock(errorMutex);
        return _operatorContext;
    }

    /**
     * Mark query as started
     */
    void start();

    /**
     * Suspend query processsing: state will be INIT
     */
    void stop();

    /**
     * Mark query as completed
     */
    void done();

    /**
     * Mark query as completed with an error
     */
    void done(const boost::shared_ptr<Exception> unwindException);

    /**
     * This section describe member fields needed for implementing send/receive functions.
     */
    Mutex _receiveMutex; //< Mutex for serialization access to _receiveXXX fields.

    /**
     * This vector holds send/receive messages for current query at this instance.
     * index in vector is source instance number.
     */
    std::vector< std::list<boost::shared_ptr< MessageDesc> > > _receiveMessages;

    /**
     * This vector holds semaphores for working with messages queue. One semaphore for every source instance.
     */
    std::vector< Semaphore> _receiveSemaphores;

    void* userDefinedContext;

    /**
     * Write statistics of query into output stream
     */
    std::ostream& writeStatistics(std::ostream& os) const;

    /**
     * Statistics monitor for query
     */
    boost::shared_ptr<StatisticsMonitor> statisticsMonitor;

    /**
     * Validates the query for errors
     * @throws scidb::SystemException if the query is in error state
     * @return true if no exception is thrown
     */
    bool validate();

    void postWarning(const class Warning& warn);

    std::vector<Warning> getWarnings();

    void clearWarnings();

    time_t getCreationTime() const {
        return _creationTime;
    }

    int32_t getErrorCode() const {
        return _error ? _error->getLongErrorCode() : 0;
    }

    std::string getErrorDescription() const {
        return _error ? _error->getErrorMessage() : "";
    }

    /**
     * @return true if the query has been executed,
     * but no processing is currently happening (i.e. the client is not fetching)
     */
    bool idle() const {
        return ((_completionStatus == OK ||
                 _completionStatus == ERROR) &&
                // one ref is in Query::_queries another is shared_from_this()
                // more refs indicate that some jobs/iterators are using the query
                shared_from_this().use_count() < 3);
    }

private:
     //disable copying
     Query(const Query&);
     Query& operator=(const Query&);
 };

class UpdateErrorHandler : public Query::ErrorHandler
{
 public:
    typedef boost::function< void(VersionID,ArrayID,ArrayID) > RollbackWork;

    UpdateErrorHandler(const boost::shared_ptr<SystemCatalog::LockDesc> & lock)
    : _lock(lock) { assert(_lock); }
    virtual ~UpdateErrorHandler() {}
    virtual void handleError(const boost::shared_ptr<Query>& query);

    static void releaseLock(const boost::shared_ptr<SystemCatalog::LockDesc>& lock,
                            const boost::shared_ptr<Query>& query);

    static void handleErrorOnCoordinator(const boost::shared_ptr<SystemCatalog::LockDesc>& lock,
                                         RollbackWork& rw);
    static void handleErrorOnWorker(const boost::shared_ptr<SystemCatalog::LockDesc>& lock,
                                    bool forceCoordLockCheck,
                                    RollbackWork& rw);
 private:
    static void doRollback(VersionID lastVersion,
                           ArrayID   baseArrayId,
                           ArrayID   newArrayId);
    void _handleError(const boost::shared_ptr<Query>& query);

    UpdateErrorHandler(const UpdateErrorHandler&);
    UpdateErrorHandler& operator=(const UpdateErrorHandler&);

 private:
    const boost::shared_ptr<SystemCatalog::LockDesc> _lock;
    static log4cxx::LoggerPtr _logger;
};


class RemoveErrorHandler : public Query::ErrorHandler
{
 public:
    RemoveErrorHandler(const boost::shared_ptr<SystemCatalog::LockDesc> & lock)
    : _lock(lock) { assert(_lock); }
    virtual ~RemoveErrorHandler() {}
    virtual void handleError(const boost::shared_ptr<Query>& query);

    static bool handleRemoveLock(const boost::shared_ptr<SystemCatalog::LockDesc>& lock,
                                 bool forceLockCheck);
 private:
    RemoveErrorHandler(const RemoveErrorHandler&);
    RemoveErrorHandler& operator=(const RemoveErrorHandler&);
 private:
    const boost::shared_ptr<SystemCatalog::LockDesc> _lock;
    static log4cxx::LoggerPtr _logger;
};

template<class Observer_tt>
size_t Query::listQueries(Observer_tt& observer)
{
    ScopedMutexLock mutexLock(queriesMutex);

    if (!observer) {
        return _queries.size();
    }

    for (std::map<QueryID, boost::shared_ptr<Query> >::const_iterator q = _queries.begin();
         q != _queries.end(); ++q) {
        observer(q->second);
    }
    return _queries.size();

}

class BroadcastAbortErrorHandler : public Query::ErrorHandler
{
 public:
    virtual void handleError(const boost::shared_ptr<Query>& query);
    virtual ~BroadcastAbortErrorHandler() {}
 private:
    static log4cxx::LoggerPtr _logger;
};

class ReplicationManager;
/**
 * The neccesary context to perform replication
 * during the execution of a query.
 */
class ReplicationContext
{
 private:

    typedef ArrayID QueueID;

    /**
     * Internal triplet container class.
     *  It is used to hold the info needed for replication:
     *  - WorkQueue where incoming replication messages inserted
     *  - Array where the replicas are to be written
     *  - Semaphore for signalling when all replicas sent
     *    from this instance to all other instances are written
     */
    class QueueInfo
    {
    public:
        explicit QueueInfo(const boost::shared_ptr<scidb::WorkQueue>& q)
        : _wq(q) { assert(q); }
        virtual ~QueueInfo()
        {
            if (_wq) { _wq->stop(); }
        }
        boost::shared_ptr<scidb::WorkQueue> getQueue()     { return _wq; }
        boost::shared_ptr<scidb::Array>     getArray()     { return _array; }
        scidb::Semaphore&                   getSemaphore() { return _replicaSem; }
        void setArray(const boost::shared_ptr<scidb::Array>& arr) { _array=arr; }
    private:
        boost::shared_ptr<scidb::WorkQueue> _wq;
        boost::shared_ptr<scidb::Array>     _array;
        Semaphore _replicaSem;
    private:
        QueueInfo(const QueueInfo&);
        QueueInfo& operator=(const QueueInfo&);
    };
    typedef boost::shared_ptr<QueueInfo> QueueInfoPtr;
    typedef std::map<QueueID, QueueInfoPtr>  QueueMap;

    /**
     * Get inbound replication queue information for an id
     * @param id queue ID
     */
    QueueInfoPtr getQueueInfo(QueueID id);

    /**
     * Get inbound replication queue for a given ArrayID
     * @param arrId array ID
     * @return WorkQueue for enqueing replication jobs
     */
    boost::shared_ptr<scidb::WorkQueue> getInboundQueue(ArrayID arrId);

 private:

    Mutex _mutex;
    QueueMap _inboundQueues;
    boost::weak_ptr<Query> _query;
    static ReplicationManager* _replicationMngr;

 public:
    /**
     * Constructor
     * @param query
     * @param nInstaneces
     */
    explicit ReplicationContext(const boost::shared_ptr<Query>& query, size_t nInstances);
    /// Destructor
    virtual ~ReplicationContext() {}

    /**
     * Set up and start an inbound replication queue
     * @param arrId array ID of arr
     * @param arr array to which write replicas
     */
    void enableInboundQueue(ArrayID arrId, const boost::shared_ptr<scidb::Array>& arr);

    /**
     * Enqueue a job to write a remote instance replica locally
     * @param arrId array ID to locate the appropriate queue
     * @param job replication job to enqueue
     */
    void enqueueInbound(ArrayID arrId, boost::shared_ptr<Job>& job);

    /**
     * Wait until all replicas originated on THIS instance have been written to the REMOTE instances
     * @param arrId array ID to identify the replicas
     */
    void replicationSync(ArrayID arrId);

    /**
     * Acknowledge processing of the last replication job from this instance on sourceId
     * @param sourceId instance ID where the replicas originated on this instance have been processed
     * @param arrId array ID to identify the replicas
     */
    void replicationAck(InstanceID sourceId, ArrayID arrId);

    /**
     * Remove the inbound replication queue and any related state
     * @param arrId array ID to locate the appropriate queue
     * @note It is the undo of enableInboundQueue()
     * @note currently NOOP
     */
    void removeInboundQueue(ArrayID arrId);

    /**
     * Get the persistent array for writing replicas
     * @param arrId array ID to locate the appropriate queue
     */
    boost::shared_ptr<scidb::Array> getPersistentArray(ArrayID arrId);

 public:

#ifndef NDEBUG // for debugging
    std::vector<Query::PendingRequests> _chunkReplicasReqs;
#endif
};

template<typename T, typename E>
T Query::runRestartableWork(boost::function<T()>& work, int tries)
{
    assert(work);
    int counter = tries;
    while (true)
    {
        //Run work
        try {
            return work();
        }
        //Detect recoverable exception
        catch (const E& e)
        {
            if (counter >= 0)
            {
                counter--;

                if (counter < 0)
                {
                    LOG4CXX_ERROR(_logger, "Query::runRestartableWork: Unable to restart work after " << tries << " tries");
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_CANNOT_RECOVER_RESTARTABLE_WORK);
                }
            }

            LOG4CXX_ERROR(_logger, "Query::runRestartableWork:"
                          << " Exception: "<< e.what()
                          << " will attempt to restart the operation");
            Thread::nanoSleep(getLockTimeoutNanoSec());
        }
    }
    assert(false);
    return T();
}

} // namespace

#endif /* QUERY_H_ */
