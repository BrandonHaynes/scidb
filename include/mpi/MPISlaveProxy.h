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
 * @file MPISlaveProxy.h
 *      This class provides an interface to the MPI slave process, which is started as an MPI job
 */

#ifndef MPISLAVEPROXY_H_
#define MPISLAVEPROXY_H_

#include <boost/shared_ptr.hpp>
#include <util/Network.h>
#include <query/Query.h>
#include <mpi/MPIManager.h>
#include <mpi/MPIUtils.h>

using namespace std;

namespace scidb
{
    /**
     * An MPI-based operator running on the coordinator launches an MPI job that lands
     * on all SciDB instances in the form of MPI slaves. This proxy interface allows the MPI-based
     * operators running an all instances to communicate with the MPI slaves and manage their life-time.
     */
    class MpiSlaveProxy {
    public:
        class InvalidStateException: public SystemException
        {
        public:
            InvalidStateException(const char* file, const char* function, int32_t line)
            : SystemException(file, function, line, "scidb", SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR,
                              "SCIDB_SE_INTERNAL", "SCIDB_LE_UNKNOWN_ERROR", uint64_t(0))
            {
            }

            ~InvalidStateException() throw () {}
            void raise() const { throw *this; }
            template <class T>
            InvalidStateException& operator <<(const T &param)
            {
                return static_cast<InvalidStateException&>(scidb::SystemException::operator<<(param));
            }
        };

        friend boost::shared_ptr<MpiSlaveProxy> newMPISlaveProxyForTests(uint64_t launchId,
                                                                         const boost::shared_ptr<Query>& q,
                                                                         const std::string& installPath,
                                                                         uint32_t timeout, uint32_t delay);
        /**
         * Constructor, used only for testing purposes
         * @param launchId identifies the MPI job launch in which this slave participates
         * @param q current query
         * @param installPath installation directory of this SciDB instance (i.e. "data directory")
         * @param timeout time after which the following operations throw an exception:
         * waitForHandshake()
         * waitForExit()
         * @param delay in seconds, currently used to slowdown errorChecking in waitForExit()
         */
        MpiSlaveProxy(uint64_t launchId, const boost::shared_ptr<Query>& q,
                      const std::string& installPath, uint32_t timeout, uint32_t delay)
        : _launchId(launchId),
          _queryId(q->getQueryID()),
          _query(q),
          _installPath(installPath),
          _inError(false),
          _MPI_SLAVE_RESPONSE_TIMEOUT(timeout),
          _delayForTestingInSec(delay)
        {
            _pids.reserve(2);
        }

    public:
        /**
         * Constructor
         * @param launchId identifies the MPI job launch in which this slave participates
         * @param q current query
         * @param installPath installation directory of this SciDB instance (i.e. "data directory")
         * @param timeout time after which the following operations throw an exception:
         * waitForHandshake()
         * waitForExit()
         */
        MpiSlaveProxy(uint64_t launchId, const boost::shared_ptr<Query>& q,
                      const std::string& installPath, uint32_t timeout)
        : _launchId(launchId),
          _queryId(q->getQueryID()),
          _query(q),
          _installPath(installPath),
          _inError(false),
          _MPI_SLAVE_RESPONSE_TIMEOUT(timeout),
          _delayForTestingInSec(0)
        {
            _pids.reserve(2);
        }

        /**
         * Constructor with a default timeout equal to the '--liveness-timeout'
         * @param launchId identifies the MPI job launch in which this slave participates
         * @param q current query
         * @param installPath installation directory of this SciDB instance (i.e. "data directory")
         */
        MpiSlaveProxy(uint64_t launchId, const boost::shared_ptr<Query>& q,
                      const std::string& installPath)
        : _launchId(launchId),
          _queryId(q->getQueryID()),
          _query(q),
          _installPath(installPath),
          _inError(false),
          _MPI_SLAVE_RESPONSE_TIMEOUT(scidb::getLivenessTimeout()),
          _delayForTestingInSec(0)
        {
            _pids.reserve(2);
        }

        /// Destructor
        virtual ~MpiSlaveProxy() throw ()
        {
            if (_connection) {
                try {
                    _connection->disconnect();
                } catch (const scidb::Exception& ) {
                }
            }
        }

        /**
         * Wait for the handshake from the local MPI slave started by the MPI launcher (on the coordinator)
         * @param ctx current operator context where the MPI realted state is kept
         * @throw MpiSlaveProxy::InvalidStateException if the handshake has alredy been received
         * @throw scidb::SystemException if the wait exceeds the timout,
         * or if the handshake is malformed and/or cannot be obtained
         */
        void waitForHandshake(boost::shared_ptr<MpiOperatorContext>& ctx);

        /// @return pid,ppid of the MPI slave process
        const std::vector<pid_t>& getPids() const
        {
            return _pids;
        }

        /**
         * Send a command to the MPI slave
         * @param cmd command to send
         * @param ctx current operator context where the MPI realted state is kept
         * @throw MpiSlaveProxy::InvalidStateException if the handshake has not been received
         * @throw scidb::SystemException if the command cannot be sent
         */
        void sendCommand(mpi::Command& cmd, boost::shared_ptr<MpiOperatorContext>& ctx);

        /**
         * Wait for the last command status from the MPI slave
         * @param cmd command to send
         * @param ctx current operator context where the MPI realted state is kept
         * @param raise whether to raise an exception on non-zero status
         * @param return the command status from the MPI slave
         * @throw MpiSlaveProxy::InvalidStateException if the handshake has not been received
         * @throw scidb::SystemException if the status cannot be received
         * @note This method never times out. It waits for status as long as the slave
         * maintains its "client" connection to SciDB.
         */
        int64_t waitForStatus(boost::shared_ptr<MpiOperatorContext>& ctx, bool raise=true);

        /**
         * Wait for the the local MPI slave to exit and disconnect
         * (a well behaved slave should disconnect only on exit).
         * @param ctx current operator context where the MPI realted state is kept
         * @throw MpiSlaveProxy::InvalidStateException if the handshake has not been received
         * @throw scidb::SystemException if the wait exceeds the timout,
         * or if the handshake is malformed and/or cannot be obtained
         */
        void waitForExit(boost::shared_ptr<MpiOperatorContext>& ctx);

        /**
         * Attempt to kill the slave process (including its parent, orted)
         * and remove the pid files that the slave may have created.
         * It does not guarantee success, so the clean up needs to occur periodically
         * @param error if true, preserve MPI related logs
         * @see MpiManager::cleanup()
         */
        void destroy(bool error=false);

        /// @return the launch ID
        uint64_t getLaunchId() { return _launchId; }
    private:
        uint64_t _launchId;
        QueryID _queryId;
        boost::weak_ptr<scidb::Query> _query;
        std::vector<pid_t> _pids;
        ClientContext::Ptr _connection;
        std::string _installPath;
        bool _inError;
        const uint32_t _MPI_SLAVE_RESPONSE_TIMEOUT;
        const uint32_t _delayForTestingInSec;
    };

} //namespace

#endif
