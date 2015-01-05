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
package org.scidb.client;

import org.scidb.io.network.Message;
import org.scidb.io.network.Message.QueryResult;
import org.scidb.io.network.Network;

import java.io.IOException;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

/**
 * SciDB connection
 */
public class Connection
{
    private Network net;
    private boolean afl = false;
    private long _queryId = 0;
    private String _queryStr = "";
    private WarningCallback warningCallback;
    private List<Long> _activeQueries = new ArrayList<Long>();

    private static Logger log = Logger.getLogger(Connection.class.getName());

    /**
     * Constructor
     */
    public Connection()
    {
        net = new Network();
    }

    /**
     * Connect to specified SciDB instance
     * @param host Host name
     * @param port Port number
     * @throws Error
     * @throws IOException
     */
    public void connect(String host, int port) throws Error, IOException
    {
        net.connect(host, port);
    }

    /**
     * Close network connection
     * @throws IOException
     */
    public void close() throws IOException
    {
        net.disconnect();
    }

    /**
     * Check if connected to server
     * @return true if connected
     */
    public boolean connected()
    {
        return net.isConnected();
    }

    /**
     * Prepare query
     * @param queryString Query string
     * @return Result with prepared query ID
     * @throws Error
     * @throws IOException
     */
    public Result prepare(String queryString) throws Error, IOException
    {
        _queryStr = queryString;
        log.fine(String.format("Preparing query '%s'", queryString));
        Message msg = new Message.Query(0, queryString, afl, "", false);
        net.write(msg);
        msg = net.read();

        switch (msg.getHeader().messageType)
        {
            case Message.mtQueryResult:
                log.fine("Got result from server");
                Result res = new Result((QueryResult) msg, this);
                _queryId = res.getQueryId();
                return res;

            case Message.mtError:
                log.fine("Got error message from server");
                throw new Error((Message.Error) msg);

            default:
                log.severe("Got unhandled network message during execution");
                throw new Error(String.format("Can not handle network message '%s'",
                        msg.getHeader().messageType));
        }
    }

    /**
     * Set query execution mode to AFL or language
     * @param afl true - AFL, false - AQL
     */
    public void setAfl(boolean afl)
    {
        this.afl = afl;
    }

    /**
     * Return AFL flag
     * @return true if AFL mode
     */
    public boolean isAfl()
    {
        return afl;
    }

    /**
     * Return AQL flag
     * @return true if AQL mode
     */
    public boolean isAql()
    {
        return !afl;
    }

    /**
     * Execute prepared query
     * @return Array result
     * @throws IOException
     * @throws Error
     */
    public Array execute() throws IOException, Error
    {
        if (_queryId == 0)
        {
            throw new Error("Query not prepared");
        }

        log.fine(String.format("Executing query %d", _queryId));
        Message msg = new Message.Query(_queryId, _queryStr, afl, "", true);
        net.write(msg);
        msg = net.read();

        switch (msg.getHeader().messageType)
        {
            case Message.mtQueryResult:
                log.fine("Got result from server");
                Result res = new Result((QueryResult) msg, this);
                _activeQueries.add(res.getQueryId());
                if (res.isSelective())
                    return new Array(res.getQueryId(), res.getSchema(), net);
                else
                    return null;

            case Message.mtError:
                log.fine("Got error message from server");
                throw new Error((Message.Error) msg);

            default:
                log.severe("Got unhandled network message during execution");
                throw new Error(String.format("Can not handle network message '%s'",
                        msg.getHeader().messageType));
        }
    }

    /**
     * Commit query
     */
    public void commit() throws IOException, Error
    {
        List<Long> activeQueries = new ArrayList<Long>(_activeQueries);
        _activeQueries.clear();
        for (long queryId: activeQueries)
        {
            log.fine(String.format("Committing query %d", queryId));
            net.write(new Message.CompleteQuery(queryId));
            Message msg = net.read();

            switch (msg.getHeader().messageType)
            {
                case Message.mtError:
                    Message.Error err = (Message.Error) msg;
                    if (err.getRecord().getLongErrorCode() != 0)
                    {
                        log.fine("Got error message from server");
                        throw new Error((Message.Error) msg);
                    }
                    log.fine("Query completed successfully");
                    break;

                default:
                    log.severe("Got unhandled network message during query completing");
                    throw new Error(String.format("Can not handle network message '%s'",
                            msg.getHeader().messageType));
            }
        }
    }

    /**
     * Rollback query
     */
    public void rollback() throws IOException, Error
    {
        List<Long> activeQueries = new ArrayList<Long>(_activeQueries);
        _activeQueries.clear();
        for (long queryId: activeQueries)
        {
            log.fine(String.format("Rolling back query %d", queryId));
            net.write(new Message.AbortQuery(queryId));
            Message msg = net.read();

            switch (msg.getHeader().messageType)
            {
                case Message.mtError:
                    Message.Error err = (Message.Error) msg;
                    if (err.getRecord().getLongErrorCode() != 0)
                    {
                        log.fine("Got error message from server");
                        throw new Error((Message.Error) msg);
                    }
                    log.fine("Query aborted successfully");
                    break;

                default:
                    log.severe("Got unhandled network message during query aborting");
                    throw new Error(String.format("Can not handle network message '%s'",
                            msg.getHeader().messageType));
            }
        }
    }

    /**
     * Set warning callback for registering execution warnings
     * @param callback Callback object
     */
    public void setWarningCallback(WarningCallback callback)
    {
        warningCallback = callback;
    }

    /**
     * Returns warning callback
     * @return Callback object
     */
    public WarningCallback getWarningCallback()
    {
        return warningCallback;
    }

    public void setTimeout(int timeout) throws SocketException
    {
        net.setTimeout(timeout);
    }

    public int getTimeout() throws SocketException
    {
        return net.getTimeout();
    }
}
