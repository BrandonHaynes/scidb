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
package org.scidb.io.network;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;

import org.scidb.util.InputStreamWithReadall;

/**
 * Class for handling reading and writing arbitrary network messages syncronously
 */
public class Network
{
    private Socket sock;

    /**
     * Connect to server
     * 
     * @param host Host name or IP
     * @param port Port number
     * @throws IOException
     */
    public void connect(String host, int port) throws IOException
    {
        sock = new Socket(host, port);
    }

    /**
     * Close connection
     * 
     * @throws IOException
     */
    public void disconnect() throws IOException
    {
        sock.close();
        sock = null;
    }

    /**
     * Set timeout to socket
     * 
     * @param timeout Timeout im milliseconds
     * @throws SocketException
     */
    public void setTimeout(int timeout) throws SocketException
    {
        sock.setSoTimeout(timeout);
    }

    /**
     * Returns timeout of socket in milliseconds
     * @return Timeout
     * @throws SocketException
     */
    public int getTimeout() throws SocketException
    {
        return sock.getSoTimeout();
    }

    /**
     * Write arbitrary SciDB network message to socket
     * 
     * @param msg Message object
     * @throws IOException
     */
    public void write(Message msg) throws IOException
    {
        msg.writeToStream(sock.getOutputStream());
    }

    /**
     * Read arbitrary network message from socket
     * 
     * @return SciDB network message
     * @throws org.scidb.client.Error
     * @throws IOException
     */
    public Message read() throws org.scidb.client.Error, IOException
    {
        return Message.parseFromStream(new InputStreamWithReadall(sock.getInputStream()));
    }

    /**
     * Check if connected to server
     * @return true if connected
     */
    public boolean isConnected()
    {
        return sock != null && sock.isConnected();
    }
}
