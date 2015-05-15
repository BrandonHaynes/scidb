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
package org.scidb.jdbc;

import java.io.IOException;
import java.sql.*;
import java.util.Map;
import java.util.Properties;

public class Connection implements java.sql.Connection
{
    private org.scidb.client.Connection scidbConnection;

    public Connection(String host, int port) throws SQLException
    {
        scidbConnection = new org.scidb.client.Connection();
        try
        {
            scidbConnection.connect(host, port);
        } catch (Exception e)
        {
            throw new SQLException(e);
        }
    }
    
    public org.scidb.client.Connection getSciDBConnection()
    {
        return scidbConnection;
    } 
    
    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public java.sql.Statement createStatement() throws SQLException
    {
        return new Statement(this);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException
    {
        throw new SQLException("Prepared statements not supported yet");
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException
    {
        throw new SQLException("Prepared statements not supported yet");
    }

    @Override
    public String nativeSQL(String sql) throws SQLException
    {
        throw new SQLException("SQL not supported yet");
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException
    {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean getAutoCommit() throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void commit() throws SQLException
    {
        try
        {
            scidbConnection.commit();
        } catch (Exception e)
        {
            throw new SQLException(e);
        }
    }

    @Override
    public void rollback() throws SQLException
    {
        try
        {
            scidbConnection.rollback();
        } catch (Exception e)
        {
            throw new SQLException(e);
        }
    }

    @Override
    public void close() throws SQLException
    {
        try
        {
            scidbConnection.close();
            scidbConnection = null;
        } catch (IOException e)
        {
            throw new SQLException(e);
        }
    }

    @Override
    public boolean isClosed() throws SQLException
    {
        return scidbConnection == null;
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException
    {
    }

    @Override
    public boolean isReadOnly() throws SQLException
    {
        return false;
    }

    @Override
    public void setCatalog(String catalog) throws SQLException
    {
    }

    @Override
    public String getCatalog() throws SQLException
    {
        return null;
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException
    {
    }

    @Override
    public int getTransactionIsolation() throws SQLException
    {
        return 0;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException
    {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException
    {
    }

    @Override
    public java.sql.Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException
    {
        return createStatement();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
            throws SQLException
    {
        return prepareStatement(sql);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException
    {
        return prepareCall(sql);
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException
    {
        // TODO Auto-generated method stub

    }

    @Override
    public void setHoldability(int holdability) throws SQLException
    {
        // TODO Auto-generated method stub

    }

    @Override
    public int getHoldability() throws SQLException
    {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public Savepoint setSavepoint() throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException
    {
        // TODO Auto-generated method stub

    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException
    {
        // TODO Auto-generated method stub

    }

    @Override
    public java.sql.Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException
    {
        return createStatement();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
            int resultSetHoldability) throws SQLException
    {
        return prepareStatement(sql);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
            int resultSetHoldability) throws SQLException
    {
        return prepareCall(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException
    {
        return prepareStatement(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException
    {
        return prepareStatement(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException
    {
        return prepareStatement(sql);
    }

    @Override
    public Clob createClob() throws SQLException
    {
        throw new SQLException("Blobs not supported");
    }

    @Override
    public Blob createBlob() throws SQLException
    {
        throw new SQLException("Blobs not supported");
    }

    @Override
    public NClob createNClob() throws SQLException
    {
        throw new SQLException("Blobs not supported");
    }

    @Override
    public SQLXML createSQLXML() throws SQLException
    {
        throw new SQLException("SQLXML not supported");
    }

    @Override
    public boolean isValid(int timeout) throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException
    {
        // TODO Auto-generated method stub

    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException
    {
        // TODO Auto-generated method stub

    }

    @Override
    public String getClientInfo(String name) throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Properties getClientInfo() throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }
}
