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

import org.scidb.client.Result;

import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.List;

public class Statement implements java.sql.Statement
{
    private Connection conn;
    private org.scidb.client.Connection scidbConnection;
    private int _currentResultNo;
    private List<String> _batchQueries = new ArrayList<String>();
    private List<java.sql.ResultSet> _results;

    Statement(Connection conn)
    {
        this.conn = conn;
        this.scidbConnection = conn.getSciDBConnection();
    }
    
    @Override
    @SuppressWarnings(value = "unchecked") //While we checking types inside we can safely ignore warnings
    public <T> T unwrap(Class<T> iface) throws SQLException
    {
        if (iface == IStatementWrapper.class)
        {
            return (T) new StatementWrapper(scidbConnection);
        }
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException
    {
        return iface == IResultSetWrapper.class;
    }

    public java.sql.ResultSet executeInternal(String sql) throws SQLException
    {
        try
        {
            Result res = scidbConnection.prepare(sql);
            if (res.isSelective())
            {
                return new ResultSet(scidbConnection.execute());
            }
            else
            {
                scidbConnection.execute();
                return null;
            }
        } catch (Exception e)
        {
            throw new SQLException(e);
        }
    }
    @Override
    public java.sql.ResultSet executeQuery(String sql) throws SQLException
    {
        _results = new ArrayList<java.sql.ResultSet>();
        _currentResultNo = -1;
        java.sql.ResultSet res = executeInternal(sql);
        if (res != null)
        {
            _results.add(res);
            _currentResultNo = 0;
        }
        return res;
    }

    @Override
    public int executeUpdate(String sql) throws SQLException
    {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void close() throws SQLException
    {
        _results = null;
    }

    @Override
    public int getMaxFieldSize() throws SQLException
    {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public int getMaxRows() throws SQLException
    {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void setMaxRows(int max) throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public int getQueryTimeout() throws SQLException
    {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void cancel() throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public SQLWarning getWarnings() throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void setCursorName(String name) throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public boolean execute(String sql) throws SQLException
    {
        executeQuery(sql);
        return _results.size() > 0;
    }

    @Override
    public java.sql.ResultSet getResultSet() throws SQLException
    {
        return _results.get(_currentResultNo);
    }

    @Override
    public int getUpdateCount() throws SQLException
    {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public boolean getMoreResults() throws SQLException
    {
        return getMoreResults(Statement.KEEP_CURRENT_RESULT);
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public int getFetchDirection() throws SQLException
    {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public int getFetchSize() throws SQLException
    {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException
    {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getResultSetType() throws SQLException
    {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void addBatch(String sql) throws SQLException
    {
        _batchQueries.add(sql);
    }

    @Override
    public void clearBatch() throws SQLException
    {
        _batchQueries.clear();
    }

    @Override
    public int[] executeBatch() throws SQLException
    {
        int[] retCodes = new int[_batchQueries.size()];
        _results = new ArrayList<java.sql.ResultSet>();
        _currentResultNo = -1;
        for (int i = 0; i < _batchQueries.size(); i++)
        {
            try
            {
                executeInternal(_batchQueries.get(i));
                java.sql.ResultSet res = executeInternal(_batchQueries.get(i));
                if (res != null)
                {
                    _results.add(res);
                    _currentResultNo = 0;
                }
                retCodes[i] = SUCCESS_NO_INFO;
            }
            catch (SQLException e)
            {
                retCodes[i] = EXECUTE_FAILED;
                throw e;
            }
        }
        return retCodes;
    }

    @Override
    public java.sql.Connection getConnection() throws SQLException
    {
        return conn;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException
    {
        switch (current)
        {
            case CLOSE_CURRENT_RESULT:
                _results.set(_currentResultNo, null);
                break;

            case CLOSE_ALL_RESULTS:
                for(int i = 0; i <= _currentResultNo; i++)
                {
                    _results.set(i, null);
                }
                break;
        }

        _currentResultNo++;
        if (_currentResultNo < _results.size())
        {
            return true;
        }
        else
        {
            return false;
        }
    }

    @Override
    public java.sql.ResultSet getGeneratedKeys() throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException
    {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException
    {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException
    {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public int getResultSetHoldability() throws SQLException
    {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public boolean isClosed() throws SQLException
    {
        return _results == null;
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public boolean isPoolable() throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }
}
