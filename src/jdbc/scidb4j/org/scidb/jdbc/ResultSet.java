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

import org.scidb.client.Array;
import org.scidb.client.*;
import org.scidb.client.Error;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.Map;

public class ResultSet implements java.sql.ResultSet
{
    private Array array;
    private boolean haveMoreChunks = false;
    private boolean haveMoreValues = false;
    private Chunk[] currentChunks;
    private Chunk[] nextChunks;
    private EmptyChunk currentEmptyBitmap;
    private EmptyChunk nextEmptyBitmap;
    private boolean wasNull = false;
    private java.sql.ResultSetMetaData metadata;
    private int emptyId;
    private boolean isFirst = true;

    public ResultSet(Array array) throws SQLException
    {
        this.array = array;
        metadata = new ResultSetMetaData(this.array.getSchema());

        try
        {
            Schema.Attribute emptyAttribute = this.array.getSchema().getEmptyIndicator();
            int attrCount = array.getSchema().getAttributes().length;

            emptyId = emptyAttribute != null ? this.array.getSchema().getEmptyIndicator().getId() : -1;
            currentChunks = new Chunk[attrCount];
            nextChunks = new Chunk[attrCount];

            //Fetch first chunks set in result and store
            array.fetch();
            for (int i = 0; i < attrCount; i++)
            {
                if (i != emptyId)
                    currentChunks[i] = array.getChunk(i);
            }
            currentEmptyBitmap = array.getEmptyBitmap();

            //Fetch second chunks set if possible to know if we have more ahead
            if(!currentChunks[0].endOfArray())
            {
                haveMoreValues = currentChunks[0].hasNext();
                array.fetch();
                for (int i = 0; i < attrCount; i++)
                {
                    if (i != emptyId)
                        nextChunks[i] = array.getChunk(i);
                }
                haveMoreChunks = !nextChunks[0].endOfArray();
                if (haveMoreChunks)
                    nextEmptyBitmap = array.getEmptyBitmap();
            }
            else
            {
                currentChunks = null;
                nextChunks = null;
                isFirst = false;
            }
        } catch (java.lang.Exception e)
        {
            throw new SQLException(e);
        }
    }

    public boolean isAttribute(int columnIndex) throws SQLException
    {
        if (columnIndex > 0 && columnIndex < array.getSchema().getDimensions().length + 1)
            return false;
        else if (columnIndex >= array.getSchema().getDimensions().length + 1 && columnIndex <= metadata.getColumnCount())
            return true;
        else
            throw new SQLException("Wrong column index " + columnIndex);
    }

    private int columnToAttributeId(int columnIndex) throws SQLException
    {
        return columnIndex - array.getSchema().getDimensions().length - 1;
    }

    private int columnToDimensionId(int columnIndex) throws SQLException
    {
        return columnIndex - 1;
    }

    @Override
    @SuppressWarnings(value = "unchecked") //While we checking types inside we can safely ignore warnings
    public <T> T unwrap(Class<T> iface) throws SQLException
    {
        if (iface == IResultSetWrapper.class)
        {
            return (T) new ResultSetWrapper(this);
        }
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException
    {
        return iface == IResultSetWrapper.class;
    }

    @Override
    public boolean next() throws SQLException
    {
        isFirst = false;
        if (currentChunks == null)
            return false;
        try
        {
            if (haveMoreValues)
            {
                for (int i = 0; i < array.getSchema().getAttributes().length; i++)
                {
                    if (i != emptyId)
                        currentChunks[i].move();
                }
                if (currentEmptyBitmap != null)
                    currentEmptyBitmap.move();
                haveMoreValues = currentChunks[0].hasNext();
            }
            //If we don't have anymore values in this chunk we check if we have more chunks with data
            //ahead and if yes move to next chunk and refresh flags
            else if(haveMoreChunks)
            {
                array.fetch();
                for (int i = 0; i < array.getSchema().getAttributes().length; i++)
                {
                    currentChunks[i] = nextChunks[i];
                    if (i != emptyId)
                        nextChunks[i] = array.getChunk(i);
                }
                haveMoreChunks = !nextChunks[0].endOfArray();
                haveMoreValues = currentChunks[0].hasNext();
                currentEmptyBitmap = nextEmptyBitmap;
                nextEmptyBitmap = array.getEmptyBitmap();
            }
            //Finally if we don't have chunks ahead clear chunks lists. Now we in "after last state"
            else
            {
                haveMoreChunks = false;
                haveMoreValues = false;
                currentChunks = null;
                nextChunks = null;
            }
        } catch (java.lang.Exception e)
        {
            throw new SQLException(e);
        }
        return true;
    }

    @Override
    public void close() throws SQLException
    {
    }

    @Override
    public boolean wasNull() throws SQLException
    {
        return wasNull;
    }

    @Override
    public String getString(int columnIndex) throws SQLException
    {
        wasNull = false;
        try
        {
            if (isAttribute(columnIndex))
            {
                int attId = columnToAttributeId(columnIndex);
                if (currentChunks[attId].isNull())
                {
                    wasNull = true;
                    return null;
                }
                else
                {
                    String attType = metadata.getColumnTypeName(columnIndex);
                    if (attType.equals(Type.TID_STRING))
                    {
                        return String.valueOf(currentChunks[attId].getString());
                    } else if (attType.equals(Type.TID_CHAR))
                    {
                        return String.valueOf(currentChunks[attId].getChar());
                    } else
                    {
                        throw new TypeException(attType, "String");
                    }
                }
            }
            else
            {
                String attType = metadata.getColumnTypeName(columnIndex);
                throw new TypeException(attType, "String");
            }
        } catch (Error e)
        {
            throw new SQLException(e);
        }
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException
    {
        wasNull = false;
        if (!metadata.getColumnTypeName(columnIndex).equals(Type.TID_BOOL))
        {
            throw new TypeException(metadata.getColumnTypeName(columnIndex), "boolean");
        }

        try
        {
            if (isAttribute(columnIndex))
            {
                int attId = columnToAttributeId(columnIndex);
                if (currentChunks[attId].isNull())
                {
                    wasNull = true;
                    return false;
                }
                else
                {
                    return currentChunks[attId].getBoolean();
                }
            }
            else
            {
                throw new TypeException(metadata.getColumnTypeName(columnIndex), "boolean");
            }
        } catch (Error e)
        {
            throw new SQLException(e);
        }
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException
    {
        wasNull = false;
        if (!metadata.getColumnTypeName(columnIndex).equals(Type.TID_INT8))
        {
            throw new TypeException(metadata.getColumnTypeName(columnIndex), "byte");
        }

        try
        {
            if (isAttribute(columnIndex))
            {
                int attId = columnToAttributeId(columnIndex);
                if (currentChunks[attId].isNull())
                {
                    wasNull = true;
                    return 0;
                }
                else
                {
                    return currentChunks[attId].getInt8();
                }
            }
            else
            {
                String attType = metadata.getColumnTypeName(columnIndex);
                throw new TypeException(attType, "byte");
            }
        } catch (Error e)
        {
            throw new SQLException(e);
        }
    }

    @Override
    public short getShort(int columnIndex) throws SQLException
    {
        wasNull = false;
        try
        {
            if (isAttribute(columnIndex))
            {
                int attId = columnToAttributeId(columnIndex);
                if (currentChunks[attId].isNull())
                {
                    wasNull = true;
                    return 0;
                } else
                {
                    String attType = metadata.getColumnTypeName(columnIndex);
                    if (attType.equals(Type.TID_INT16))
                    {
                        return currentChunks[attId].getInt16();
                    } else if (attType.equals(Type.TID_INT8))
                    {
                        return currentChunks[attId].getInt8();
                    } else if (attType.equals(Type.TID_UINT8))
                    {
                        return currentChunks[attId].getUint8();
                    } else
                    {
                        throw new TypeException(attType, "short");
                    }
                }
            }
            else
            {
                String attType = metadata.getColumnTypeName(columnIndex);
                throw new TypeException(attType, "short");
            }
        } catch (Error e)
        {
            throw new SQLException(e);
        }
    }

    @Override
    public int getInt(int columnIndex) throws SQLException
    {
        wasNull = false;
        try
        {
            if (isAttribute(columnIndex))
            {
                int attId = columnToAttributeId(columnIndex);
                if (currentChunks[attId].isNull())
                {
                    wasNull = true;
                    return 0;
                }
                else
                {
                    String attType = metadata.getColumnTypeName(columnIndex);
                    if (attType.equals(Type.TID_INT32))
                    {
                        return currentChunks[attId].getInt32();
                    } else if (attType.equals(Type.TID_INT16))
                    {
                        return currentChunks[attId].getInt16();
                    } else if (attType.equals(Type.TID_INT8))
                    {
                        return currentChunks[attId].getInt8();
                    } else if (attType.equals(Type.TID_UINT16))
                    {
                        return currentChunks[attId].getUint16();
                    } else if (attType.equals(Type.TID_UINT8))
                    {
                        return currentChunks[attId].getUint8();
                    } else
                    {
                        throw new TypeException(attType, "int");
                    }
                }
            }
            else
            {
                String attType = metadata.getColumnTypeName(columnIndex);
                throw new TypeException(attType, "int");
            }
        } catch (Error e)
        {
            throw new SQLException(e);
        }
    }

    @Override
    public long getLong(int columnIndex) throws SQLException
    {
        wasNull = false;
        try
        {
            if (isAttribute(columnIndex))
            {
                int attId = columnToAttributeId(columnIndex);
                if (currentChunks[attId].isNull())
                {
                    wasNull = true;
                    return 0;
                }
                else
                {
                    String attType = metadata.getColumnTypeName(columnIndex);
                    if (attType.equals(Type.TID_INT64))
                    {
                        return currentChunks[attId].getInt64();
                    } else if (attType.equals(Type.TID_INT32))
                    {
                        return currentChunks[attId].getInt32();
                    } else if (attType.equals(Type.TID_INT16))
                    {
                        return currentChunks[attId].getInt16();
                    } else if (attType.equals(Type.TID_INT8))
                    {
                        return currentChunks[attId].getInt8();
                    } else if (attType.equals(Type.TID_UINT32))
                    {
                        return currentChunks[attId].getUint32();
                    } else if (attType.equals(Type.TID_UINT16))
                    {
                        return currentChunks[attId].getUint16();
                    } else if (attType.equals(Type.TID_UINT8))
                    {
                        return currentChunks[attId].getUint8();
                    } else
                    {
                        throw new TypeException(attType, "long");
                    }
                }
            }
            else
            {
                if (currentEmptyBitmap != null)
                    return currentEmptyBitmap.getCoordinates()[columnToDimensionId(columnIndex)];
                else
                    return currentChunks[0].getCoordinates()[columnToDimensionId(columnIndex)];
            }
        } catch (Error e)
        {
            throw new SQLException(e);
        }
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException
    {
        wasNull = false;
        try
        {
            if (isAttribute(columnIndex))
            {
                int attId = columnToAttributeId(columnIndex);
                if (currentChunks[attId].isNull())
                {
                    wasNull = true;
                    return 0;
                }
                else
                {
                    String attType = metadata.getColumnTypeName(columnIndex);
                    if (attType.equals(Type.TID_FLOAT))
                    {
                        return currentChunks[attId].getFloat();
                    } else if (attType.equals(Type.TID_INT64))
                    {
                        return currentChunks[attId].getInt64();
                    } else if (attType.equals(Type.TID_INT32))
                    {
                        return currentChunks[attId].getInt32();
                    } else if (attType.equals(Type.TID_INT16))
                    {
                        return currentChunks[attId].getInt16();
                    } else if (attType.equals(Type.TID_INT8))
                    {
                        return currentChunks[attId].getInt8();
                    } else if (attType.equals(Type.TID_UINT64))
                    {
                        return currentChunks[attId].getUint64().floatValue();
                    } else if (attType.equals(Type.TID_UINT32))
                    {
                        return currentChunks[attId].getUint32();
                    } else if (attType.equals(Type.TID_UINT16))
                    {
                        return currentChunks[attId].getUint16();
                    } else if (attType.equals(Type.TID_UINT8))
                    {
                        return currentChunks[attId].getUint8();
                    } else
                    {
                        throw new TypeException(attType, "float");
                    }
                }
            }
            else
            {
                String attType = metadata.getColumnTypeName(columnIndex);
                throw new TypeException(attType, "float");
            }
        } catch (Error e)
        {
            throw new SQLException(e);
        }
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException
    {
        wasNull = false;
        try
        {
            if (isAttribute(columnIndex))
            {
                int attId = columnToAttributeId(columnIndex);
                if (currentChunks[attId].isNull())
                {
                    wasNull = true;
                    return 0;
                }
                else
                {
                    String attType = metadata.getColumnTypeName(columnIndex);
                    if (attType.equals(Type.TID_DOUBLE))
                    {
                        return currentChunks[attId].getDouble();
                    } else if (attType.equals(Type.TID_FLOAT))
                    {
                        return currentChunks[attId].getFloat();
                    } else if (attType.equals(Type.TID_INT64))
                    {
                        return currentChunks[attId].getInt64();
                    } else if (attType.equals(Type.TID_INT32))
                    {
                        return currentChunks[attId].getInt32();
                    } else if (attType.equals(Type.TID_INT16))
                    {
                        return currentChunks[attId].getInt16();
                    } else if (attType.equals(Type.TID_INT8))
                    {
                        return currentChunks[attId].getInt8();
                    } else if (attType.equals(Type.TID_UINT64))
                    {
                        return currentChunks[attId].getUint64().floatValue();
                    } else if (attType.equals(Type.TID_UINT32))
                    {
                        return currentChunks[attId].getUint32();
                    } else if (attType.equals(Type.TID_UINT16))
                    {
                        return currentChunks[attId].getUint16();
                    } else if (attType.equals(Type.TID_UINT8))
                    {
                        return currentChunks[attId].getUint8();
                    } else
                    {
                        throw new TypeException(attType, "double");
                    }
                }
            }
            else
            {
                String attType = metadata.getColumnTypeName(columnIndex);
                throw new TypeException(attType, "double");
            }
        } catch (Error e)
        {
            throw new SQLException(e);
        }
    }

    @Deprecated
    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException
    {
        BigDecimal dec = getBigDecimal(columnIndex);
        if (dec == null)
            return null;
        return dec.setScale(scale);
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Deprecated
    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getString(String columnLabel) throws SQLException
    {
        return getString(findColumn(columnLabel));
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException
    {
        return getBoolean(findColumn(columnLabel));
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException
    {
        return getByte(findColumn(columnLabel));
    }

    @Override
    public short getShort(String columnLabel) throws SQLException
    {
        return getShort(findColumn(columnLabel));
    }

    @Override
    public int getInt(String columnLabel) throws SQLException
    {
        return getInt(findColumn(columnLabel));
    }

    @Override
    public long getLong(String columnLabel) throws SQLException
    {
        return getLong(findColumn(columnLabel));
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException
    {
        return getFloat(findColumn(columnLabel));
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException
    {
        return getDouble(findColumn(columnLabel));
    }

    @Deprecated
    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException
    {
        //noinspection deprecation
        return getBigDecimal(findColumn(columnLabel), scale);
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException
    {
        return getBytes(findColumn(columnLabel));
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException
    {
        return getDate(findColumn(columnLabel));
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException
    {
        return getTime(findColumn(columnLabel));
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException
    {
        return getTimestamp(findColumn(columnLabel));
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException
    {
        return getAsciiStream(findColumn(columnLabel));
    }

    @Deprecated
    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException
    {
        //noinspection deprecation
        return getUnicodeStream(findColumn(columnLabel));
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException
    {
        return getBinaryStream(findColumn(columnLabel));
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
    public String getCursorName() throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public java.sql.ResultSetMetaData getMetaData() throws SQLException
    {
        return metadata;
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException
    {
        for (int i = 1; i <= metadata.getColumnCount(); i++)
        {
            if (columnLabel.equals(metadata.getColumnName(i)))
                return i;
        }
        return 0;
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException
    {
        wasNull = false;
        try
        {
            if (isAttribute(columnIndex))
            {
                int attId = columnToAttributeId(columnIndex);
                if (currentChunks[attId].isNull())
                {
                    wasNull = true;
                    return null;
                }
                else
                {
                    String attType = metadata.getColumnTypeName(columnIndex);
                    if (attType.equals(Type.TID_INT64))
                    {
                        return new BigDecimal(currentChunks[attId].getInt64());
                    } else if (attType.equals(Type.TID_INT32))
                    {
                        return new BigDecimal(currentChunks[attId].getInt32());
                    } else if (attType.equals(Type.TID_INT16))
                    {
                        return new BigDecimal(currentChunks[attId].getInt16());
                    } else if (attType.equals(Type.TID_INT8))
                    {
                        return new BigDecimal(currentChunks[attId].getInt8());
                    } else if (attType.equals(Type.TID_UINT64))
                    {
                        return new BigDecimal(currentChunks[attId].getUint64());
                    } else if (attType.equals(Type.TID_UINT32))
                    {
                        return new BigDecimal(currentChunks[attId].getUint32());
                    } else if (attType.equals(Type.TID_UINT16))
                    {
                        return new BigDecimal(currentChunks[attId].getUint16());
                    } else if (attType.equals(Type.TID_UINT8))
                    {
                        return new BigDecimal(currentChunks[attId].getUint8());
                    } else
                    {
                        throw new TypeException(attType, "BigDecimal");
                    }
                }
            }
            else
            {
                if (currentEmptyBitmap != null)
                    return BigDecimal.valueOf(currentEmptyBitmap.getCoordinates()[columnToDimensionId(columnIndex)]);
                else
                    return BigDecimal.valueOf(currentChunks[0].getCoordinates()[columnToDimensionId(columnIndex)]);
            }
        } catch (Error e)
        {
            throw new SQLException(e);
        }
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException
    {
        return getBigDecimal(findColumn(columnLabel));
    }

    /**
     * We can implement forward only cursor, so we always return false
     * @return always false
     * @throws SQLException
     */
    @Override
    public boolean isBeforeFirst() throws SQLException
    {
        return false;
    }

    @Override
    public boolean isAfterLast() throws SQLException
    {
        return currentChunks == null;
    }

    @Override
    public boolean isFirst() throws SQLException
    {
        return isFirst;
    }

    @Override
    public boolean isLast() throws SQLException
    {
        return currentChunks != null && !haveMoreChunks && !haveMoreValues;
    }

    @Override
    public void beforeFirst() throws SQLException
    {
    }

    @Override
    public void afterLast() throws SQLException
    {
    }

    @Override
    public boolean first() throws SQLException
    {
        return false;
    }

    @Override
    public boolean last() throws SQLException
    {
        return false;
    }

    @Override
    public int getRow() throws SQLException
    {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public boolean absolute(int row) throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean relative(int rows) throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean previous() throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
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
    public int getType() throws SQLException
    {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getConcurrency() throws SQLException
    {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public boolean rowUpdated() throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean rowInserted() throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean rowDeleted() throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void updateNull(String columnLabel) throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length)
            throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length)
            throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length)
            throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void insertRow() throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void updateRow() throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void deleteRow() throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void refreshRow() throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void cancelRowUpdates() throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void moveToInsertRow() throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void moveToCurrentRow() throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public Statement getStatement() throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public java.sql.Array getArray(int columnIndex) throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public java.sql.Array getArray(String columnLabel) throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void updateArray(int columnIndex, java.sql.Array x) throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void updateArray(String columnLabel, java.sql.Array x) throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException
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
    public boolean isClosed() throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public String getNString(int columnIndex) throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getNString(String columnLabel) throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length)
            throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length)
            throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length)
            throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length)
            throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length)
            throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length)
            throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException
    {
        // TODO Auto-generated method stub
        
    }
}
