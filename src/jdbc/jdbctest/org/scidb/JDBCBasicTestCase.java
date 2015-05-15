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
package org.scidb;

import junit.framework.TestCase;
import org.scidb.jdbc.IResultSetWrapper;
import org.scidb.jdbc.IStatementWrapper;

import java.sql.*;

public class JDBCBasicTestCase extends TestCase
{
    private static String iqueryHost = "localhost";
    private static String iqueryPort = "1239";

    private Connection conn;

    public static void setIqueryHost(String host)
    {
        iqueryHost = host;
    }

    public static void setIqueryPort(String port)
    {
        iqueryPort = port;
    }

    public JDBCBasicTestCase(String s)
    {
        super(s);
        try
        {
            Class.forName("org.scidb.jdbc.Driver");
        }
        catch (ClassNotFoundException e)
        {
            System.out.println("Driver is not in the CLASSPATH -> " + e);
        }
    }

    public void setUp() throws SQLException {

        String connString = "jdbc:scidb://";

        connString += iqueryHost + ":" + iqueryPort + "/";

        conn = DriverManager.getConnection(connString);
    }

    public void tearDown() throws SQLException {
        conn.close();
    }

    public void testDDLExecution() throws SQLException
    {
        Statement st = conn.createStatement();
        ResultSet res = st.executeQuery("create array A<i: int64, s: string>[x=0:5,3,0, y=0:9,5,0]");
        assertNull(res);
        conn.commit();


        res = st.executeQuery("drop array A");
        assertNull(res);
        conn.commit();
    }

    public void testSelectiveQuery() throws SQLException
    {
        Statement st = conn.createStatement();
        ResultSet res = st.executeQuery("select * from array(<a:string null, c: char null>[x=0:3,2,0], '[(\"1\",\"1\")(\"\", null)][(\"2\")]')");
        ResultSetMetaData meta = res.getMetaData();

        assertEquals("build", meta.getTableName(0));
        assertEquals(3, meta.getColumnCount());

        IResultSetWrapper resWrapper = res.unwrap(IResultSetWrapper.class);
        assertEquals("x", meta.getColumnName(1));
        assertEquals("int64", meta.getColumnTypeName(1));
        assertFalse(resWrapper.isColumnAttribute(1));
        assertEquals("a", meta.getColumnName(2));
        assertEquals("string", meta.getColumnTypeName(2));
        assertEquals("c", meta.getColumnName(3));
        assertEquals("char", meta.getColumnTypeName(3));
        assertTrue(resWrapper.isColumnAttribute(3));

        StringBuilder sb = new StringBuilder();
        while(!res.isAfterLast())
        {
            sb.append(res.getLong("x") + ":" + res.wasNull() + ":" + res.getString("a") + ":" + res.wasNull() + ":"
                    + res.getString("c") + ":" + res.wasNull() + ":");
            res.next();
        }
        assertEquals("0:false:1:false:1:false:1:false::false:null:true:2:false:2:false:null:true:", sb.toString());
    }

    public void testSignedIntegerDataTypes() throws SQLException
    {
        Statement st = conn.createStatement();
        ResultSet res = st.executeQuery("select * from array(<i8:int8 null, i16: int16 null, i32: int32 null, " +
                "i64: int64 null>[x=0:3,2,0], '[(1, 260, 67000, 10000000), (-1, -260, -67000, -10000000)]" +
                "[(null, null, null, null)]')");
        ResultSetMetaData meta = res.getMetaData();

        assertEquals("build", meta.getTableName(0));
        assertEquals(5, meta.getColumnCount());

        IResultSetWrapper resWrapper = res.unwrap(IResultSetWrapper.class);
        assertEquals("x", meta.getColumnName(1));
        assertEquals("int64", meta.getColumnTypeName(1));
        assertFalse(resWrapper.isColumnAttribute(1));

        assertEquals("i8", meta.getColumnName(2));
        assertEquals("int8", meta.getColumnTypeName(2));
        assertTrue(resWrapper.isColumnAttribute(2));
        assertEquals("i16", meta.getColumnName(3));
        assertEquals("int16", meta.getColumnTypeName(3));
        assertTrue(resWrapper.isColumnAttribute(3));
        assertEquals("i32", meta.getColumnName(4));
        assertEquals("int32", meta.getColumnTypeName(4));
        assertTrue(resWrapper.isColumnAttribute(4));
        assertEquals("i64", meta.getColumnName(5));
        assertEquals("int64", meta.getColumnTypeName(5));
        assertTrue(resWrapper.isColumnAttribute(5));

        StringBuilder sbMain = new StringBuilder();
        StringBuilder sbConvert1 = new StringBuilder();
        StringBuilder sbConvert2 = new StringBuilder();
        StringBuilder sbConvert3 = new StringBuilder();
        StringBuilder sbConvertFloat = new StringBuilder();
        while(!res.isAfterLast())
        {
            sbMain.append(res.getLong("x") + ":" + res.wasNull() + ":" + res.getByte("i8") + ":" + res.wasNull() + ":" +
                    res.getShort("i16") + ":" + res.wasNull() + ":" + res.getInt("i32") + ":" + res.wasNull()
                    + ":" + res.getLong("i64") + ":" + res.getBigDecimal("i64") + ":" + res.wasNull() + ":");
            sbConvert1.append(res.getShort("i8") + ":" + res.getInt("i16") + ":" + res.getLong("i32") + ":"
                    + res.getBigDecimal("i32") + ":");
            sbConvert2.append(res.getInt("i8") + ":" + res.getLong("i16") + ":" + res.getBigDecimal("i16") + ":");
            sbConvert3.append(res.getLong("i8") + ":" + res.getBigDecimal("i8") + ":" + res.getBigDecimal("i16") + ":"
                    + res.getBigDecimal("i32") + ":");
            sbConvertFloat.append(res.getFloat("i8") + ":" + res.getDouble("i8") + ":" +
                    res.getFloat("i16") + ":" + res.getDouble("i16") + ":" +
                    res.getFloat("i32") + ":" + res.getDouble("i32") + ":" +
                    res.getFloat("i64") + ":" + res.getDouble("i64") + ":");
            res.next();
        }
        assertEquals("0:false:1:false:260:false:67000:false:10000000:10000000:false:1:false:-1:false:-260:false:-67000:false:-10000000:-10000000:false:2:false:0:true:0:true:0:true:0:null:true:", sbMain.toString());
        assertEquals("1:260:67000:67000:-1:-260:-67000:-67000:0:0:0:null:", sbConvert1.toString());
        assertEquals("1:260:260:-1:-260:-260:0:0:null:", sbConvert2.toString());
        assertEquals("1:1:260:67000:-1:-1:-260:-67000:0:null:null:null:", sbConvert3.toString());
        assertEquals("1.0:1.0:260.0:260.0:67000.0:67000.0:1.0E7:1.0E7:-1.0:-1.0:-260.0:-260.0:-67000.0:-67000.0:-1.0E7:-1.0E7:0.0:0.0:0.0:0.0:0.0:0.0:0.0:0.0:", sbConvertFloat.toString());
    }

    public void testUnsignedIntegerDataTypes() throws SQLException
    {
        Statement st = conn.createStatement();
        ResultSet res = st.executeQuery("select * from array(<i8:uint8, i16: uint16, i32: uint32, i64: uint64>" +
                "[x=0:3,2,0], '[(1, 260, 67000, 10000000)][]')");
        ResultSetMetaData meta = res.getMetaData();

        assertEquals("build", meta.getTableName(0));
        assertEquals(5, meta.getColumnCount());

        IResultSetWrapper resWrapper = res.unwrap(IResultSetWrapper.class);
        assertEquals("x", meta.getColumnName(1));
        assertEquals("int64", meta.getColumnTypeName(1));
        assertFalse(resWrapper.isColumnAttribute(1));

        assertEquals("i8", meta.getColumnName(2));
        assertEquals("uint8", meta.getColumnTypeName(2));
        assertTrue(resWrapper.isColumnAttribute(2));
        assertEquals("i16", meta.getColumnName(3));
        assertEquals("uint16", meta.getColumnTypeName(3));
        assertTrue(resWrapper.isColumnAttribute(3));
        assertEquals("i32", meta.getColumnName(4));
        assertEquals("uint32", meta.getColumnTypeName(4));
        assertTrue(resWrapper.isColumnAttribute(4));
        assertEquals("i64", meta.getColumnName(5));
        assertEquals("uint64", meta.getColumnTypeName(5));
        assertTrue(resWrapper.isColumnAttribute(5));

        StringBuilder sbMain = new StringBuilder();
        StringBuilder sbConvert1 = new StringBuilder();
        StringBuilder sbConvert2 = new StringBuilder();
        StringBuilder sbConvert3 = new StringBuilder();
        StringBuilder sbConvertFloat = new StringBuilder();
        while(!res.isAfterLast())
        {
            sbMain.append(res.getLong("x") + ":" + res.wasNull() + ":" + res.getShort("i8") + ":" + res.wasNull() +
                    ":" + res.getInt("i16") + ":" + res.wasNull() + ":" + res.getLong("i32") + ":" + res.wasNull()
                    + ":" + res.getBigDecimal("i64") + ":" + res.wasNull() + ":");
            sbConvert1.append(res.getInt("i8") + ":" + res.getLong("i16") + ":" + res.getLong("i32") + ":" + res.getBigDecimal("i32") + ":");
            sbConvert2.append(res.getInt("i8") + ":" + res.getLong("i16") + ":" + res.getBigDecimal("i16") + ":");
            sbConvert3.append(res.getLong("i8") + ":" + res.getBigDecimal("i8") + ":");
            sbConvertFloat.append(res.getFloat("i8") + ":" + res.getDouble("i8") + ":" +
                    res.getFloat("i16") + ":" + res.getDouble("i16") + ":" +
                    res.getFloat("i32") + ":" + res.getDouble("i32") + ":" +
                    res.getFloat("i64") + ":" + res.getDouble("i64") + ":");
            res.next();
        }
        assertEquals("0:false:1:false:260:false:67000:false:10000000:false:", sbMain.toString());
        assertEquals("1:260:67000:67000:", sbConvert1.toString());
        assertEquals("1:260:260:", sbConvert2.toString());
        assertEquals("1:1:", sbConvert3.toString());
        assertEquals("1.0:1.0:260.0:260.0:67000.0:67000.0:1.0E7:1.0E7:", sbConvertFloat.toString());
    }

    public void testFloatDataTypes() throws SQLException
    {
        Statement st = conn.createStatement();
        ResultSet res = st.executeQuery("select * from array(<f:float, d: double>[x=0:3,2,0], " +
                "'[(3.141592653589793238, 3.141592653589793238)][]')");
        ResultSetMetaData meta = res.getMetaData();

        assertEquals("build", meta.getTableName(0));
        assertEquals(3, meta.getColumnCount());

        IResultSetWrapper resWrapper = res.unwrap(IResultSetWrapper.class);
        assertEquals("x", meta.getColumnName(1));
        assertEquals("int64", meta.getColumnTypeName(1));
        assertFalse(resWrapper.isColumnAttribute(1));

        assertEquals("f", meta.getColumnName(2));
        assertEquals("float", meta.getColumnTypeName(2));
        assertTrue(resWrapper.isColumnAttribute(2));
        assertEquals("d", meta.getColumnName(3));
        assertEquals("double", meta.getColumnTypeName(3));
        assertTrue(resWrapper.isColumnAttribute(3));

        StringBuilder sbMain = new StringBuilder();
        while(!res.isAfterLast())
        {
            sbMain.append(res.getLong("x") + ":" + res.getFloat("f") + ":" + res.wasNull() + ":" +
                    res.getDouble("d") + ":" + res.wasNull() + ":" + (float)res.getDouble("f") + ":");
            res.next();
        }
        assertEquals("0:3.1415927:false:3.141592653589793:false:3.1415927:", sbMain.toString());
    }

    public void testNonEmptyCoordinates() throws SQLException
    {
        Statement st = conn.createStatement();
        ResultSet res = st.executeQuery("select * from array(<a:int32>[x=0:3,2,0], '[10,9][8,7]')");
        ResultSetMetaData meta = res.getMetaData();

        assertEquals("build", meta.getTableName(0));
        assertEquals(2, meta.getColumnCount());

        IResultSetWrapper resWrapper = res.unwrap(IResultSetWrapper.class);
        assertEquals("x", meta.getColumnName(1));
        assertEquals("int64", meta.getColumnTypeName(1));
        assertFalse(resWrapper.isColumnAttribute(1));

        assertEquals("a", meta.getColumnName(2));
        assertEquals("int32", meta.getColumnTypeName(2));
        assertTrue(resWrapper.isColumnAttribute(2));

        StringBuilder sbMain = new StringBuilder();
        while(!res.isAfterLast())
        {
            sbMain.append(res.getLong("x") + ":" + res.getInt("a") + ",");
            res.next();
        }
        assertEquals("0:10,1:9,2:8,3:7,", sbMain.toString());
    }

    public void testNonEmptyCoordinatesDefaults() throws SQLException
    {
        Statement st = conn.createStatement();
        ResultSet res = st.executeQuery("select * from array(<a:int32>[x=0:3,2,0], '[(10)()][(8)()]')");
        ResultSetMetaData meta = res.getMetaData();

        assertEquals("build", meta.getTableName(0));
        assertEquals(2, meta.getColumnCount());

        IResultSetWrapper resWrapper = res.unwrap(IResultSetWrapper.class);
        assertEquals("x", meta.getColumnName(1));
        assertEquals("int64", meta.getColumnTypeName(1));
        assertFalse(resWrapper.isColumnAttribute(1));

        assertEquals("a", meta.getColumnName(2));
        assertEquals("int32", meta.getColumnTypeName(2));
        assertTrue(resWrapper.isColumnAttribute(2));

        StringBuilder sbMain = new StringBuilder();
        while(!res.isAfterLast())
        {
            sbMain.append(res.getLong("x") + ":" + res.getInt("a") + ",");
            res.next();
        }
	assertEquals("0:10,2:8,", sbMain.toString());
    }

    public void testBooleanTypes() throws SQLException
    {
        Statement st = conn.createStatement();
        ResultSet res = st.executeQuery("select * from array(<b:bool null>[x=0:3,2,0], '[(null),(true)][(false),(null)]')");
        ResultSetMetaData meta = res.getMetaData();

        assertEquals("build", meta.getTableName(0));
        assertEquals(2, meta.getColumnCount());

        IResultSetWrapper resWrapper = res.unwrap(IResultSetWrapper.class);
        assertEquals("x", meta.getColumnName(1));
        assertEquals("int64", meta.getColumnTypeName(1));
        assertFalse(resWrapper.isColumnAttribute(1));

        assertEquals("b", meta.getColumnName(2));
        assertEquals("bool", meta.getColumnTypeName(2));
        assertTrue(resWrapper.isColumnAttribute(2));

        StringBuilder sbMain = new StringBuilder();
        while(!res.isAfterLast())
        {
            sbMain.append(res.getLong("x") + ":" + res.wasNull() + ":" + res.getBoolean("b") + ":" + res.wasNull() + ":");
            res.next();
        }
        assertEquals("0:false:false:true:1:false:true:false:2:false:false:false:3:false:false:true:", sbMain.toString());
    }

    public void testStatementWrapper() throws SQLException
    {
        Statement st = conn.createStatement();

        IStatementWrapper stWrapper = st.unwrap(IStatementWrapper.class);
        assertTrue(stWrapper.isAql());
        stWrapper.setAfl(true);
        assertTrue(stWrapper.isAfl());
        assertFalse(stWrapper.isAql());

        ResultSet res = st.executeQuery("build(<a:int32>[x=0:3,4,0], x)");
        ResultSetMetaData meta = res.getMetaData();

        assertEquals("build", meta.getTableName(0));
        assertEquals(2, meta.getColumnCount());

        IResultSetWrapper resWrapper = res.unwrap(IResultSetWrapper.class);
        assertEquals("x", meta.getColumnName(1));
        assertEquals("int64", meta.getColumnTypeName(1));
        assertFalse(resWrapper.isColumnAttribute(1));

        assertEquals("a", meta.getColumnName(2));
        assertEquals("int32", meta.getColumnTypeName(2));
        assertTrue(resWrapper.isColumnAttribute(2));

        StringBuilder sbMain = new StringBuilder();
        while(!res.isAfterLast())
        {
            sbMain.append(res.getLong("x") + ":" + res.wasNull() + ":" + res.getInt("a") + ":" + res.wasNull() + ":");
            res.next();
        }
        assertEquals("0:false:0:false:1:false:1:false:2:false:2:false:3:false:3:false:", sbMain.toString());
    }

    public void testBatchExecutionMultipleResults() throws SQLException
    {
        Statement st = conn.createStatement();
        st.addBatch("select * from array(<a:int32>[x=0:2,3,0], '[1,2,3]')");
        st.addBatch("select * from array(<b:int32>[y=0:2,3,0], '[4,5,6]')");
        st.executeBatch();

        StringBuilder sbMain = new StringBuilder();
        do
        {
            ResultSet res = st.getResultSet();
            sbMain.append("{");
            while(!res.isAfterLast())
            {
                sbMain.append(res.getLong(1) + ":" + res.getInt(2) + ":");
                res.next();
            }
            sbMain.append("}");
        } while (st.getMoreResults(Statement.CLOSE_ALL_RESULTS));
        conn.commit();
        assertEquals("{0:1:1:2:2:3:}{0:4:1:5:2:6:}", sbMain.toString());
    }
}
