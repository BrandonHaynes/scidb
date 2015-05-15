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

import org.scidb.jdbc.IResultSetWrapper;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

class JDBCExample
{
    public static void main(String [] args) throws IOException
    {
        try
        {
            Class.forName("org.scidb.jdbc.Driver");
        }
        catch (ClassNotFoundException e)
        {
            System.out.println("Driver is not in the CLASSPATH -> " + e);
        }

        String iqueryHost = "localhost";
        String iqueryPort = "1239";

        switch (args.length)
        {
            case 1:
                iqueryHost = args[0];
                break;
            case 2:
                iqueryHost = args[0];
                iqueryPort = args[1];
                break;
            default:
                break;
        }

        try
        {
            String connString = "jdbc:scidb://" + iqueryHost + ":" + iqueryPort + "/";

            Connection conn = DriverManager.getConnection(connString);
            Statement st = conn.createStatement();
            //create array A<a:string>[x=0:2,3,0, y=0:2,3,0];
            //select * into A from array(A, '[["a","b","c"]["d","e","f"]["123","456","789"]]');
            ResultSet res = st.executeQuery("select * from array(<a:string>[x=0:2,3,0, y=0:2,3,0], '[[\"a\",\"b\",\"c\"][\"d\",\"e\",\"f\"][\"123\",\"456\",\"789\"]]')");
            ResultSetMetaData meta = res.getMetaData();

            System.out.println("Source array name: " + meta.getTableName(0));
            System.out.println(meta.getColumnCount() + " columns:");

            IResultSetWrapper resWrapper = res.unwrap(IResultSetWrapper.class);
            for (int i = 1; i <= meta.getColumnCount(); i++)
            {
                System.out.println(meta.getColumnName(i) + " - " + meta.getColumnTypeName(i) + " - is attribute:" + resWrapper.isColumnAttribute(i));
            }
            System.out.println("=====");

            System.out.println("x y a");
            System.out.println("-----");
            while(!res.isAfterLast())
            {
                System.out.println(res.getLong("x") + " " + res.getLong("y") + " " + res.getString("a"));
                res.next();
            }
        }
        catch (SQLException e)
        {
            System.out.println(e);
        }

    	System.exit(0);
    }
}
