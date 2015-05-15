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

import junit.textui.*;

public class JDBCTest
{
    public static void main(String[] args)
    {
        String iqueryHost;
        String iqueryPort;

        switch (args.length)
        {
            case 1:
                iqueryHost = args[0];
                JDBCBasicTestCase.setIqueryHost(iqueryHost);
                break;
            case 2:
                iqueryHost = args[0];
                iqueryPort = args[1];
                JDBCBasicTestCase.setIqueryHost(iqueryHost);
                JDBCBasicTestCase.setIqueryPort(iqueryPort);
                break;
            default:
                break;
        }

        TestRunner.run(JDBCBasicTestCase.class);
    }
}
