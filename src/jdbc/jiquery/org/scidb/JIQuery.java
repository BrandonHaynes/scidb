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

import java.util.Arrays;

import org.scidb.client.*;

import static java.lang.System.exit;
import static java.lang.System.out;

public class JIQuery
{
    public static void main(String[] args)
    {
        Connection conn = new Connection();
        try
        {
            if (args.length < 1)
            {
                System.err.println("First argument should be query string");
                exit(1);
            }
            String queryString = args[0];
            System.out.printf("Will execute query '%s'\n", queryString);

            conn.setWarningCallback(new WarningCallback()
            {
                @Override
                public void handleWarning(String whatStr)
                {
                    System.out.println("Warning: " + whatStr);
                }
            });

            conn.connect("localhost", 1239);
            Result res = conn.prepare(queryString);
            Array arr = conn.execute();

            if (arr == null)
            {
                return;
            }

            out.println(arr.getSchema().toString());

            arr.fetch();
            while (!arr.getEmptyBitmap().endOfArray())
            {
                while (!arr.endOfChunk())
                {
                    if (arr.getEmptyBitmap() != null)
                    {
                        System.out.print("(");
                        System.out.print(Arrays.toString(arr.getEmptyBitmap().getCoordinates()));
                        System.out.print(")");
                    }
                    System.out.print("[");
                    System.out.print(arr.getChunk(0).getString());
                    System.out.println("]");
                    arr.move();
                }
                arr.fetch();
            }
        } catch (Exception e)
        {
            e.printStackTrace();
        }
    }
}
