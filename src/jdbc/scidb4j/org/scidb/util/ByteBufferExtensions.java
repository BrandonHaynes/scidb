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
package org.scidb.util;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class ByteBufferExtensions
{
    public static BigInteger getUnsignedLong(ByteBuffer buf)
    {
        byte[] b = new byte[8];
        buf.get(b);
        
        if (buf.order() == ByteOrder.BIG_ENDIAN)
        {
            return new BigInteger(1, b);
        }
        else
        {
            byte[] swapped = new byte[b.length];
            for (int i = 0; i < swapped.length; i++)
            {
                swapped[i] = b[b.length - 1 - i];
            }
            return new BigInteger(1, swapped);
        }
    }

    public static long getUnsignedInt(ByteBuffer buf)
    {
        byte[] b = new byte[4];
        buf.get(b);

        if (buf.order() == ByteOrder.BIG_ENDIAN)
        {
            return new BigInteger(1, b).longValue();
        }
        else
        {
            byte[] swapped = new byte[b.length];
            for (int i = 0; i < swapped.length; i++)
            {
                swapped[i] = b[b.length - 1 - i];
            }
            return new BigInteger(1, swapped).longValue();
        }
    }

    public static int getUnsignedShort(ByteBuffer buf)
    {
        byte[] b = new byte[2];
        buf.get(b);

        if (buf.order() == ByteOrder.BIG_ENDIAN)
        {
            return new BigInteger(1, b).intValue();
        }
        else
        {
            byte[] swapped = new byte[b.length];
            for (int i = 0; i < swapped.length; i++)
            {
                swapped[i] = b[b.length - 1 - i];
            }
            return new BigInteger(1, swapped).intValue();
        }
    }

    public static short getUnsignedByte(ByteBuffer buf)
    {
        return buf.get();
    }
}
