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

import java.io.IOException;
import java.io.InputStream;

/**
 * A InputStreamWithReadall extends InputStream with a readAll(...) method, to read all requested bytes.
 * Internally, repeated call to read(...) is made, until all requested bytes are read or error occurs.
 *
 * @author Donghui Zhang
 *
 */
public class InputStreamWithReadall extends InputStream
{
    private InputStream _in;
    
    /**
     * Creates a InputStreamWithReadall and saves its argument, the input stream in, for later use.
     *
     * @param in    the underlying input stream.
     */
    public InputStreamWithReadall(java.io.InputStream in)
    {
        _in = in;
    }

    /**
     * Read all requested bytes from this byte-input stream into the specified byte array.
     * @note
     *    This method has the same signature as, and is similar to, the read() method.
     *    The difference is that upon a partial read, this method repeatedly requests for more bytes, until:
     *    <br>- all the requested bytes are read, or
     *    <br>- end of file has been reached, or
     *    <br>- an IOException is thrown.
     * @param b         the buffer into which the data is read.
     * @param offset    the start offset in buffer at which the data is written.
     * @param len       number of requested bytes
     *
     * @return the number of requested bytes (if all requested bytes are read), or
     *         the actual number of bytes read (if end of file has been reached, after at least one byte is read), or
     *         -1 (if the first byte cannot be read due to end of file).
     * @throws IOException  if the first byte cannot be read for any reason other than end of file,
     *         or if some other I/O error occurs.
     */
    public int readAll(byte[] b, int offset, int len) throws java.io.IOException
    {
        int nBytesTotal = 0;
        while (nBytesTotal < len) {
            int nBytes = read(b, offset+nBytesTotal, len-nBytesTotal);
            if (nBytes == -1) {
                return nBytesTotal==0 ? -1 : nBytesTotal;
            }
            nBytesTotal += nBytes;
        }
        return len;
    }

    @Override
    public int read() throws IOException {
        // TODO Auto-generated method stub
        return _in.read();
    }
}
