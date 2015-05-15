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
package org.scidb.client;

import org.scidb.io.network.Message.Chunk;
import org.scidb.util.ByteBufferExtensions;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.logging.Logger;

public class EmptyChunk implements IChunk
{
    private int attributeId;
    private long arrayId;
    private boolean eof;

    // Fields for RLE chunk header
    private ByteBuffer chunkData;
    private Header header;
    private Segment[] segments;
    private int payloadStart;
    private boolean _end = false;

    // Fields for iterating over items
    private int curSeg = 0;
    private long curLogicalPosition;
    private long nNonEmptyItems = 0;
    private long[] _startPos;
    private long[] _chunkLen;
    private long[] _coordinates;


    private static Logger log = Logger.getLogger(EmptyChunk.class.getName());

    public EmptyChunk(Chunk msg, Array array) throws Error
    {
        org.scidb.io.network.ScidbMsg.Chunk record = msg.getRecord();
        attributeId = record.getAttributeId();
        arrayId = record.getArrayId();
        eof = record.getEof();

        if (!eof)
        {
            chunkData = ByteBuffer.wrap(msg.getData());
            chunkData.order(ByteOrder.LITTLE_ENDIAN);
            int compressionMethod = record.getCompressionMethod();

            int coordCount = record.getCoordinatesCount();
            _coordinates = new long[coordCount];
            _startPos = new long[coordCount];
            long[] endPos = new long[coordCount];
            _chunkLen = new long[coordCount];
            for (int dimNo = 0; dimNo < record.getCoordinatesCount(); dimNo++)
            {
                long chunkCoordinate = record.getCoordinates(dimNo);
                Schema.Dimension dim = array.getSchema().getDimensions()[dimNo];
                long endCoord = chunkCoordinate + dim.getChunkInterval() - 1;
                if (endCoord > dim.getEndMax())
                    endCoord = dim.getEndMax();
                _startPos[dimNo] = chunkCoordinate;
                endPos[dimNo] = endCoord;
                _chunkLen[dimNo] = endCoord - chunkCoordinate + 1;
            }

            if (array.getSchema().getAttributes()[attributeId].isEmptyIndicator())
            {
                log.fine("Got bitmap chunk");
            } else
            {
                log.fine("Got just chunk");
            }

            header = new Header(chunkData);

            if (compressionMethod != 0)
            {
                throw new Error("Compressed chunks not yet supported");
            }

            segments = new Segment[header.nSegs];
            for (int i = 0; i < segments.length; i++)
            {
                segments[i] = new Segment(chunkData);
            }
            if (segments.length == 0)
                _end = true;

            payloadStart = chunkData.position();

            curLogicalPosition = segments[0].lPosition;
        }
    }

    public int getAttributeId()
    {
        return attributeId;
    }

    public boolean endOfArray()
    {
        return eof;
    }

    public  boolean endOfChunk()
    {
        return _end;
    }

    public boolean hasNext()
    {
        int _curSeg = curSeg;
        long _curLogicalPosition = curLogicalPosition;
        long nNonEmptyItemsTmp = nNonEmptyItems;
        boolean endTmp = _end;
        boolean r = move();
        curSeg = _curSeg;
        curLogicalPosition = _curLogicalPosition;
        nNonEmptyItems = nNonEmptyItemsTmp;
        _end = endTmp;
        return r;
    }

    public boolean move()
    {
        if (_end)
            return false;
        curLogicalPosition++;
        nNonEmptyItems++;
        if (curLogicalPosition >= (segments[curSeg].lPosition + segments[curSeg].length) )
        {
            curSeg++;
            if ( (curSeg == segments.length) || (nNonEmptyItems > header.nNonEmptyElements) )
            {
                _end = true;
                return false;
            }
            curLogicalPosition = segments[curSeg].lPosition;
        }
        return true;
    }

    public long[] getCoordinates()
    {
        long l = curLogicalPosition;
        for (int i = _coordinates.length - 1; i >= 0 ; i--)
        {
            _coordinates[i] = _startPos[i] + l % _chunkLen[i];
            l /= _chunkLen[i];
        }

        return _coordinates;
    }

    public long getArrayId()
    {
        return arrayId;
    }

    public static class Header
    {
        final BigInteger RleBitmapPayloadMagic = new BigInteger("17216886072620792492");
        int nSegs;
        long nNonEmptyElements;

        public Header(ByteBuffer src)
        {
            BigInteger magic = ByteBufferExtensions.getUnsignedLong(src);
            if (magic.equals(RleBitmapPayloadMagic))
            {
                log.fine("Magic is empty bitmap payload");
            } else
            {
                log.fine("Magic is shit");
            }

            nSegs = (int) src.getLong();
            nNonEmptyElements = src.getLong();
        }
    }

    public class Segment
    {
        long lPosition; /**< start position of sequence of set bits */
        long length;    /**< number of set bits */
        long pPosition; /**< index of value in payload */

        public Segment(ByteBuffer src)
        {
            lPosition = src.getLong();
            length = src.getLong();
            pPosition = src.getLong();
        }
    }
}
