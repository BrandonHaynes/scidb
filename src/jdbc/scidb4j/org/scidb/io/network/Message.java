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
package org.scidb.io.network;

import java.io.IOException;
import org.scidb.util.InputStreamWithReadall;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import com.google.protobuf.GeneratedMessage;

/**
 * Base class for constructing network messages locally and from socket stream
 */
public abstract class Message
{
    private GeneratedMessage record;
    private Header header;

    public static final short mtNone = 0;
    public static final short mtExecuteQuery = 1;
    public static final short mtPreparePhysicalPlan = 2;
    public static final short mtExecutePhysicalPlan = 3;
    public static final short mtFetch = 4;
    public static final short mtChunk = 5;
    public static final short mtChunkReplica = 6;
    public static final short mtRecoverChunk = 7;
    public static final short mtReplicaSyncRequest = 8;
    public static final short mtReplicaSyncResponse = 9;
    public static final short mtAggregateChunk = 10;
    public static final short mtQueryResult = 11;
    public static final short mtError = 12;
    public static final short mtSyncRequest = 13;
    public static final short mtSyncResponse = 14;
    public static final short mtCancelQuery = 15;
    public static final short mtRemoteChunk = 16;
    public static final short mtNotify = 17;
    public static final short mtWait = 18;
    public static final short mtBarrier = 19;
    public static final short mtMPISend = 20;
    public static final short mtAlive = 21;
    public static final short mtPrepareQuery = 22;
    public static final short mtResourcesFileExistsRequest = 23;
    public static final short mtResourcesFileExistsResponse = 24;
    public static final short mtAbort = 25;
    public static final short mtCommit = 26;
    public static final short mtCompleteQuery = 27;
    public static final short mtControl = 28;
    public static final short mtSystemMax = 29;

    /**
     * Make message from header
     *
     * @param header Message header
     */
    public Message(Header header)
    {
        this.header = header;
    }

    /**
     * Make message from stream
     *
     * It will read header first and then construct proper message
     *
     * @param is Input stream
     * @return Network message
     * @throws org.scidb.client.Error
     * @throws IOException
     */
    public static Message parseFromStream(InputStreamWithReadall is) throws org.scidb.client.Error, IOException
    {
        Header hdr = Header.parseFromStream(is);

        switch (hdr.messageType)
        {
            case mtError:
                return new Error(hdr, is);

            case mtQueryResult:
                return new QueryResult(hdr, is);

            case mtChunk:
                return new Chunk(hdr, is);
            default:
                throw new org.scidb.client.Error("Unknown network message type: " + hdr.messageType);
        }
    }

    /**
     * Serialize message to stream
     *
     * @param os Output stream for writing
     * @throws IOException
     */
    public void writeToStream(OutputStream os) throws IOException
    {
        ByteBuffer buf = ByteBuffer.allocate(32);
        buf.clear();
        buf.order(ByteOrder.LITTLE_ENDIAN);
        buf.putShort(header.netProtocolVersion);
        buf.putShort(header.messageType);
        buf.putInt(getRecordSize());
        buf.putInt(header.binarySize);
        buf.putInt(0); // Structure data aligning padding
        buf.putLong(header.sourceInstanceID);
        buf.putLong(header.queryID);
        buf.flip();
        os.write(buf.array());
        if (record != null)
            record.writeTo(os);
    }

    /**
     * Returns size of serialized protobuf part
     *
     * @return Size of serialized protobuf part
     */
    public int getRecordSize()
    {
        return (record != null) ? record.getSerializedSize() : 0;
    }

    /**
     * Set serialized protobuf part
     *
     * @param record Protobuf record
     */
    private void setRecord(com.google.protobuf.GeneratedMessage record)
    {
        this.record = record;
    }

    /**
     * Get serialized protobuf part
     *
     * @return Protobuf record
     */
    public com.google.protobuf.GeneratedMessage getRecord()
    {
        return record;
    }

    /**
     * Returns message header structure
     *
     * @return Header
     */
    public Header getHeader()
    {
        return header;
    }

    /**
     * Message header which delimit protobuf parts.
     * Note to developers: the headerSize is 32, even though the total size of all fields in class Header
     * is 28. The reason is that the corresponding server-side C++ structure is 8-byte aligned.
     */
    public static class Header
    {   /// Must match the server network protocol version (in src/network/BaseConnection.h)
        public static final short NET_PROTOCOL_CURRENT_VER = 4;

        public static final int headerSize = 32;
        public short netProtocolVersion; // uint16_t
        public short messageType; // uint16_t
        public int recordSize; // uint32_t
        public int binarySize; // uint32_t
        public long sourceInstanceID; // uint64_t
        public long queryID; // uint64_t

        /**
         * Default void constructor
         */
        public Header()
        {
            this.netProtocolVersion = NET_PROTOCOL_CURRENT_VER;
            this.messageType = (short) 0;
            this.sourceInstanceID = ~0;
            this.recordSize = 0;
            this.binarySize = 0;
            this.queryID = 0;
        }

        /**
         * Construct header and fill query id and message type
         *
         * @param queryId Query ID
         * @param messageType Message type
         */
        public Header(long queryId, int messageType)
        {
            this();
            this.messageType = (short) messageType;
            this.queryID = queryId;
        }

        /**
         * Make header from stream
         *
         * @param is Stream for reading
         * @return Header
         * @throws IOException
         */
        public static Header parseFromStream(InputStreamWithReadall is) throws IOException
        {
            Header res = new Header();
            byte[] b = new byte[Header.headerSize];
            if (is.readAll(b, 0, Header.headerSize) != Header.headerSize) {
                throw new IOException("Failed to read the full Message::Header");
            }
            ByteBuffer buf = ByteBuffer.wrap(b);

            buf.order(ByteOrder.LITTLE_ENDIAN);
            res.netProtocolVersion = buf.getShort();
            res.messageType = buf.getShort();
            res.recordSize = buf.getInt();
            res.binarySize = buf.getInt();
            buf.getInt(); // Structure data aligning padding
            res.sourceInstanceID = buf.getLong();
            res.queryID = buf.getLong();

            return res;
        }
    }

    /**
     * Query preparing and executing message
     *
     * Only for sending
     */
    public static class Query extends Message
    {
        /**
         * Constructor
         *
         * @param queryId Query ID
         * @param queryString Query string
         * @param afl true=AFL, false=AQL
         * @param programOptions Program options
         * @param execute true=execute, false=prepare
         */
        public Query(long queryId, String queryString, Boolean afl, String programOptions, Boolean execute)
        {
            super(new Message.Header(queryId, execute ? mtExecuteQuery : mtPrepareQuery));
            ScidbMsg.Query.Builder recBuilder = ScidbMsg.Query.newBuilder();
            recBuilder.setQuery(queryString);
            recBuilder.setAfl(afl);
            recBuilder.setProgramOptions(programOptions);
            super.setRecord(recBuilder.build());
        }
    }

    /**
     * Error message
     *
     * Only for receiving
     */
    public static class Error extends Message
    {
        /**
         * Constructor
         *
         * @param hdr Header
         * @param is Input stream
         * @throws IOException
         */
        public Error(Header hdr, InputStreamWithReadall is) throws IOException
        {
            super(hdr);
            assert (hdr.messageType == mtError);
            byte[] buf = new byte[hdr.recordSize];
            if (is.readAll(buf, 0, hdr.recordSize) != hdr.recordSize)  {
                throw new IOException("Failed to read the full Error::Header.");
            }

            super.setRecord(ScidbMsg.Error.parseFrom(buf));
        }

        /**
         * Returns Cast base protobuf record to Error and return
         *
         * @return Error protobuf record
         */
        @Override
        public ScidbMsg.Error getRecord()
        {
            return (ScidbMsg.Error) super.getRecord();
        }
    }

    /**
     * Query result message
     *
     * Only for receiving
     */
    public static class QueryResult extends Message
    {
        /**
         * Constructor
         *
         * @param hdr Header
         * @param is Input stream
         * @throws IOException
         */
        public QueryResult(Header hdr, InputStreamWithReadall is) throws IOException
        {
            super(hdr);
            assert (hdr.messageType == mtQueryResult);
            byte[] buf = new byte[hdr.recordSize];
            if (is.readAll(buf, 0, hdr.recordSize) != hdr.recordSize)  {
                throw new IOException("Failed to read the full QueryResult::Header");
            }

            super.setRecord(ScidbMsg.QueryResult.parseFrom(buf));
        }

        /**
         * Returns Cast base protobuf record to QueryResult and return
         *
         * @return QueryResult protobuf record
         */
        @Override
        public ScidbMsg.QueryResult getRecord()
        {
            return (ScidbMsg.QueryResult) super.getRecord();
        }
    }

    /**
     * Fetch chunk message
     *
     * Only for sending
     */
    public static class Fetch extends Message
    {
        /**
         * Constructor
         *
         * @param queryId Query ID
         * @param attributeId Attribute to fetch
         * @param arrayName Array name to fetch
         */
        public Fetch(long queryId, int attributeId, String arrayName)
        {
            super(new Message.Header(queryId, mtFetch));
            ScidbMsg.Fetch.Builder recBuilder = ScidbMsg.Fetch.newBuilder();
            recBuilder.setAttributeId(attributeId);
            recBuilder.setArrayName(arrayName);
            super.setRecord(recBuilder.build());
        }
    }

    /**
     * Chunk message
     *
     * Only for receiving
     */
    public static class Chunk extends Message
    {
        private byte[] chunkData = null;

        /**
         * Constructor
         *
         * @param hdr Header
         * @param is Input stream
         * @throws IOException
         */
        public Chunk(Header hdr, InputStreamWithReadall is) throws IOException
        {
            super(hdr);
            assert (hdr.messageType == mtChunk);
            byte[] buf = new byte[hdr.recordSize];
            if (is.readAll(buf, 0, hdr.recordSize) != hdr.recordSize)  {
                throw new IOException("Failed to read the full Chunk::Header");
            }

            chunkData = new byte[hdr.binarySize];
            if (is.readAll(chunkData, 0, hdr.binarySize) != hdr.binarySize) {
                throw new IOException("Failed to read the full Chunk data");
            }

            super.setRecord(ScidbMsg.Chunk.parseFrom(buf));
        }

        /**
         * Get chunk binary data
         * @return Array with chunk data
         */
        public byte[] getData()
        {
            return chunkData;
        }

        /**
         * Returns Cast base protobuf record to Chunk and return
         *
         * @return Chunk protobuf record
         */
        @Override
        public ScidbMsg.Chunk getRecord()
        {
            return (ScidbMsg.Chunk) super.getRecord();
        }
    }

    /**
     * Message for commiting query
     */
    public static class CompleteQuery extends Message
    {
        public CompleteQuery(long queryId)
        {
            super(new Message.Header(queryId, mtCompleteQuery));
        }
    }

    /**
     * Message for rollbacking query
     */
    public static class AbortQuery extends Message
    {
        public AbortQuery(long queryId)
        {
            super(new Message.Header(queryId, mtCancelQuery));
        }
    }
}
