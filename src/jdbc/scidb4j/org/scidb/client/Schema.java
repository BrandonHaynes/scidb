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

import java.util.Arrays;

/**
 * Array schema of query result
 */
public class Schema
{
    private String name;
    private Attribute[] attributes;
    private Dimension[] dimensions;
    private Attribute emptyIndicator;

    /**
     * Constructor
     *
     * @param name Array name
     * @param attributes Attributes array
     * @param dimensions Dimensions array
     */
    public Schema(String name, Attribute[] attributes, Dimension[] dimensions)
    {
        this.name = name;
        this.attributes = attributes;
        this.dimensions = dimensions;
        for (Attribute att: attributes)
        {
            if (att.isEmptyIndicator())
            {
                emptyIndicator = att;
                break;
            }
        }
    }

    /**
     * Returns attributes array
     *
     * @return Attributes
     */
    public Attribute[] getAttributes()
    {
        return attributes;
    }

    /**
     * Returns attributes array
     *
     * @return Attributes
     */
    public Attribute[] getAttributes(boolean withoutEmptyFlag)
    {

        return attributes;
    }

    /**
     * Returns dimensions array
     * @return Dimensions
     */
    public Dimension[] getDimensions()
    {
        return dimensions;
    }

    /**
     * Returns array name
     * @return Array name
     */
    public String getName()
    {
        return name;
    }

    public Attribute getEmptyIndicator()
    {
        return emptyIndicator;
    }

    /**
     * Array attribute
     */
    public static class Attribute
    {
        private int id;
        private String name;
        private String type;
        private boolean nullable;
        private boolean emptyIndicator;

        /**
         * Constructor
         *
         * @param id Attribute ID
         * @param name Attribute name
         * @param type Attribute type
         * @param flags Attribute flags
         */
        public Attribute(int id, String name, String type, int flags)
        {
            this.id = id;
            this.name = name;
            this.type = type;
            this.nullable = (flags & 1/*IS_NULLABLE*/) != 0;
            this.emptyIndicator = (flags & 2/*IS_EMPTY_INDICATOR*/) != 0;
        }

        /**
         * Returns attribute ID
         * @return Attribute ID
         */
        public int getId()
        {
            return id;
        }

        /**
         * Returns attribute name
         * @return Attribute name
         */
        public String getName()
        {
            return name;
        }

        /**
         * Returns attribute type
         * @return Attribute type
         */
        public String getType()
        {
            return type;
        }

        /**
         * Returns nullable flag
         * @return true if nullable
         */
        public boolean isNullable()
        {
            return nullable;
        }

        /**
         * Returns empty indicator flag
         * @return true if attribute is empty indicator
         */
        public boolean isEmptyIndicator()
        {
            return emptyIndicator;
        }

        @Override
        public String toString()
        {
            return String.format("%s:%s", name, type);
        }
    }

    /**
     * Array dimension
     */
    public static class Dimension
    {
        private String name;
        private long startMin = 0;
        private long currStart = 0;
        private long currEnd = 0;
        private long endMax = 0;
        private long chunkInterval = 0;

        /**
         * Constructor
         * @param name dimension name
         * @param startMin dimension minimum start
         * @param currStart dimension current start
         * @param currEnd dimension current end
         * @param endMax endMax dimension maximum end
         * @param chunkInterval  chunk size in this dimension
         */
        public Dimension(String name,
                         long startMin,
                         long currStart,
                         long currEnd,
                         long endMax,
                         long chunkInterval)
        {
            this.name = name;
            this.startMin = startMin;
            this.currStart = currStart;
            this.currEnd = currEnd;
            this.endMax = endMax;
            this.chunkInterval = chunkInterval;
        }

        /**
         * Returns dimension name
         * @return Dimension name
         */
        public String getName()
        {
            return name;
        }

        public long getStartMin()
        {
            return startMin;
        }

        public long getCurrStart()
        {
            return currStart;
        }

        public long getCurrEnd()
        {
            return currEnd;
        }

        public long getEndMax()
        {
            return endMax;
        }

        public long getChunkInterval()
        {
            return chunkInterval;
        }
    }

    @Override
    public String toString()
    {
        return String.format("%s <%s>", name, Arrays.toString(attributes));
    }
}
