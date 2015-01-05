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

import org.scidb.io.network.Message.QueryResult;
import org.scidb.io.network.ScidbMsg;

/**
 * Query result
 */
public class Result
{
    private Schema schema;
    private long queryId;
    private boolean selective;
    private String explainLogical;
    private String explainPhysical;

    /**
     * Constructor
     * @param result Query result network message
     * @param conn Connection
     */
    public Result(QueryResult result, Connection conn)
    {
        ScidbMsg.QueryResult rec = result.getRecord();
        String schemaName = rec.getArrayName();
        Schema.Attribute[] attributes = new Schema.Attribute[rec.getAttributesCount()];
        Schema.Dimension[] dimensions = new Schema.Dimension[rec.getDimensionsCount()];

        int i = 0;
        for (ScidbMsg.QueryResult.AttributeDesc att : rec.getAttributesList())
        {
            attributes[i] = new Schema.Attribute(att.getId(), att.getName(), att.getType(), att
                    .getFlags());
            i++;
        }

        i = 0;
        for (ScidbMsg.QueryResult.DimensionDesc dim : rec.getDimensionsList())
        {
            dimensions[i] = new Schema.Dimension(dim.getName(),
                                                 dim.getStartMin(),
                                                 dim.getCurrStart(),
                                                 dim.getCurrEnd(),
                                                 dim.getEndMax(),
                                                 dim.getChunkInterval());
            i++;
        }

        this.queryId = result.getHeader().queryID;
        this.schema = new Schema(schemaName, attributes, dimensions);
        this.selective = rec.getSelective();
        this.explainLogical = rec.getExplainLogical();
        this.explainPhysical = rec.getExplainPhysical();

        if (rec.getWarningsCount() > 0 && conn.getWarningCallback() != null)
        {
            for (ScidbMsg.QueryResult.Warning warn : rec.getWarningsList())
            {
                conn.getWarningCallback().handleWarning(warn.getWhatStr());
            }
        }
    }

    /**
     * Returns result schema
     * @return Schema
     */
    public Schema getSchema()
    {
        return schema;
    }

    /**
     * Returns result query ID
     * @return Query ID
     */
    public long getQueryId()
    {
        return queryId;
    }

    /**
     * Returns selective flag
     * @return true - if selective
     */
    public boolean isSelective()
    {
        return selective;
    }

    /**
     * Returns explained logical plan
     * @return Logical plan
     */
    public String getExplainLogical()
    {
        return explainLogical;
    }

    /**
     * Returns explained physical plan
     * @return Physical plan
     */
    public String getExplainPhysical()
    {
        return explainPhysical;
    }
}
