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

public interface IChunk
{
    public int getAttributeId();

    /**
     * @return true if it's eof flag and there is no data in it. Access data
     *         causes an errors.
     */
    public boolean endOfArray();

    public boolean endOfChunk();

    /**
     * Check if current item is not last
     *
     * @return true if the next call of move return true and the next item
     *         exists
     */
    public boolean hasNext();

    /**
     * move current item forward. The first item must be available always.
     *
     * @return true if move is success and new item can be read. false is there
     *         is no new item. do not call getXXX method in this case.
     */
    public boolean move();

    public long getArrayId();

    public long[] getCoordinates();
}
