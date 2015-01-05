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
package org.scidb.jdbc;

import java.sql.SQLException;

/**
 * Interface which exposes some SciDB's features through JDBC
 */
public interface IResultSetWrapper
{
    /**
     * Checks if column derived from array attribute
     * @param columnIndex Column index
     * @return true if attribute
     * @throws SQLException
     */
    public boolean isColumnAttribute(int columnIndex) throws SQLException;

    /**
     * Checks if column derived from array attribute
     * @param columnLabel Column label
     * @return true if attribute
     * @throws SQLException
     */
    public boolean isColumnAttribute(String columnLabel) throws SQLException;

    /**
     * Checks if column derived from array dimension
     * @param columnIndex Column label
     * @return true if dimension
     * @throws SQLException
     */
    public boolean isColumnDimension(int columnIndex) throws SQLException;

    /**
     * Checks if column derived from array dimension
     * @param columnLabel Column label
     * @return true if dimension
     * @throws SQLException
     */
    public boolean isColumnDimension(String columnLabel) throws SQLException;
}
