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

/**
 * Class describing all SciDB built-in types
 */
public class Type
{
    public static final String TID_INDICATOR = "indicator";
    public static final String TID_CHAR = "char";
    public static final String TID_INT8 = "int8";
    public static final String TID_INT16 = "int16";
    public static final String TID_INT32 = "int32";
    public static final String TID_INT64 = "int64";
    public static final String TID_UINT8 = "uint8";
    public static final String TID_UINT16 = "uint16";
    public static final String TID_UINT32 = "uint32";
    public static final String TID_UINT64 = "uint64";
    public static final String TID_FLOAT = "float";
    public static final String TID_DOUBLE = "double";
    public static final String TID_BOOL = "bool";
    public static final String TID_STRING = "string";
    public static final String TID_DATETIME = "datetime";
    public static final String TID_DATETIMETZ = "datetimetz";
    public static final String TID_VOID = "void";
    public static final String TID_BINARY = "binary";
    public static final String TID_FIXED_STRING = "string_*";    
}
