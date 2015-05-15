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

/****************************************************************************/

#error "Arena documention only."                         // Not for inclusion

/****************************************************************************/

/**
 *  @namespace  scidb::arena
 *
 *  @brief      Introduces facilities for controlling and tracking the use of
 *              memory within a %SciDB server instance.
 *
 *  @see        http://trac.scidb.net/wiki/Development/OperatorMemoryManagement
 *              for a description of the main issues that this library aims to
 *              address.
 *
 *  @author     jbell@paradigm4.com.
 */

/**
 *  @namespace  scidb::arena::managed
 *
 *  @brief      Introduces specializations of the standard library containers
 *              that work with arenas.
 *
 *  @details    The managed namespace includes specializations for a number of
 *              the more useful standard library containers that acquire their
 *              internal memory from an abstract %arena. They are designed for
 *              'drop in' compatibility with the standard library classes from
 *              which they inherit - they would be defined as template aliases
 *              if this language feature were available to us - and are housed
 *              within the managed namespace to allow them to coexist with and
 *              selectively replace their standard counterparts.
 *
 *              The goal here is to make converting existing code to work with
 *              arenas as simple and unobtrusive as possible,  and, insofar as
 *              is possible, tame the God-awful error messages that the use of
 *              allocator template arguments  otherwise tends to throw up when
 *              things go wrong.
 *
 *  @author     jbell@paradigm4.com.
 */

/****************************************************************************/
