--
--  BEGIN_COPYRIGHT
-- 
--  This file is part of SciDB.
--  Copyright (C) 2008-2011 SciDB, Inc.
-- 
--  SciDB is free software: you can redistribute it and/or modify
--  it under the terms of the GNU General Public License as published by
--  the Free Software Foundation version 3 of the License, or
--  (at your option) any later version.
-- 
--  SciDB is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
--  INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
--  NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
--  the GNU General Public License for the complete license terms.
-- 
--  You should have received a copy of the GNU General Public License
--  along with SciDB.  If not, see <http://www.gnu.org/licenses/>.
-- 
-- END_COPYRIGHT 
--
--   File: test_for_bad_schema_data.sql
--
--  About: 
--
--    The purpose of this file is to hold a list of SQL queries which 
--    ensure that the postgres rules we are applying are appropriate 
--    to ensure that our life is made simple inside the server.
--
-- 1. Check the rules and procedures. 
--
--  1.1 Create the array, populating the array table.
insert into "array"
( name, partitioning_schema, flags ) 
values
( 'test', 1, 0 );
--
--  1.2 Add an attribute for the new array. 
insert into "array_attribute"
( array_id, id, name,  type, flags, default_compression_method )
SELECT A.id, 1, 'foo', 1, 0, 1 
  FROM "array" A 
 WHERE A.name = 'test';
--
--  1.3 Add an dimension for the new array. 
INSERT into "array_dimension"
( array_id, id, name, startMin, currStart, currEnd, endMax, chunk_interval,
  chunk_overlap )
SELECT A.id, 1, 'bar', 0, 0,  100, 0, 10, 0
  FROM "array" A 
 WHERE A.name = 'test';
--
-- Test # 1: 
--
-- 2.1 Try to insert a duplicate attribute. This should produce an exception 
--     due to the UNIQUE check constraint in the array_attribute table. 
--     NOTE: The "id" column needs a value > 1 to stay good with respect to 
--           the table's primary key.
insert into "array_attribute"
( array_id, id, name,  type, flags, default_compression_method )
SELECT A.id, 2, 'foo', 1, 0, 1 
  FROM "array" A 
 WHERE A.name = 'test';
-- ERROR:  duplicate key value violates unique constraint "array_attribute_array_id_key"
--
-- 2.2 Try to insert a duplicate dimension. This should produce an exception 
--     due to the UNIQUE check constraint in the array_dimension table. 
--     NOTE: The "id" column needs a value > 1 to stay good with respect to 
--           the table's primary key.
INSERT into "array_dimension"
( array_id, id, name, startMin, currStart, currEnd, endMax, chunk_interval,
  chunk_overlap )
SELECT A.id, 2, 'bar', 0, 0,  100, 0, 10, 0
  FROM "array" A 
 WHERE A.name = 'test';
-- ERROR:  duplicate key value violates unique constraint "array_dimension_array_id_key"
--
-- 2.3 Try to add an attribute with the same name as the dimension.
insert into "array_attribute"
( array_id, id, name,  type, flags, default_compression_method )
SELECT A.id, 2, 'bar', 1, 0, 1 
  FROM "array" A 
 WHERE A.name = 'test';
-- ERROR:  Repeated attribute / dimension name in array test
--
-- 2.4 Try to add a duplicate dimension with the same name as an attribute. 
INSERT into "array_dimension"
( array_id, id, name, startMin, currStart, currEnd, endMax, chunk_interval,
  chunk_overlap )
SELECT A.id, 2, 'foo', 0, 0,  100, 0, 10, 0
  FROM "array" A 
 WHERE A.name = 'test';
-- ERROR:  Repeated attribute / dimension name in array test
--
-- 3. Add another attribute, and a dimension.
--
--  3.1 Add another attribute. 
insert into "array_attribute"
( array_id, id, name,  type, flags, default_compression_method )
SELECT A.id, 2, 'mug', 2, 0, 1
  FROM "array" A
 WHERE A.name = 'test';
--
--  3.2 Add another dimension for the array. 
INSERT into "array_dimension"
( array_id, id, name, startMin, currStart, currEnd, endMax, chunk_interval,
  chunk_overlap )
SELECT A.id, 2, 'wump', 0, 0,  100, 0, 10, 0
  FROM "array" A
 WHERE A.name = 'test';
--
-- 4.0 Repeat the earlier efforts to insert invalid attributes and dimensions.
--
--  4.1 Attempt to insert a duplicate attribute name.
insert into "array_attribute"
( array_id, id, name,  type, flags, default_compression_method )
SELECT A.id, 3, 'foo', 1, 0, 1
  FROM "array" A
 WHERE A.name = 'test';
-- ERROR:  duplicate key value violates unique constraint "array_attribute_array_id_key"
--
--  4.2 Attempt to insert a duplicate attribute name.
INSERT into "array_dimension"
( array_id, id, name, startMin, currStart, currEnd, endMax, chunk_interval,
  chunk_overlap )
SELECT A.id, 3, 'bar', 0, 0,  100, 0, 10, 0
  FROM "array" A
 WHERE A.name = 'test';
-- ERROR:  duplicate key value violates unique constraint "array_dimension_array_id_key"
--
-- 4.3 Try to add an attribute with the same name as the dimension.
insert into "array_attribute"
( array_id, id, name,  type, flags, default_compression_method )
SELECT A.id, 3, 'bar', 1, 0, 1 
  FROM "array" A 
 WHERE A.name = 'test';
-- ERROR:  Repeated attribute / dimension name in array test
--
-- 4.4 Try to add a duplicate dimension with the same name as an attribute. 
INSERT into "array_dimension"
( array_id, id, name, startMin, currStart, currEnd, endMax, chunk_interval,
  chunk_overlap )
SELECT A.id, 3, 'foo', 0, 0,  100, 0, 10, 0
  FROM "array" A 
 WHERE A.name = 'test';
-- ERROR:  Repeated attribute / dimension name in array test
--
-- 5. Add another array. The initial inserts ought to work without a 
--    problem, as the rules are defined at the array_id level. 
-- 
--  5.1 
insert into "array"
( name, partitioning_schema, flags )
values
( 'test_two', 1, 0 );
--
--  5.2 Add an attribute for the new array. 
insert into "array_attribute"
( array_id, id, name,  type, flags, default_compression_method )
SELECT A.id, 1, 'foo', 1, 0, 1
  FROM "array" A
 WHERE A.name = 'test_two';
--
--  5.3 Add an dimension for the new array. 
INSERT into "array_dimension"
( array_id, id, name, startMin, currStart, currEnd, endMax, chunk_interval,
  chunk_overlap )
SELECT A.id, 1, 'bar', 0, 0,  100, 0, 10, 0
  FROM "array" A
 WHERE A.name = 'test_two';
--
-- 6: Repeat trying to add bad attributes and dimensions to both the 
--      first, and the second array. 
-- 6.1 Try to insert a duplicate attribute. This should produce an exception 
--     due to the UNIQUE check constraint in the array_attribute table. 
insert into "array_attribute"
( array_id, id, name,  type, flags, default_compression_method )
SELECT A.id, 3, 'foo', 1, 0, 1
  FROM "array" A
 WHERE A.name = 'test';
-- ERROR:  duplicate key value violates unique constraint "array_attribute_array_id_key"
--
-- 6.2 Try to insert a duplicate dimension. This should produce an exception 
--     due to the UNIQUE check constraint in the array_dimension table. 
INSERT into "array_dimension"
( array_id, id, name, startMin, currStart, currEnd, endMax, chunk_interval,
  chunk_overlap )
SELECT A.id, 3, 'bar', 0, 0,  100, 0, 10, 0
  FROM "array" A
 WHERE A.name = 'test';
-- ERROR:  duplicate key value violates unique constraint "array_dimension_array_id_key"
--
-- 6.3 Try to add an attribute with the same name as the dimension.
insert into "array_attribute"
( array_id, id, name,  type, flags, default_compression_method )
SELECT A.id, 3, 'bar', 1, 0, 1
  FROM "array" A
 WHERE A.name = 'test';
-- ERROR:  Repeated attribute / dimension name in array test
--
-- 6.4 Try to add a duplicate dimension with the same name as an attribute. 
INSERT into "array_dimension"
( array_id, id, name, startMin, currStart, currEnd, endMax, chunk_interval,
  chunk_overlap )
SELECT A.id, 3, 'foo', 0, 0,  100, 0, 10, 0
  FROM "array" A
 WHERE A.name = 'test';
-- ERROR:  Repeated attribute / dimension name in array test
--
-- Try also for the new array. 
-- 6.5 Repeat attribute to array two.
insert into "array_attribute"
( array_id, id, name,  type, flags, default_compression_method )
SELECT A.id, 2, 'foo', 1, 0, 1
  FROM "array" A
 WHERE A.name = 'test_two';
-- ERROR:  duplicate key value violates unique constraint "array_attribute_array_id_key"
--
-- 6.6  Add an dimension for the second array. 
INSERT into "array_dimension"
( array_id, id, name, startMin, currStart, currEnd, endMax, chunk_interval,
  chunk_overlap )
SELECT A.id, 2, 'bar', 0, 0,  100, 0, 10, 0
  FROM "array" A
 WHERE A.name = 'test_two';
-- ERROR:  duplicate key value violates unique constraint "array_attribute_array_id_key"
--
-- And the other way.
--
-- 6.7 Add an attriute with the same name as a dimension in the same array.
insert into "array_attribute"
( array_id, id, name,  type, flags, default_compression_method )
SELECT A.id, 2, 'bar', 1, 0, 1
  FROM "array" A
 WHERE A.name = 'test_two';
-- ERROR:  Repeated attribute / dimension name in array test_two
--
-- 6.8 Add a dimension with the same name as an attribute in the same array.
INSERT into "array_dimension"
( array_id, id, name, startMin, currStart, currEnd, endMax, chunk_interval,
  chunk_overlap )
SELECT A.id, 2, 'foo', 0, 0,  100, 0, 10, 0
  FROM "array" A
 WHERE A.name = 'test_two';
-- ERROR:  Repeated attribute / dimension name in array test_two
--
-- 7.0 Reports the state of the arrays. 
SELECT A.Name,
       'Attr:' || T.Name,
       T.id
  FROM "array" A, "array_attribute" T
 wHERE T.array_id = A.id
 UNION ALL 
SELECT A.Name,
       'Dim: ' || D.Name,
       D.id
  FROM "array" A, "array_dimension" D
 wHERE D.array_id = A.id;




