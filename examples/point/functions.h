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

/*
 * @file functions.h
 *
 * @author roman.simakov@gmail.com
 *
 * @brief Examples of scalar functions for working with new type POINT.
 *
 */

#ifndef FUNCTIONS_H
#define FUNCTIONS_H


struct Point
{
    float x;
    float y;
};

enum
{
    POINT_E_CANT_CONVERT_TO_POINT = SCIDB_USER_ERROR_CODE_START
};

void construct_point(const scidb::Value** args, scidb::Value* res, void*)
{
    *(Point*)res->data() = Point();
}

void str2Point(const scidb::Value** args, scidb::Value* res, void*)
{
    Point* p = (Point*)res->data();

    if (sscanf(args[0]->getString(), "(%f,%f)", &p->x, &p->y) != 2)
        throw PLUGIN_USER_EXCEPTION("libpoint", scidb::SCIDB_SE_UDO, POINT_E_CANT_CONVERT_TO_POINT)
            << args[0]->getString();
}

void point2Str(const scidb::Value** args, scidb::Value* res, void*)
{
    Point* p = (Point*)args[0]->data();

    stringstream ss;
    ss << '(' << p->x << ',' << p->y << ')';

    res->setString(ss.str().c_str());
}

void sumPoints(const scidb::Value** args, scidb::Value* res, void*)
{
    Point* p0 = (Point*)args[0]->data();
    Point* p1 = (Point*)args[1]->data();
    Point* p = (Point*)res->data();

    p->x = p0->x + p1->x;
    p->y = p0->y + p1->y;
}


#endif // FUNCTIONS_H
