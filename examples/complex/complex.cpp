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
 * @file complex.cpp
 *
 * @author knizhnik@garret.ru
 *
 * @brief Shared library that loads into SciDB a Complex data type. 
 */

#include <vector>
#include <boost/assign.hpp>

#include "query/Operator.h"
#include "query/FunctionLibrary.h"
#include "query/FunctionDescription.h"
#include "query/TypeSystem.h"
#include "system/ErrorsLibrary.h"

using namespace std;
using namespace scidb;
using namespace boost::assign;

enum
{
    COMPLEX_ERROR1 = SCIDB_USER_ERROR_CODE_START
};

struct Complex
{
    double re;
    double im;

    Complex(int n = 0): re(n), im(0) {}

    /** C++ operators implementation for aggregating */
    Complex operator*(const Complex& b) const
    {
        Complex c;
        c.re = (re * b.re - im * b.im);
        c.im = (im * b.re + re * b.im);
        return c;
    }

    Complex operator*(uint64_t v) const
    {
        Complex r;
        r.re = re * v;
        r.im = im * v;
        return r;
    }

    Complex operator/(uint64_t v) const
    {
        Complex c;
        c.re = (re * v) / (v * v);
        c.im = (im * v) / (v * v);
        return c;
    }

    Complex operator-(const Complex& b) const
    {
        Complex c;
        c.re = re - b.re;
        c.im = im - b.im;
        return c;
    }

    Complex& operator+=(const Complex& b)
    {
        re += b.re;
        im += b.im;
        return *this;
    }
};

static void addComplex(const Value** args, Value* res, void*)
{
    Complex& a = *(Complex*)args[0]->data();
    Complex& b = *(Complex*)args[1]->data();
    Complex& c = *(Complex*)res->data();

    c.re = a.re + b.re;    
    c.im = a.im + b.im;
}

static void subComplex(const Value** args, Value* res, void*)
{
    Complex& a = *(Complex*)args[0]->data();
    Complex& b = *(Complex*)args[1]->data();
    Complex& c = *(Complex*)res->data();

    c.re = a.re - b.re;    
    c.im = a.im - b.im;
}

static void mulComplex(const Value** args, Value* res, void*)
{
    Complex& a = *(Complex*)args[0]->data();
    Complex& b = *(Complex*)args[1]->data();
    Complex& c = *(Complex*)res->data();

    c.re = (a.re*b.re - a.im*b.im);
    c.im = (a.im*b.re + a.re*b.im);
}

static void divComplex(const Value** args, Value* res, void*)
{
    Complex& a = *(Complex*)args[0]->data();
    Complex& b = *(Complex*)args[1]->data();
    Complex& c = *(Complex*)res->data();

    c.re = (a.re*b.re + a.im*b.im) / (b.re*b.re + b.im*b.im);    
    c.im = (a.im*b.re - a.re*b.im) / (b.re*b.re + b.im*b.im);
}

static void eqComplex(const Value** args, Value* res, void*)
{
    Complex& a = *(Complex*)args[0]->data();
    Complex& b = *(Complex*)args[1]->data();
    res->setBool(a.re == b.re && a.im == b.im);
}

static void neComplex(const Value** args, Value* res, void*)
{
    Complex& a = *(Complex*)args[0]->data();
    Complex& b = *(Complex*)args[1]->data();
    res->setBool(a.re != b.re || a.im != b.im);
}

static void reComplex(const Value** args, Value* res, void*)
{
   Complex& a = *(Complex*)args[0]->data();
   res->setDouble(a.re);
}

static void imComplex(const Value** args, Value* res, void*)
{
   Complex& a = *(Complex*)args[0]->data();
   res->setDouble(a.im);
}
 
static void constructComplex(const Value** args, Value* res, void*)
{
    Complex& c = *(Complex*)res->data();
    c.re = args[0]->getDouble();
    c.im = args[1]->getDouble();
}

static void constructDefaultComplex(const Value** args, Value* res, void*)
{
    *(Complex*)res->data() = Complex();
}

static void double2complex(const Value** args, Value* res, void*)
{
    Complex& c = *(Complex*)res->data();
    c.re = args[0]->getDouble();
    c.im = 0;
}
  
static void integer2complex(const Value** args, Value* res, void*)
{
    Complex& c = *(Complex*)res->data();
    c.re = args[0]->getInt64();
    c.im = 0;
}
  
static void string2complex(const Value** args, Value* res, void*)
{
    Complex& c = *(Complex*)res->data();
    if (sscanf(args[0]->getString(), "(%lf+%lf*i)", &c.re, &c.im) != 2)
        throw PLUGIN_USER_EXCEPTION("complex", SCIDB_SE_UDO, COMPLEX_ERROR1);
}
  
static void complex2string(const Value** args, Value* res, void*)
{
    Complex& a = *(Complex*)args[0]->data();
    stringstream ss;
    ss << "(" << a.re << "+" << a.im << "*i)";
    res->setString(ss.str().c_str());
}
  
REGISTER_FUNCTION(+, list_of("complex")("complex"), "complex", addComplex);
REGISTER_FUNCTION(-, list_of("complex")("complex"), "complex", subComplex);
REGISTER_FUNCTION(*, list_of("complex")("complex"), "complex", mulComplex);
REGISTER_FUNCTION(/, list_of("complex")("complex"), "complex", divComplex);
REGISTER_FUNCTION(=, list_of("complex")("complex"), "bool", eqComplex);
REGISTER_FUNCTION(<>, list_of("complex")("complex"), "bool", neComplex);

REGISTER_FUNCTION(re, list_of("complex"), "double", reComplex);
REGISTER_FUNCTION(im, list_of("complex"), "double", imComplex);

REGISTER_CONVERTER(double, complex, IMPLICIT_CONVERSION_COST, double2complex);
REGISTER_CONVERTER(int64, complex, IMPLICIT_CONVERSION_COST, integer2complex);
REGISTER_CONVERTER(string, complex, EXPLICIT_CONVERSION_COST, string2complex);
REGISTER_CONVERTER(complex, string, EXPLICIT_CONVERSION_COST, complex2string);

vector<Type> _types;
EXPORTED_FUNCTION const vector<Type>& GetTypes()
{
    return _types;
}

vector<FunctionDescription> _functionDescs;
EXPORTED_FUNCTION const vector<FunctionDescription>& GetFunctions()
{
    return _functionDescs;
}

vector<AggregatePtr> _aggregates;
EXPORTED_FUNCTION const vector<AggregatePtr>& GetAggregates()
{
    return _aggregates;
}

/**
 * Class for registering/unregistering user defined objects
 */
static class ComplexLibrary
{
public:
    ComplexLibrary()
    {
        Type complexType("complex", sizeof(Complex) * 8);
        _types.push_back(complexType);

        _functionDescs.push_back(FunctionDescription("complex", ArgTypes(), TypeId("complex"), &constructDefaultComplex));
        _functionDescs.push_back(FunctionDescription("complex", list_of("double")("double"), TypeId("complex"), &constructComplex));

        // Aggregates
        _aggregates.push_back(AggregatePtr(new BaseAggregate<AggSum, Complex, Complex>("sum", complexType, complexType)));
        _aggregates.push_back(AggregatePtr(new BaseAggregate<AggAvg, Complex, Complex>("avg", complexType, complexType)));
        _aggregates.push_back(AggregatePtr(new BaseAggregate<AggVar, Complex, Complex>("var", complexType, complexType)));

        _errors[COMPLEX_ERROR1] = "Failed to parse complex number";
        scidb::ErrorsLibrary::getInstance()->registerErrors("complex", &_errors);
    }
    ~ComplexLibrary()
    {
        scidb::ErrorsLibrary::getInstance()->unregisterErrors("complex");
    }

private:
    scidb::ErrorsLibrary::ErrorsMessages _errors;
} _instance;
