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
 * @file UDT.cpp
 *
 * @author knizhnik@garret.ru
 *
 * @brief C++ wrapper for Value class allowing t use it in expressions
 */

#include "query/UDT.h"

namespace scidb
{
    
#define UDT_BIN_OP(opcode, mnem, cpp)                                   \
    UDT::Val UDT::Val::cpp(UDT::Val const& other) const {               \
        FunctionPointer fptr = udt->fptrs[opcode];                      \
        if (fptr == NULL) {                                             \
            FunctionDescription fdesc;                                  \
            const std::vector<TypeId> inputArgTypes(2, udt->tid);       \
            std::vector<FunctionPointer> converters;                    \
            bool supportsVectorMode;                                    \
            if (!FunctionLibrary::getInstance()->findFunction(#mnem, inputArgTypes, fdesc, converters, supportsVectorMode, false)\
                || fdesc.getOutputArg() != udt->tid                     \
                || converters.size() != 0)                              \
                throw USER_EXCEPTION(SCIDB_SE_UDO, SCIDB_LE_FUNCTION_NOT_FOUND) << #mnem; \
            udt->fptrs[opcode] = fptr = fdesc.getFuncPtr();             \
        }                                                               \
        Val result(udt);                                                \
        const Value* args[2];                                                  \
        args[0] = &value;                                                \
        args[1] = &other.value;                                          \
        fptr(args, &result.value, NULL);                                 \
        return result;                                                  \
    }
    
#define UDT_UNARY_OP(opcode, mnem, cpp)                                 \
    UDT::Val UDT::Val::cpp() const {                                    \
        FunctionPointer fptr = udt->fptrs[opcode];                      \
        if (fptr == NULL) {                                             \
            FunctionDescription fdesc;                                  \
            const std::vector<TypeId> inputArgTypes(1, udt->tid);       \
            std::vector<FunctionPointer> converters;                    \
            bool supportsVectorMode;                                    \
            if (!FunctionLibrary::getInstance()->findFunction(#mnem, inputArgTypes, fdesc, converters, supportsVectorMode, false)\
                || fdesc.getOutputArg() != udt->tid                     \
                || converters.size() != 0)                              \
                throw USER_EXCEPTION(SCIDB_SE_UDO, SCIDB_LE_FUNCTION_NOT_FOUND) << #mnem; \
            udt->fptrs[opcode] = fptr = fdesc.getFuncPtr();             \
        }                                                               \
        Val result(udt);                                                \
        const Value* v = &value;                                              \
        fptr(&v, &result.value, NULL);                                  \
        return result;                                                  \
    }

#define UDT_CMP_OP(opcode, mnem, cpp)                                   \
    bool UDT::Val::cpp(Val const& other) const {                        \
        FunctionPointer fptr = udt->fptrs[opcode];                      \
        if (fptr == NULL) {                                             \
            FunctionDescription fdesc;                                  \
            const std::vector<TypeId> inputArgTypes(2, udt->tid);       \
            std::vector<FunctionPointer> converters;                    \
            bool supportsVectorMode;                                    \
            if (!FunctionLibrary::getInstance()->findFunction(#mnem, inputArgTypes, fdesc, converters, supportsVectorMode, false)\
                || fdesc.getOutputArg() != TID_BOOL                     \
                || converters.size() != 0)                              \
                throw USER_EXCEPTION(SCIDB_SE_UDO, SCIDB_LE_FUNCTION_NOT_FOUND) << #mnem; \
            udt->fptrs[opcode] = fptr = fdesc.getFuncPtr();             \
        }                                                               \
        Value result;                                                   \
        const Value* args[2];                                                  \
        args[0] = &value;                                                \
        args[1] = &other.value;                                          \
        fptr(args, &result, NULL);                                       \
        return !result.isNull() && result.getBool();                    \
    }

    #define UDT_CNV(src_type, method, cpp_type)                         \
        UDT::Val UDT::Val::operator =(cpp_type other) {                 \
            if (udt->tid == TID_##src_type) {                           \
                value.set##method(other);                               \
            } else {                                                    \
                Value temp;                                             \
                const Value* v = &temp;                                 \
                temp.set##method(other);                                \
                FunctionPointer fptr = udt->fptrs[FROM_##src_type];     \
                if (fptr == NULL) {                                     \
                    udt->fptrs[FROM_##src_type] = fptr = FunctionLibrary::getInstance()->findConverter(TID_##src_type, udt->tid); \
                }                                                       \
                fptr(&v, &value, NULL);                               \
            }                                                           \
            if (dst != NULL) {                                          \
                memcpy(dst, value.data(), value.size());                \
            }                                                           \
            return *this;                                               \
        }                                                               \
        UDT::Val::operator cpp_type() const {                           \
            if (value.isNull())                                         \
                throw USER_EXCEPTION(SCIDB_SE_UDO, SCIDB_LE_CANT_CONVERT_NULL); \
            if (udt->tid == TID_##src_type) {                           \
                return value.get##method();                             \
            } else {                                                    \
                Value temp;                                             \
                FunctionPointer fptr = udt->fptrs[TO_##src_type];       \
                if (fptr == NULL) {                                     \
                    udt->fptrs[TO_##src_type] = fptr = FunctionLibrary::getInstance()->findConverter(udt->tid, TID_##src_type); \
                }                                                       \
                const Value* v = &value;                                \
                fptr(&v, &temp, NULL);                               \
                return temp.get##method();                              \
            }                                                           \
       }

#include "query/UDT.d"

    UDT::Val UDT::Val::operator=(UDT::Val const& other) {
        value = other.value;
        if (dst != NULL) {
            memcpy(dst, value.data(), value.size());
        }
        return *this;
    }

    UDT::Val UDT::Val::operator=(Value const& other) {
        value = other;
        if (dst != NULL) {
            memcpy(dst, value.data(), value.size());
        }
        return *this;
    }               

    UDT::Val min(UDT::Val const& v1, UDT::Val const& v2)  {
        return v1 > v2 ? v2 : v1;
    }

    UDT::Val max(UDT::Val const& v1, UDT::Val const& v2)  {
        return v1 < v2 ? v2 : v1;
    }

    UDT::Val abs(UDT::Val const& v)  {
        return v >= -v ? v : -v;
    }
}
