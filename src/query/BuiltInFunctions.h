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
 * @file BuiltInFunctions.h
 *
 * @author roman.simakov@gmail.com
 *
 * BuiltIn scalar function implementations
 */

typedef int8_t Int8;
typedef int16_t Int16;
typedef int32_t Int32;
typedef int64_t Int64;
typedef uint8_t Uint8;
typedef uint16_t Uint16;
typedef uint32_t Uint32;
typedef uint64_t Uint64;
typedef double Double;
typedef float Float;
typedef uint64_t DateTime;
typedef char Char;
typedef bool Bool;

/**
 *  MACROS FOR BINARY OPERATORS
 *
 *  LN - Long name, e.g. PLUS
 *  T - arg types, e.g. TID_INT32
 *  R - result type, e.g. TID_INT32
 *  PN - Parser name, e.g. "+"
 *  CN - C++ operation
 *  TM - Method name that was specified in BUILTIN_METHODS macros of TypeSystem.h to get value
 *  RM - Method name that was specified in BUILTIN_METHODS macros of TypeSystem.h to set value
 */
#define BINARY_OP(LN, T, R, PN, CN, TM, RM)                                         \
    void LN##_##T(const Value** args, Value* res, void*)                           \
    {                                                                               \
        if (args[0]->isVector()) {                                                   \
            res->setVector(args[0]->size()/sizeof(TM)*sizeof(RM));                    \
            RM* dp = (RM*)res->data();                                               \
            TM* lp = (TM*)args[0]->data();                                           \
            TM* rp = (TM*)args[1]->data();                                           \
            if (args[1]->isVector()) {                                               \
                assert(args[0]->size() == args[1]->size());                           \
                for (size_t i = 0, count = args[0]->size() / sizeof(TM); i < count; i++) {  \
                    dp[i] = lp[i] CN rp[i];                                         \
                }                                                                   \
            } else {                                                                \
                TM imm = rp[0];                                                     \
                for (size_t i = 0, count = args[0]->size() / sizeof(TM); i < count; i++) {  \
                    dp[i] = lp[i] CN imm;                                           \
                }                                                                   \
            }                                                                       \
        } else if (args[1]->isVector()) {                                            \
            res->setVector(args[1]->size()/sizeof(TM)*sizeof(RM));                    \
            RM* dp = (RM*)res->data();                                               \
            TM imm = *(TM*)args[0]->data();                                          \
            TM* rp = (TM*)args[1]->data();                                           \
            for (size_t i = 0, count = args[1]->size() / sizeof(TM); i < count; i++) {  \
                dp[i] = imm CN rp[i];                                             \
            }                                                                       \
        } else {                                                                    \
            if (args[0]->isNull() || args[1]->isNull()) {                             \
                res->setNull(); /* What missing reason to set here? */               \
            } else {                                                                \
                res->set##RM(args[0]->get##TM() CN args[1]->get##TM());                \
            }                                                                       \
        }                                                                           \
    }

#define BINARY_BOP(LN, T, R, PN, CN, TM, RM)                                        \
    void LN##_##T(const Value** args, Value* res, void*)                           \
    {                                                                               \
        if (args[0]->isVector()) {                                                   \
            res->setVector((args[0]->size()/sizeof(TM)+7) >> 3);                      \
            char* dp = (char*)res->data();                                           \
            TM* lp = (TM*)args[0]->data();                                           \
            TM* rp = (TM*)args[1]->data();                                           \
            if (args[1]->isVector()) {                                               \
                assert(args[0]->size() == args[1]->size());                           \
                for (size_t i = 0, count = args[0]->size() / sizeof(TM); i < count; i++) {  \
                    if (lp[i] CN rp[i]) dp[i >> 3] |= (1 << (i & 7)); else dp[i >> 3] &= ~(1 << (i & 7)); \
                }                                                                   \
            } else {                                                                \
                TM imm = rp[0];                                                     \
                for (size_t i = 0, count = args[0]->size() / sizeof(TM); i < count; i++) {  \
                    if (lp[i] CN imm) dp[i >> 3] |= (1 << (i & 7)); else dp[i >> 3] &= ~(1 << (i & 7)); \
                }                                                                   \
            }                                                                       \
        } else if (args[1]->isVector()) {                                            \
            res->setVector((args[1]->size()/sizeof(TM)+7) >> 3);                      \
            char* dp = (char*)res->data();                                     \
            TM imm = *(TM*)args[0]->data();                                          \
            TM* rp = (TM*)args[1]->data();                                           \
            for (size_t i = 0, count = args[1]->size() / sizeof(TM); i < count; i++) {  \
                if (imm CN rp[i]) dp[i >> 3] |= (1 << (i & 7)); else dp[i >> 3] &= ~(1 << (i & 7)); \
            }                                                                       \
        } else {                                                                    \
            if (args[0]->isNull() || args[1]->isNull()) {                             \
                res->setNull(); /* What missing reason to set here? */               \
            } else {                                                                \
                res->set##RM(args[0]->get##TM() CN args[1]->get##TM());                \
            }                                                                       \
        }                                                                           \
    }

#define BINARY_BBOP(LN, T, R, PN, CN, TM, RM)                                       \
    void LN##_##T(const Value** args, Value* res, void*)                           \
    {                                                                               \
        if (args[0]->isVector()) {                                                   \
            res->setVector(args[0]->size());                                          \
            char* dp = (char*)res->data();                                           \
            char* lp = (char*)args[0]->data();                                       \
            char* rp = (char*)args[1]->data();                                       \
            if (args[1]->isVector()) {                                               \
                assert(args[0]->size() == args[1]->size());                           \
                for (size_t i = 0, count = args[0]->size() * 8; i < count; i++) {    \
                    if ((lp[i >> 3] CN rp[i >> 3]) & (1 << (i & 7))) dp[i >> 3] |= (1 << (i & 7)); else dp[i >> 3] &= ~(1 << (i & 7)); \
                }                                                                   \
            } else {                                                                \
                char imm = rp[0];                                                   \
                for (size_t i = 0, count = args[0]->size() * 8; i < count; i++) {    \
                    if (((lp[i >> 3] >> (i & 7)) CN imm) & 1) dp[i >> 3] |= (1 << (i & 7)); else dp[i >> 3] &= ~(1 << (i & 7)); \
                }                                                                   \
            }                                                                       \
        } else if (args[1]->isVector()) {                                            \
            res->setVector(args[1]->size());                                          \
            char* dp = (char*)res->data();                                           \
            char imm = *(char*)args[0]->data();                                      \
            char* rp = (char*)args[1]->data();                                       \
            for (size_t i = 0, count = args[1]->size() * 8; i < count; i++) {        \
                if ((imm CN (rp[i >> 3] >> (i & 7))) & 1) dp[i >> 3] |= (1 << (i & 7)); else dp[i >> 3] &= ~(1 << (i & 7)); \
            }                                                                       \
         } else {                                                                   \
            if (args[0]->isNull() || args[1]->isNull()) {                             \
                res->setNull(); /* What missing reason to set here? */               \
            } else {                                                                \
                res->set##RM(args[0]->get##TM() CN args[1]->get##TM());                \
            }                                                                       \
        }                                                                           \
    }

#define LOGICAL_OP(LN, T, R, PN, CN, OP, TM, RM)                                    \
    void LN##_##T(const Value** args, Value* res, void*)                           \
    {                                                                               \
        if (args[0]->isVector()) {                                                   \
            res->setVector(args[0]->size());                                          \
            char* dp = (char*)res->data();                                           \
            char* lp = (char*)args[0]->data();                                       \
            char* rp = (char*)args[1]->data();                                       \
            if (args[1]->isVector()) {                                               \
                assert(args[0]->size() == args[1]->size());                           \
                for (size_t i = 0, count = args[0]->size(); i < count; i++) {        \
                     dp[i] = lp[i] OP rp[i];                                        \
                }                                                                   \
            } else {                                                                \
                char imm = char(~(*(bool*)rp-1));                                   \
                for (size_t i = 0, count = args[0]->size(); i < count; i++) {        \
                     dp[i] = lp[i] OP imm;                                          \
                }                                                                   \
            }                                                                       \
        } else if (args[1]->isVector()) {                                            \
            res->setVector(args[1]->size());                                          \
            char* dp = (char*)res->data();                                           \
            char imm = char(~(*(bool*)args[0]->data()-1));                           \
            char* rp = (char*)args[1]->data();                                       \
            for (size_t i = 0, count = args[1]->size(); i < count; i++) {            \
                dp[i] = imm OP rp[i];                                               \
            }                                                                       \
        } else {                                                                    \
            if (args[0]->isNull() || args[1]->isNull()) {                             \
                res->setNull(); /* What missing reason to set here? */               \
            } else {                                                                \
                res->set##RM(args[0]->get##TM() CN args[1]->get##TM());                \
            }                                                                       \
        }                                                                           \
    }

#define LOGICAL_OR(LN, T, R, PN)                                    \
    void LN##_##T(const Value** args, Value* res, void*)                           \
    {                                                                               \
        if (args[0]->isVector()) {                                                   \
            res->setVector(args[0]->size());                                          \
            char* dp = (char*)res->data();                                           \
            char* lp = (char*)args[0]->data();                                       \
            char* rp = (char*)args[1]->data();                                       \
            if (args[1]->isVector()) {                                               \
                assert(args[0]->size() == args[1]->size());                           \
                for (size_t i = 0, count = args[0]->size(); i < count; i++) {        \
                     dp[i] = lp[i] | rp[i];                                        \
                }                                                                   \
            } else {                                                                \
                char imm = char(~(*(bool*)rp-1));                                   \
                for (size_t i = 0, count = args[0]->size(); i < count; i++) {        \
                     dp[i] = lp[i] | imm;                                          \
                }                                                                   \
            }                                                                       \
        } else if (args[1]->isVector()) {                                            \
            res->setVector(args[1]->size());                                          \
            char* dp = (char*)res->data();                                           \
            char imm = char(~(*(bool*)args[0]->data()-1));                           \
            char* rp = (char*)args[1]->data();                                       \
            for (size_t i = 0, count = args[1]->size(); i < count; i++) {            \
                dp[i] = imm | rp[i];                                               \
            }                                                                       \
        } else {                                                                    \
        if ((args[0]->isNull() && args[1]->isNull()) ||                               \
            (args[0]->isNull() && !args[1]->getBool()) ||                         \
            (!args[0]->getBool() && args[1]->isNull()) ) {                             \
                res->setNull(); /* What missing reason to set here? */               \
            } else {                                                                \
                res->setBool(args[0]->getBool() || args[1]->getBool());                \
            }                                                                       \
        }                                                                           \
    }

#define LOGICAL_AND(LN, T, R, PN)                                    \
    void LN##_##T(const Value** args, Value* res, void*)                           \
    {                                                                               \
        if (args[0]->isVector()) {                                                   \
            res->setVector(args[0]->size());                                          \
            char* dp = (char*)res->data();                                           \
            char* lp = (char*)args[0]->data();                                       \
            char* rp = (char*)args[1]->data();                                       \
            if (args[1]->isVector()) {                                               \
                assert(args[0]->size() == args[1]->size());                           \
                for (size_t i = 0, count = args[0]->size(); i < count; i++) {        \
                     dp[i] = lp[i] & rp[i];                                        \
                }                                                                   \
            } else {                                                                \
                char imm = char(~(*(bool*)rp-1));                                   \
                for (size_t i = 0, count = args[0]->size(); i < count; i++) {        \
                     dp[i] = lp[i] & imm;                                          \
                }                                                                   \
            }                                                                       \
        } else if (args[1]->isVector()) {                                            \
            res->setVector(args[1]->size());                                          \
            char* dp = (char*)res->data();                                           \
            char imm = char(~(*(bool*)args[0]->data()-1));                           \
            char* rp = (char*)args[1]->data();                                       \
            for (size_t i = 0, count = args[1]->size(); i < count; i++) {            \
                dp[i] = imm & rp[i];                                               \
            }                                                                       \
        } else {                                                                    \
        if ((args[0]->isNull() && args[1]->isNull()) ||                               \
            (args[0]->isNull() && args[1]->getBool()) ||                         \
            (args[0]->getBool() && args[1]->isNull()) ) {                             \
                res->setNull(); /* What missing reason to set here? */               \
            } else {                                                                \
                res->setBool(args[0]->getBool() && args[1]->getBool());                \
            }                                                                       \
        }                                                                           \
    }

#define DIVISION_OP(LN, T, R, PN, CN, TM, RM)                                       \
    void LN##_##T(const Value** args, Value* res, void*)                           \
    {                                                                               \
        if (args[0]->isVector()) {                                                   \
            res->setVector(args[0]->size()/sizeof(TM)*sizeof(RM));                    \
            RM* dp = (RM*)res->data();                                               \
            TM* lp = (TM*)args[0]->data();                                           \
            TM* rp = (TM*)args[1]->data();                                           \
            if (args[1]->isVector()) {                                               \
                assert(args[0]->size() == args[1]->size());                           \
                for (size_t i = 0, count = args[0]->size() / sizeof(TM); i < count; i++) {  \
                    if(rp[i] == 0)\
                        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_DIVISION_BY_ZERO);\
                    dp[i] = lp[i] CN rp[i];                                         \
                }                                                                   \
            } else {                                                                \
                if(rp[0] == 0)\
                    throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_DIVISION_BY_ZERO);\
                for (size_t i = 0, count = args[0]->size() / sizeof(TM); i < count; i++) {  \
                    dp[i] = lp[i] CN rp[0];                                         \
                }                                                                   \
            }                                                                       \
        } else if (args[1]->isVector()) {                                            \
            res->setVector(args[1]->size()/sizeof(TM)*sizeof(RM));                    \
            RM* dp = (RM*)res->data();                                               \
            TM* lp = (TM*)args[0]->data();                                           \
            TM* rp = (TM*)args[1]->data();                                           \
            for (size_t i = 0, count = args[1]->size() / sizeof(TM); i < count; i++) {  \
                if(rp[i] == 0)\
                    throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_DIVISION_BY_ZERO);\
                dp[i] = lp[0] CN rp[i];                                             \
            }                                                                       \
        } else {                                                                    \
            if (args[0]->isNull() || args[1]->isNull()) {                             \
                res->setNull(); /* What missing reason to set here? */               \
            }                                                                       \
            else { \
                if (args[1]->get##TM() == 0)\
                    throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_DIVISION_BY_ZERO);\
                res->set##RM(args[0]->get##TM() CN args[1]->get##TM());                \
            }                                                                       \
        }                                                                           \
    }

/**
 *  MACROS FOR UNARY OPERATORS
 *
 *  LN - Long name, e.g. PLUS
 *  T - arg types, e.g. TID_INT32
 *  R - result type, e.g. TID_INT32
 *  PN - Parser name, e.g. "+"
 *  CN - C++ operation
 *  TM - Method name that was specified in BUILTIN_METHODS macros of TypeSystem.h to get value
 *  RM - Method name that was specified in BUILTIN_METHODS macros of TypeSystem.h to set value
 */
#define UNARY_OP(LN, T, R, PN, CN, TM, RM)                                                  \
    void UNARY_##LN##_##T(const Value** args, Value* res, void*)                  \
    {                                                                                       \
        if (args[0]->isVector()) {                                                           \
            res->setVector(args[0]->size()/sizeof(TM)*sizeof(RM));                            \
            RM* dst = (RM*)res->data();                                                      \
            TM* src = (TM*)args[0]->data();                                                  \
            for (size_t i = 0, count = args[0]->size() / sizeof(TM); i < count; i++) {       \
                dst[i] = CN src[i];                                                         \
            }                                                                               \
        } else {                                                                            \
            if (args[0]->isNull()) {                                                         \
                res->setNull(args[0]->getMissingReason());                                    \
            } else {                                                                        \
                res->set##RM(CN args[0]->get##TM());                                          \
            }                                                                               \
        }                                                                                   \
    }

#define UNARY_NOT(LN, T, R, PN, CN, TM, RM)                                                  \
    void UNARY_##LN##_##T(const Value** args, Value* res, void*)                  \
    {                                                                                       \
        if (args[0]->isVector()) {                                                           \
            res->setVector(args[0]->size());                                                  \
            char* dst = (char*)res->data();                                                  \
            char* src = (char*)args[0]->data();                                              \
            for (size_t i = 0, count = args[0]->size(); i < count; i++) {                    \
                dst[i] = ~src[i];                                                           \
            }                                                                               \
        } else {                                                                            \
            if (args[0]->isNull()) {                                                         \
                res->setNull(args[0]->getMissingReason());                                    \
            } else {                                                                        \
                res->set##RM(CN args[0]->get##TM());                                          \
            }                                                                               \
        }                                                                                   \
    }

/**
 *  MACROS FOR FUNCTION WITH 1 ARGUMENTS
 *
 *  LN - Long name, e.g. PLUS
 *  T - type of the 1st arg, e.g. TID_INT32
 *  R - result type, e.g. TID_INT32
 *  N - function name
 *  TM - Method name that was specified in BUILTIN_METHODS macros of TypeSystem.h to get value
 *  RM - Method name that was specified in BUILTIN_METHODS macros of TypeSystem.h to set value
 */
#define FUNCTION_A1(LN, T, R, PN, CN, TM, RM)                                               \
    void LN##_##T(const Value** args, Value* res, void*)                                   \
    {                                                                                       \
        if (args[0]->isVector()) {                                                           \
            res->setVector(args[0]->size()/sizeof(TM)*sizeof(RM));                            \
            RM* dst = (RM*)res->data();                                                      \
            TM* src = (TM*)args[0]->data();                                                  \
            for (size_t i = 0, count = args[0]->size() / sizeof(TM); i < count; i++) {       \
                dst[i] = CN(src[i]);                                                        \
            }                                                                               \
        } else {                                                                            \
            if (args[0]->isNull()) {                                                         \
                res->setNull(args[0]->getMissingReason());                                    \
            }                                                                               \
            else {                                                                          \
                res->set##RM(CN(args[0]->get##TM()));                                         \
            }                                                                               \
        }                                                                                   \
    }

/**
 *  MACROS FOR FUNCTION WITH 2 ARGUMENTS
 *
 *  LN - Long name, e.g. PLUS
 *  T1 - type of the 1st arg, e.g. TID_INT32
 *  T2 - type of the 2nd arg, e.g. TID_INT32
 *  R - result type, e.g. TID_INT32
 *  N - function name
 *  T1M - Method name that was specified in BUILTIN_METHODS macros of TypeSystem.h to get value of 1st arg
 *  T2M - Method name that was specified in BUILTIN_METHODS macros of TypeSystem.h to get value of 2st arg
 *  RM - Method name that was specified in BUILTIN_METHODS macros of TypeSystem.h to set value
 */
#define FUNCTION_A2(LN, T1, T2, R, PN, CN, T1M, T2M, RM)                                    \
    void LN##_##T1##_##T2(const Value** args, Value* res, void*)                           \
    {                                                                                       \
        if (args[0]->isVector()) {                                                           \
            res->setVector(args[0]->size()/sizeof(T1M)*sizeof(RM));                           \
            RM* dst = (RM*)res->data();                                                      \
            T1M* p1 = (T1M*)args[0]->data();                                                 \
            T2M* p2 = (T2M*)args[1]->data();                                                 \
            if (args[1]->isVector()) {                                                       \
                assert(args[0]->size() / sizeof(T1M) == args[1]->size() / sizeof(T2M));       \
                for (size_t i = 0, count = args[0]->size() / sizeof(T1M); i < count; i++) {  \
                    dst[i] = CN(p1[i], p2[i]);                                              \
                }                                                                           \
            } else {                                                                        \
                for (size_t i = 0, count = args[0]->size() / sizeof(T1M); i < count; i++) {  \
                    dst[i] = CN(p1[i], p2[0]);                                              \
                }                                                                           \
            }                                                                               \
        } else if (args[1]->isVector()) {                                                    \
            res->setVector(args[1]->size()/sizeof(T2M)*sizeof(RM));                           \
            RM* dst = (RM*)res->data();                                                      \
            T1M* p1 = (T1M*)args[0]->data();                                                 \
            T2M* p2 = (T2M*)args[1]->data();                                                 \
            for (size_t i = 0, count = args[1]->size() / sizeof(T2M); i < count; i++) {      \
                dst[i] = CN(p1[0], p2[i]);                                                  \
            }                                                                               \
        } else {                                                                            \
            if (args[0]->isNull() || args[1]->isNull()) {                                     \
                res->setNull(); /* What missing reason to set here? */                       \
            }                                                                               \
            else {                                                                          \
                res->set##RM(CN(args[0]->get##T1M(), args[1]->get##T2M()));                    \
            }                                                                               \
        }                                                                                   \
    }

/**
 *  MACROS FOR CONVERTOR
 *
 *  T - arg types, e.g. TID_INT32
 *  R - result type, e.g. TID_INT32
 *  TM - Method name that was specified in BUILTIN_METHODS macros of TypeSystem.h to get value
 *  RM - Method name that was specified in BUILTIN_METHODS macros of TypeSystem.h to set value
 */
#define CONVERTOR(T, R, TM, RM, COST)                                        \
    void CONV##_##T##_##TO##_##R(const Value** args, Value* res, void*)   \
    {                                                                               \
        if (args[0]->isVector()) {                                                   \
            res->setVector(args[0]->size()/sizeof(TM)*sizeof(RM));                    \
            RM* dst = (RM*)res->data();                                              \
            TM* src = (TM*)args[0]->data();                                          \
            for (size_t i = 0, count = args[0]->size() / sizeof(TM); i < count; i++) {       \
                dst[i] = RM(src[i]);                                                \
            }                                                                       \
        } else {                                                                    \
            if (args[0]->isNull()) {                                                 \
                res->setNull(args[0]->getMissingReason());                            \
            }                                                                       \
            else {                                                                  \
                res->set##RM(args[0]->get##TM());                                     \
            }                                                                       \
        }                                                                           \
    }

#define CONVERTOR_BOOL(T, R, TM, RM, COST)                                        \
    void CONV##_##T##_##TO##_##R(const Value** args, Value* res, void*)   \
    {                                                                               \
        if (args[0]->isVector()) {                                                   \
            res->setVector((args[0]->size()/sizeof(TM) + 7) >> 3);                    \
            char* dst = (char*)res->data();                                          \
            TM* src = (TM*)args[0]->data();                                          \
            for (size_t i = 0, count = args[0]->size() / sizeof(TM); i < count; i++) {       \
                 if (src[i]) dst[i >> 3] |= (1 << (i & 7)); else dst[i >> 3] &= ~(1 << (i & 7)); \
            }                                                                       \
        } else {                                                                    \
            if (args[0]->isNull()) {                                                 \
                res->setNull(args[0]->getMissingReason());                            \
            }                                                                       \
            else {                                                                  \
                res->set##RM(args[0]->get##TM());                                     \
            }                                                                       \
        }                                                                           \
    }


#define CONVERTOR_TO_STR(T, TM)                                                     \
    void CONV##_##T##_##TO_String(const Value** args, Value* res, void*)   \
    {                                                                               \
        if (args[0]->isNull()) {                                                     \
            res->setNull(args[0]->getMissingReason());                                \
        }  else {                                                                   \
            std::stringstream ss;                                                   \
            ss << args[0]->get##TM();                                                \
            std::string const& str = ss.str();                                      \
            res->setData(str.c_str(), str.length()+1);                               \
        }                                                                           \
    }

#define CONVERTOR_FROM_STR(T, TM)                                                   \
    void CONV##_##T##_##FROM_String(const Value** args, Value* res, void*)   \
    {                                                                               \
        if (args[0]->isNull()) {                                                     \
            res->setNull(args[0]->getMissingReason());                                \
        } else {                                                                    \
            std::stringstream ss(args[0]->getString());                              \
            TM val;                                                                 \
            ss >> val;                                                              \
            if (ss.fail())                                                          \
                throw USER_EXCEPTION(SCIDB_SE_TYPESYSTEM, SCIDB_LE_FAILED_PARSE_STRING); \
            res->set##TM(val);                                                       \
        }                                                                           \
    }

#include "BuiltInFunctions.inc"

#undef CONVERTOR
#undef CONVERTOR_BOOL
#undef CONVERTOR_TO_STR
#undef CONVERTOR_FROM_STR
#undef FUNCTION_A1
#undef FUNCTION_A2
#undef UNARY_OP
#undef UNARY_NOT
#undef BINARY_OP
#undef BINARY_BOP
#undef BINARY_BBOP
#undef LOGICAL_OP
#undef DIVISION_OP
#undef LOGICAL_OR
#undef LOGICAL_AND

// iif implementation
void iif(const Value** args, Value* res, void*)
{
    if (!args[0]->isNull() && args[0]->getBool())
        *res = *args[1];
    else
        *res = *args[2];
}

// is_null implementation
void isNull(const Value** args, Value* res, void*)
{
    res->setBool(args[0]->isNull());
}

void isNan(const Value** args, Value* res, void*)
{
    res->setBool(isnan(args[0]->getDouble()));
}

// missing_reason implementation
void missingReason(const Value** args, Value* res, void*)
{
    res->setInt32(args[0]->getMissingReason());
}

void missing(const Value** args, Value* res, void*)
{
    res->setNull(args[0]->getInt32());
}

void identicalConversion(const Value** args, Value* res, void*)
{
    *res = *args[0];
}

void boolMax(const Value** args, Value* res, void*)
{
    res->setBool(args[0]->getBool() || args[1]->getBool());
}

void boolMin(const Value** args, Value* res, void*)
{
    res->setBool(args[0]->getBool() && args[1]->getBool());
}

// NULL converter
void convNullToAny(const Value** args, Value* res, void*)
{
    res->setNull(args[0]->getMissingReason());
}

void convChar2Str(const Value** args, Value* res, void*)
{
    if (args[0]->isNull()) {
        res->setNull();
        return;
    }
    char str[2] = {args[0]->getChar(), 0};
    res->setString(str);
}

void convStr2Char(const Value** args, Value* res, void*)
{
    if (args[0]->isNull()) {
        res->setNull();
        return;
    }
    res->setChar(args[0]->getString()[0]);
}

void strchar(const Value** args, Value* res, void*)
{
    if (args[0]->isNull()) {
        res->setNull();
        return;
    }
    res->setChar(args[0]->getString()[0]);
}

void strPlusStr(const Value** args, Value* res, void*)
{
    if (args[0]->isNull() || args[1]->isNull()) {
        res->setNull();
        return;
    }
    const string str1(args[0]->getString());
    const string str2(args[1]->getString());
    res->setString((str1 + str2).c_str());
}

void subStr(const Value** args, Value* res, void*)
{
    if (args[0]->isNull() || args[1]->isNull() || args[2]->isNull()) {
        res->setNull();
        return;
    }
    const string str(args[0]->getString());
    res->setString(str.substr(args[1]->getInt32(), args[2]->getInt32()).c_str());
}

void strEq(const Value** args, Value* res, void*)
{
    if (args[0]->isNull() || args[1]->isNull()) {
        res->setNull();
        return;
    }
    res->setBool(strcmp(args[0]->getString(), args[1]->getString()) == 0);
}

void strRegex(const Value** args, Value* res, void*)
{
    if (args[0]->isNull() || args[1]->isNull()) {
        res->setNull();
        return;
    }
    // res.setBool(strcmp(args[0].getString(), args[1].getString()) == 0);
    // NOTE: Can improve the performance of this function by
    //       memoizing the result of the call to boost::regex_re().
    boost::regex re(args[1]->getString());
    res->setBool(boost::regex_match(args[0]->getString(),re ));
}

void strNotEq(const Value** args, Value* res, void*)
{
    if (args[0]->isNull() || args[1]->isNull()) {
        res->setNull();
        return;
    }
    res->setBool(strcmp(args[0]->getString(), args[1]->getString()) != 0);
}

void strLen(const Value** args, Value* res, void*)
{
    if (args[0]->isNull()) {
        res->setNull();
        return;
    }
    res->setInt32(args[0]->size()-1);
}

void strLess(const Value** args, Value* res, void*)
{
    if (args[0]->isNull() || args[1]->isNull()) {
        res->setNull();
        return;
    }
    res->setBool(strcmp(args[0]->getString(), args[1]->getString()) < 0);
}

void strMin(const Value** args, Value* res, void*)
{
    if (args[0]->isNull() || args[1]->isNull()) {
        res->setNull();
        return;
    }
    res->setString(strcmp(args[0]->getString(), args[1]->getString()) > 0 ? args[1]->getString() : args[0]->getString());
}

void strGreater(const Value** args, Value* res, void*)
{
    if (args[0]->isNull() || args[1]->isNull()) {
        res->setNull();
        return;
    }
    res->setBool(strcmp(args[0]->getString(), args[1]->getString()) > 0);
}

void strMax(const Value** args, Value* res, void*)
{
    if (args[0]->isNull() || args[1]->isNull()) {
        res->setNull();
        return;
    }
    res->setString(strcmp(args[0]->getString(), args[1]->getString()) > 0 ? args[0]->getString() : args[1]->getString());
}

void strLessOrEq(const Value** args, Value* res, void*)
{
    if (args[0]->isNull() || args[1]->isNull()) {
        res->setNull();
        return;
    }
    res->setBool(strcmp(args[0]->getString(), args[1]->getString()) <= 0);
}

void strGreaterOrEq(const Value** args, Value* res, void*)
{
    if (args[0]->isNull() || args[1]->isNull()) {
        res->setNull();
        return;
    }
    res->setBool(strcmp(args[0]->getString(), args[1]->getString()) >= 0);
}

void strFTime(const Value** args, Value* res, void*)
{
    if (args[0]->isNull() || args[1]->isNull()) {
        res->setNull();
        return;
    }
    char buf[STRFTIME_BUF_LEN];
    *buf = '\0';
    struct tm tm;
    time_t dt = (time_t)args[0]->getDateTime();
    gmtime_r(&dt, &tm);
    strftime(buf, sizeof(buf), args[1]->getString(), &tm);
    res->setString(buf);
}

void currentTime(const Value** args, Value* res, void*)
{
    res->setDateTime(time(NULL));
}

void convDateTime2Str(const Value** args, Value* res, void*)
{
    if (args[0]->isNull()) {
        res->setNull();
        return;
    }
    char buf[STRFTIME_BUF_LEN];
    struct tm tm;
    time_t dt = (time_t)args[0]->getDateTime();
    gmtime_r(&dt, &tm);
    strftime(buf, sizeof(buf), DEFAULT_STRFTIME_FORMAT, &tm);
    res->setString(buf);
}

void convStr2DateTime(const Value** args, Value* res, void*)
{
    if (args[0]->isNull()) {
        res->setNull();
        return;
    }
    res->setDateTime(parseDateTime(args[0]->getString()));
}

void addIntToDateTime(const Value** args, Value* res, void*)
{
    if (args[0]->isNull() || args[1]->isNull()) {
        res->setNull();
        return;
    }
    res->setDateTime(args[0]->getDateTime() + args[1]->getInt64());
}

void subIntFromDateTime(const Value** args, Value* res, void*)
{
    if (args[0]->isNull() || args[1]->isNull()) {
        res->setNull();
        return;
    }
    res->setDateTime(args[0]->getDateTime() - args[1]->getInt64());
}

void dayOfWeekT(const Value** args, Value* res, void*)
{
    if (args[0]->isNull())
    {
        res->setNull();
        return;
    }

    time_t *seconds = (time_t*) args[0]->data();
    struct tm tm;
    gmtime_r(seconds,&tm);
    res->setUint8(tm.tm_wday);
}

void hourOfDayT(const Value** args, Value* res, void*)
{
    if (args[0]->isNull())
    {
        res->setNull();
        return;
    }

    time_t *seconds = (time_t*) args[0]->data();
    struct tm tm;
    gmtime_r(seconds,&tm);
    res->setUint8(tm.tm_hour);
}

void dayOfWeekTZ(const Value** args, Value* res, void*)
{
    if (args[0]->isNull())
    {
        res->setNull();
        return;
    }

    std::pair<time_t,time_t>* t = (std::pair<time_t,time_t>*) args[0]->data();
    time_t seconds = t->first;
    struct tm tm;
    gmtime_r(&seconds,&tm);
    res->setUint8(tm.tm_wday);
}

void hourOfDayTZ(const Value** args, Value* res, void*)
{
    if (args[0]->isNull())
    {
        res->setNull();
        return;
    }

    std::pair<time_t,time_t>* t = (std::pair<time_t,time_t>*) args[0]->data();
    time_t seconds = t->first;
    struct tm tm;
    gmtime_r(&seconds,&tm);
    res->setUint8(tm.tm_hour);
}

void scidb_random(const Value** args, Value* res, void*)
{
    res->setUint32(::random());
}

void tzToGmt(const Value** args, Value* res, void*)
{
    if (args[0]->isNull())
    {
        res->setNull();
        return;
    }

    std::pair<time_t,time_t>* t = (std::pair<time_t,time_t>*) args[0]->data();
    res->setDateTime(t->first-t->second);
}

void stripOffset(const Value** args, Value* res, void*)
{
    if (args[0]->isNull())
    {
        res->setNull();
        return;
    }

    std::pair<time_t,time_t>* t = (std::pair<time_t,time_t>*) args[0]->data();
    res->setDateTime(t->first);
}

void appendOffset(const Value** args, Value* res, void*)
{
    if (args[0]->isNull() || args[1]->isNull())
    {
        res->setNull();
        return;
    }

    std::pair<time_t,time_t> rt(args[0]->getDateTime(), args[1]->getInt64());
    res->setData(&rt, 2*sizeof(time_t));
}

void applyOffset(const Value** args, Value* res, void*)
{
    if (args[0]->isNull() || args[1]->isNull())
    {
        res->setNull();
        return;
    }

    std::pair<time_t,time_t> rt(args[0]->getDateTime() + args[1]->getInt64(),args[1]->getInt64());
    res->setData(&rt, 2*sizeof(time_t));
}

void getOffset(const Value** args, Value* res, void*)
{
    if (args[0]->isNull())
    {
        res->setNull();
        return;
    }

    std::pair<time_t,time_t>* t1 = (std::pair<time_t,time_t>*) args[0]->data();
    res->setInt64(t1->second);
}

void tzEq(const Value** args, Value* res, void*)
{
    if (args[0]->isNull() || args[1]->isNull())
    {
        res->setNull();
        return;
    }

    std::pair<time_t,time_t>* t1 = (std::pair<time_t,time_t>*) args[0]->data();
    std::pair<time_t,time_t>* t2 = (std::pair<time_t,time_t>*) args[1]->data();
    res->setBool(t1->first-t1->second==t2->first-t2->second);
}

void tzNotEq(const Value** args, Value* res, void*)
{
    if (args[0]->isNull() || args[1]->isNull())
    {
        res->setNull();
        return;
    }

    std::pair<time_t,time_t>* t1 = (std::pair<time_t,time_t>*) args[0]->data();
    std::pair<time_t,time_t>* t2 = (std::pair<time_t,time_t>*) args[1]->data();
    res->setBool(t1->first-t1->second!=t2->first-t2->second);
}

void tzLess(const Value** args, Value* res, void*)
{
    if (args[0]->isNull()||args[1]->isNull())
    {
        res->setNull();
        return;
    }

    std::pair<time_t,time_t>* t1 = (std::pair<time_t,time_t>*) args[0]->data();
    std::pair<time_t,time_t>* t2 = (std::pair<time_t,time_t>*) args[1]->data();
    res->setBool(t1->first-t1->second<t2->first-t2->second);
}

void tzGreater(const Value** args, Value* res, void*)
{
    if (args[0]->isNull()||args[1]->isNull())
    {
        res->setNull();
        return;
    }

    std::pair<time_t,time_t>* t1 = (std::pair<time_t,time_t>*) args[0]->data();
    std::pair<time_t,time_t>* t2 = (std::pair<time_t,time_t>*) args[1]->data();
    res->setBool(t1->first-t1->second>t2->first-t2->second);
}

void tzLessOrEq(const Value** args, Value* res, void*)
{
    if (args[0]->isNull()||args[1]->isNull())
    {
        res->setNull();
        return;
    }

    std::pair<time_t,time_t>* t1 = (std::pair<time_t,time_t>*) args[0]->data();
    std::pair<time_t,time_t>* t2 = (std::pair<time_t,time_t>*) args[1]->data();
    res->setBool(t1->first-t1->second<=t2->first-t2->second);
}

void tzGreaterOrEq(const Value** args, Value* res, void*)
{
    if (args[0]->isNull() || args[1]->isNull())
    {
        res->setNull();
        return;
    }

    std::pair<time_t,time_t>* t1 = (std::pair<time_t,time_t>*) args[0]->data();
    std::pair<time_t,time_t>* t2 = (std::pair<time_t,time_t>*) args[1]->data();
    res->setBool(t1->first-t1->second>=t2->first-t2->second);
}

void currentTimeTz(const Value** args, Value* res, void*)
{
    std::pair<time_t,time_t> t;
    t.first = time(NULL);
    struct tm localTm;
    localtime_r(&t.first, &localTm);
    time_t gm = timegm(&localTm);
    t.second=gm-t.first;
    t.first=t.first+t.second;

    res->setData(&t, 2*sizeof(time_t));
}

void convDateTimeTz2Str(const Value** args, Value* res, void*)
{
    if (args[0]->isNull())
    {
        res->setNull();
        return;
    }

    char buf[STRFTIME_BUF_LEN + 8];
    time_t *seconds = (time_t*) args[0]->data();
    time_t *offset = seconds+1;

    struct tm tm;
    gmtime_r(seconds,&tm);
    size_t offs = strftime(buf, sizeof(buf), DEFAULT_STRFTIME_FORMAT, &tm);

    char sign = *offset > 0 ? '+' : '-';

    time_t aoffset = *offset > 0 ? *offset : (*offset) * -1;

    sprintf(buf+offs, " %c%02d:%02d",
            sign,
            (int32_t) aoffset/3600,
            (int32_t) (aoffset%3600)/60);

    res->setString(buf);
}

void formatDouble(const Value** args, Value* res, void*)
{
    char buf[256];
    sprintf(buf, args[1]->getString(), args[0]->getDouble());
    res->setString(buf);
}

void convStr2DateTimeTz(const  Value** args,  Value* res, void*)
{
    if (args[0]->isNull())
    {
        res->setNull();
        return;
    }

    parseDateTimeTz(args[0]->getString(), *res);
}

#ifndef SCIDB_CLIENT
void length(const Value** args, Value* res, void*)
{
    const string arrayName = args[0]->getString();
    const string dimName = args[1]->getString();
    ArrayDesc arrayDesc;
    SystemCatalog::getInstance()->getArrayDesc(arrayName, LAST_VERSION, arrayDesc);
    const Dimensions& dims = arrayDesc.getDimensions();
    const size_t nDims = dims.size();

    for (size_t i = 0; i < nDims; i++) {
        if (dimName == dims[i].getBaseName()) {
            res->setInt64(dims[i].getLength());
            return;
        }
    }
    throw USER_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_DIMENSION_NOT_EXIST) << dimName;
}

void first_index(const Value** args, Value* res, void*)
{
    const string arrayName = args[0]->getString();
    const string dimName = args[1]->getString();
    ArrayDesc arrayDesc;
    SystemCatalog::getInstance()->getArrayDesc(arrayName, LAST_VERSION, arrayDesc);
    const Dimensions& dims = arrayDesc.getDimensions();
    const size_t nDims = dims.size();

    for (size_t i = 0; i < nDims; i++) {
        if (dimName == dims[i].getBaseName()) {
            res->setInt64(dims[i].getStart());
            return;
        }
    }
    throw USER_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_DIMENSION_NOT_EXIST) << dimName;
}

void last_index(const Value** args, Value* res, void*)
{
    const string arrayName = args[0]->getString();
    const string dimName = args[1]->getString();
    ArrayDesc arrayDesc;
    SystemCatalog::getInstance()->getArrayDesc(arrayName, LAST_VERSION, arrayDesc);
    const Dimensions& dims = arrayDesc.getDimensions();
    const size_t nDims = dims.size();

    for (size_t i = 0; i < nDims; i++) {
        if (dimName == dims[i].getBaseName()) {
            res->setInt64(dims[i].getEndMax());
            return;
        }
    }
    throw USER_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_DIMENSION_NOT_EXIST) << dimName;
}

void low(const Value** args, Value* res, void*)
{
    const string arrayName = args[0]->getString();
    const string dimName = args[1]->getString();
    ArrayDesc arrayDesc;
    SystemCatalog::getInstance()->getArrayDesc(arrayName, LAST_VERSION, arrayDesc);
    const Dimensions& dims = arrayDesc.getDimensions();
    const size_t nDims = dims.size();
    Coordinates lowBoundary = SystemCatalog::getInstance()->getLowBoundary(arrayDesc.getId());

    for (size_t i = 0; i < nDims; i++) {
        if (dimName == dims[i].getBaseName()) {
            res->setInt64(lowBoundary[i]);
            return;
        }
    }
    throw USER_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_DIMENSION_NOT_EXIST) << dimName;
}

void high(const Value** args, Value* res, void*)
{
    const string arrayName = args[0]->getString();
    const string dimName = args[1]->getString();
    ArrayDesc arrayDesc;
    SystemCatalog::getInstance()->getArrayDesc(arrayName, LAST_VERSION, arrayDesc);
    const Dimensions& dims = arrayDesc.getDimensions();
    const size_t nDims = dims.size();
    Coordinates highBoundary = SystemCatalog::getInstance()->getHighBoundary(arrayDesc.getId());

    for (size_t i = 0; i < nDims; i++) {
        if (dimName == dims[i].getBaseName()) {
            res->setInt64(highBoundary[i]);
            return;
        }
    }
    throw USER_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_DIMENSION_NOT_EXIST) << dimName;
}

void length1(const Value** args, Value* res, void*)
{
    const string arrayName = args[0]->getString();
    ArrayDesc arrayDesc;
    SystemCatalog::getInstance()->getArrayDesc(arrayName, LAST_VERSION, arrayDesc);
    const Dimensions& dims = arrayDesc.getDimensions();
    if (dims.size() != 1)
        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_DIMENSION_EXPECTED);
    res->setInt64(dims[0].getLength());
}

void first_index1(const Value** args, Value* res, void*)
{
    const string arrayName = args[0]->getString();
    ArrayDesc arrayDesc;
    SystemCatalog::getInstance()->getArrayDesc(arrayName, LAST_VERSION, arrayDesc);
    const Dimensions& dims = arrayDesc.getDimensions();
    if (dims.size() != 1)
        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_DIMENSION_EXPECTED);
    res->setInt64(dims[0].getStart());
}

void last_index1(const Value** args, Value* res, void*)
{
    const string arrayName = args[0]->getString();
    ArrayDesc arrayDesc;
    SystemCatalog::getInstance()->getArrayDesc(arrayName, LAST_VERSION, arrayDesc);
    const Dimensions& dims = arrayDesc.getDimensions();
    if (dims.size() != 1)
        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_DIMENSION_EXPECTED);
    res->setInt64(dims[0].getEndMax());
}

void low1(const Value** args, Value* res, void*)
{
    const string arrayName = args[0]->getString();
    ArrayDesc arrayDesc;
    SystemCatalog::getInstance()->getArrayDesc(arrayName, LAST_VERSION, arrayDesc);
    const Dimensions& dims = arrayDesc.getDimensions();
    if (dims.size() != 1)
        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_DIMENSION_EXPECTED);
    Coordinates lowBoundary = SystemCatalog::getInstance()->getLowBoundary(arrayDesc.getId());
    res->setInt64(lowBoundary[0]);
}

void high1(const Value** args, Value* res, void*)
{
    const string arrayName = args[0]->getString();
    ArrayDesc arrayDesc;
    SystemCatalog::getInstance()->getArrayDesc(arrayName, LAST_VERSION, arrayDesc);
    const Dimensions& dims = arrayDesc.getDimensions();
    if (dims.size() != 1)
        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_DIMENSION_EXPECTED);
    Coordinates highBoundary = SystemCatalog::getInstance()->getHighBoundary(arrayDesc.getId());
    res->setInt64(highBoundary[0]);
}

void instanceId(const Value** args, Value* res, void*)
{
   // there is no per-query ID mapping applied, this is the physical instance ID
    res->setInt64( Cluster::getInstance()->getLocalInstanceId());
}

#endif
