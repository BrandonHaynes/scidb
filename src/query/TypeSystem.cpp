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

#include <iomanip>
#include <cerrno>

#include <boost/tuple/tuple.hpp>
#include <boost/algorithm/string.hpp>

#include <log4cxx/logger.h>

#include <query/TypeSystem.h>
#include <query/FunctionLibrary.h>

/****************************************************************************/

using namespace std;
using namespace boost;

/****************************************************************************/
namespace scidb {
/****************************************************************************/

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.typesystem"));

std::ostream& operator<<(std::ostream& os,const Type& t)
{
    return os << t.typeId();
}

/** PGB: Note that this will only generate the subset of the input list
 *     of types that are actually in the TypeLibrary.
 */
std::ostream& operator<<(std::ostream& os,const std::vector<TypeId>& r)
{
    for (size_t i=0,n=r.size(); i!=n; ++i)
    {
        if (i != 0)
        {
            os << ',';
        }

        os << ' ' << TypeLibrary::getType(r[i]);
    }

    return os;
}

std::ostream& operator<<(std::ostream& os,const std::vector<Type>& r)
{
    os << ' ';
    insertRange(os,r,", ");
    return os;
}

template<class K,class V,class C>
bool inFlatMap(PointerRange<const Keyed<K,V,C> > m,const K& k,V& v)
{
    const Keyed<K,V,C> *f,*l;                            // First+last matches

    boost::tie(f,l) = std::equal_range(m.begin(),m.end(),k);

    if (f != l)                                          // We found a match?
    {
        assert(l - f == 1);                              // ...match is unique
        v = f->value;
        return true;
    }

    return false;
}

TypeEnum typeId2TypeEnum(const TypeId& t,bool noThrow)
{
    static const Keyed<const char*,TypeEnum,less_strcmp> m[] =
    {
        {TID_BINARY    ,TE_BINARY    },
        {TID_BOOL      ,TE_BOOL      },
        {TID_CHAR      ,TE_CHAR      },
        {TID_DATETIME  ,TE_DATETIME  },
        {TID_DATETIMETZ,TE_DATETIMETZ},
        {TID_DOUBLE    ,TE_DOUBLE    },
        {TID_FLOAT     ,TE_FLOAT     },
        {TID_INDICATOR ,TE_INDICATOR },
        {TID_INT16     ,TE_INT16     },
        {TID_INT32     ,TE_INT32     },
        {TID_INT64     ,TE_INT64     },
        {TID_INT8      ,TE_INT8      },
        {TID_STRING    ,TE_STRING    },
        {TID_UINT16    ,TE_UINT16    },
        {TID_UINT32    ,TE_UINT32    },
        {TID_UINT64    ,TE_UINT64    },
        {TID_UINT8     ,TE_UINT8     },
        {TID_VOID      ,TE_VOID      },
    };

    TypeEnum e = TE_INVALID;

    if (inFlatMap(pointerRange(m),t.c_str(),e) || noThrow)
    {
        return e;
    }

    // Probably a user-defined type of some kind.  XXX We need to do a
    // better job of supporting those here!

    throw USER_EXCEPTION(SCIDB_SE_TYPE, SCIDB_LE_TYPE_NOT_REGISTERED) << t;
}

/**
 *  Return true if this supertype is base type for subtype.
 *  return true if subtype is direct or indirect subtype of supertype
 */
bool Type::isSubtype(TypeId const& subtype,TypeId const& supertype)
{
    return TypeLibrary::getType(subtype).isSubtypeOf(supertype);
}

std::ostream& operator<<(std::ostream& s,const Value& v)
{
    s << "scidb::Value(";

    switch (v.size())
    {
        case 1: s<<"0x"<<hex<<setfill('0')<<v.get<uint8_t> ()<< dec;break;
        case 2: s<<"0x"<<hex<<setfill('0')<<v.get<uint16_t>()<< dec;break;
        case 4: s<<"0x"<<hex<<setfill('0')<<v.get<uint32_t>()<< dec;break;
        case 8: s<<"0x"<<hex<<setfill('0')<<v.get<uint64_t>()<< dec;break;
        default:s<<"size="<< v.size()<<", data="<< v.data();        break;
    }

    if (v.isNull())
    {
        s << ", missingReason=" << v.getMissingReason();
    }

    return s << ')';
}

/**
 * TypeLibrary implementation
 */

TypeLibrary TypeLibrary::_instance;

TypeLibrary::TypeLibrary()
{
#if defined(SCIDB_CLIENT)
    registerBuiltInTypes();
#endif
}

void TypeLibrary::registerBuiltInTypes()
{
    static struct builtin {const char* name;size_t bits;} const builtins[] =
    {
        {TID_INDICATOR,    1                     },
        {TID_CHAR,         8                     },
        {TID_INT8,         8                     },
        {TID_INT16,        16                    },
        {TID_INT32,        32                    },
        {TID_INT64,        64                    },
        {TID_UINT8,        8                     },
        {TID_UINT16,       16                    },
        {TID_UINT32,       32                    },
        {TID_UINT64,       64                    },
        {TID_FLOAT,        32                    },
        {TID_DOUBLE,       64                    },
        {TID_BOOL,         1                     },
        {TID_STRING,       0                     },
        {TID_DATETIME,     sizeof(time_t) * 8    },
        {TID_VOID,         0                     },
        {TID_BINARY,       0                     },
        {TID_DATETIMETZ,   2 * sizeof(time_t) * 8}
       //TID_FIXED_STRING intentionally left out, see below.
    };

    for (size_t i=0; i != SCIDB_SIZE(builtins); ++i)
    {
        const builtin& bti = builtins[i];

        Type t(bti.name, bti.bits);

        _instance._registerType(t);
        _instance._builtinTypesById [bti.name] = t;
        _instance._defaultValuesById[bti.name] = Value(t);
    }
}

bool TypeLibrary::_hasType(const TypeId& typeId) const
{
    if (_builtinTypesById.find(typeId) != _builtinTypesById.end())
    {
        return true;
    }
    else
    {
        ScopedMutexLock cs(_mutex);

        return _typesById.find(typeId) != _typesById.end();
    }
}

const Type& TypeLibrary::_getType(const TypeId& typeId)
{
    map<TypeId,Type,__lesscasecmp >::const_iterator i = _builtinTypesById.find(typeId);
    if (i != _builtinTypesById.end())
    {
        return i->second;
    }
    else
    {
        ScopedMutexLock cs(_mutex);
        i = _typesById.find(typeId);
        if (i == _typesById.end())
        {
            size_t pos = typeId.find_first_of('_');
            if (pos != string::npos)
            {
                string genericTypeId = typeId.substr(0, pos + 1) + '*';
                i = _typesById.find(genericTypeId);
                if (i != _typesById.end())
                {
                    Type limitedType(typeId, atoi(typeId.substr(pos + 1).c_str()) * 8,
                            i->second.baseType());
                    _typeLibraries.addObject(typeId);
                    return _typesById[typeId] = limitedType;
                }
            }
            LOG4CXX_DEBUG(logger, "_getType('" << typeId << "') not found");
            throw SYSTEM_EXCEPTION(SCIDB_SE_TYPESYSTEM, SCIDB_LE_TYPE_NOT_REGISTERED)<< typeId;
        }
        return i->second;
    }
}

std::vector<Type> TypeLibrary::getTypes(PointerRange<TypeId> ts)
{
    ScopedMutexLock cs(_instance._mutex);

    std::vector<Type> v; v.reserve(ts.size());

    for (size_t i=0,n=ts.size(); i!=n; ++i)
    {
        v.push_back(_instance._getType(ts[i]));
    }

    return v;
}

void TypeLibrary::_registerType(const Type& type)
{
    ScopedMutexLock cs(_mutex);
    map<string, Type, __lesscasecmp>::const_iterator i = _typesById.find(type.typeId());
    if (i == _typesById.end()) {
        _typesById[type.typeId()] = type;
        _typeLibraries.addObject(type.typeId());
    } else {
        if (i->second.bitSize() != type.bitSize() || i->second.baseType() != type.baseType())  {
            throw SYSTEM_EXCEPTION(SCIDB_SE_TYPESYSTEM, SCIDB_LE_TYPE_ALREADY_REGISTERED) << type.typeId();
        }
    }
}

size_t TypeLibrary::_typesCount() const
{
    ScopedMutexLock cs(_mutex);
    size_t count = 0;
    for (map<string, Type, __lesscasecmp>::const_iterator i = _typesById.begin();
         i != _typesById.end();
         ++i)
    {
        if (i->first[0] != '$')
            ++count;
    }
    return count;
}

std::vector<TypeId> TypeLibrary::_typeIds() const
{
    ScopedMutexLock cs(_mutex);
    std::vector<TypeId> v;v.reserve(_typesById.size());

    for (map<TypeId,Type,__lesscasecmp>::const_iterator i = _typesById.begin(); i!=_typesById.end(); ++i)
    {
        if (i->first[0] != '$')
        {
            v.push_back(i->first);
        }
    }

    return v;
}

const Value& TypeLibrary::_getDefaultValue(const TypeId& typeId)
{
    std::map<TypeId, Value, __lesscasecmp>::const_iterator iter = _defaultValuesById.find(typeId);

    if (iter != _defaultValuesById.end())
    {
        return iter->second;
    }

    Value _defaultValue(_getType(typeId));

    FunctionDescription functionDesc;
    vector<FunctionPointer> converters;
    if (FunctionLibrary::getInstance()->findFunction(typeId, vector<TypeId>(), functionDesc, converters, false))
    {
        functionDesc.getFuncPtr()(0, &_defaultValue, 0);
    }
    else
    {
        stringstream ss;
        ss << typeId << "()";
        throw USER_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_FUNCTION_NOT_FOUND) << ss.str();
    }

    return _defaultValuesById[typeId] = _defaultValue;
}

/**
 * Quote a string as a TID_STRING ought to be quoted.
 *
 * @description
 * Copy a string to an output stream, inserting backslashes before
 * characters in the quoteThese string.
 *
 * @param os the output stream
 * @param s the string to copy out
 * @param quoteThese string of characters requiring backslashes
 */
static void tidStringQuote(std::ostream& os, const char *s, const char *quoteThese)
{
    while (char c = *s++) {
        if (strchr(quoteThese, c))
            os << '\\';
        os << c;
    }
}

/**
 * Helper Value functions implementation
 *
 * NOTE: This will only work efficiently for the built in types. If you try
 *       use this for a UDT it needs to do a lookup to try and find a UDF.
 */
string ValueToString(const TypeId& type, const Value& value, int precision)
{
    std::stringstream ss;

    /*
    ** Start with the most common ones, and do the least common ones
    ** last.
    */
    if ( value.isNull() ) {
        if (value.getMissingReason() == 0) {
            ss << "null";
        } else {
            ss << '?' << value.getMissingReason();
        }
    } else if ( TID_DOUBLE == type ) {
        double val = value.getDouble();
        if (isnan(val) || val==0)
            val = abs(val);
        ss.precision(precision);
        ss << val;
    } else if ( TID_INT64 == type ) {
        ss << value.getInt64();
    } else if ( TID_INT32 == type ) {
        ss << value.getInt32();
    } else if ( TID_STRING == type ) {
        char const* str = value.getString();
        if (str == NULL) {
            ss << "null";
        } else {
            ss << '\'';
            tidStringQuote(ss, str, "\\'");
            ss << '\'';
        }
    } else if ( TID_CHAR == type ) {

        ss << '\'';
        const char ch = value.getChar();
        if (ch == '\0') {
                ss << "\\0";
        } else if (ch == '\n') {
                ss << "\\n";
        } else if (ch == '\r') {
                ss << "\\r";
        } else if (ch == '\t') {
                ss << "\\t";
        } else if (ch == '\f') {
                ss << "\\f";
        } else {
                if (ch == '\'' || ch == '\\') {
                ss << '\\';
                }
                ss << ch;
        }
        ss << '\'';

    } else if ( TID_FLOAT == type ) {
        ss << value.getFloat();
    } else if (( TID_BOOL == type ) || ( TID_INDICATOR == type )) {
        ss << (value.getBool() ? "true" : "false");
    } else if ( TID_DATETIME == type ) {

        char buf[STRFTIME_BUF_LEN];
        struct tm tm;
        time_t dt = (time_t)value.getDateTime();

        gmtime_r(&dt, &tm);
        strftime(buf, sizeof(buf), DEFAULT_STRFTIME_FORMAT, &tm);
        ss << '\'' << buf << '\'';

    } else if ( TID_DATETIMETZ == type) {

            char buf[STRFTIME_BUF_LEN + 8];
            time_t *seconds = (time_t*) value.data();
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


            ss << '\'' << buf << '\'';
    } else if ( TID_INT8 == type ) {
        ss << (int)value.getInt8();
    } else if ( TID_INT16 == type ) {
        ss << value.getInt16();
    } else if ( TID_UINT8 == type ) {
        ss << (int)value.getUint8();
    } else if ( TID_UINT16 == type ) {
        ss << value.getUint16();
    } else if ( TID_UINT32 == type ) {
        ss << value.getUint32();
    } else if ( TID_UINT64 == type ) {
        ss << value.getUint64();
    } else if ( TID_VOID == type ) {
        ss << "<void>";
    } else  {
        ss << "<" << type << ">";
    }

    return ss.str();
}

inline char mStringToMonth(const char* s)
{
    assert(s != 0);                                      // Validate arguments

    static const Keyed<const char*,char,less_strcasecmp> map[] =
    {
        {"apr", 4},
        {"aug", 8},
        {"feb", 2},
        {"dec",12},
        {"jan", 1},
        {"jul", 7},
        {"jun", 6},
        {"mar", 3},
        {"may", 5},
        {"nov",11},
        {"oct",10},
        {"sep", 9}
    };

    char n;

    if (inFlatMap(pointerRange(map),s,n))
    {
        return n;
    }

    throw USER_EXCEPTION(SCIDB_SE_TYPE_CONVERSION,SCIDB_LE_INVALID_MONTH_REPRESENTATION) << string(s);
}

/**
 * Parse a string that contains (hopefully) a DateTime constant into
 * the internal representation.
 * @param string containing DateTime value
 * @return standard time_t.
 */
time_t parseDateTime(std::string const& str)
{
    struct tm t;
    time_t now = time(NULL);
    if (str == "now") {
        return now;
    }
    gmtime_r(&now, &t);
    int n;
    int sec_frac;
    char const* s = str.c_str();
    t.tm_mon += 1;
    t.tm_hour = t.tm_min = t.tm_sec = 0;
    char mString[4]="";
    char amPmString[3]="";

    if (( sscanf(s, "%d-%3s-%d %d.%d.%d %2s%n", &t.tm_mday, &mString[0], &t.tm_year, &t.tm_hour, &t.tm_min, &t.tm_sec, &amPmString[0], &n) == 7 ||
          sscanf(s, "%d-%3s-%d %d.%d.%d%n", &t.tm_mday, &mString[0], &t.tm_year, &t.tm_hour, &t.tm_min, &t.tm_sec, &n) == 6 ||
          sscanf(s, "%d-%3s-%d%n", &t.tm_mday, &mString[0], &t.tm_year, &n) == 3 ||
          sscanf(s, "%d%3s%d:%d:%d:%d%n", &t.tm_mday, &mString[0], &t.tm_year, &t.tm_hour, &t.tm_min, &t.tm_sec, &n) == 6 ) && n == (int) str.size())
    {
        t.tm_mon = mStringToMonth(mString);
        if(amPmString[0]=='P')
        {
            t.tm_hour += 12;
        }
    }
    else
    {
        if((sscanf(s, "%d/%d/%d %d:%d:%d%n", &t.tm_mon, &t.tm_mday, &t.tm_year, &t.tm_hour, &t.tm_min, &t.tm_sec, &n) != 6 &&
            sscanf(s, "%d.%d.%d %d:%d:%d%n", &t.tm_mday, &t.tm_mon, &t.tm_year, &t.tm_hour, &t.tm_min, &t.tm_sec, &n) != 6 &&
            sscanf(s, "%d-%d-%d %d:%d:%d.%d%n", &t.tm_year, &t.tm_mon, &t.tm_mday, &t.tm_hour, &t.tm_min, &t.tm_sec, &sec_frac, &n) != 7 &&
            sscanf(s, "%d-%d-%d %d.%d.%d.%d%n", &t.tm_year, &t.tm_mon, &t.tm_mday, &t.tm_hour, &t.tm_min, &t.tm_sec, &sec_frac, &n) != 7 &&
            sscanf(s, "%d-%d-%d %d.%d.%d%n", &t.tm_year, &t.tm_mon, &t.tm_mday, &t.tm_hour, &t.tm_min, &t.tm_sec, &n) != 6 &&
            sscanf(s, "%d-%d-%d %d:%d:%d%n", &t.tm_year, &t.tm_mon, &t.tm_mday, &t.tm_hour, &t.tm_min, &t.tm_sec, &n) != 6 &&
            sscanf(s, "%d/%d/%d %d:%d%n", &t.tm_mon, &t.tm_mday, &t.tm_year, &t.tm_hour, &t.tm_min, &n) != 5 &&
            sscanf(s, "%d.%d.%d %d:%d%n", &t.tm_mday, &t.tm_mon, &t.tm_year, &t.tm_hour, &t.tm_min, &n) != 5 &&
            sscanf(s, "%d-%d-%d %d:%d%n", &t.tm_year, &t.tm_mon, &t.tm_mday, &t.tm_hour, &t.tm_min, &n) != 5 &&
            sscanf(s, "%d-%d-%d%n", &t.tm_year, &t.tm_mon, &t.tm_mday, &n) != 3 &&
            sscanf(s, "%d/%d/%d%n", &t.tm_mon, &t.tm_mday, &t.tm_year, &n) != 3 &&
            sscanf(s, "%d.%d.%d%n", &t.tm_mday, &t.tm_mon, &t.tm_year, &n) != 3 &&
            sscanf(s, "%d:%d:%d%n", &t.tm_hour, &t.tm_min, &t.tm_sec, &n) != 3 &&
            sscanf(s, "%d:%d%n", &t.tm_hour, &t.tm_min, &n) != 2)
                    || n != (int)str.size())
            throw USER_EXCEPTION(SCIDB_SE_TYPE_CONVERSION, SCIDB_LE_FAILED_PARSE_STRING) << str << TID_DATETIME;
    }

    if (!(t.tm_mon >= 1 && t.tm_mon <= 12  && t.tm_mday >= 1 && t.tm_mday <= 31 && t.tm_hour >= 0
          && t.tm_hour <= 23 && t.tm_min >= 0 && t.tm_min <= 59 && t.tm_sec >= 0 && t.tm_sec <= 60))
        throw USER_EXCEPTION(SCIDB_SE_TYPE_CONVERSION, SCIDB_LE_INVALID_SPECIFIED_DATE);

    t.tm_mon -= 1;
    if (t.tm_year >= 1900) {
        t.tm_year -= 1900;
    } else if (t.tm_year < 100) {
        t.tm_year += 100;
    }
    return timegm(&t);
}

void parseDateTimeTz(std::string const& str, Value& result)
{
    if (str == "now")
    {
        pair<time_t,time_t> r;
        time_t now = time(NULL);
        struct tm localTm;
        localtime_r(&now, &localTm);
        r.second = timegm(&localTm) - now;
        r.first = now + r.second;
        result.setData(&r, 2*sizeof(time_t));
    }

    struct tm t;
    int offsetHours, offsetMinutes, secFrac, n;
    char mString[4]="";
    char amPmString[3]="";

    char const* s = str.c_str();
    t.tm_mon += 1;
    t.tm_hour = t.tm_min = t.tm_sec = 0;

    if ((sscanf(s, "%d-%3s-%d %d.%d.%d %2s %d:%d%n", &t.tm_mday, &mString[0], &t.tm_year, &t.tm_hour, &t.tm_min, &t.tm_sec, &amPmString[0], &offsetHours, &offsetMinutes, &n) == 9)
        && n == (int)str.size())
    {
        t.tm_mon = mStringToMonth(mString);
        if(amPmString[0]=='P')
        {
            t.tm_hour += 12;
        }
    }
    else
    {
        if((sscanf(s, "%d/%d/%d %d:%d:%d %d:%d%n", &t.tm_mon, &t.tm_mday, &t.tm_year, &t.tm_hour, &t.tm_min, &t.tm_sec, &offsetHours, &offsetMinutes, &n) != 8 &&
            sscanf(s, "%d.%d.%d %d:%d:%d %d:%d%n", &t.tm_mday, &t.tm_mon, &t.tm_year, &t.tm_hour, &t.tm_min, &t.tm_sec, &offsetHours, &offsetMinutes, &n) != 8 &&
            sscanf(s, "%d-%d-%d %d:%d:%d.%d %d:%d%n", &t.tm_year, &t.tm_mon, &t.tm_mday, &t.tm_hour, &t.tm_min, &t.tm_sec, &secFrac, &offsetHours, &offsetMinutes, &n) != 9 &&
            sscanf(s, "%d-%d-%d %d:%d:%d %d:%d%n", &t.tm_year, &t.tm_mon, &t.tm_mday, &t.tm_hour, &t.tm_min, &t.tm_sec, &offsetHours, &offsetMinutes, &n) != 8 &&
            sscanf(s, "%d-%d-%d %d.%d.%d.%d %d:%d%n", &t.tm_year, &t.tm_mon, &t.tm_mday, &t.tm_hour, &t.tm_min, &t.tm_sec, &secFrac, &offsetHours, &offsetMinutes, &n) != 9 &&
            sscanf(s, "%d-%d-%d %d.%d.%d %d:%d%n", &t.tm_year, &t.tm_mon, &t.tm_mday, &t.tm_hour, &t.tm_min, &t.tm_sec, &offsetHours, &offsetMinutes, &n) != 8 &&
            sscanf(s, "%d-%3s-%d %d.%d.%d %2s %d:%d%n", &t.tm_mday, &mString[0], &t.tm_year, &t.tm_hour, &t.tm_min, &t.tm_sec, &amPmString[0], &offsetHours, &offsetMinutes, &n) != 9)
              || n != (int)str.size())
            throw USER_EXCEPTION(SCIDB_SE_TYPE_CONVERSION, SCIDB_LE_FAILED_PARSE_STRING) << str << TID_DATETIMETZ;
    }

    if (offsetHours < 0 && offsetMinutes > 0)
    {
        offsetMinutes *= -1;
    }

    if (!(t.tm_mon >= 1 && t.tm_mon <= 12  &&
          t.tm_mday >= 1 && t.tm_mday <= 31 &&
          t.tm_hour >= 0 && t.tm_hour <= 23 &&
          t.tm_min >= 0 && t.tm_min <= 59 &&
          t.tm_sec >= 0 && t.tm_sec <= 60 &&
          offsetHours>=-13 && offsetHours<=13 &&
          offsetMinutes>=-59 && offsetMinutes<=59))
        throw USER_EXCEPTION(SCIDB_SE_TYPE_CONVERSION, SCIDB_LE_INVALID_SPECIFIED_DATE);

    t.tm_mon -= 1;
    if (t.tm_year >= 1900) {
        t.tm_year -= 1900;
    } else if (t.tm_year < 100) {
        t.tm_year += 100;
    }

    pair<time_t,time_t> r;
    r.first = timegm(&t);
    r.second = (offsetHours * 3600 + offsetMinutes * 60);
    result.setData(&r, 2*sizeof(time_t));
}

bool isBuiltinType(const TypeId& t)
{// use a flat map here
    return TID_DOUBLE == t
        || TID_INT64 == t
        || TID_INT32 == t
        || TID_CHAR == t
        || TID_STRING == t
        || TID_FLOAT == t
        || TID_INT8 == t
        || TID_INT16 == t
        || TID_UINT8 == t
        || TID_UINT16 == t
        || TID_UINT32 == t
        || TID_UINT64 == t
        || TID_INDICATOR == t
        || TID_BOOL == t
        || TID_DATETIME == t
        || TID_VOID == t
        || TID_DATETIMETZ == t
        || TID_BINARY == t;
}

TypeId propagateType(const TypeId& type)
{
    return TID_INT8 == type || TID_INT16 == type || TID_INT32 == type
    ? TID_INT64
    : TID_UINT8 == type || TID_UINT16 == type || TID_UINT32 == type
    ? TID_UINT64
    : TID_FLOAT == type ? TID_DOUBLE : type;
}

TypeId propagateTypeToReal(const TypeId& type)
{
    return TID_INT8 == type || TID_INT16 == type || TID_INT32 == type || TID_INT64 == type
        || TID_UINT8 == type || TID_UINT16 == type || TID_UINT32 == type || TID_UINT64 == type
        || TID_FLOAT == type ? TID_DOUBLE : type;
}

void StringToValue(const TypeId& type, const string& str, Value& value)
{
    if ( TID_DOUBLE == type ) {
        if (str == "NA") {
            // backward compatibility
            value.setDouble(NAN);
        } else {
            value.setDouble(atof(str.c_str()));
        }
    } else if ( TID_INT64 == type ) {
        int64_t val = StringToInteger<int64_t>(str.c_str(), TID_INT64);
        value.setInt64(val);
    } else if ( TID_INT32 == type ) {
        int32_t val = StringToInteger<int32_t>(str.c_str(), TID_INT32);
        value.setInt32(val);
    } else if (  TID_CHAR == type )  {
        value.setChar(str[0]);
    } else if ( TID_STRING == type ) {
        value.setString(str.c_str());
    } else if ( TID_FLOAT == type ) {
        if (str == "NA") {
            // backward compatibility
            value.setFloat(NAN);
        } else {
            value.setFloat(atof(str.c_str()));
        }
    } else if ( TID_INT8 == type ) {
        int8_t val = StringToInteger<int8_t>(str.c_str(), TID_INT8);
        value.setInt8(val);
    } else if (TID_INT16 == type) {
        int16_t val = StringToInteger<int16_t>(str.c_str(), TID_INT16);
        value.setInt16(val);
    } else if ( TID_UINT8 == type ) {
        uint8_t val = StringToInteger<uint8_t>(str.c_str(), TID_UINT8);
        value.setUint8(val);
    } else if ( TID_UINT16 == type ) {
        uint16_t val = StringToInteger<uint16_t>(str.c_str(), TID_UINT16);
        value.setUint16(val);
    } else if ( TID_UINT32 == type ) {
        uint32_t val = StringToInteger<uint32_t>(str.c_str(), TID_UINT32);
        value.setUint32(val);
    } else if ( TID_UINT64 == type ) {
        uint64_t val = StringToInteger<uint64_t>(str.c_str(), TID_UINT64);
        value.setUint64(val);
    } else if (( TID_INDICATOR == type ) || ( TID_BOOL == type )) {
        if (str == "true") {
            value.setBool(true);
        } else if (str == "false") {
            value.setBool(false);
        } else {
            throw SYSTEM_EXCEPTION(SCIDB_SE_TYPE_CONVERSION, SCIDB_LE_TYPE_CONVERSION_ERROR2)
                << str << "string" << "bool";
        }
    } else if ( TID_DATETIME == type ) {
        value.setDateTime(parseDateTime(str));
    } else if ( TID_DATETIMETZ == type) {
        parseDateTimeTz(str, value);
    } else if ( TID_VOID == type ) {
        throw SYSTEM_EXCEPTION(SCIDB_SE_TYPE_CONVERSION, SCIDB_LE_TYPE_CONVERSION_ERROR2)
            << str << "string" << type;
    } else {
        std::stringstream ss;
        ss << type;
        throw SYSTEM_EXCEPTION(SCIDB_SE_TYPE_CONVERSION, SCIDB_LE_TYPE_CONVERSION_ERROR2)
            << str << "string" << type;
    }
}

double ValueToDouble(const TypeId& type, const Value& value)
{
    std::stringstream ss;
    if ( TID_DOUBLE == type ) {
        return value.getDouble();
    } else if ( TID_INT64 == type ) {
        return value.getInt64();
    } else if ( TID_INT32 == type ) {
        return value.getInt32();
    } else if ( TID_CHAR == type ) {
        return value.getChar();
    } else if ( TID_STRING == type ) {
        double d;
        int n;
        char const* str = value.getString();
        if (sscanf(str, "%lf%n", &d, &n) != 1 || n != (int)strlen(str))
            throw USER_EXCEPTION(SCIDB_SE_TYPE_CONVERSION, SCIDB_LE_FAILED_PARSE_STRING) << str << "double";
        return d;
    } else if ( TID_FLOAT == type ) {
        return value.getFloat();
    } else if ( TID_INT8 == type ) {
        return value.getInt8();
    } else if ( TID_INT16 == type ) {
        return value.getInt16();
    } else if ( TID_UINT8 == type ) {
        return value.getUint8();
    } else if ( TID_UINT16 == type ) {
        return value.getUint16();
    } else if ( TID_UINT32 == type ) {
        return value.getUint32();
    } else if ( TID_UINT64 == type ) {
        return value.getUint64();
    } else if (( TID_INDICATOR == type ) || ( TID_BOOL == type )) {
        return value.getBool();
    } else if ( TID_DATETIME == type ) {
        return value.getDateTime();
    } else {
        throw SYSTEM_EXCEPTION(SCIDB_SE_TYPE_CONVERSION, SCIDB_LE_TYPE_CONVERSION_ERROR)
            << type << "double";
    }
}

void DoubleToValue(const TypeId& type, double d, Value& value)
{
      if (  TID_DOUBLE == type ) {
        value.setDouble(d);
      } else if ( TID_INT64 == type ) {
        value.setInt64((int64_t)d);
      } else if ( TID_UINT32 == type ) {
        value.setUint32((uint32_t)d);
      } else if ( TID_CHAR == type ) {
        value.setChar((char)d);
      } else if ( TID_FLOAT == type ) {
        value.setFloat((float)d);
      } else if ( TID_INT8 == type ) {
        value.setInt8((int8_t)d);
      } else if ( TID_INT16 == type ) {
        value.setInt32((int32_t)d);
      } else if ( TID_UINT8 == type ) {
        value.setUint8((uint8_t)d);
      } else if ( TID_UINT16 == type ) {
        value.setUint16((uint16_t)d);
      } else if ( TID_UINT64 == type ) {
        value.setUint64((uint64_t)d);
      } else if (( TID_INDICATOR == type ) || ( TID_BOOL == type )) {
        value.setBool(d != 0.0);
      } else if ( TID_STRING == type ) {
          std::stringstream ss;
          ss << d;
          value.setString(ss.str().c_str());
      } else if (  TID_DATETIME == type ) {
        return value.setDateTime((time_t)d);
    } else {
        throw SYSTEM_EXCEPTION(SCIDB_SE_TYPE_CONVERSION, SCIDB_LE_TYPE_CONVERSION_ERROR)
            << "double" << type;
    }
}

template<>  TypeId type2TypeId<char>()      {return TID_CHAR;  }
template<>  TypeId type2TypeId<int8_t>()    {return TID_INT8;  }
template<>  TypeId type2TypeId<int16_t>()   {return TID_INT16; }
template<>  TypeId type2TypeId<int32_t>()   {return TID_INT32; }
template<>  TypeId type2TypeId<int64_t>()   {return TID_INT64; }
template<>  TypeId type2TypeId<uint8_t>()   {return TID_UINT8; }
template<>  TypeId type2TypeId<uint16_t>()  {return TID_UINT16;}
template<>  TypeId type2TypeId<uint32_t>()  {return TID_UINT32;}
template<>  TypeId type2TypeId<uint64_t>()  {return TID_UINT64;}
template<>  TypeId type2TypeId<float>()     {return TID_FLOAT; }
template<>  TypeId type2TypeId<double>()    {return TID_DOUBLE;}

/****************************************************************************/

/**
 *  Construct and return value that carries a tile that consists of the single
 *  constant value 'v' repeated indefinitely.
 */
Value makeTileConstant(const TypeId& t,const Value& v)
{
    Value               w(TypeLibrary::getType(t),Value::asTile);
    RLEPayload*   const p = w.getTile();
    RLEPayload::Segment s(0,0,true,v.isNull());

    if (!s._null)
    {
        std::vector<char> varPart;

        p->appendValue(varPart,v,0);
        p->setVarPart(varPart);
    }

    p->addSegment(s);
    p->flush(INFINITE_LENGTH);

    return w;
}

/**
 *  Throw a type system exception with code 'e'.
 */
void Value::fail(int e)
{
    throw SYSTEM_EXCEPTION(SCIDB_SE_TYPESYSTEM,e);       // Throw exception e
}

/**
 *  Return true if the object looks to be in good shape.  Centralizes a number
 *  of consistency checks that would otherwise clutter up the code, and, since
 *  only ever called from within assertions, can be eliminated entirely by the
 *  compiler from the release build.
 */
bool Value::consistent() const
{
    assert(_code >= -2);                                 // Check status code
    assert(implies(large(_size), _data!=0));             // Check data buffer
    assert(implies(_size==0,_data==0 || isTile()));      // Check buffer size
    assert(implies(isTile(),_tile!=0));                  // Check tile pointer
    assert(implies(isTile(),_size==0));                  // Check buffer size

    return true;                                         // Appears to be good
}


/**
 * Shorthand for throwing USER_EXCEPTION(SCIDB_SE_TYPE_CONVERSION,SCIDB_LE_FAILED_PARSE_STRING)
 * @param s string being parsed
 * @param tid TypeId of desired destination type
 * @param what additional error information
 * @throws USER_EXCEPTION(SCIDB_SE_TYPE_CONVERSION,SCIDB_LE_FAILED_PARSE_STRING)
 * @returns never returns, always throws
 */
static void throwFailedParseEx(const char *s, const TypeId& tid, const char* what)
{
    stringstream ss;
    ss << tid << " (" << what << ')';
    throw USER_EXCEPTION(SCIDB_SE_TYPE_CONVERSION,
                         SCIDB_LE_FAILED_PARSE_STRING) << s << ss.str();
}

/**
 * @defgroup Templates to yield correct strtoimax(3)/strtoumax(3) return type.
 * @{
 */
template <bool isSigned>
struct maxint;

template <>
struct maxint<true> { typedef intmax_t type; };

template <>
struct maxint<false> { typedef uintmax_t type; };
/**@}*/

/**
 * Convert string to integral type T, backward compatibly with sscanf(3).
 *
 * @description We know we have an integer type here, so sscanf(3) is
 * overkill: we can call strtoimax(3)/strtoumax(3) with less overhead.
 * Also disallow octal input: the string is either base 10 or (with
 * leading 0x or 0X) base 16.
 *
 * @param s null-terminated string
 * @param type TypeId used to generate exception messages
 * @return the T integer value parsed from the string @c s
 * @throws USER_EXCEPTION(SCIDB_SE_TYPE_CONVERSION,SCIDB_LE_FAILED_PARSE_STRING)
 *
 * @note The "non-digits" check is backward compatible with the old
 * sscanf(3) implementation.  "42hike" is not an integer.
 */
template <typename T>
T StringToInteger(const char *s, const TypeId& tid)
{
    ASSERT_EXCEPTION(s, "StringToInteger<> was given NULL string pointer");

    // FWIW "VMIN" is used by termio headers, so: V_MIN, V_MAX.
    typedef typename maxint< std::numeric_limits<T>::is_signed >::type MaxT;
    MaxT const V_MAX = std::numeric_limits<T>::max();
    MaxT const V_MIN = std::numeric_limits<T>::min();

    // For signed values, this is "right-sized" all-ones.
    MaxT const MASK = (V_MAX << 1) | 1;

    while (::isspace(*s))
        ++s;
    char next = *s ? *(s + 1) : '\0';
    int base = (*s == '0' && (next == 'x' || next == 'X')) ? 16 : 10;

    errno = 0;
    char *endptr = 0;
    MaxT v;
    if (std::numeric_limits<T>::is_signed) {
        v = ::strtoimax(s, &endptr, base);
    } else {
        v = ::strtoumax(s, &endptr, base);
    }
    if (errno) {
        throwFailedParseEx(s, tid, ::strerror(errno));
    }
    if (v > V_MAX) {
        if (!std::numeric_limits<T>::is_signed)
            throwFailedParseEx(s, tid, "unsigned overflow");
        if (base == 10 || (v & ~MASK))
            throwFailedParseEx(s, tid, "signed overflow");
        // Else it's a negative number entered as hex, and only *looks* > V_MAX.
        // Sign-extend it so that the cast will do the right thing.
        v |= ~MASK;
    }
    if (std::numeric_limits<T>::is_signed && v < V_MIN) {
        throwFailedParseEx(s, tid, "signed underflow");
    }
    // Allow trailing whitespace, but nothing else.  [csv2scidb compat]
    if (!iswhitespace(endptr)) {
        throwFailedParseEx(s, tid, "non-digits");
    }
    return static_cast<T>(v);
}

// Explicit instantiations for our favorite types.
template int8_t StringToInteger<int8_t>(const char *s, const TypeId& tid);
template uint8_t StringToInteger<uint8_t>(const char *s, const TypeId& tid);
template int16_t StringToInteger<int16_t>(const char *s, const TypeId& tid);
template uint16_t StringToInteger<uint16_t>(const char *s, const TypeId& tid);
template int32_t StringToInteger<int32_t>(const char *s, const TypeId& tid);
template uint32_t StringToInteger<uint32_t>(const char *s, const TypeId& tid);
template int64_t StringToInteger<int64_t>(const char *s, const TypeId& tid);
template uint64_t StringToInteger<uint64_t>(const char *s, const TypeId& tid);

/****************************************************************************/
}
/****************************************************************************/
